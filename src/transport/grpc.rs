use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status, Streaming};

use crate::config::{AuthStyle, GrpcConfig};
use crate::error::{ConnectError, StreamError};
use crate::filters::Filters;
use crate::notice::{Notice, NoticeHook};
use crate::proto::shreder_binary as pb;
use crate::proto::shredstream::com as rpc;
use crate::stats::{bump, Stats};
use crate::update::TransactionUpdate;
use crate::StatsSnapshot;

use rpc::decoded_shredstream_service_client::DecodedShredStreamServiceClient;

pub const DEFAULT_PORT: u16 = 9991;

#[derive(Clone)]
struct Auth {
    key: &'static str,
    value: AsciiMetadataValue,
}

impl Interceptor for Auth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request.metadata_mut().insert(self.key, self.value.clone());
        Ok(request)
    }
}

type Client = DecodedShredStreamServiceClient<InterceptedService<Channel, Auth>>;

fn normalize_endpoint(endpoint: &str) -> Result<String, ConnectError> {
    let e = endpoint.trim();
    let e = e.strip_prefix("http://").unwrap_or(e);
    if e.is_empty() || e.contains('/') || e.starts_with("https://") {
        return Err(ConnectError::InvalidEndpoint(endpoint.into()));
    }
    let has_port = match e.rfind(':') {
        Some(i) => {
            e[i + 1..].chars().all(|c| c.is_ascii_digit())
                && !e[i + 1..].is_empty()
                && (!e.contains(']') || i > e.rfind(']').unwrap())
        }
        None => false,
    };
    Ok(if has_port {
        format!("http://{e}")
    } else {
        format!("http://{e}:{DEFAULT_PORT}")
    })
}

fn filters_to_proto(filters: &Filters) -> pb::SubscribeBinaryTransactionsRequest {
    pb::SubscribeBinaryTransactionsRequest {
        transactions: filters
            .0
            .iter()
            .map(|(name, f)| {
                (
                    name.clone(),
                    pb::SubscribeRequestFilterBinaryTransactions {
                        account_include: f.include.clone(),
                        account_exclude: f.exclude.clone(),
                        account_required: f.required.clone(),
                    },
                )
            })
            .collect(),
    }
}

struct Jitter(u64);

impl Jitter {
    fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15);
        Jitter(seed | 1)
    }
    fn next(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[allow(clippy::large_enum_variant)]
enum State {
    Disconnected,
    Connected {
        stream: Streaming<pb::SubscribeBinaryTransactionsResponse>,
        req_tx: mpsc::Sender<pb::SubscribeBinaryTransactionsRequest>,
        since: Instant,
    },
    Terminal(StreamError),
    Finished,
}

pub struct GrpcClient {
    endpoint: Endpoint,
    auth: Auth,
    filters: Filters,
    state: State,
    attempt: u32,
    ever_connected: bool,
    last_data_loss: Option<Instant>,
    jitter: Jitter,
    reconnect: crate::ReconnectPolicy,
    stats: Arc<Stats>,
    hook: NoticeHook,
}

impl GrpcClient {
    pub async fn connect(cfg: GrpcConfig) -> Result<Self, ConnectError> {
        cfg.filters.validate()?;
        let url = normalize_endpoint(&cfg.endpoint)?;
        let endpoint = Endpoint::from_shared(url)
            .map_err(|_| ConnectError::InvalidEndpoint(cfg.endpoint.clone()))?
            .connect_timeout(cfg.connect_timeout)
            .tcp_nodelay(true)
            .initial_stream_window_size(4 * 1024 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024);

        let (key, value) = match cfg.auth_style {
            AuthStyle::Bearer => (
                "authorization",
                format!("Bearer {}", cfg.token)
                    .parse::<AsciiMetadataValue>()
                    .map_err(|_| ConnectError::InvalidToken)?,
            ),
            AuthStyle::XToken => (
                "x-token",
                cfg.token
                    .parse::<AsciiMetadataValue>()
                    .map_err(|_| ConnectError::InvalidToken)?,
            ),
        };
        let _: &MetadataValue<_> = &value;

        let mut client = GrpcClient {
            endpoint,
            auth: Auth { key, value },
            filters: cfg.filters,
            state: State::Disconnected,
            attempt: 0,
            ever_connected: false,
            last_data_loss: None,
            jitter: Jitter::new(),
            reconnect: cfg.reconnect,
            stats: Arc::new(Stats::default()),
            hook: NoticeHook::default(),
        };
        client.try_connect().await.map_err(|s| match classify(&s) {
            Classified::Terminal(StreamError::AuthRefused) => ConnectError::Transport(
                "authentication refused (check your token and product)".into(),
            ),
            _ => ConnectError::Transport(s.to_string()),
        })?;
        Ok(client)
    }

    async fn try_connect(&mut self) -> Result<(), Status> {
        let channel = self
            .endpoint
            .connect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let mut client: Client =
            DecodedShredStreamServiceClient::with_interceptor(channel, self.auth.clone());
        let (req_tx, req_rx) = mpsc::channel::<pb::SubscribeBinaryTransactionsRequest>(8);
        req_tx
            .try_send(filters_to_proto(&self.filters))
            .expect("fresh channel has capacity");
        let response = client
            .subscribe_decoded_transactions(ReceiverStream::new(req_rx))
            .await?;
        let stream = response.into_inner();
        if self.ever_connected {
            bump!(self.stats.reconnects);
            self.hook.fire(Notice::Reconnected);
        }
        self.ever_connected = true;
        self.state = State::Connected {
            stream,
            req_tx,
            since: Instant::now(),
        };
        Ok(())
    }

    async fn backoff_delay(&mut self) -> Duration {
        self.attempt = self.attempt.saturating_add(1);
        let exp = self.reconnect.initial.as_secs_f64()
            * self
                .reconnect
                .multiplier
                .powi(self.attempt.saturating_sub(1) as i32);
        let capped = exp.min(self.reconnect.max.as_secs_f64());
        Duration::from_secs_f64(capped * self.jitter.next())
    }

    pub async fn next_update(&mut self) -> Option<Result<TransactionUpdate, StreamError>> {
        loop {
            match &mut self.state {
                State::Finished => return None,
                State::Terminal(e) => {
                    let e = e.clone();
                    self.state = State::Finished;
                    return Some(Err(e));
                }
                State::Disconnected => {
                    let delay = self.backoff_delay().await;
                    self.hook.fire(Notice::Reconnecting {
                        attempt: self.attempt,
                        delay,
                    });
                    tokio::time::sleep(delay).await;
                    let _ = self.try_connect().await;
                }
                State::Connected { stream, since, .. } => {
                    let stable = since.elapsed() >= self.reconnect.reset_after;
                    match stream.message().await {
                        Ok(Some(response)) => {
                            if stable {
                                self.attempt = 0;
                            }
                            match self.to_update(response) {
                                Some(update) => return Some(Ok(update)),
                                None => continue,
                            }
                        }
                        Ok(None) => {
                            self.state = State::Disconnected;
                        }
                        Err(status) => match classify(&status) {
                            Classified::Terminal(e) => {
                                self.state = State::Terminal(e);
                            }
                            Classified::DataLoss => {
                                let recent = self
                                    .last_data_loss
                                    .is_some_and(|t| t.elapsed() < Duration::from_secs(10));
                                self.last_data_loss = Some(Instant::now());
                                if recent {
                                    self.state = State::Disconnected;
                                } else {
                                    if self.try_connect().await.is_err() {
                                        self.state = State::Disconnected;
                                    }
                                }
                            }
                            Classified::Recoverable => {
                                self.state = State::Disconnected;
                            }
                        },
                    }
                }
            }
        }
    }

    fn to_update(
        &self,
        response: pb::SubscribeBinaryTransactionsResponse,
    ) -> Option<TransactionUpdate> {
        let received_at = SystemTime::now();
        let update = match response.transaction {
            Some(u) => u,
            None => {
                bump!(self.stats.decode_errors);
                return None;
            }
        };
        let tx = match update.transaction {
            Some(t) if !t.binary_transaction.is_empty() => t,
            _ => {
                bump!(self.stats.decode_errors);
                return None;
            }
        };
        bump!(self.stats.messages);
        bump!(self.stats.bytes, tx.binary_transaction.len() as u64);
        self.stats
            .last_slot
            .store(update.slot, std::sync::atomic::Ordering::Relaxed);
        let created_at = response.created_at.and_then(|t| {
            let base = UNIX_EPOCH.checked_add(Duration::from_secs(t.seconds.try_into().ok()?))?;
            base.checked_add(Duration::from_nanos(t.nanos.try_into().ok()?))
        });
        Some(TransactionUpdate::new(
            update.slot,
            tx.binary_transaction,
            tx.signatures,
            Arc::from(response.filters),
            created_at,
            received_at,
        ))
    }

    pub async fn update_filters(&mut self, filters: Filters) -> Result<(), StreamError> {
        filters
            .validate()
            .map_err(|e| StreamError::InvalidFilter(e.to_string()))?;
        self.filters = filters;
        if let State::Connected { req_tx, .. } = &self.state {
            let _ = req_tx.send(filters_to_proto(&self.filters)).await;
        }
        Ok(())
    }

    pub fn on_notice(&self, f: impl Fn(Notice) + Send + Sync + 'static) {
        self.hook.set(f);
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn close(&mut self) {
        self.state = State::Finished;
    }

    pub fn into_stream(
        self,
    ) -> impl futures_core::Stream<Item = Result<TransactionUpdate, StreamError>> + Send {
        futures_unfold(self)
    }
}

impl std::fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcClient")
            .field("endpoint", &self.endpoint.uri().to_string())
            .field(
                "state",
                &match self.state {
                    State::Disconnected => "disconnected",
                    State::Connected { .. } => "connected",
                    State::Terminal(_) => "terminal",
                    State::Finished => "finished",
                },
            )
            .finish_non_exhaustive()
    }
}

enum Classified {
    Terminal(StreamError),
    DataLoss,
    Recoverable,
}

fn classify(status: &Status) -> Classified {
    match status.code() {
        Code::Unauthenticated => Classified::Terminal(StreamError::AuthRefused),
        Code::PermissionDenied => Classified::Terminal(StreamError::Kicked),
        Code::InvalidArgument => {
            Classified::Terminal(StreamError::InvalidFilter(status.message().to_string()))
        }
        Code::DataLoss => Classified::DataLoss,
        _ => Classified::Recoverable,
    }
}

type UnfoldFut = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = (Option<Result<TransactionUpdate, StreamError>>, GrpcClient),
            > + Send,
    >,
>;

fn futures_unfold(
    client: GrpcClient,
) -> impl futures_core::Stream<Item = Result<TransactionUpdate, StreamError>> + Send {
    struct Unfold {
        fut: Option<UnfoldFut>,
    }
    impl futures_core::Stream for Unfold {
        type Item = Result<TransactionUpdate, StreamError>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let fut = match self.fut.as_mut() {
                Some(f) => f,
                None => return std::task::Poll::Ready(None),
            };
            match fut.as_mut().poll(cx) {
                std::task::Poll::Ready((item, mut client)) => {
                    self.fut = item.is_some().then(|| {
                        Box::pin(async move {
                            let item = client.next_update().await;
                            (item, client)
                        }) as _
                    });
                    std::task::Poll::Ready(item)
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }
    Unfold {
        fut: Some(Box::pin(async move {
            let mut client = client;
            let item = client.next_update().await;
            (item, client)
        })),
    }
}
