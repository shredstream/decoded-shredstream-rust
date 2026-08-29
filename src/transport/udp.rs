use std::future::Future;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

use crate::codec::{Push, StreamDecoder, MAX_DATAGRAM};
use crate::config::UdpConfig;
use crate::notice::{Notice, NoticeHook};
use crate::stats::{bump, Stats};
use crate::update::TransactionUpdate;
use crate::StatsSnapshot;

fn bind_socket(cfg: &UdpConfig, hook: &NoticeHook, stats: &Stats) -> io::Result<UdpSocket> {
    let addr = SocketAddr::new(cfg.host, cfg.port);
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_recv_buffer_size(cfg.recv_buffer_bytes)?;
    socket.bind(&addr.into())?;
    let raw = socket.recv_buffer_size().unwrap_or(0);
    let effective = if cfg!(target_os = "linux") { raw / 2 } else { raw };
    stats
        .recv_buffer_bytes
        .store(effective as u64, Ordering::Relaxed);
    if effective < cfg.recv_buffer_bytes {
        hook.fire(Notice::RecvBufferClamped {
            requested: cfg.recv_buffer_bytes,
            effective,
        });
    }
    Ok(socket.into())
}

pub fn run_blocking(
    cfg: UdpConfig,
    mut f: impl FnMut(TransactionUpdate) -> ControlFlow<()>,
) -> io::Result<()> {
    let hook = NoticeHook::default();
    let socket = bind_socket(&cfg, &hook, &Stats::default())?;
    let mut decoder = StreamDecoder::new();
    let mut buf = [0u8; MAX_DATAGRAM + 64];
    loop {
        let n = socket.recv(&mut buf)?;
        if let Push::Update(update) = decoder.push(&buf[..n]) {
            if f(update).is_break() {
                return Ok(());
            }
        }
    }
}

pub struct UdpClient {
    ring: Arc<ArrayQueue<TransactionUpdate>>,
    notify: Arc<Notify>,
    stats: Arc<Stats>,
    hook: NoticeHook,
    closed: Arc<AtomicBool>,
    local_addr: SocketAddr,
    thread: Option<std::thread::JoinHandle<()>>,
    waiting: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl UdpClient {
    pub fn bind(cfg: UdpConfig) -> io::Result<Self> {
        let hook = NoticeHook::default();
        let stats = Arc::new(Stats::default());
        let socket = bind_socket(&cfg, &hook, &stats)?;
        socket.set_read_timeout(Some(Duration::from_millis(50)))?;
        let local_addr = socket.local_addr()?;

        let ring = Arc::new(ArrayQueue::new(cfg.queue_capacity.max(1)));
        let notify = Arc::new(Notify::new());
        let closed = Arc::new(AtomicBool::new(false));

        let thread = {
            let ring = Arc::clone(&ring);
            let notify = Arc::clone(&notify);
            let stats = Arc::clone(&stats);
            let closed = Arc::clone(&closed);
            let hook = hook.clone();
            std::thread::Builder::new()
                .name("decoded-shredstream-recv".into())
                .spawn(move || {
                    struct Shutdown {
                        closed: Arc<AtomicBool>,
                        notify: Arc<Notify>,
                    }
                    impl Drop for Shutdown {
                        fn drop(&mut self) {
                            self.closed.store(true, Ordering::Relaxed);
                            self.notify.notify_one();
                        }
                    }
                    let _shutdown = Shutdown {
                        closed: Arc::clone(&closed),
                        notify: Arc::clone(&notify),
                    };
                    let mut decoder = StreamDecoder::with(Arc::clone(&stats), hook);
                    let mut buf = [0u8; MAX_DATAGRAM + 64];
                    while !closed.load(Ordering::Relaxed) {
                        let n = match socket.recv(&mut buf) {
                            Ok(n) => n,
                            Err(e)
                                if e.kind() == io::ErrorKind::WouldBlock
                                    || e.kind() == io::ErrorKind::TimedOut =>
                            {
                                continue
                            }
                            Err(_) => break,
                        };
                        if let Push::Update(update) = decoder.push(&buf[..n]) {
                            if ring.force_push(update).is_some() {
                                bump!(stats.queue_dropped);
                            }
                            notify.notify_one();
                        }
                    }
                })
                .expect("spawn receive thread")
        };

        Ok(UdpClient {
            ring,
            notify,
            stats,
            hook,
            closed,
            local_addr,
            thread: Some(thread),
            waiting: None,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn next_update(&mut self) -> Option<TransactionUpdate> {
        loop {
            if let Some(update) = self.ring.pop() {
                return Some(update);
            }
            if self.closed.load(Ordering::Relaxed) {
                return self.ring.pop();
            }
            let notify = Arc::clone(&self.notify);
            let notified = async move { notify.notified().await };
            notified.await;
        }
    }

    pub fn try_next_update(&self) -> Option<TransactionUpdate> {
        self.ring.pop()
    }

    pub fn on_notice(&self, f: impl Fn(Notice) + Send + Sync + 'static) {
        self.hook.set(f);
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn close(&mut self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for UdpClient {
    fn drop(&mut self) {
        self.close();
    }
}

impl futures_core::Stream for UdpClient {
    type Item = TransactionUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(update) = self.ring.pop() {
                self.waiting = None;
                return Poll::Ready(Some(update));
            }
            if self.closed.load(Ordering::Relaxed) {
                return Poll::Ready(self.ring.pop());
            }
            if self.waiting.is_none() {
                let notify = Arc::clone(&self.notify);
                self.waiting = Some(Box::pin(async move { notify.notified().await }));
            }
            match self.waiting.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.waiting = None;
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
