use std::time::Duration;

#[cfg(feature = "grpc")]
use crate::filters::{Filter, Filters};

#[derive(Clone, Debug)]
pub struct UdpConfig {
    pub port: u16,
    pub host: std::net::IpAddr,
    pub recv_buffer_bytes: usize,
    pub queue_capacity: usize,
}

impl Default for UdpConfig {
    fn default() -> Self {
        UdpConfig {
            port: 0,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            recv_buffer_bytes: 64 << 20,
            queue_capacity: 8_192,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReconnectPolicy {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    pub reset_after: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        ReconnectPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(5),
            multiplier: 2.0,
            reset_after: Duration::from_secs(30),
        }
    }
}

#[cfg(feature = "grpc")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthStyle {
    #[default]
    Bearer,
    XToken,
}

#[cfg(feature = "grpc")]
#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub endpoint: String,
    pub token: String,
    pub auth_style: AuthStyle,
    pub filters: Filters,
    pub reconnect: ReconnectPolicy,
    pub connect_timeout: Duration,
}

#[cfg(feature = "grpc")]
impl GrpcConfig {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        GrpcConfig {
            endpoint: endpoint.into(),
            token: token.into(),
            auth_style: AuthStyle::default(),
            filters: Filters::new(),
            reconnect: ReconnectPolicy::default(),
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub fn auth_style(mut self, style: AuthStyle) -> Self {
        self.auth_style = style;
        self
    }

    pub fn filter(mut self, name: impl Into<String>, filter: Filter) -> Self {
        self.filters = self.filters.add(name, filter);
        self
    }

    pub fn filters(mut self, filters: Filters) -> Self {
        self.filters = filters;
        self
    }

    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
}
