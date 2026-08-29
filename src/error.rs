#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamError {
    #[error("authentication refused (check your token and product)")]
    AuthRefused,
    #[error("kicked by operator — do not reconnect in a loop")]
    Kicked,
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
    #[error("client closed")]
    Closed,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterError {
    #[error("filter key {0:?} is not a base58-encoded 32-byte public key")]
    InvalidKey(String),
    #[error("filter {0:?} has {1} keys (max 1000 per filter)")]
    TooManyKeys(String, usize),
    #[error("{0} named filters (max 16)")]
    TooManyFilters(usize),
    #[error("empty filter map delivers nothing — use Filter::all() to receive everything")]
    Empty,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    #[error("invalid endpoint {0:?}")]
    InvalidEndpoint(String),
    #[error(transparent)]
    Filter(#[from] FilterError),
    #[error("token contains invalid metadata characters")]
    InvalidToken,
    #[error("transport: {0}")]
    Transport(String),
}
