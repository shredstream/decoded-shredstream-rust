//! Client for the Decoded ShredStream of ShredStream.com: pre-execution Solana
//! transactions, decoded from shreds.
//!
//! The complete documentation is the README and the `docs/` directory of this
//! repository: quickstarts, filters, API reference, errors and performance.

#![forbid(unsafe_code)]

pub mod codec;
mod config;
mod error;
mod filters;
mod notice;
mod stats;
mod transport;
mod message;
mod update;

#[cfg(feature = "grpc")]
#[doc(hidden)]
pub mod proto;

pub use config::{ReconnectPolicy, UdpConfig};
pub use error::{ConnectError, FilterError, StreamError};
pub use filters::{Filter, Filters};
pub use notice::Notice;
pub use stats::StatsSnapshot;
pub use transport::udp::{run_blocking, UdpClient};
pub use update::{Signature, TransactionUpdate};

pub use message::{
    decode_message, message_bytes, AddressTableLookup, CompiledInstruction, CompiledMessage,
    MessageDecodeError, MessageHeader,
};

#[cfg(feature = "grpc")]
pub use config::{AuthStyle, GrpcConfig};
#[cfg(feature = "grpc")]
pub use transport::grpc::GrpcClient;
#[cfg(feature = "grpc")]
pub use transport::grpc::DEFAULT_PORT;
