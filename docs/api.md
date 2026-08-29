# API reference — Decoded ShredStream Rust client

Complete surface of the crate. Start with the [README](../README.md) to
receive your first transactions.

## 📦 Cargo features

One cargo feature, `grpc` (on by default), pulls `tonic`, `prost`,
`prost-types` and `tokio-stream`, and provides `GrpcClient`, `GrpcConfig`
and `AuthStyle`. UDP only, without the gRPC dependencies:

```toml
decoded-shredstream = { version = "0.1", default-features = false }
```

## 🧩 Data model

### TransactionUpdate

One update is one Solana transaction. The type is the same on both
transports; only `filters()` and `created_at()` carry different values, as
noted below.

| Accessor | Returns | gRPC | UDP |
|---|---|---|---|
| `slot()` | `u64` | slot of the transaction | slot of the transaction |
| `bytes()` | `&[u8]` | raw Solana wire transaction | raw Solana wire transaction |
| `signature()` | `Signature` | first signature | first signature |
| `signatures()` | `&[Signature]` | all signatures | all signatures |
| `filters()` | `&[String]` | names of the filters that matched | always empty |
| `created_at()` | `Option<SystemTime>` | server emission time, wall clock — not a latency reference | always `None` |
| `received_at()` | `SystemTime` | local receive time | local receive time |
| `parse()` | `Result<CompiledMessage, MessageDecodeError>` | decoded message | decoded message |

`TransactionUpdate` is not `Clone`.

### Signature

```rust
pub struct Signature(pub [u8; 64]);
```

A 64-byte Ed25519 signature, stored as received. `as_bytes()` returns
`&[u8; 64]`; `to_base58()` returns the base58 `String`, which is also what
`Display` prints.

## 🔐 Authentication

gRPC mode only. The token is sent as metadata on every request, in one of two
headers:

| `AuthStyle` | Header |
|---|---|
| `AuthStyle::Bearer` (default) | `authorization: Bearer <token>` |
| `AuthStyle::XToken` | `x-token: <token>` |

```rust
use decoded_shredstream::{AuthStyle, GrpcConfig};

fn config() -> GrpcConfig {
    GrpcConfig::new("your-endpoint.shredstream.com", "YOUR_TOKEN").auth_style(AuthStyle::XToken)
}
```

## 🔗 Interoperability

Consumers that need owned `solana-transaction` types can build them from the
bytes:

```rust
use decoded_shredstream::TransactionUpdate;
use solana_transaction::versioned::VersionedTransaction;

fn decode(update: &TransactionUpdate) -> Result<VersionedTransaction, bincode::Error> {
    bincode::deserialize(update.bytes())
}
```

## 📚 Types and methods

### `UdpClient`

```rust
impl UdpClient {
    pub fn bind(cfg: UdpConfig) -> std::io::Result<Self>;
    pub fn local_addr(&self) -> std::net::SocketAddr;
    pub async fn next_update(&mut self) -> Option<TransactionUpdate>;
    pub fn try_next_update(&self) -> Option<TransactionUpdate>;
    pub fn on_notice(&self, f: impl Fn(Notice) + Send + Sync + 'static);
    pub fn stats(&self) -> StatsSnapshot;
    pub fn close(&mut self);
}
```

`bind` binds the port and starts the receive thread; it returns an error if
the port cannot be bound or the socket cannot be configured. `local_addr`
resolves the actual port when the configuration asked for port 0.
`next_update` waits for the next update and returns `None` once the client is
closed and the queue drained. `try_next_update` never waits. `on_notice`
replaces any callback already registered. `close` stops reception; queued updates stay
readable, and `Drop` calls it.

`UdpClient` implements `futures_core::Stream<Item = TransactionUpdate>`. Use
it with an extension trait such as `futures::StreamExt` or
`tokio_stream::StreamExt`.

### `run_blocking`

```rust
pub fn run_blocking(
    cfg: UdpConfig,
    f: impl FnMut(TransactionUpdate) -> std::ops::ControlFlow<()>,
) -> std::io::Result<()>;
```

Binds the port and runs the receive loop on the calling thread, invoking `f`
for every update. Returning `ControlFlow::Break(())` stops the loop and
returns `Ok(())`. While the callback runs, nothing reads the socket.

### `UdpConfig`

```rust
pub struct UdpConfig {
    pub port: u16,
    pub host: std::net::IpAddr,
    pub recv_buffer_bytes: usize,
    pub queue_capacity: usize,
}
```

| Field | Default | Meaning |
|---|---|---|
| `port` | 0 | local port to bind; must be the registered port |
| `host` | `0.0.0.0` | local address to bind |
| `recv_buffer_bytes` | 67108864 (64 MiB) | requested `SO_RCVBUF` |
| `queue_capacity` | 8192 | updates held between the receive thread and the consumer |

### `GrpcClient`

```rust
impl GrpcClient {
    pub async fn connect(cfg: GrpcConfig) -> Result<Self, ConnectError>;
    pub async fn next_update(&mut self) -> Option<Result<TransactionUpdate, StreamError>>;
    pub async fn update_filters(&mut self, filters: Filters) -> Result<(), StreamError>;
    pub fn on_notice(&self, f: impl Fn(Notice) + Send + Sync + 'static);
    pub fn stats(&self) -> StatsSnapshot;
    pub fn close(&mut self);
    pub fn into_stream(
        self,
    ) -> impl futures_core::Stream<Item = Result<TransactionUpdate, StreamError>> + Send;
}
```

`connect` validates the configuration, opens the connection and subscribes.
`next_update` waits for the next update, yields a terminal `StreamError` once
and returns `None` afterwards. `update_filters` replaces the whole map without
interrupting the stream. `close` ends the stream, so the next `next_update`
returns `None`. `GrpcClient` implements `Debug`.

### `GrpcConfig`

```rust
pub struct GrpcConfig {
    pub endpoint: String,
    pub token: String,
    pub auth_style: AuthStyle,
    pub filters: Filters,
    pub reconnect: ReconnectPolicy,
    pub connect_timeout: std::time::Duration,
}

impl GrpcConfig {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self;
    pub fn auth_style(self, style: AuthStyle) -> Self;
    pub fn filter(self, name: impl Into<String>, filter: Filter) -> Self;
    pub fn filters(self, filters: Filters) -> Self;
    pub fn reconnect(self, policy: ReconnectPolicy) -> Self;
    pub fn connect_timeout(self, timeout: std::time::Duration) -> Self;
}
```

`endpoint` is `host` or `host:port`; `DEFAULT_PORT` (9991) applies when it
carries no port. IPv6 literals must be bracketed, as in `[::1]:9991`.

`new` starts from an empty filter map, `AuthStyle::Bearer`, the default
`ReconnectPolicy` and a 5 s connect timeout. `filter` adds one named filter,
replacing any filter of the same name; `filters` replaces the whole map.

### `ReconnectPolicy`

```rust
pub struct ReconnectPolicy {
    pub initial: std::time::Duration,
    pub max: std::time::Duration,
    pub multiplier: f64,
    pub reset_after: std::time::Duration,
}
```

Defaults: 100 ms, 5 s, 2.0, 30 s. See
[Errors & reconnection](errors.md).

### `Filter` and `Filters`

```rust
impl Filter {
    pub fn new() -> Self;
    pub fn all() -> Self;
    pub fn include<I, S>(self, keys: I) -> Result<Self, FilterError>
    where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn exclude<I, S>(self, keys: I) -> Result<Self, FilterError>
    where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn required<I, S>(self, keys: I) -> Result<Self, FilterError>
    where I: IntoIterator<Item = S>, S: Into<String>;
}

impl Filters {
    pub fn new() -> Self;
    pub fn add(self, name: impl Into<String>, filter: Filter) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn validate(&self) -> Result<(), FilterError>;
}
```

`Filter::new` and `Filter::all` both return a filter without constraints,
which matches everything until a list is added. `include`, `exclude` and
`required` append to their list and validate the keys.

### `CompiledMessage`

```rust
pub struct CompiledMessage<'a> {
    pub version: Option<u8>,               // None for a legacy message
    pub header: MessageHeader,
    pub static_accounts: Vec<&'a [u8]>,    // 32 bytes each, wire order
    pub lifetime_token: &'a [u8],          // recent blockhash, or a nonce
    pub instructions: Vec<CompiledInstruction<'a>>,
    pub address_table_lookups: Vec<AddressTableLookup<'a>>, // empty on legacy
}

pub struct CompiledInstruction<'a> {
    pub program_index: u8,                 // index into static_accounts
    pub account_indices: &'a [u8],
    pub data: &'a [u8],
}

pub struct AddressTableLookup<'a> {
    pub table_address: &'a [u8],
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

pub fn decode_message(buf: &[u8]) -> Result<CompiledMessage<'_>, MessageDecodeError>;
pub fn message_bytes(tx: &[u8]) -> Result<&[u8], MessageDecodeError>;
```

`decode_message` is the decoder `parse()` uses, exported for message bytes
obtained elsewhere; `message_bytes` splits a serialized transaction.

### Errors

```rust
pub enum StreamError { AuthRefused, Kicked, InvalidFilter(String), Closed }
pub enum FilterError { InvalidKey(String), TooManyKeys(String, usize), TooManyFilters(usize), Empty }
pub enum ConnectError { InvalidEndpoint(String), Filter(FilterError), InvalidToken, Transport(String) }
```

`MessageDecodeError` carries a short reason. The three enums above are
`#[non_exhaustive]`: match with a `_` arm.

### `Notice`

```rust
pub enum Notice {
    DecodeError,
    RecvBufferClamped { requested: usize, effective: usize },
    Reconnecting { attempt: u32, delay: std::time::Duration },
    Reconnected,
}
```

`#[non_exhaustive]`: match with a `_` arm. Delivered to the callback passed to
`on_notice`. See [Errors & reconnection](errors.md).

### `StatsSnapshot`

```rust
pub struct StatsSnapshot {
    pub messages: u64,
    pub bytes: u64,
    pub datagrams: u64,
    pub decode_errors: u64,
    pub skipped_msg_type: u64,
    pub queue_dropped: u64,
    pub reconnects: u64,
    pub last_slot: u64,
    pub recv_buffer_bytes: u64,
}
```

Field by field in [Statistics](#-statistics).

### Module `codec`

The UDP framing is exposed so you can decode datagrams received on a socket you
manage yourself. One datagram is one frame: a 16-byte header followed by the
payload, all integer fields little-endian, 1408 bytes at most.

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 2 | `magic` | `0x5AE7` |
| 2 | 1 | `version` | 2 |
| 3 | 1 | `msg_type` | 1 for events, 2 for transactions |
| 4 | 1 | `flags` | reserved, 0 |
| 5 | 1 | `frag_index` | 0-based fragment index |
| 6 | 1 | `frag_count` | 1 when the frame is not fragmented |
| 7 | 1 | `padding` | 0 |
| 8 | 8 | `seq` | sequence number of the datagram |

For a transaction frame the payload is the slot as a little-endian `u64`
followed by the raw Solana wire transaction, and `frag_count` is always 1.
`seq` increments once per datagram, whatever the message type.

```rust
pub const FRAME_MAGIC: u16 = 0x5AE7;
pub const FRAME_VERSION: u8 = 2;
pub const FRAME_HEADER_LEN: usize = 16;
pub const MAX_DATAGRAM: usize = 1408;
pub const MSG_EVENT: u8 = 1;
pub const MSG_TRANSACTION: u8 = 2;

pub struct FrameHeader {
    pub version: u8,
    pub msg_type: u8,
    pub flags: u8,
    pub frag_index: u8,
    pub frag_count: u8,
    pub seq: u64,
}

pub enum FrameError { TooShort, BadMagic, UnsupportedVersion, Truncated, BadFragment }
impl FrameError { pub fn code(&self) -> &'static str; }

pub enum Push { Update(TransactionUpdate), Skipped, Error(FrameError) }

pub fn parse_header(buf: &[u8]) -> Result<FrameHeader, FrameError>;

impl StreamDecoder {
    pub fn new() -> Self;
    pub fn stats(&self) -> StatsSnapshot;
    pub fn push(&mut self, datagram: &[u8]) -> Push;
}
```

`StreamDecoder` is what `UdpClient` runs internally. Use it to decode
datagrams received on a socket you manage yourself: feed each datagram to
`push` in arrival order, and match the returned `Push`. `code()` returns a
stable string for logging: `too_short`, `bad_magic`, `unsupported_version`,
`truncated`, `bad_fragment`.

```rust
use std::net::UdpSocket;

use decoded_shredstream::codec::{Push, StreamDecoder, MAX_DATAGRAM};

fn decode_loop(socket: &UdpSocket) -> std::io::Result<()> {
    let mut decoder = StreamDecoder::new();
    let mut buf = [0u8; MAX_DATAGRAM + 64];
    loop {
        let n = socket.recv(&mut buf)?;
        if let Push::Update(update) = decoder.push(&buf[..n]) {
            println!("slot={} sig={}", update.slot(), update.signature());
        }
    }
}
```

The gRPC contract is `proto/decoded.proto`, which declares the service and
imports `proto/shreder_binary.proto` for its message types. Both files are
included, and generating bindings in another language needs the two together.

## ⚙️ Performance notes

- **Consume without delay.** A gRPC consumer that falls behind gets
  `DATA_LOSS` and loses the messages the server dropped; a UDP consumer that
  falls behind overwrites its oldest queued updates. Move heavy work to your
  own threads.
- **Keep base58 out of the receive loop.** Compare raw bytes; render
  `to_base58()` only for display.
- **Choose the tightest loop you need.** `run_blocking` invokes your callback
  directly on the receive thread, with no queue and no thread handoff.
  `UdpClient::try_next_update` returns immediately and lets you poll from a
  thread you pin yourself.
- **Size the UDP receive buffer.** The client requests 64 MiB because the
  stream arrives in bursts. If the operating system grants less,
  `Notice::RecvBufferClamped` fires and `recv_buffer_bytes` reports the granted
  size; raise `net.core.rmem_max` on Linux or `kern.ipc.maxsockbuf` on macOS.
  The notice fires during `bind`, so read `recv_buffer_bytes` after `bind` if
  the callback is registered later.

## 📊 Statistics

`client.stats()` returns a `StatsSnapshot` on both clients, and
`StreamDecoder::stats()` on the codec. Every counter is a `u64`, cumulative
since the client was created, and counters that do not apply to the active
transport stay at 0.

| Field | Transport | Meaning |
|---|---|---|
| `messages` | both | transaction updates decoded |
| `bytes` | both | bytes received: full datagrams over UDP, transaction bytes over gRPC |
| `datagrams` | UDP | datagrams received, including those later rejected |
| `decode_errors` | both | datagrams or responses rejected as undecodable |
| `skipped_msg_type` | UDP | frames carrying another message type |
| `queue_dropped` | UDP | updates overwritten because the consumer fell behind |
| `reconnects` | gRPC | successful reconnections |
| `last_slot` | both | slot of the most recently decoded transaction |
| `recv_buffer_bytes` | UDP | receive buffer size the operating system granted |

```rust
use decoded_shredstream::UdpClient;

fn report(client: &UdpClient) {
    let s = client.stats();
    println!(
        "{} transactions, {} dropped by a slow consumer, last slot {}",
        s.messages, s.queue_dropped, s.last_slot,
    );
}
```
