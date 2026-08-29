# Errors and reconnection — Decoded ShredStream Rust client

What ends the stream, what the client recovers from on its own, and how it
reports both. See the [README](../README.md) for the common path.

## 🛑 Terminal errors

Three error types, each covering one stage.

**`ConnectError`**, returned by `GrpcClient::connect`:

| Variant | Cause |
|---|---|
| `InvalidEndpoint(String)` | the endpoint is not a `host` or `host:port` address |
| `Filter(FilterError)` | the filter map failed validation |
| `InvalidToken` | the token cannot be sent as gRPC metadata |
| `Transport(String)` | the first connection attempt failed, refused authentication included |

**`FilterError`**, returned when building or validating filters:
`InvalidKey(String)`, `TooManyKeys(String, usize)`, `TooManyFilters(usize)`,
`Empty`. See [Filters](../README.md#-filters).

**`StreamError`**, returned by `GrpcClient::next_update`. These are terminal:
the error is yielded once, then the next call returns `None`.

| Variant | Meaning |
|---|---|
| `AuthRefused` | the token is missing, invalid, or not valid for this stream |
| `Kicked` | the server ended the session and denies further access |
| `InvalidFilter(String)` | the server rejected the filter map |
| `Closed` | the client was closed locally |

`GrpcClient::close` ends the stream by making the next `next_update` return
`None` rather than an error.

## 🔄 Recoverable conditions

Every other interruption is recoverable and never surfaces as an error: the
client reconnects, re-sends the current filter map and keeps waiting inside
`next_update`. This covers transport failures, an unexpected end of stream,
and `DATA_LOSS`, which the server sends when the client consumed too slowly.

Reconnection delays grow exponentially, with jitter, from `initial` up to
`max`, and restart from `initial` once a connection has been delivering data
for `reset_after`. Defaults are 100 ms, 5 s, ×2.0 and 30 s, all configurable
through `ReconnectPolicy`:

```rust
use std::time::Duration;

use decoded_shredstream::{GrpcConfig, ReconnectPolicy};

fn config() -> GrpcConfig {
    GrpcConfig::new("your-endpoint.shredstream.com", "YOUR_TOKEN")
        .reconnect(ReconnectPolicy {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(10),
            ..Default::default()
        })
        .connect_timeout(Duration::from_secs(3))
}
```

Each attempt emits `Notice::Reconnecting` and a restored stream emits
`Notice::Reconnected` and increments `reconnects`. The stream is real-time
only: whatever was published during an outage is not replayed.

UDP has no connection to restore. A consumer that falls behind loses the
oldest queued updates, counted in `queue_dropped`.

## 🔔 Notices

Both clients report telemetry through the same callback:

```rust
use decoded_shredstream::{Notice, UdpClient};

fn watch(client: &UdpClient) {
    client.on_notice(|notice| match notice {
        Notice::RecvBufferClamped {
            requested,
            effective,
        } => eprintln!("receive buffer clamped to {effective} instead of {requested}"),
        _ => {}
    });
}
```

The callback runs on the receive path, so it must return quickly. `Notice` is
`#[non_exhaustive]`: match with a `_` arm. Its variants are `DecodeError`,
`RecvBufferClamped`, `Reconnecting` and `Reconnected`.
