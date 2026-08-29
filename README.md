# Decoded ShredStream — Rust client

Rust client for the Decoded ShredStream of ShredStream.com: pre-execution
Solana transactions, decoded from shreds — the serialized
`VersionedTransaction`, its signatures and its slot, delivered over gRPC or
UDP push the moment they propagate.

> **Before execution** — transactions carry no status, logs, balance changes
> or inner instructions, and some will fail on-chain. Use a post-execution
> source to confirm.

```toml
[dependencies]
decoded-shredstream = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use decoded_shredstream::{Filter, GrpcClient, GrpcConfig};

let mut client = GrpcClient::connect(
    GrpcConfig::new(endpoint, token).filter("all", Filter::all()),
).await?;

while let Some(update) = client.next_update().await {
    let update = update?;
    println!("{} {}", update.slot(), update.signature());
}
```

> **Requirements** — Rust 1.75 or later, a Tokio runtime, and a Decoded
> ShredStream subscription on ShredStream.com.

A UDP-only build can drop the gRPC dependencies — see
[cargo features](docs/api.md#-cargo-features).

## 🔑 Access

Decoded ShredStream is a subscription product, available from ShredStream.com.
One subscription covers both transports, and you can move from one to the
other whenever you need to.

- **gRPC** — you receive an endpoint and an access token. Use the endpoint
  exactly as issued.
- **UDP** — you register your server's IP and port; datagrams are pushed to it.

### Choosing a transport

Both carry the same data; they differ on what the protocol guarantees.

| | gRPC | UDP |
|---|---|---|
| Latency | higher | **lowest** |
| Delivery | ordered, retransmitted | best-effort, no retransmission |
| Server-side filters | **yes** | no — you receive the full stream |

## ⚡ Quickstart — gRPC

```rust
use decoded_shredstream::{Filter, GrpcClient, GrpcConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GrpcClient::connect(
        GrpcConfig::new("your-endpoint.shredstream.com:PORT", "YOUR_TOKEN")
            .filter("all", Filter::all()),
    )
    .await?;

    while let Some(update) = client.next_update().await {
        let update = update?;
        println!(
            "slot={} sig={} {}B matched={:?}",
            update.slot(),
            update.signature(),
            update.bytes().len(),
            update.filters(),
        );
    }
    Ok(())
}
```

## 📡 Quickstart — UDP

```rust
use decoded_shredstream::{UdpClient, UdpConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut client = UdpClient::bind(UdpConfig {
        port: 8002,
        ..Default::default()
    })?;
    println!("listening on {}", client.local_addr());

    while let Some(update) = client.next_update().await {
        println!(
            "slot={} sig={} {}B",
            update.slot(),
            update.signature(),
            update.bytes().len(),
        );
    }
    Ok(())
}
```

`port` must be the port you registered in your account — `8002` is only an
example.

## 🔍 Transaction parsing

Every transaction exposes `bytes()`, in the standard Solana wire format, and
its signatures without any decoding:

```rust
update.bytes();       // the complete transaction
update.signature();   // fee payer signature
update.signatures();  // every signature, in wire order
```

Everything else is available through `parse()`:

```rust
while let Some(update) = client.next_update().await {
    match update.parse() {
        Ok(message) => {
            println!(
                "slot={} version={:?} accounts={} instructions={}",
                update.slot(),
                message.version,
                message.static_accounts.len(),
                message.instructions.len(),
            );
            for ix in &message.instructions {
                let program = message.static_accounts[ix.program_index as usize];
                println!("  {} ({} bytes)", bs58::encode(program).into_string(), ix.data.len());
            }
        }
        Err(e) => eprintln!("{e}"),
    }
}
```

## 🎯 Filters

Filters exist in gRPC mode only. They are evaluated by the server; the client
never filters locally. UDP delivers the full stream.

A subscription carries a map of named filters. A transaction is delivered as
soon as it matches at least one filter of the map, and the response lists the
names of every filter it matched, readable with `update.filters()`.

Within a single filter the three lists are combined with AND:

| List | Semantics |
|---|---|
| `include` | the transaction references at least one of the listed accounts; an empty list adds no constraint |
| `exclude` | the transaction references none of the listed accounts |
| `required` | the transaction references all of the listed accounts |

Matching uses the static account keys of the transaction, signers included.
Addresses resolved through Address Lookup Tables cannot be filtered on.

Keys are validated as they are added, so `include`, `exclude` and `required`
return `Result`:

```rust
use decoded_shredstream::{Filter, FilterError, GrpcConfig};

fn build(
    endpoint: String,
    token: String,
    account: String,
    other: String,
) -> Result<GrpcConfig, FilterError> {
    Ok(GrpcConfig::new(endpoint, token)
        .filter("watched", Filter::new().include([account.clone()])?)
        .filter("pair", Filter::new().required([account, other])?)
        .filter("everything", Filter::all()))
}
```

The map can be replaced while the stream runs, without reconnecting and
without interrupting delivery. The new map is also the one re-sent by any
later reconnection:

```rust
use decoded_shredstream::{Filter, Filters, GrpcClient};

async fn narrow(
    client: &mut GrpcClient,
    account: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let filters = Filters::new().add("watched", Filter::new().include([account])?);
    client.update_filters(filters).await?;
    Ok(())
}
```

## 🔄 Errors & reconnection

Recoverable interruptions never reach you: the client reconnects on its own,
re-sends the current filter map and keeps waiting inside `next_update`. Only
three conditions end the stream — a refused token, a session closed by the
server, and a filter map the server rejects:

```rust
while let Some(update) = client.next_update().await {
    match update {
        Ok(update) => println!("{}", update.slot()),
        Err(e) => {
            eprintln!("stream ended: {e}");
            break;
        }
    }
}
```

Every error type, the backoff policy and the telemetry notices are in
[docs/errors.md](docs/errors.md).

## 📖 Documentation

This README is what you need to receive transactions. The rest lives beside it:

| Document | Contents |
|---|---|
| [docs/api.md](docs/api.md) | Every type and method: clients, configuration, filters, updates, UDP codec, performance notes and counters |
| [docs/errors.md](docs/errors.md) | Error types, reconnection policy, telemetry notices |

## 💡 Examples

| Example | Shows |
|---|---|
| `udp_quickstart` | binding the port and printing transactions |
| `grpc_quickstart` | connecting and subscribing to everything |
| `grpc_filters` | named filters and replacing the map mid-stream |
| `parse_transaction` | cheap accessors first, then `parse()` for the message |
| `raw_bytes_pipeline` | forwarding raw bytes without deserializing |
| `low_latency` | inline callback on the receive thread |

```sh
DECODED_SHREDSTREAM_UDP_PORT=8002 cargo run --release --example udp_quickstart
DECODED_SHREDSTREAM_ENDPOINT=your-endpoint.shredstream.com:PORT DECODED_SHREDSTREAM_TOKEN=... cargo run --release --example grpc_quickstart
DECODED_SHREDSTREAM_ENDPOINT=... DECODED_SHREDSTREAM_TOKEN=... ACCOUNT=<base58> cargo run --release --example grpc_filters
```

## ⚖️ License

Apache-2.0. See `LICENSE`.
