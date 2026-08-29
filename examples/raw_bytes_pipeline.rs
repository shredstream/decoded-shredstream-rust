use decoded_shredstream::{UdpClient, UdpConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("DECODED_SHREDSTREAM_UDP_PORT")
        .unwrap_or_else(|_| "8002".into())
        .parse()
        .unwrap();
    let mut client = UdpClient::bind(UdpConfig {
        port,
        ..Default::default()
    })?;

    let mut count = 0u64;
    let mut total_bytes = 0u64;
    while let Some(update) = client.next_update().await {
        let raw: &[u8] = update.bytes();
        let sig = update.signature();

        total_bytes += raw.len() as u64;
        count += 1;
        if count % 10_000 == 0 {
            println!("{count} transactions, {total_bytes} bytes, last sig {sig}");
        }
    }
    Ok(())
}
