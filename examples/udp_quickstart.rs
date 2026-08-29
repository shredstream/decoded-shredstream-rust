use decoded_shredstream::{UdpClient, UdpConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("DECODED_SHREDSTREAM_UDP_PORT")
        .unwrap_or_else(|_| "8002".into())
        .parse()
        .expect("DECODED_SHREDSTREAM_UDP_PORT must be a port number");

    let mut client = UdpClient::bind(UdpConfig {
        port,
        ..Default::default()
    })?;
    println!("listening on {}", client.local_addr());

    while let Some(update) = client.next_update().await {
        println!(
            "slot={} sig={} {}B filters=- (UDP delivers the full stream)",
            update.slot(),
            update.signature(),
            update.bytes().len(),
        );
    }
    Ok(())
}
