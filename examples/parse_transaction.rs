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

    while let Some(update) = client.next_update().await {
        println!("slot={} sig={}", update.slot(), update.signature());

        match update.parse() {
            Ok(message) => {
                println!(
                    "  version={:?} accounts={} instructions={}",
                    message.version,
                    message.static_accounts.len(),
                    message.instructions.len(),
                );
                for ix in &message.instructions {
                    let program = message.static_accounts[ix.program_index as usize];
                    println!(
                        "    {} ({} bytes)",
                        bs58::encode(program).into_string(),
                        ix.data.len()
                    );
                }
            }
            Err(e) => eprintln!("  {e}"),
        }
    }
    Ok(())
}
