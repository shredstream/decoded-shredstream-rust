use std::time::Duration;

use decoded_shredstream::{Filter, Filters, GrpcClient, GrpcConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DECODED_SHREDSTREAM_ENDPOINT").expect("set DECODED_SHREDSTREAM_ENDPOINT");
    let token = std::env::var("DECODED_SHREDSTREAM_TOKEN").expect("set DECODED_SHREDSTREAM_TOKEN");
    let account = std::env::var("ACCOUNT").expect("set ACCOUNT (base58 static key to watch)");

    let mut client = GrpcClient::connect(
        GrpcConfig::new(endpoint, token)
            .filter("watched-account", Filter::new().include([account.clone()])?)
            .filter("everything", Filter::all()),
    )
    .await?;

    let narrow = Filters::new().add("watched-account", Filter::new().include([account])?);
    let mut swapped = false;
    let started = std::time::Instant::now();

    while let Some(update) = client.next_update().await {
        let update = update?;
        println!("sig={} matched={:?}", update.signature(), update.filters());

        if !swapped && started.elapsed() > Duration::from_secs(10) {
            client.update_filters(narrow.clone()).await?;
            swapped = true;
            println!("--- filter map narrowed, stream continues without a gap ---");
        }
    }
    Ok(())
}
