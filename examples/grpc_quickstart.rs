use decoded_shredstream::{Filter, GrpcClient, GrpcConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DECODED_SHREDSTREAM_ENDPOINT").expect("set DECODED_SHREDSTREAM_ENDPOINT");
    let token = std::env::var("DECODED_SHREDSTREAM_TOKEN").expect("set DECODED_SHREDSTREAM_TOKEN");

    let mut client = GrpcClient::connect(
        GrpcConfig::new(endpoint, token)
            .filter("all", Filter::all()),
    )
    .await?;

    while let Some(update) = client.next_update().await {
        let update = update?;
        println!(
            "slot={} sig={} matched={:?}",
            update.slot(),
            update.signature(),
            update.filters(),
        );
    }
    Ok(())
}
