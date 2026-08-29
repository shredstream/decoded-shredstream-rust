use std::ops::ControlFlow;

use decoded_shredstream::{run_blocking, UdpConfig};

fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("DECODED_SHREDSTREAM_UDP_PORT")
        .unwrap_or_else(|_| "8002".into())
        .parse()
        .unwrap();

    let mut count = 0u64;
    run_blocking(
        UdpConfig {
            port,
            ..Default::default()
        },
        |update| {
            let _slot = update.slot();
            let _raw = update.bytes();

            count += 1;
            if count >= 1_000_000 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        },
    )?;
    println!("done: {count} transactions");

    Ok(())
}
