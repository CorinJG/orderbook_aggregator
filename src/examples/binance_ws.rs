//! A binary to just run the binance websocket client and print top-<depth> updates to stdout.

use tokio::sync::mpsc;

use orderbook_aggregator::{config::Config, websocket};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::default();
    let (tx, mut rx) = mpsc::channel(4);
    tokio::select! {
        client_result = websocket::binance::run_client(config.depth, "eth_btc".parse()?, tx, config.ws_buffer_time_ms) => {
            match client_result {
                Ok(_) => {},
                Err(e) => {println!("{:?}", e)},
            }
        },
        _ = async {
            while let Some(m) = rx.recv().await {
                println!("{m:?}");
            }
        } => {},
    }
    Ok(())
}
