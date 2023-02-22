//! A binary to just run the bitstamp websocket client and print top-<depth> updates to stdout.

use tokio::sync::mpsc;

use orderbook_aggregator::websocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(4);
    tokio::select! {
        client_result = websocket::bitstamp::run_client(10, "eth_btc".parse()?, tx) => {
            match client_result {
                Ok(_) => {},
                Err(e) => {println!("{:?}", e)},
            }
        },
        _ = async {
            while let Some(m) = rx.recv().await {
                println!("{m}");
            }
        } => {},
    }
    Ok(())
}
