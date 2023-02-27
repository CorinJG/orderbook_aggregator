//! A binary to just run the binance websocket client and print top-<depth> updates to stdout.

use tokio::sync::mpsc;

use orderbook_aggregator::{
    config::Config,
    websocket::{self, OrderbookWebsocketClient},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::default();
    let (tx, mut rx) = mpsc::channel(8);
    let client = websocket::binance::BinanceOrderbookWebsocketClient::new(
        config.depth,
        config.currency_pair.clone(),
        tx,
        config.ws_buffer_time_ms,
    );

    tokio::select! {
        client_result = client.manage_connection() => {
            match client_result {
                Ok(_) => (),
                Err(e) => println!("{:?}", e),
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
