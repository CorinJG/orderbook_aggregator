use orderbook_aggregator::websocket;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(4);
    tokio::select! {
        client_result = websocket::binance::run_client(10, "ethbtc", tx) => {
            match client_result {
                Ok(v) => {},
                Err(e) => {println!("{:?}", e)},
            }
        },
        _ = async {
            while let Some(m) = rx.recv().await {
                println!("{m:?}");
            }
        } => {},
    }
}
