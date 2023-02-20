use orderbook_aggregator::websocket;

#[tokio::main]
async fn main() {
    websocket::binance::run_client().await;
}