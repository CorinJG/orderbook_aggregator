use orderbook_aggregator::config::read_config;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = read_config();
}
