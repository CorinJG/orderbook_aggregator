use orderbook_aggregator::aggregator::Aggregator;
use orderbook_aggregator::proto::orderbook::Summary;
use orderbook_aggregator::{config::Config, grpc_server};

pub async fn test_run_aggregator(aggregator: Aggregator) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
        match aggregator.sender.send(Summary::default()) {
            Ok(_) => println!("sent stub Summary"),
            Err(_) => println!("no clients connected"),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = Config::default();
    // let config = orderbook_aggregator::config::read_config();

    // channel for Aggregator to forward updates to the gRPC server
    let (tx, _) = tokio::sync::broadcast::channel::<Summary>(4);
    let grpc_aggregator_service = grpc_server::OrderbookAggregatorService::new(tx.clone());
    tokio::spawn(grpc_server::run_grpc_server(
        grpc_aggregator_service,
        config.addr,
    ));
    test_run_aggregator(Aggregator::new(tx)).await;
}
