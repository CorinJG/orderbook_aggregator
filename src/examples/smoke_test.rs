use orderbook_aggregator::{
    aggregator::Aggregator,
    config::Config,
    grpc_server,
    proto::orderbook::Summary,
    websocket::{binance, bitstamp},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::default();
    // channel for Aggregator to forward updates to the gRPC server
    let (tx, _) = tokio::sync::broadcast::channel::<Summary>(4);
    // channel for websocket clients to send updates to the aggregator
    let (_, rx) = tokio::sync::mpsc::channel(4);
    let mut aggregator = Aggregator::new(
        config.depth,
        rx,
        tx.clone(),
        config.exchanges[0].parse().unwrap(),
        config.exchanges[1].parse().unwrap(),
    );
    let grpc_aggregator_service = grpc_server::OrderbookAggregatorService::new(tx);
    tokio::spawn(grpc_server::run_grpc_server(
        grpc_aggregator_service,
        config.addr,
    ));
    aggregator.run().await?;
    Ok(())
}
