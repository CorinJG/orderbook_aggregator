use orderbook_aggregator::{
    aggregator::Aggregator, config, grpc_server, proto::orderbook::Summary, websocket,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = config::read_config();
    // channel for Aggregator to forward updates to the gRPC server
    let (tx, _) = tokio::sync::broadcast::channel::<Summary>(4);
    // channel for websocket clients to send updates to the aggregator
    let (ws_client_tx, rx) = tokio::sync::mpsc::channel(4);
    let mut aggregator = Aggregator::new(
        config.depth,
        rx,
        tx.clone(),
        config.exchanges[0].parse().unwrap(),
        config.exchanges[1].parse().unwrap(),
    );
    let grpc_aggregator_service = grpc_server::OrderbookAggregatorService::new(tx);

    let binance_ws = websocket::binance::run_client(
        config.depth,
        config.currency_pair.clone(),
        ws_client_tx.clone(),
        config.ws_buffer_time_ms,
    );
    let bitstamp_ws = websocket::bitstamp::run_client(
        config.depth,
        config.currency_pair.clone(),
        ws_client_tx,
        config.ws_buffer_time_ms,
    );

    tokio::select! {
        r = binance_ws => println!("{r:?}"),
        r = bitstamp_ws => println!("{r:?}"),
        r = aggregator.run() => println!("{r:?}"),
        r = grpc_server::run_grpc_server(
            grpc_aggregator_service,
            config.addr,
        ) => println!("{r:?}"),
    }

    Ok(())
}
