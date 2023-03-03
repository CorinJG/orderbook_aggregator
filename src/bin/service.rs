use futures::{stream::FuturesUnordered, StreamExt};
use tokio::task::JoinHandle;

use orderbook_aggregator::{
    aggregator::Aggregator,
    config::{self, Exchange::*},
    grpc_server,
    proto::orderbook::Summary,
    websocket::{self, OrderbookWebsocketClient},
};

/// Run all websocket clients until one returns / errors
async fn run_ws_clients(mut ws_clients: FuturesUnordered<JoinHandle<anyhow::Result<()>>>) {
    if let Some(r) = ws_clients.next().await {
        println!("{r:?}");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = config::read_config();

    // channel for Aggregator to forward updates to the gRPC server
    let (grpc_tx, _) = tokio::sync::broadcast::channel::<Summary>(4);

    // channel for websocket clients to send updates to the aggregator
    let (ws_client_tx, ws_client_rx) = tokio::sync::mpsc::channel(32);

    let mut aggregator = Aggregator::new(config.depth, ws_client_rx, grpc_tx.clone());

    let grpc_server = grpc_server::OrderbookAggregatorService::new(grpc_tx);

    // use FuturesUnordered to run a number of websocket clients unknown until runtime
    let ws_clients = FuturesUnordered::new();
    for exchange in config.exchanges {
        match exchange {
            Binance => {
                let ws_client = websocket::binance::BinanceOrderbookWebsocketClient::new(
                    config.currency_pair.clone(),
                    ws_client_tx.clone(),
                );
                ws_clients.push(tokio::spawn(
                    async move { ws_client.manage_connection().await },
                ));
            }
            Bitstamp => {
                let ws_client = websocket::bitstamp::BitstampOrderbookWebsocketClient::new(
                    config.currency_pair.clone(),
                    ws_client_tx.clone(),
                );
                ws_clients.push(tokio::spawn(
                    async move { ws_client.manage_connection().await },
                ));
            }
        }
    }

    // terminate when any task finishes/errors
    tokio::select! {
        _ = run_ws_clients(ws_clients) => (),
        r = aggregator.run() => println!("{r:?}"),
        r = grpc_server::run_grpc_server(
            grpc_server,
            config.addr,
        ) => println!("{r:?}"),
    }
}
