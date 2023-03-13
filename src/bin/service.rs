use futures::{stream::FuturesUnordered, StreamExt};

use orderbook_aggregator::{
    aggregator::Aggregator,
    config::{Exchange::*, CONFIG},
    grpc_server,
    websocket::{self, WebsocketClient},
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // channel for Aggregator to forward updates to the gRPC server
    let (grpc_tx, grpc_rx) = tokio::sync::watch::channel(None);

    // channel for websocket clients to send updates to the aggregator
    let (ws_client_tx, ws_client_rx) = tokio::sync::mpsc::channel(32);

    let mut aggregator = Aggregator::new(CONFIG.depth, ws_client_rx, grpc_tx);

    let grpc_server = grpc_server::OrderbookAggregatorService::new(grpc_rx);

    // use FuturesUnordered to run a number of websocket clients unknown until runtime
    let mut ws_clients = FuturesUnordered::new();
    for exchange in &CONFIG.exchanges {
        match exchange {
            Binance => {
                let ws_client = websocket::binance::BinanceOrderbookWebsocketClient::new(
                    CONFIG.depth,
                    CONFIG.currency_pair.clone(),
                    ws_client_tx.clone(),
                );
                ws_clients.push(tokio::spawn(
                    async move { ws_client.manage_connection().await },
                ));
            }
            Bitstamp => {
                let ws_client = websocket::bitstamp::BitstampOrderbookWebsocketClient::new(
                    CONFIG.currency_pair.clone(),
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
        Some(r) = ws_clients.next() => println!("{r:?}"),
        r = aggregator.run() => println!("{r:?}"),
        r = grpc_server::run_grpc_server(
            grpc_server,
            CONFIG.addr,
        ) => println!("{r:?}"),
    }
}
