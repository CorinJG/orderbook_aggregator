pub mod aggregator;
pub mod config;
pub mod grpc_server;
pub mod orderbook;
pub mod websocket;

pub mod orderbook_summary {
    tonic::include_proto!("orderbook_summary");
}
