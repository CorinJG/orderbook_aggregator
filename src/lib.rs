pub mod aggregator;
pub mod config;
pub mod grpc_server;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}