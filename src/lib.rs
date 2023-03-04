pub mod aggregator;
pub mod config;
pub mod grpc_server;
pub mod messages;
pub mod utils;
pub mod websocket;

pub mod proto {
    pub mod orderbook {
        tonic::include_proto!("orderbook");
    }
}
