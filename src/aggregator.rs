use tokio::sync::broadcast::Sender;

use crate::proto::orderbook::Summary;

pub struct Aggregator {
    // send updates to the gRPC server
    pub sender: Sender<Summary>,
}

impl Aggregator {
    pub fn new(sender: Sender<Summary>) -> Self {
        Self { sender }
    }
}
