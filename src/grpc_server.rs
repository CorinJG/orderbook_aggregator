use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;

use crate::orderbook::orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer};
use crate::orderbook::{Empty, Summary};


#[derive(Debug)]
pub struct OrderbookAggregatorService {
    // receiver halves are used to notify connected rpc clients 
    client_updater: broadcast::Sender<Summary>,
}

impl OrderbookAggregatorService {
    pub fn new(client_updater: broadcast::Sender<Summary>) -> Self {
        Self { client_updater }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    type BookSummaryStream = ReceiverStream<Result<Summary, Status>>;
    async fn book_summary(&self, _request: Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        let (tx, rx) = mpsc::channel(15);
        // clone broadcast receiver for each client connected to this stream
        let mut client_updater = self.client_updater.subscribe();
        tokio::spawn(async move {
            while let Ok(summary) = client_updater.recv().await {
                tx.send(Ok(summary)).await.unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    } 
}

/// Background task to run grpc server and forward latest Summary from Aggregator to connected grpc clients
pub async fn run_grpc_server(
    grpc_aggregator_service: OrderbookAggregatorService, 
    addr: String,
) {
    let addr = addr.parse().expect("invalid address");

    let service = OrderbookAggregatorServer::new(grpc_aggregator_service);
    Server::builder().add_service(service).serve(addr).await; // todo handle Result here
}