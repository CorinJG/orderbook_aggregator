use std::net::SocketAddr;

use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use crate::proto::orderbook::{
    orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
    Empty, Summary,
};

#[derive(Debug)]
pub struct OrderbookAggregatorService {
    // cloned receiver halves are used to notify connected rpc clients
    aggregator_rx: watch::Receiver<Option<Summary>>,
}

impl OrderbookAggregatorService {
    pub fn new(aggregator_rx: watch::Receiver<Option<Summary>>) -> Self {
        Self { aggregator_rx }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    type BookSummaryStream = ReceiverStream<Result<Summary, Status>>;
    async fn book_summary(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        println!("new grpc client connected");
        let (tx, rx) = mpsc::channel(4);
        // clone broadcast receiver for each client connected to this stream
        let mut aggregator_rx = self.aggregator_rx.clone();
        tokio::spawn(async move {
            while aggregator_rx.changed().await.is_ok() {
                if let Some(summary) = aggregator_rx.borrow().clone() {
                    if let Err(e) = tx.try_send(Ok(summary)) {
                        match e {
                            TrySendError::Full(..) => {
                                panic!("back-pressure on a grpc client channel")
                            }
                            TrySendError::Closed(..) => {
                                println!("grpc client disconnected");
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Run a grpc server using the provided [OrderbookAggregatorService].
pub async fn run_grpc_server(
    grpc_aggregator_service: OrderbookAggregatorService,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let service = OrderbookAggregatorServer::new(grpc_aggregator_service);
    println!("grpc server running");
    Server::builder().add_service(service).serve(addr).await
}
