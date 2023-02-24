//! Aggregator which receives asynchronous websocket client updates from multiple
//! exchanges and aggregates into a single orderbook.
//! 
//! Clients don't send diffs. They send snapshots truncated to <depth>.
//! This is sufficient for the aggregator to deduce the top-n asks or bids.
//! 
//! When websocket clients upstream become disconnected, they notify the
//! aggregator and their data is dropped from the aggregated orderbook state.

use std::collections::BTreeMap;
use std::fmt::Formatter;

use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use tokio::sync::{broadcast::Sender, mpsc};

use crate::{
    config::Exchange,
    orderbook::Orderbook,
    proto::orderbook::{Level, Summary},
};

/// Messages from websocket clients to the aggregator (one way only).
#[derive(Debug)]
pub enum OrderbookUpdateMessage {
    // websocket client disconnected from ws channel
    Disconnect {
        exchange: Exchange,
    },
    // the ws client's latest updated depth-n orderbook, also signals that the ws client is connected
    OrderbookUpdate {
        exchange: Exchange,
        orderbook: Orderbook,
    },
}
use OrderbookUpdateMessage::*;

/// State and resources for the aggregator service.
pub struct Aggregator {
    depth: usize,
    // aggreagator's internal state for the aggregated orderbook
    aggregated_orderbook: Option<AggregatedOrderbook>,
    client1: Exchange,
    client2: Exchange,
    // receive updates from websocket clients
    ws_client_rx: mpsc::Receiver<OrderbookUpdateMessage>,
    // send updates to the gRPC server
    grpc_tx: Sender<Summary>,
    connection_status: ConnectionStatus,
}

/// The status of websocket client connections to their respective exchange websockets.
#[derive(Default)]
struct ConnectionStatus {
    client1: bool,
    client2: bool,
}

/// Aggregated orderbook mapping (price, exchange) to quantity.
struct AggregatedOrderbook {
    asks: BTreeMap<(Decimal, Exchange), Decimal>,
    bids: BTreeMap<(Decimal, Exchange), Decimal>,
}

impl std::fmt::Debug for AggregatedOrderbook {
    /// Display all levels with asks ordered by increasing price and bids by decreasing price.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AggregatedOrderbook {{ asks: {{ ")?;
        for ((p, e), q) in self.asks.iter() {
            write!(f, "[{p}, {q}, {e:?}], ")?;
        }
        write!(f, "... }},\nbids: {{ ")?;
        for ((p, e), q) in self.bids.iter().rev() {
            write!(f, "[{p}, {q}, {e:?}], ")?;
        }
        write!(f, "... }} }}")
    }
}

impl AggregatedOrderbook {
    /// Initialize state from a (first) update.
    fn from_exchange_orderbook(exchange: Exchange, orderbook: Orderbook) -> Self {
        let (asks, bids) = orderbook.into_asks_bids();
        Self {
            asks: BTreeMap::from_iter(
                asks.into_iter()
                    .map(|(price, quantity)| ((price, exchange), quantity)),
            ),
            bids: BTreeMap::from_iter(
                bids.into_iter()
                    .map(|(price, quantity)| ((price, exchange), quantity)),
            ),
        }
    }

    /// As the client updates are snapshots and not deltas, flush all existing orders for the exchange
    /// and then insert the latest orders.
    fn apply_updates(&mut self, exchange: Exchange, latest_orderbook: Orderbook) {
        self.flush_exchange_orders(exchange);
        let (asks, bids) = latest_orderbook.into_asks_bids();
        for (price, quantity) in asks {
            self.asks.insert((price, exchange), quantity);
        }
        for (price, quantity) in bids {
            self.bids.insert((price, exchange), quantity);
        }
    }

    /// Flush all orders from the aggregated orderbook for given exchange. The is required on every 
    /// snapshot from a client as well as when a client becomes disconnected from it's websocket.
    fn flush_exchange_orders(&mut self, exchange: Exchange) {
        self.asks = BTreeMap::from_iter(
            self.asks
                .iter()
                .filter(|((_, e), _)| e != &exchange)
                .map(|(&(p, e), &q)| ((p, e), q)),
        );
        self.bids = BTreeMap::from_iter(
            self.bids
                .iter()
                .filter(|((_, e), _)| e != &exchange)
                .map(|(&(p, e), &q)| ((p, e), q)),
        );
    }

    /// Construct a Summary from the aggregated orderbook.
    fn to_summary(&self, depth: usize) -> Summary {
        let spread = self.asks.keys().next().map(|p| p.0).unwrap()
            - self.bids.keys().rev().next().map(|p| p.0).unwrap();
        Summary {
            spread: spread.to_f64().unwrap(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|((p, e), q)| Level {
                    exchange: e.into(),
                    price: p.to_f64().unwrap(),
                    amount: q.to_f64().unwrap(),
                })
                .collect(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|((p, e), q)| Level {
                    exchange: e.into(),
                    price: p.to_f64().unwrap(),
                    amount: q.to_f64().unwrap(),
                })
                .collect(),
        }
    }
}

impl Aggregator {
    pub fn new(
        depth: usize,
        ws_client_rx: mpsc::Receiver<OrderbookUpdateMessage>,
        grpc_tx: Sender<Summary>,
        client1: Exchange,
        client2: Exchange,
    ) -> Self {
        Self {
            depth,
            aggregated_orderbook: None,
            ws_client_rx,
            grpc_tx,
            client1,
            client2,
            connection_status: ConnectionStatus::default(),
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(m) = self.ws_client_rx.recv().await {
            match m {
                Disconnect { exchange } => {
                    if let Some(ob) = self.aggregated_orderbook.as_mut() {
                        ob.flush_exchange_orders(exchange);
                    }
                    if exchange == self.client1 {
                        self.connection_status.client1 = false;
                    } else if exchange == self.client2 {
                        self.connection_status.client2 = false;
                    }
                }

                OrderbookUpdate {
                    exchange,
                    orderbook,
                } => {
                    if exchange == self.client1 {
                        self.connection_status.client1 = true;
                    } else {
                        self.connection_status.client2 = true;
                    }
                    self.apply_updates(exchange, orderbook);
                    if self.connection_status.client1 && self.connection_status.client2 {
                        match self.grpc_tx.send(
                            self.aggregated_orderbook
                                .as_ref()
                                .unwrap()
                                .to_summary(self.depth),
                        ) {
                            Ok(_) => (),  // logging: 'sent summary to grpc'
                            Err(_) => (), // logging: 'no summary sent - no grpc clients'
                        };
                    }
                }
            }
        }
        Err(anyhow!("aggregator terminated unexpectedly"))
    }

    /// Send an empty Summary message to clients for testing.
    pub fn send_test(&self) -> anyhow::Result<()> {
        self.grpc_tx.send(Summary::default())?;
        Ok(())
    }

    /// Apply updates to the aggreagted orderbook using the latest snapshot from the exchange.
    fn apply_updates(&mut self, exchange: Exchange, latest_orderbook: Orderbook) {
        match self.aggregated_orderbook {
            Some(ref mut o) => {
                o.apply_updates(exchange, latest_orderbook);
            }
            None => {
                self.aggregated_orderbook = Some(AggregatedOrderbook::from_exchange_orderbook(
                    exchange,
                    latest_orderbook,
                ));
            }
        }
    }
}
