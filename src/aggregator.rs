//! Aggregator which receives websocket client updates from multiple exchanges and aggregates
//! into a single orderbook. Exchange clients upstream send [OrderbookUpdateMessage]s.
//!
//! When websocket clients become disconnected, they notify the aggregator and their data is
//! dropped from the aggregated orderbook.

use std::collections::BTreeMap;
use std::fmt::Formatter;

use anyhow::bail;
use rust_decimal::{prelude::*, Decimal};
use rust_decimal_macros::dec;
use tokio::sync::{broadcast, mpsc};


use crate::{
    config::Exchange,
    messages::{
        OrderbookSnapshot,
        OrderbookUpdateMessage::{self, *},
    },
    proto::orderbook::{Level, Summary},
};

/// State and resources for the aggregator service.
pub struct Aggregator {
    depth: usize,
    // aggregator's internal state for the aggregated orderbook
    aggregated_orderbook: AggregatedOrderbook,
    // receive updates from websocket clients
    ws_client_rx: mpsc::Receiver<OrderbookUpdateMessage>,
    // send updates to the gRPC server
    grpc_tx: broadcast::Sender<Summary>,
}

/// Aggregated orderbook mapping (price, exchange) to quantity.
#[derive(Default)]
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
    fn new() -> Self {
        Self {
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
        }
    }
    
    /// Update from a snapshot, flushing old orders for this exchange first.
    fn update_from_snapshot(&mut self, exchange: Exchange, snapshot: OrderbookSnapshot) {
        self.flush_exchange_orders(exchange);
        for ask in snapshot.asks {
            self.asks.insert((ask.0, exchange), ask.1);
        }
        for bid in snapshot.bids {
            self.bids.insert((bid.0, exchange), bid.1);
        }
    }

    /// Update from a delta message.
    fn update_from_delta(
        &mut self,
        exchange: Exchange,
        ask_updates: Vec<(Decimal, Decimal)>,
        bid_updates: Vec<(Decimal, Decimal)>,
    ) {
        for ask in ask_updates {
            if ask.1 == dec!(0) {
                self.asks.remove(&(ask.0, exchange));
            } else {
                self.asks.insert((ask.0, exchange), ask.1);
            }
        }
        for bid in bid_updates {
            if bid.1 == dec!(0) {
                self.bids.remove(&(bid.0, exchange));
            } else {
                self.bids.insert((bid.0, exchange), bid.1);
            }
        }
    }

    /// Flush all orders for given exchange. The is necessary on snapshot as well as on disconnect.
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
        grpc_tx: broadcast::Sender<Summary>,
    ) -> Self {
        Self {
            depth,
            aggregated_orderbook: AggregatedOrderbook::default(),
            ws_client_rx,
            grpc_tx,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(m) = self.ws_client_rx.recv().await {
            match m {
                Disconnect { exchange } => {
                    self.aggregated_orderbook.flush_exchange_orders(exchange)
                }
                Snapshot {
                    exchange,
                    orderbook,
                } => self
                    .aggregated_orderbook
                    .update_from_snapshot(exchange, orderbook),
                Delta {
                    exchange,
                    ask_updates,
                    bid_updates,
                } => {
                    self.aggregated_orderbook
                        .update_from_delta(exchange, ask_updates, bid_updates)
                }
            }
            // send a new summary on any update, including exchange client disconnection
            self.send_summary();
        }
        bail!("aggregator terminated unexpectedly");
    }

    /// Send Summary to gRPC server.
    fn send_summary(&self) {
        let summary = self.aggregated_orderbook.to_summary(self.depth);
        match self.grpc_tx.send(summary) {
            Ok(_) => (),  // logging: 'sent summary to grpc'
            Err(_) => (), // logging: 'no summary sent - no grpc clients'
        }
    }

    /// Send an empty Summary message to clients for testing.
    pub fn send_test(&self) -> anyhow::Result<()> {
        self.grpc_tx.send(Summary::default())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use super::*;
    use Exchange::*;

    #[test]
    fn aggregation() {
        let orderbook1 = Orderbook::from_asks_bids(
            vec![
                (dec!(4), dec!(1)),
                (dec!(5), dec!(1)),
                (dec!(6), dec!(1)),
            ],
            vec![
                (dec!(3), dec!(1)),
                (dec!(2), dec!(1)),
                (dec!(1), dec!(1)),
            ],
        );
        let orderbook2 = Orderbook::from_asks_bids(
            vec![
                (dec!(4), dec!(1)),
                (dec!(5), dec!(1)),
                (dec!(7), dec!(1)),
            ],
            vec![
                (dec!(3), dec!(1)),
                (dec!(2), dec!(1)),
                (dec!(1), dec!(1)),
            ],
        );
        let mut a = AggregatedOrderbook::from_exchange_orderbook(Binance, orderbook1);
        a.apply_updates(Bitstamp, orderbook2);
        let mut target = AggregatedOrderbook::new();
        target.asks.insert((dec!(4), Binance), dec!(1));
        target.asks.insert((dec!(5), Binance), dec!(1));
        target.asks.insert((dec!(6), Binance), dec!(1));
        target.asks.insert((dec!(4), Bitstamp), dec!(1));
        target.asks.insert((dec!(5), Bitstamp), dec!(1));
        target.asks.insert((dec!(7), Bitstamp), dec!(1));
        target.bids.insert((dec!(1), Binance), dec!(1));
        target.bids.insert((dec!(2), Binance), dec!(1));
        target.bids.insert((dec!(3), Binance), dec!(1));
        target.bids.insert((dec!(3), Bitstamp), dec!(1));
        target.bids.insert((dec!(2), Bitstamp), dec!(1));
        target.bids.insert((dec!(1), Bitstamp), dec!(1));
        assert_eq!(a, target);
        
        let orderbook3 = Orderbook::from_asks_bids(
            vec![
                (dec!(4.5), dec!(2)),
                (dec!(5.5), dec!(2)),
                (dec!(6.5), dec!(2)),
            ],
            vec![
                (dec!(2.5), dec!(2)),
                (dec!(3.5), dec!(2)),
                (dec!(3.75), dec!(2)),
            ],
        );
        a.apply_updates(Bitstamp, orderbook3);
        let mut target = AggregatedOrderbook::new();
        target.asks.insert((dec!(4), Binance), dec!(1));
        target.asks.insert((dec!(5), Binance), dec!(1));
        target.asks.insert((dec!(6), Binance), dec!(1));
        target.asks.insert((dec!(4.5), Bitstamp), dec!(2));
        target.asks.insert((dec!(5.5), Bitstamp), dec!(2));
        target.asks.insert((dec!(6.5), Bitstamp), dec!(2));
        target.bids.insert((dec!(1), Binance), dec!(1));
        target.bids.insert((dec!(2), Binance), dec!(1));
        target.bids.insert((dec!(3), Binance), dec!(1));
        target.bids.insert((dec!(2.5), Bitstamp), dec!(2));
        target.bids.insert((dec!(3.5), Bitstamp), dec!(2));
        target.bids.insert((dec!(3.75), Bitstamp), dec!(2));
        assert_eq!(a, target);
        
        let summary = Summary {
            spread: 0.25f64,
            asks: vec!(
                Level {
                    exchange: "binance".into(),
                    price: 4f64,
                    amount: 1f64,
                },
                Level {
                    exchange: "bitstamp".into(),
                    price: 4.5f64,
                    amount: 2f64,
                },
                Level {
                    exchange: "binance".into(),
                    price: 5f64,
                    amount: 1f64,
                },
                Level {
                    exchange: "bitstamp".into(),
                    price: 5.5f64,
                    amount: 2f64,
                },
            ),
            bids: vec!(
                Level {
                    exchange: "bitstamp".into(),
                    price: 3.75f64,
                    amount: 2f64,
                },
                Level {
                    exchange: "bitstamp".into(),
                    price: 3.5f64,
                    amount: 2f64,
                },
                Level {
                    exchange: "binance".into(),
                    price: 3f64,
                    amount: 1f64,
                },
                Level {
                    exchange: "bitstamp".into(),
                    price: 2.5f64,
                    amount: 2f64,
                },
            ),
        };
        assert_eq!(a.to_summary(4), summary);
    }
}
