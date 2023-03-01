//! Internal message protocol from websocket exchange clients to the aggregator (one-way).

use rust_decimal::Decimal;

use crate::config::Exchange;

/// Message type for publishing order book updates to downstream subscribers (one way only).
/// Websocket clients which have access to a diff/delta stream send an initial Snapshot followed by subsequent
/// Delta messages.
/// Websocket clients which only have a periodic snapshot stream forward periodic Snapshot messages.
/// (If an exchange has no websocket support, a client which makes periodic rest requests can use this protocol too.)
#[derive(Debug)]
pub enum OrderbookUpdateMessage {
    // websocket client disconnected from ws channel
    Disconnect {
        exchange: Exchange,
    },
    // the ws client's latest snapshot of the orderbook, also signals that the client is connected
    Snapshot {
        exchange: Exchange,
        orderbook: OrderbookSnapshot,
    },

    // a delta message contains a list of order book levels (price, quantity) with the new quantity
    Delta {
        exchange: Exchange,
        ask_updates: Vec<(Decimal, Decimal)>,
        bid_updates: Vec<(Decimal, Decimal)>,
    },
}

/// A snapshot of an order book for a particular exchange, currency pair.
/// Asks can be expected to be ascending in price and bids descending in price.
#[derive(Debug)]
pub struct OrderbookSnapshot {
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
}
