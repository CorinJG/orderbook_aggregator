//! Internal message protocol from websocket exchange clients to the aggregator (one-way).

use rust_decimal::Decimal;

use crate::config::Exchange;

/// Message type for publishing order book updates from clients to the aggregator.
#[derive(Debug)]
pub enum OrderbookUpdateMessage {
    // websocket client disconnected from ws channel
    Disconnect {
        exchange: Exchange,
    },
    // the latest snapshot truncated to <depth> levels, also signals that the client is connected
    DepthSnapshot {
        exchange: Exchange,
        orderbook: OrderbookSnapshot,
    },
}

/// A snapshot of an order book for a particular exchange, currency pair.
/// Asks can be expected to be ascending in price and bids descending in price.
#[derive(Debug)]
pub struct OrderbookSnapshot {
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
}
