//! Types and [OrderbookWebsocketClient] trait implementation for connecting to the Binance
//! orderbook "Diff Depth" websocket channel and maintaining local orderbook which tracks
//! remote state.
//!
//! Binance provides a public [`"Diff. Depth"`] websocket channel which streams update
//! events with associated first- and last- update IDs. This enables our client
//! to establish initial synchrony with the stream by making a separate rest request
//! for an orderbook snapshot and then applying event updates which occur after this
//! snapshot, discarding buffered events with a last_update_id from before that of the
//! snapshot. In particular, in order to attach to the stream we must receive a
//! websocket event having both first_update_id <= snapshot.update_id and
//! last_update_id >= snapshot.update_id + 1.
//!
//! [`"Diff. Depth"`]: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#diff-depth-stream

use std::pin::Pin;
use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tungstenite::{error::Error, protocol::Message};

use crate::{
    aggregator::OrderbookUpdateMessage::{self, *},
    config::{CurrencyPair, Exchange},
    orderbook::Orderbook,
};

use super::{OrderbookWebsocketClient, WebsocketConnectionResult, WebsocketDisconnectError, WsWriter};

const EXCHANGE: Exchange = Exchange::Binance;
const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";

/// Type to deserialize the initial Binance rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
pub struct OrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

// The structure of an orderbook diff/delta websocket message received by our client.
#[derive(Debug, Deserialize)]
struct WsMessage {
    #[serde(rename = "e")]
    event: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    last_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    asks: Vec<(Decimal, Decimal)>,
}

/// A type which connects to a websocket orderbook channel and sends update messages downstream.
pub struct BinanceOrderbookWebsocketClient {
    depth: usize,
    symbol_lower: String,
    symbol_upper: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    ws_buffer_time_ms: u64,
}

impl BinanceOrderbookWebsocketClient {
    pub fn new(
        depth: usize,
        currency_pair: CurrencyPair,
        downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
        ws_buffer_time_ms: u64,
    ) -> Self {
        let symbol_lower = [currency_pair.base(), currency_pair.quote()].join("");
        let symbol_upper = [
            currency_pair.base().to_uppercase().as_str(),
            currency_pair.quote().to_uppercase().as_str(),
        ]
        .join("");
        Self {
            depth,
            symbol_lower,
            symbol_upper,
            downstream_tx,
            ws_buffer_time_ms,
        }
    }
}

#[async_trait]
impl OrderbookWebsocketClient for BinanceOrderbookWebsocketClient {
    type OrderbookSnapshot = OrderbookSnapshot;

    async fn connect(&self) -> WebsocketConnectionResult {
        let connect_addr = format!("{}/{}@depth@100ms", WS_BASE_URL, self.symbol_lower);
        tokio_tungstenite::connect_async(connect_addr).await
    }

    async fn subscribe(&self, _: Pin<&mut WsWriter>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn buffer_messages(&self) {
        tokio::time::sleep(Duration::from_millis(self.ws_buffer_time_ms)).await;
    }

    async fn request_snapshot(&self) -> anyhow::Result<Option<OrderbookSnapshot>> {
        Ok(Some(
            reqwest::get(format!(
                "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
                self.symbol_upper
            ))
            .await?
            .json()
            .await?,
        ))
    }

    /// Process the websocket events, applying updates which have an ID after the initial
    /// snapshot ID to the local orderbook state.
    /// On update, forward the latest top-<depth> orderbook downstream.
    /// Performs validation of event type and symbol as well as ensuring no gaps in update IDs.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        initial_snapshot: Option<OrderbookSnapshot>,
    ) -> anyhow::Result<()> {
        let initial_snapshot = initial_snapshot.unwrap();
        let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

        // validation of first_update_id is different for the first event applied to the snapshot
        let mut synchronized = false;
        let mut prev_last_update_id = initial_snapshot.last_update_id;
        while let Some(message) = read.next().await.transpose()? {
            if message.is_ping() || message.is_pong() {
                continue;
            }
            let WsMessage {
                event,
                symbol,
                first_update_id,
                last_update_id,
                bids,
                asks,
            } = serde_json::from_slice(&message.into_data())?;
            if event != "depthUpdate" {
                bail!("unexpected event field: {event}");
            }
            if symbol != self.symbol_upper {
                bail!("unexpected symbol field: {symbol}");
            }
            // discard events where last_update_id is older than rest response update_id
            if last_update_id <= prev_last_update_id {
                continue;
            }
            if !synchronized {
                if first_update_id > prev_last_update_id + 1 {
                    bail!("missing event");
                }
                synchronized = true;
                println!("binance ws client synchronized");
            } else if first_update_id != prev_last_update_id + 1 {
                bail!("missing event, gap in sequence");
            }
            prev_last_update_id = last_update_id;
            orderbook.apply_updates(asks, bids);
            self.downstream_tx
                .try_send(OrderbookUpdate {
                    exchange: EXCHANGE,
                    orderbook: orderbook.to_truncated(self.depth),
                })
                .context("binance error sending downstream")?;
        }
        bail!(WebsocketDisconnectError(EXCHANGE));
    }
}
