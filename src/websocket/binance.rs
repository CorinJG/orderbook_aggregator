//! Types and [WebsocketClient] trait implementation for connecting to the Binance
//! orderbook "Diff Depth" websocket channel and sending updates downstream.
//!
//! Binance provides a public [`"Diff. Depth"`] websocket channel which streams update
//! events with associated first- and last- update ids. This enables our client
//! to establish synchrony with the stream by buffering delta messages and making a separate
//! rest request for a snapshot. Bufffered events with a last_update_id from before
//! that of the snapshot are discarded. In order to synchronize with the stream we must receive
//! a websocket event having both `first_update_id <= snapshot.update_id` and `last_update_id >= snapshot.update_id + 1`.
//!
//! [`"Diff. Depth"`]: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#diff-depth-stream

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tungstenite::{error::Error, protocol::Message};

use crate::{
    config::{CurrencyPair, Exchange},
    messages::OrderbookUpdateMessage,
    utils::{Millis, Seconds},
};

use super::BTreeBook;
use super::{
    WebsocketClient,
    WebsocketClientError::{self, *},
    WebsocketConnectionResult, WsWriter,
};

const EXCHANGE: Exchange = Exchange::Binance;
const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";
// how often to re-sync a delta stream from a snapshot
const SYNCHRONIZATION_PERIOD: Seconds = 600;
// brief period of applying updates to internal state without forwarding downstream following sync
const FAST_FORWARD_TIME: Millis = 500;
// how long to buffer ws messages when syncing from a snapshot
const WS_BUFFER_DURATION: Millis = 3_000;

/// Type to deserialize the Binance rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
pub struct RawOrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

impl From<RawOrderbookSnapshot> for BTreeBook {
    fn from(value: RawOrderbookSnapshot) -> Self {
        Self {
            asks: BTreeMap::from_iter(value.asks.into_iter()),
            bids: BTreeMap::from_iter(value.bids.into_iter()),
        }
    }
}

// A deserialized binance orderbook diff/delta websocket message.
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

/// The client type to implement [WebsocketClient] on.
/// This client implementation sends [OrderbookUpdateMessage]s downstream using a provided channel.
pub struct BinanceOrderbookWebsocketClient {
    depth: usize,
    symbol_lower: String,
    symbol_upper: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    orderbook: Mutex<BTreeBook>,
}

impl BinanceOrderbookWebsocketClient {
    pub fn new(
        depth: usize,
        currency_pair: CurrencyPair,
        downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
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
            orderbook: Mutex::new(BTreeBook::default()),
        }
    }

    async fn get_rest_snapshot(&self) -> anyhow::Result<RawOrderbookSnapshot> {
        Ok(Client::builder()
            .timeout(Duration::from_millis(3_000))
            .build()?
            .get(format!(
                "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
                self.symbol_upper,
            ))
            .send()
            .await?
            .json()
            .await?)
    }

    /// Process events post-synchronization, sending latest (truncated) snapshots downstream on change.
    /// Performs validation of event type and symbol as well as ensuring no gaps in update ids.
    async fn _process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        // flag for forwarding updates
        forward: bool,
    ) -> Result<(), WebsocketClientError> {
        let mut prev_last_update_id = None;
        while let Some(message) = read
            .next()
            .await
            .transpose()
            .context("tungstenite error during binance message processing")?
        {
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
            } = serde_json::from_slice(&message.into_data())
                .context("serde_json error during binance ws message processing")?;
            self.validate_ws_message(event, symbol, "depthUpdate")?;
            if let Some(prev_last_id) = prev_last_update_id {
                if first_update_id != prev_last_id + 1 {
                    return Err(InvariantViolation(
                        EXCHANGE,
                        "missing event in sequence".into(),
                    ));
                }
            }
            prev_last_update_id = Some(last_update_id);
            self.orderbook.lock().unwrap().apply_deltas(asks, bids);
            if forward {
                self.downstream_tx
                    .try_send(
                        self.orderbook
                            .lock()
                            .unwrap()
                            .to_depth_snapshot(EXCHANGE, self.depth),
                    )
                    .context("binance error sending downstream")?;
            }
        }
        // disconnected
        self.downstream_tx
            .try_send(OrderbookUpdateMessage::Disconnect { exchange: EXCHANGE })
            .map_err(|e| {
                println!("binance error sending disconnect message");
                Other(anyhow!(e))
            })?;
        Err(Disconnect(EXCHANGE))
    }

    /// Ensure the symbol and event fields are what we expect.
    fn validate_ws_message(
        &self,
        event: String,
        symbol: String,
        expected_event: &'static str,
    ) -> Result<(), WebsocketClientError> {
        if event != expected_event {
            return Err(InvariantViolation(
                EXCHANGE,
                format!("unexpected event: {}", event),
            ));
        }
        if symbol != self.symbol_upper {
            return Err(InvariantViolation(
                EXCHANGE,
                format!("unexpected symbol: {}", symbol),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl WebsocketClient for BinanceOrderbookWebsocketClient {
    async fn connect(&self) -> WebsocketConnectionResult {
        let connect_addr = format!("{}/{}@depth@100ms", WS_BASE_URL, self.symbol_lower);
        tokio_tungstenite::connect_async(connect_addr).await
    }

    // no subscription necessary
    async fn subscribe(&self, _: Pin<&mut WsWriter>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn synchronize(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError> {
        // buffer messages
        tokio::time::sleep(Duration::from_millis(WS_BUFFER_DURATION)).await;

        // get an initial snapshot to sync the stream
        let initial_snapshot = self.get_rest_snapshot().await?;

        let mut synchronized = false;
        // attempt to synchronize - fail if no event immediately following the snapshot with no gaps
        while let Some(message) = read
            .next()
            .await
            .transpose()
            .context("tungstenite error during binance synchronizaion")?
        {
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
            } = serde_json::from_slice(&message.into_data())
                .context("serde_json error during binance synchronization")?;
            self.validate_ws_message(event, symbol, "depthUpdate")?;
            // discard events where last_update_id is older than snapshot update_id
            if last_update_id <= initial_snapshot.last_update_id {
                continue;
            }
            if first_update_id > initial_snapshot.last_update_id + 1 {
                eprintln!("binance first event's update id > snapshot last update id + 1");
                return Err(Synchronization(EXCHANGE));
            }
            println!("binance client synchronized");
            if !synchronized {
                synchronized = true;
                *self.orderbook.lock().unwrap() = BTreeBook::from(initial_snapshot);
            }
            self.orderbook.lock().unwrap().apply_deltas(asks, bids);
            break;
        }
        if !synchronized {
            // disconnected
            self.downstream_tx
                .try_send(OrderbookUpdateMessage::Disconnect { exchange: EXCHANGE })
                .map_err(|e| {
                    println!("binance error sending disconnect message");
                    Other(anyhow!(e))
                })?;
            return Err(Disconnect(EXCHANGE));
        }
        // now "fast-forward", applying deltas for remaining messages in the buffer *without sending updates downstream*
        match timeout(
            Duration::from_millis(FAST_FORWARD_TIME),
            self._process_messages(read, false),
        )
        .await
        {
            Ok(result) => {
                // completed
                result.map(|_| unreachable!("_process_messages() never terminates with Ok"))
            }
            Err(_) => {
                // timeout elapsed
                println!("binance fast-forward complete");
                Ok(())
            }
        }
    }

    /// Process websocket messages "ad infinitum", timing out with error when the synchronization period elapses.
    async fn process_messages(
        &self,
        read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError> {
        timeout(
            Duration::from_secs(SYNCHRONIZATION_PERIOD),
            self._process_messages(read, true),
        )
        .await
        .map_err(|_| {
            // timeout elapsed
            if let Err(e) = self
                .downstream_tx
                .try_send(OrderbookUpdateMessage::Disconnect { exchange: EXCHANGE })
            {
                println!("binance error sending disconnect message");
                Other(anyhow!(e))
            } else {
                SynchronizationExpired(EXCHANGE)
            }
        })?
    }
}
