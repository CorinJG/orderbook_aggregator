//! Types and [OrderbookWebsocketClient] trait implementation for connecting to the Binance
//! orderbook "Diff Depth" websocket channel and sending internal update messages downstream.
//!
//! Binance provides a public [`"Diff. Depth"`] websocket channel which streams update
//! events with associated first- and last- update ids. This enables our client
//! to establish synchrony with the stream by buffering delta messages and making a separate
//! rest request for an orderbook snapshot. Bufffered events with a last_update_id from before
//! that of the snapshot are discarded.
//!
//! In particular, in order to synchronize with the stream we must receive a websocket event
//! having both first_update_id <= snapshot.update_id and last_update_id >= snapshot.update_id + 1.
//!
//! [`"Diff. Depth"`]: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#diff-depth-stream

use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tungstenite::{error::Error, protocol::Message};

use crate::{
    config::{CurrencyPair, Exchange},
    messages::{
        OrderbookSnapshot,
        OrderbookUpdateMessage::{self, *},
    },
    utils::{Millis, Seconds},
};

use super::{
    OrderbookWebsocketClient,
    WebsocketClientError::{self, *},
    WebsocketConnectionResult, WsWriter,
};

const EXCHANGE: Exchange = Exchange::Binance;
const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";
// re-synchronize when this expires
const SYNCHRONIZATION_TIMEOUT: Seconds = 600;
// how long to buffer websocket messages before requesting an initial snapshot
const WS_BUFFER_DURATION: Millis = 3_000;

/// Type to deserialize the Binance rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
pub struct RawOrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
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

/// Ensure the symbol and event fields are what we expect.
fn validate_ws_message(
    event: &String,
    symbol: &String,
    expected_event: &'static str,
    expected_symbol: &String,
) -> Result<(), WebsocketClientError> {
    if event != expected_event {
        return Err(InvariantViolation(
            EXCHANGE,
            format!("unexpected event: {}", event),
        ));
    }
    if symbol != expected_symbol {
        return Err(InvariantViolation(
            EXCHANGE,
            format!("unexpected symbol: {}", symbol),
        ));
    }
    Ok(())
}

/// A type which connects to a websocket orderbook channel and sends update messages downstream.
pub struct BinanceOrderbookWebsocketClient {
    symbol_lower: String,
    symbol_upper: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
}

impl BinanceOrderbookWebsocketClient {
    pub fn new(
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
            symbol_lower,
            symbol_upper,
            downstream_tx,
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

    /// Process post-synchronization events, forwarding delta updates downstream.
    /// Performs validation of event type and symbol as well as ensuring no gaps in update ids.
    async fn _process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
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
            validate_ws_message(&event, &symbol, "depthUpdate", &self.symbol_upper)?;
            if let Some(prev_last_id) = prev_last_update_id {
                if first_update_id != prev_last_id + 1 {
                    return Err(InvariantViolation(
                        EXCHANGE,
                        "missing event in sequence".into(),
                    ));
                }
            }
            prev_last_update_id = Some(last_update_id);
            self.downstream_tx
                .try_send(Delta {
                    exchange: EXCHANGE,
                    ask_updates: asks,
                    bid_updates: bids,
                })
                .context("binance error sending downstream")?;
        }
        Err(Disconnect(EXCHANGE))
    }
}

#[async_trait]
impl OrderbookWebsocketClient for BinanceOrderbookWebsocketClient {
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

        // get an initial snapshot to synchronize the stream
        let initial_snapshot = self.get_rest_snapshot().await?;

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
            validate_ws_message(&event, &symbol, "depthUpdate", &self.symbol_upper)?;
            // discard events where last_update_id is older than snapshot update_id
            if last_update_id <= initial_snapshot.last_update_id {
                continue;
            }
            if first_update_id > initial_snapshot.last_update_id + 1 {
                eprintln!("binance first event's update id > snapshot last update id + 1");
                return Err(Synchronization(EXCHANGE));
            }
            println!("binance ws client synchronized");
            // forward snapshot and first delta message downstream and return
            self.downstream_tx
                .try_send(Snapshot {
                    exchange: EXCHANGE,
                    orderbook: OrderbookSnapshot {
                        asks: initial_snapshot.asks,
                        bids: initial_snapshot.bids,
                    },
                })
                .context("binance error sending downstream")?;
            self.downstream_tx
                .try_send(Delta {
                    exchange: EXCHANGE,
                    ask_updates: asks,
                    bid_updates: bids,
                })
                .context("binance error sending downstream")?;
            return Ok(());
        }
        Err(Disconnect(EXCHANGE))
    }

    /// Process websocket messages "ad infinitum", timing out with error when the synchronization period elapses.
    async fn process_messages(
        &self,
        read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError> {
        if timeout(
            Duration::from_secs(SYNCHRONIZATION_TIMEOUT),
            self._process_messages(read),
        )
        .await
        .is_err()
        {
            self.downstream_tx
                .try_send(OrderbookUpdateMessage::Disconnect { exchange: EXCHANGE })
                .context("binance error sending disconnect message")?;
            return Err(SynchronizationExpired(EXCHANGE));
        }
        Ok(())
    }
}
