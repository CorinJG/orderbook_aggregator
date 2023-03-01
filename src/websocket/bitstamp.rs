//! Types and [OrderbookWebsocketClient] trait implementation connecting to the Bitstamp websocket
//! and maintaining a local orderbook which tracks remote state using diff channel.
//!
//! Bitstamp uses timestamps in its orderbook diff channel. In order to establish initial
//! synchronization with the event stream, we make a separate restful API request to get
//! a full orderbook snapshot and then attach to the stream using this as a starting point.
//! Our client will buffer websocket events for a small amount of time before making the
//! intial snapshot request and then once we receive it we discard messages from the buffer
//! which have a timestamp from before the snapshot. The first update event applied will be
//! the first one with a timestamp greater than the initial snapshot timestamp.
//! In particular, we need to check that we had at least one message on the websocket from
//! prior to the initial snapshot - otherwise we can't deduce that we have received the
//! very first event after the snapshot.

use std::pin::Pin;
use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use futures_util::{SinkExt, Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tungstenite::{error::Error, protocol::Message};

use crate::config::{CurrencyPair, Exchange};
use crate::messages::{
    OrderbookSnapshot,
    OrderbookUpdateMessage::{self, *},
};
use crate::utils::deserialize_using_parse;

use super::{
    OrderbookWebsocketClient, WebsocketClientError::*, WebsocketConnectionResult, WsWriter,
};

const EXCHANGE: Exchange = Exchange::Bitstamp;
const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// Target deserialization type for bitstamp rest order book snapshot.
#[derive(Debug, Deserialize)]
pub struct RawOrderbookSnapshot {
    #[serde(deserialize_with = "deserialize_using_parse")]
    microtimestamp: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

impl From<RawOrderbookSnapshot> for OrderbookSnapshot {
    fn from(value: RawOrderbookSnapshot) -> Self {
        Self {
            asks: value.asks,
            bids: value.bids,
        }
    }
}

/// The format of a websocket message received by our client.
#[derive(Debug, Deserialize)]
struct WsMessage {
    event: String,
    channel: String,
    data: WsMessageData,
}

/// The inner data payload of a websocket message.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct WsMessageData {
    #[serde(deserialize_with = "deserialize_using_parse")]
    microtimestamp: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// Construct a websocket diff stream subscription message for the given symbol.
fn construct_subscription_message(symbol: &str) -> String {
    let front = r#"{"event": "bts:subscribe", "data": {"channel": "diff_order_book_"#;
    let back = r#""}}"#;
    format!("{front}{symbol}{back}")
}

pub struct BitstampOrderbookWebsocketClient {
    symbol: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    ws_buffer_time_ms: u64,
}

impl BitstampOrderbookWebsocketClient {
    pub fn new(
        currency_pair: CurrencyPair,
        downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
        ws_buffer_time_ms: u64,
    ) -> Self {
        Self {
            symbol: [currency_pair.base(), currency_pair.quote()].join(""),
            downstream_tx,
            ws_buffer_time_ms,
        }
    }
}

#[async_trait]
impl OrderbookWebsocketClient for BitstampOrderbookWebsocketClient {
    type RawOrderbookSnapshot = RawOrderbookSnapshot;

    async fn connect(&self) -> WebsocketConnectionResult {
        let connect_addr = WS_BASE_URL;
        tokio_tungstenite::connect_async(connect_addr).await
    }

    async fn subscribe(&self, mut write: Pin<&mut WsWriter>) -> anyhow::Result<()> {
        let subscribe_message = construct_subscription_message(&self.symbol);
        Ok(write
            .send(tungstenite::Message::binary(subscribe_message))
            .await?)
    }

    async fn buffer_messages(&self) {
        tokio::time::sleep(Duration::from_millis(self.ws_buffer_time_ms)).await;
    }

    async fn request_snapshot(&self) -> anyhow::Result<Option<RawOrderbookSnapshot>> {
        Ok(Some(
            reqwest::get(format!(
                "https://www.bitstamp.net/api/v2/order_book/{}/",
                self.symbol
            ))
            .await?
            .json()
            .await?,
        ))
    }

    /// Retrieve websocket messages until we can synchronize with the snapshot.
    /// Then forward the snapshot and the first pertinent delta event downstream and return.
    async fn synchronize(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        initial_snapshot: Option<Self::RawOrderbookSnapshot>,
    ) -> anyhow::Result<()> {
        let initial_snapshot = initial_snapshot.unwrap();
        let expected_channel = format!("diff_order_book_{}", self.symbol);
        // flag for whether we've got at least one delta in the buffer from before the snapshot
        let mut prior_event = false;
        // break once we establish inital synchrony as the validation logic is different after this point
        while let Some(message) = read.next().await.transpose()? {
            if message.is_ping() || message.is_pong() {
                continue;
            }
            let WsMessage {
                event,
                data,
                channel,
            } = serde_json::from_slice(&message.into_data())?;
            match event.as_ref() {
                "data" => {
                    if channel != expected_channel {
                        bail!(format!("unexpected channel: {channel}"));
                    }
                    if data.microtimestamp < initial_snapshot.microtimestamp {
                        if !prior_event {
                            prior_event = true;
                        }
                        continue;
                    } else {
                        if !prior_event {
                            bail!(SynchronizationError(EXCHANGE));
                        }
                        println!("bitstamp client synchronized");
                        // forward the snapshot and the first delta, then return
                        self.downstream_tx
                            .try_send(Snapshot {
                                exchange: EXCHANGE,
                                orderbook: OrderbookSnapshot::from(initial_snapshot),
                            })
                            .context("bitstamp error sending downstream")?;
                        self.downstream_tx
                            .try_send(Delta {
                                exchange: EXCHANGE,
                                ask_updates: data.asks,
                                bid_updates: data.bids,
                            })
                            .context("bitstamp error sending downstream")?;
                        return Ok(());
                    }
                }
                "bts:subscription_succeeded" => (),
                other => bail!("bitstamp unexpected event type: {other}"),
            }
        }
        bail!(DisconnectError(EXCHANGE));
    }

    /// Process the websocket events, validating them and forwarding downstream.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> anyhow::Result<()> {
        let expected_channel = format!("diff_order_book_{}", self.symbol);
        let mut prev_timestamp = None;
        while let Some(message) = read.next().await.transpose()? {
            if message.is_ping() || message.is_pong() {
                continue;
            }
            let WsMessage {
                event,
                data,
                channel,
            } = serde_json::from_slice(&message.into_data())?;
            match event.as_ref() {
                "data" => {
                    if channel != expected_channel {
                        bail!("unexpected channel: {channel}");
                    }
                    if let Some(prev_ts) = prev_timestamp {
                        if data.microtimestamp <= prev_ts {
                            bail!("event timestamps out of sequence");
                        }
                    }
                    prev_timestamp = Some(data.microtimestamp);
                    self.downstream_tx
                        .try_send(Delta {
                            exchange: EXCHANGE,
                            ask_updates: data.asks,
                            bid_updates: data.bids,
                        })
                        .context("bitstamp error sending downstream")?;
                }
                "bts:subscription_succeeded" => (),
                other => bail!("bitstamp unexpected event type: {other}"),
            }
        }
        bail!(DisconnectError(EXCHANGE))
    }
}
