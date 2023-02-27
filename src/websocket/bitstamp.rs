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

use crate::aggregator::OrderbookUpdateMessage::{self, *};
use crate::config::{CurrencyPair, Exchange};
use crate::orderbook::Orderbook;
use crate::utils::deserialize_using_parse;

use super::{OrderbookWebsocketClient, WebsocketConnectionResult, WebsocketDisconnectError, WsWriter};

const EXCHANGE: Exchange = Exchange::Bitstamp;
const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// Type to deserialize a raw rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
pub struct OrderbookSnapshot {
    #[serde(deserialize_with = "deserialize_using_parse")]
    microtimestamp: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
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
    depth: usize,
    symbol: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    ws_buffer_time_ms: u64,
}

impl BitstampOrderbookWebsocketClient {
    pub fn new(
        depth: usize,
        currency_pair: CurrencyPair,
        downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
        ws_buffer_time_ms: u64,
    ) -> Self {
        Self {
            depth,
            symbol: [currency_pair.base(), currency_pair.quote()].join(""),
            downstream_tx,
            ws_buffer_time_ms,
        }
    }
}

#[async_trait]
impl OrderbookWebsocketClient for BitstampOrderbookWebsocketClient {
    type OrderbookSnapshot = OrderbookSnapshot;

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

    async fn request_snapshot(&self) -> anyhow::Result<Option<OrderbookSnapshot>> {
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

    /// Process the websocket events, applying updates with timestamps after the initial snapshot
    /// to the local orderbook state.
    /// On update, forward the top-<depth> orderbook downstream.
    /// Performs validation of channel, event type and symbol and verifies timestamps are increasing.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        initial_snapshot: Option<Self::OrderbookSnapshot>,
    ) -> anyhow::Result<()> {
        let initial_snapshot = initial_snapshot.unwrap();
        let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

        let expected_channel = format!("diff_order_book_{}", self.symbol);
        // flag for the websocket has received at least one event older than snapshot
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
                        bail!("unexpected channel: {channel}");
                    }
                    if data.microtimestamp <= initial_snapshot.microtimestamp {
                        if !prior_event {
                            prior_event = true;
                        }
                        continue;
                    } else {
                        if !prior_event {
                            bail!(
                                "bitstamp synchronization failed: all buffered messages later than snapshot"
                            );
                        }
                        orderbook.apply_updates(data.asks, data.bids);
                        self.downstream_tx
                            .try_send(OrderbookUpdate {
                                exchange: EXCHANGE,
                                orderbook: orderbook.to_truncated(self.depth),
                            })
                            .context("bitstamp error sending downstream")?;
                        break;
                    }
                }
                "bts:subscription_succeeded" => (),
                other => bail!("unexpected event type: {other}"),
            }
        }
        if !prior_event {
            bail!(
                "websocket closed unexpectedly during synchronization"
            );
        }
        println!("bitstamp ws client synchronized");

        // internal state is now synchronized with the channel, continue processing remaining messages "ad infinitum"
        let mut prev_timestamp = initial_snapshot.microtimestamp;
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
                    if data.microtimestamp < prev_timestamp {
                        bail!("websocket sent an event out of sequence");
                    }
                    prev_timestamp = data.microtimestamp;
                    orderbook.apply_updates(data.asks, data.bids);
                    self.downstream_tx
                        .try_send(OrderbookUpdate {
                            exchange: EXCHANGE,
                            orderbook: orderbook.to_truncated(self.depth),
                        })
                        .context("bitstamp error sending downstream")?;
                }
                "bts:subscription_succeeded" => (),
                other => bail!("unexpected event type: {other}"),
            }
        }
        bail!(WebsocketDisconnectError(EXCHANGE));
    }
}
