//! Types and [OrderbookWebsocketClient] trait implementation connecting to the Bitstamp websocket
//! "order book" channel, which is simply a top 100 snapshot stream.
//! I believe the "live full order book" channel has some issues and can't be relied upon to
//! push all order book delta messages.

use std::pin::Pin;

use anyhow::{bail, Context};
use async_trait::async_trait;
use futures_util::{SinkExt, Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tungstenite::{error::Error, protocol::Message};

use crate::config::{CurrencyPair, Exchange};
use crate::messages::OrderbookSnapshot;
use crate::messages::OrderbookUpdateMessage::{self, *};
use crate::utils::deserialize_using_parse;

use super::{
    OrderbookWebsocketClient, WebsocketClientError::*, WebsocketConnectionResult, WsWriter,
};

const EXCHANGE: Exchange = Exchange::Bitstamp;
const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// The format of an "order book" websocket message received by our client.
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

/// Construct a websocket order book stream subscription message for the given symbol.
fn construct_subscription_message(symbol: &str) -> String {
    let front = r#"{"event": "bts:subscribe", "data": {"channel": "order_book_"#;
    let back = r#""}}"#;
    format!("{front}{symbol}{back}")
}

pub struct BitstampOrderbookWebsocketClient {
    symbol: String,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
}

impl BitstampOrderbookWebsocketClient {
    pub fn new(
        currency_pair: CurrencyPair,
        downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    ) -> Self {
        Self {
            symbol: [currency_pair.base(), currency_pair.quote()].join(""),
            downstream_tx,
        }
    }
}

#[async_trait]
impl OrderbookWebsocketClient for BitstampOrderbookWebsocketClient {
    type RawOrderbookSnapshot = ();

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
        // no need to buffer using the order book channel
    }

    // we don't use a rest request since we use the top-100 channel and not the delta channel
    async fn request_snapshot(&self) -> anyhow::Result<Option<()>> {
        Ok(None)
    }

    /// Retrieve websocket messages until we can synchronize with the snapshot.
    /// Then forward the snapshot and the first pertinent delta event downstream and return.
    async fn synchronize(
        &self,
        _read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        _initial_snapshot: Option<Self::RawOrderbookSnapshot>,
    ) -> anyhow::Result<()> {
        return Ok(());
    }

    /// Process the websocket events, validating them and forwarding downstream.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> anyhow::Result<()> {
        println!("bitstamp ws client connected");
        let expected_channel = format!("order_book_{}", self.symbol);
        // check that bitstamp are sending monotonic timestamps
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
                        .try_send(Snapshot {
                            exchange: EXCHANGE,
                            orderbook: OrderbookSnapshot {
                                asks: data.asks,
                                bids: data.bids,
                            },
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
