//! Types and async functions for connecting to the Bitstamp websocket and maintaining
//! a local orderbook which tracks remote state using diff channel.
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

use anyhow::anyhow;
use futures_util::{SinkExt, Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::connect_async;
use tungstenite::{error::Error, protocol::Message};

use crate::config::CurrencyPair;
use crate::orderbook::Orderbook;
use crate::utils::deserialize_using_parse;

const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// Type to deserialize a raw rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
struct OrderbookSnapshot {
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
    data: Data,
}

/// The inner data payload of a websocket message.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Data {
    #[serde(deserialize_with = "deserialize_using_parse")]
    microtimestamp: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// Process the websocket events, applying updates with timestamps after the initial snapshot
/// to the local orderbook state.
/// On update, forward the top-<depth> orderbook downstream.
/// Performs validation of channel, event type and symbol and verifies timestamps are increasing.
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    initial_snapshot: OrderbookSnapshot,
    depth: usize,
    expected_symbol: &str,
    tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

    let expected_channel = format!("diff_order_book_{expected_symbol}");
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
                    return Err(anyhow!("unexpected channel: {channel}"));
                }
                if data.microtimestamp <= initial_snapshot.microtimestamp {
                    if !prior_event {
                        prior_event = true;
                    }
                    continue;
                } else {
                    if !prior_event {
                        return Err(anyhow!(
                            "buffered websocket messages all later than initial snapshot"
                        ));
                    }
                    orderbook.apply_updates(data.asks, data.bids);
                    tx.try_send(orderbook.to_truncated(depth))?;
                    break;
                }
            }
            "bts:subscription_succeeded" => (),
            other => return Err(anyhow!("unexpected event type: {other}")),
        }
    }
    if !prior_event {
        return Err(anyhow!(
            "websocket closed unexpectedly during synchronization"
        ));
    }

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
                    return Err(anyhow!("unexpected channel: {channel}"));
                }
                if data.microtimestamp < prev_timestamp {
                    return Err(anyhow!("websocket sent an event out of sequence"));
                }
                prev_timestamp = data.microtimestamp;
                orderbook.apply_updates(data.asks, data.bids);
                tx.send(orderbook.to_truncated(depth)).await?;
            }
            "bts:subscription_succeeded" => (),
            other => return Err(anyhow!("unexpected event type: {other}")),
        }
    }
    Err(anyhow!("unexpected websocket connection close"))
}

/// Construct a websocket diff stream subscription message for the given symbol.
fn construct_subscription_message(symbol: &str) -> String {
    let front = r#"{"event": "bts:subscribe", "data": {"channel": "diff_order_book_"#;
    let back = r#""}}"#;
    format!("{front}{symbol}{back}")
}

/// Long-running websocket client tracking remote orderbook state locally using a diff/delta
/// event stream. This requires initially buffering event updates whilst we await an initial snapshot
/// from a restful endpoint.
/// After an initial orderbook snapshot has arrived can being processing buffered websocket events.
/// Returns with error on disconnection or invalid state.
/// It's the responsibility of the calling client to attempt reconnection.
/// Forwards top-<depth> [Orderbook]s to the channel provided.
pub async fn run_client(
    depth: usize,
    symbol: CurrencyPair,
    downstream_tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let connect_addr = WS_BASE_URL;
    let url = url::Url::parse(connect_addr)?;

    let (ws_stream, _response) = connect_async(url).await?;
    let (mut write, read) = ws_stream.split();

    let symbol_lower = [symbol.base(), symbol.quote()].join("");
    let subscribe_message = construct_subscription_message(&symbol_lower);
    write
        .send(tungstenite::Message::binary(subscribe_message))
        .await?;

    // give the websocket a chance to buffer
    tokio::time::sleep(Duration::from_secs(3)).await;

    // wrap the rest request in a timer so we aren't buffering indefinitely
    let initial_snapshot: OrderbookSnapshot = timeout(Duration::from_secs(5), async {
        reqwest::get(format!(
            "https://www.bitstamp.net/api/v2/order_book/{symbol_lower}/"
        ))
        .await?
        .json()
        .await
    })
    .await??;

    tokio::pin!(read);
    process_events(
        read.as_mut(),
        initial_snapshot,
        depth,
        &symbol_lower,
        downstream_tx,
    )
    .await
}
