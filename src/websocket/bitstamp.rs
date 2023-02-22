//! Types and async functions for connecting to the Bitstamp websocket and maintaining
//! a local orderbook which tracks remote state using diff channel.

use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use futures_util::{SinkExt, Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::connect_async;
use tungstenite::{error::Error, protocol::Message};

use crate::orderbook::Orderbook;
use crate::websocket::utils::deserialize_number_from_string;

const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// Type to deserialize a raw rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
struct OrderbookSnapshot {
    #[serde(deserialize_with = "deserialize_number_from_string")]
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
    #[serde(deserialize_with = "deserialize_number_from_string")]
    microtimestamp: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// Process the websocket events, applying ones with timestamps after the initial snapshot
/// to the internal orderbook state.
/// On update, forward the top-<depth> orderbook downstream.
/// Performs validation of channel, event type and symbol as well as ensuring timestamps are increasing.
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    initial_snapshot: OrderbookSnapshot,
    depth: usize,
    expected_symbol: &str,
    tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

    let mut prev_microtimestamp = initial_snapshot.microtimestamp;
    // once the buffer has been drained it's a serious error if a timestamp is older than last seen
    let mut orderbook_state_initialized = false;
    let expected_channel = format!("diff_order_book_{expected_symbol}");
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
                if !orderbook_state_initialized {
                    // discard events where timestamp is older than last seen
                    if data.microtimestamp <= prev_microtimestamp {
                        continue;
                    }
                    orderbook_state_initialized = true;
                } else if data.microtimestamp < prev_microtimestamp {
                        return Err(anyhow!("exchange event order cannot be relied upon"));
                }
                if channel != expected_channel {
                    let unexpected_channel = channel;
                    return Err(anyhow!("unexpected channel: {unexpected_channel}"));
                }
                prev_microtimestamp = data.microtimestamp;
                orderbook.apply_updates(data.asks, data.bids);
                tx.send(orderbook.truncate(depth)).await?;
            }
            "bts:subscription_succeeded" => (),
            other => return Err(anyhow!("unexpected event type: {other}")),
        }
    }
    Err(anyhow!("unexpected websocket connection close"))
}

/// Long-running websocket client task tracking remote orderbook state locally using a diff/delta
/// event stream. This requires initially buffering event updates whilst we await an initial snapshot
/// from a restful endpoint.
/// After an initial orderbook snapshot has arrived can being processing buffered websocket events.
/// Returns with error on disconnection or invalid state.
/// It's the responsibility of the calling client to attempt reconnection.
/// Forwards top-<depth> [Orderbook]s to the channel provided.
pub async fn run_client(
    depth: usize,
    symbol: &str,
    downstream_tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let connect_addr = WS_BASE_URL;
    let url = url::Url::parse(connect_addr)?;

    let (ws_stream, _response) = connect_async(url).await?;
    let (mut write, read) = ws_stream.split();
    let subscribe_message =
        r#"{"event": "bts:subscribe", "data": {"channel": "diff_order_book_ethbtc"}}"#;
    write
        .send(tungstenite::Message::binary(subscribe_message))
        .await?;

    // wrap the rest request in a timer so we aren't buffering indefinitely
    let initial_snapshot: OrderbookSnapshot = timeout(Duration::from_secs(5), async {
        reqwest::get(format!(
            "https://www.bitstamp.net/api/v2/order_book/{symbol}/"
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
        symbol,
        downstream_tx,
    )
    .await
}
