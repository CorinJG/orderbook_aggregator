//! Types and async functions for connecting to the Binance websocket and maintaining
//! a local orderbook which tracks remote state using diff channel.
//!
//! Binance provides a public "diff depth" websocket channel which streams update
//! events with associated first- and last- update IDs. This enables our client
//! to establish initial synchrony with the stream by making a separate (rest) request
//! for an orderbook snapshot and then applying event updates which occur after this
//! snapshot, discarding buffered events with a last_update_id from before that of the
//! snapshot. In particular, in order to attach to the stream we must receive a
//! websocket event with first_update_id <= snapshot.update_id and
//! last_update_id >= snapshot.update_id + 1.

use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::connect_async;
use tungstenite::{error::Error, protocol::Message};

use crate::{
    aggregator::OrderbookUpdateMessage::{self, *},
    config::{CurrencyPair, Exchange},
    orderbook::Orderbook,
};

const EXCHANGE: Exchange = Exchange::Binance;
const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";

/// Type to deserialize the initial Binance rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
struct OrderbookSnapshot {
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

/// Process the websocket events, applying updates which have an ID after the initial
/// snapshot ID to the local orderbook state.
/// On update, forward the latest top-<depth> orderbook downstream.
/// Performs validation of event type and symbol as well as ensuring no gaps in update IDs.
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    initial_snapshot: OrderbookSnapshot,
    depth: usize,
    expected_symbol: &str,
    tx: mpsc::Sender<OrderbookUpdateMessage>,
) -> anyhow::Result<()> {
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
            return Err(anyhow!("unexpected event field: {event}"));
        }
        if symbol != expected_symbol {
            return Err(anyhow!("unexpected symbol field: {symbol}"));
        }
        // discard events where last_update_id is older than rest response update_id
        if last_update_id <= prev_last_update_id {
            continue;
        }
        if !synchronized {
            if first_update_id > prev_last_update_id + 1 {
                return Err(anyhow!("missing event"));
            }
            synchronized = true;
            println!("binance ws client synchronized");
        } else if first_update_id != prev_last_update_id + 1 {
            return Err(anyhow!("missing event, gap in sequence"));
        }
        prev_last_update_id = last_update_id;
        orderbook.apply_updates(asks, bids);
        tx.try_send(OrderbookUpdate {
            exchange: EXCHANGE,
            orderbook: orderbook.to_truncated(depth),
        })
        .context("binance error sending downstream")?;
    }
    Err(anyhow!("unexpected websocket connection close"))
}

/// Long-running websocket client tracking remote orderbook state locally using a diff/delta
/// event stream. This requires initially buffering event updates whilst we await an initial snapshot
/// from a restful endpoint.
/// After an initial orderbook snapshot has arrived can being processing buffered websocket events.
/// Returns with error on disconnection or invalid state.
/// It's the responsibility of the calling client to attempt reconnection.
/// Forwards top-<depth> [Orderbook]s to the channel provided
pub async fn run_client(
    depth: usize,
    symbol: CurrencyPair,
    downstream_tx: mpsc::Sender<OrderbookUpdateMessage>,
    ws_buffer_time: u64,
) -> anyhow::Result<()> {
    let symbol_lower = [symbol.base(), symbol.quote()].join("");
    let symbol_upper = [
        symbol.base().to_uppercase().as_str(),
        symbol.quote().to_uppercase().as_str(),
    ]
    .join("");

    let connect_addr = format!("{WS_BASE_URL}/{symbol_lower}@depth@100ms");
    let url = url::Url::parse(&connect_addr)?;

    let (ws_stream, _response) = connect_async(url).await?;
    let (_, read) = ws_stream.split();

    // give the websocket a chance to buffer
    tokio::time::sleep(Duration::from_millis(ws_buffer_time)).await;

    // wrap the rest request in a timer so we aren't buffering indefinitely
    let initial_snapshot: OrderbookSnapshot = timeout(Duration::from_secs(5), async {
        reqwest::get(format!(
            "https://api.binance.com/api/v3/depth?symbol={symbol_upper}&limit=1000"
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
        &symbol_upper,
        downstream_tx,
    )
    .await
}
