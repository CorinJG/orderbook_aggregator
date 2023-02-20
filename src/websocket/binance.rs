//! Types and async functions for connecting to the Binance websocket and maintaining 
//! a local orderbook which tracks remote state using diff channel. 

use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::connect_async;
use tungstenite::{error::Error, protocol::Message};

use crate::orderbook::Orderbook;

const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";

/// Type to deserialize the initial Binance rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
struct OrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

/// The possible websocket message types received by our client.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WsMessage {
    // binance includes timestamp in ping frames (every 180s)
    Ping(u64),

    // the orderbook delta message
    DiffDepth {
        // timestamp not used for now
        // #[serde(rename = "E")]
        // timestamp: u64,
        
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
    },
}
use WsMessage::*;

/// Ingest the buffered diff events, applying the ones with updates after the snapshot to the
/// initial snapshot.
/// Validates update IDs ensuring none are missing.
/// Returns an [Orderbook] and the latest update_id which has been processed.
fn ingest_buffer(
    initial_snapshot: OrderbookSnapshot,
    message_buffer: VecDeque<WsMessage>,
) -> anyhow::Result<(Orderbook, u64)> {
    // initialize our internal orderbook from the initial snapshot
    let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

    // drop events where last_update_id older than initial snapshot update_id
    let mut updates = message_buffer
        .into_iter()
        .skip_while(|message| match message {
            DiffDepth { last_update_id, .. } => *last_update_id <= initial_snapshot.last_update_id,
            Ping(_) => true,
        });
    // validate that new first_update_id = previous last_update_id for all events in buffer
    let mut prev_last_update_id = None;
    // the first message in the buffer has an additional validation step
    match updates.next() {
        Some(message) => match message {
            DiffDepth {
                first_update_id,
                last_update_id,
                asks,
                bids,
                ..
            } => {
                if first_update_id > initial_snapshot.last_update_id + 1 {
                    return Err(anyhow::anyhow!(
                        "missing event in buffer, first_update_id > initial_snapshot.last_update_id + 1"));
                }
                prev_last_update_id = Some(last_update_id);
                orderbook.ingest_updates(asks, bids);
            }
            Ping(_) => (),
        },
        None => return Ok((orderbook, initial_snapshot.last_update_id)),
    };
    // process the remaining messages in the buffer
    for message in updates {
        match message {
            DiffDepth {
                first_update_id,
                last_update_id,
                asks,
                bids,
                ..
            } => {
                if first_update_id != prev_last_update_id.unwrap() + 1 {
                    return Err(anyhow::anyhow!("missing event in buffer, gap in sequence"));
                }
                prev_last_update_id = Some(last_update_id);
                orderbook.ingest_updates(asks, bids);
            }
            Ping(_) => (),
        }
    }
    Ok((
        orderbook,
        prev_last_update_id.unwrap_or(initial_snapshot.last_update_id),
    ))
}

/// Add websocket events to a buffer whilst waiting for the initial orderbook snapshot.
/// Discards ping frames. Performs validation of event type and symbol.
async fn buffer_messages(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    message_buffer: &mut VecDeque<WsMessage>,
    expected_symbol: &str,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let message: WsMessage = serde_json::from_slice(&data)?;
        println!("{:?}", message);
        // don't add Ping messages to the buffer
        if let DiffDepth { ref event, ref symbol, .. } = message {
            if event != "depthUpdate" {
                println!("unexpected event field {:?}", event);
                return Err(anyhow::anyhow!("unexpected event field: {event}"));
            }
            if symbol != expected_symbol {
                println!("unexpected symbol field {:?}", symbol);
                return Err(anyhow::anyhow!("unexpected symbol field: {symbol}"));
            }
            message_buffer.push_back(message);
        }
    }
    Ok(())
}

/// For every event arriving in the websocket, update the local orderbook state and then 
/// forward orderbook with truncated to <depth> downstream.
/// This is how websocket messages are to be processed after we already have initialized 
/// from a restful snapshot.
/// Performs validation of event type and symbol as well as ensuring update IDs are in lockstep.
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    tx: mpsc::Sender<Orderbook>,
    mut orderbook: Orderbook,
    mut prev_last_update_id: u64,
    expected_symbol: &str,
    depth: usize,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let message: WsMessage = serde_json::from_slice(&data)?;
        // ignore Ping messages
        if let DiffDepth {
            event,
            symbol,
            first_update_id,
            last_update_id,
            bids,
            asks,
            ..
        } = message
        {
            if event != "depthUpdate" {
                return Err(anyhow::anyhow!("unexpected event field: {event}"));
            }
            if symbol != expected_symbol {
                return Err(anyhow::anyhow!("unexpected symbol field: {symbol}"));
            }
            if last_update_id <= prev_last_update_id {
                // discard message if all its updates are older than last processed
                continue;
            }
            if first_update_id > prev_last_update_id + 1 {
                // todo return an actual error here and re-try
                return Err(anyhow::anyhow!(
                    "unrecoverable state, ws message buffering failed, missing message(s)"
                ));
            }
            prev_last_update_id = last_update_id;
            orderbook.ingest_updates(asks, bids);
            tx.send(orderbook.truncate(depth)).await?;
        }
    }
    Ok(())
}

/// To use the diff delta channel, we need an initial orderbook snapshot (from rest endpoint).
async fn get_initial_snapshot(symbol: &str) -> anyhow::Result<OrderbookSnapshot> {
    Ok(
        reqwest::get(format!("https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"))
            .await?
            .json::<OrderbookSnapshot>()
            .await?,
    )
}

/// Long-running websocket client task tracking remote orderbook state locally using a diff/delta 
/// event stream. This requires initially buffering event updates whilst we await an initial snapshot
/// from a restful endpoint as described in the Binance websocket API [`documentation`]. The buffer is processed once after 
/// a successful rest response with the initial state has arrived.
/// Returns errors on disconnections or state errors. 
/// It's the responsibility of the calling client to attempt reconnection, say in a loop with a delay.
/// Forwards top-<depth> [Orderbook]s using the provided channel sender.
/// 
/// [`documentation`]: http://thatwaseasy.example.com
pub async fn run_client(
    // depth of the forwarded "partial orderbook"
    depth: usize,
    // symbol as the exchange formats it
    symbol: &str,
    // forward orderbook depth updates
    downstream_tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let connect_addr = format!("{WS_BASE_URL}/{symbol}@depth@100ms");
    let url = url::Url::parse(&connect_addr)?;
    
    // uppercase used in websocket json
    let symbol = symbol.to_uppercase();

    let (ws_stream, _response) = connect_async(url).await?;

    // buffer ws messages whilst awaiting initial rest snapshot
    let mut message_buffer: VecDeque<WsMessage> = VecDeque::new();

    // client never sends messages to server (pong frames handled by tungstenite)
    let (_, read) = ws_stream.split();
    tokio::pin!(read);

    let initial_snapshot: OrderbookSnapshot = tokio::select! {
        _ = buffer_messages(read.as_mut(), &mut message_buffer, &symbol) => {
            return Err(anyhow::anyhow!("websocket stream ended unexpectedly"))
        },

        // introduce a small delay to give the buffer a chance to populate
        // wrap the rest request in a timeout so we aren't buffering forever
        r = async { 
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            timeout(Duration::from_secs(5), get_initial_snapshot(&symbol)).await 
        } => r??,
    };

    let (diff_orderbook, prev_last_update_id) = ingest_buffer(initial_snapshot, message_buffer)?;
    // no longer need to buffer, process diffs immediately
    process_events(
        read.as_mut(),
        downstream_tx,
        diff_orderbook,
        prev_last_update_id,
        &symbol,
        depth,
    )
    .await?;

    Ok(())
}
