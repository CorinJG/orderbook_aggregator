use std::collections::VecDeque;
use std::pin::Pin;

use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::{error::Error, protocol::Message};

use crate::orderbook::Orderbook;

const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";

/// To track the state of the orderbook using an initial rest snapshot
/// and websocket diff messages.
#[derive(Debug, Default, Deserialize)]
struct BinanceOrderbookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceWsMessage {
    // binance includes timestamp in ping frames (every 180s)
    Ping(u64),
    // the orderbook delta message
    DiffDepth {
        #[serde(rename = "e")]
        event: String,
        #[serde(rename = "E")]
        timestamp: u64,
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
use BinanceWsMessage::*;

/// Once we have received the initial rest snapshot, ingest the buffered diff events,
/// processing the ones with pertinent update_ids (updates after the initial snapshot).
/// Returns an Orderbook and the latest update_id to reconcile with subsequently processed
/// messages.
fn ingest_buffer(
    orderbook: BinanceOrderbookSnapshot,
    message_buffer: VecDeque<BinanceWsMessage>,
) -> anyhow::Result<(Orderbook, u64)> {
    // initialize our internal orderbook from the initial snapshot
    let mut diff_orderbook = Orderbook::from_asks_bids(orderbook.asks, orderbook.bids);

    let mut updates = message_buffer.into_iter().skip_while(|message| {
        // drop events where last_update_id older than initial snapshot update_id
        match message {
            DiffDepth { last_update_id, .. } => *last_update_id <= orderbook.last_update_id,
            Ping(_) => true,
        }
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
                if first_update_id > orderbook.last_update_id + 1 {
                    return Err(anyhow::anyhow!(
                        "missing event in buffer - first_update_id > initial_snapshot.last_update_id+1"));
                }
                prev_last_update_id = Some(last_update_id);
                diff_orderbook.ingest_updates(asks, bids);
            }
            Ping(_) => (),
        },
        None => return Ok((diff_orderbook, orderbook.last_update_id)),
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
                    return Err(anyhow::anyhow!("missing event in buffer: gap in sequence"));
                }
                prev_last_update_id = Some(last_update_id);
                diff_orderbook.ingest_updates(asks, bids);
            }
            Ping(_) => (),
        }
    }
    Ok((
        diff_orderbook,
        prev_last_update_id.unwrap_or(orderbook.last_update_id),
    ))
}

async fn buffer_messages(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    message_buffer: &mut VecDeque<BinanceWsMessage>,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let binance_message: BinanceWsMessage = serde_json::from_slice(&data)?;
        // don't add Ping messages to the buffer
        if let DiffDepth { .. } = binance_message {
            message_buffer.push_back(binance_message);
        }
    }
    Ok(())
}

/// Once the initial snapshot has been received, process events immediately and forward
/// downstream.
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    tx: mpsc::Sender<Orderbook>,
    mut diff_orderbook: Orderbook,
    mut prev_last_update_id: u64,
    depth: usize,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let binance_message: BinanceWsMessage = serde_json::from_slice(&data)?;
        // ignore Ping messages
        if let DiffDepth {
            first_update_id,
            last_update_id,
            bids,
            asks,
            ..
        } = binance_message
        {
            if last_update_id <= prev_last_update_id {
                // discard message if all updates are older than last processed
                continue
            }
            if first_update_id > prev_last_update_id + 1 {
                // todo return an actual error here and re-try
                return Err(anyhow::anyhow!(
                    "unrecoverable state, ws message buffering failed, missing message(s)"
                ));
            }
            prev_last_update_id = last_update_id;
            diff_orderbook.ingest_updates(asks, bids);
            tx.send(diff_orderbook.truncate(depth)).await?;
        }
    }
    Ok(())
}

/// When using the diff delta channel, we need an initial orderbook snapshot (from rest endpoint).
async fn get_initial_snapshot() -> anyhow::Result<BinanceOrderbookSnapshot> {
    Ok(
        reqwest::get("https://api.binance.com/api/v3/depth?symbol=ETHBTC&limit=1000")
            .await?
            .json::<BinanceOrderbookSnapshot>()
            .await?,
    )
}

/// Long-running websocket client task.
pub async fn run_client(
    depth: usize,
    downstream_tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let connect_addr = format!("{WS_BASE_URL}/ethbtc@depth@100ms");
    let url = url::Url::parse(&connect_addr)?;

    let (ws_stream, _response) = connect_async(url).await?;

    // buffer ws messages whilst awaiting initial rest snapshot
    let mut message_buffer: VecDeque<BinanceWsMessage> = VecDeque::new();

    // client never sends messages to server (pong frames handled by tungstenite)
    let (_, read) = ws_stream.split();
    tokio::pin!(read);

    let mut orderbook: BinanceOrderbookSnapshot = tokio::select! {
        _ = buffer_messages(read.as_mut(), &mut message_buffer) => {
            return Err(anyhow::anyhow!("websocket stream ended unexpectedly"))
        },
        r = get_initial_snapshot() => r?,
    };

    let (diff_orderbook, prev_last_update_id) = ingest_buffer(orderbook, message_buffer)?;
    // after this the client will not buffer, processing diffs immediately
    process_events(
        read.as_mut(),
        downstream_tx,
        diff_orderbook,
        prev_last_update_id,
        depth,
    )
    .await?;

    Ok(())
}
