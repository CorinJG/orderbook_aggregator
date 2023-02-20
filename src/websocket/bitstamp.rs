//! Types and async functions for connecting to the Bitstamp websocket and maintaining 
//! a local orderbook which tracks remote state using a diff channel. 

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

const WS_BASE_URL: &str = "wss://ws.bitstamp.net";

/// Type to deserialize a raw rest orderbook snapshot into.
#[derive(Debug, Deserialize)]
struct OrderbookSnapshot {
    microtimestamp: u64,
    asks: Vec<(Decimal, Decimal)>,
    bids: Vec<(Decimal, Decimal)>,
}

/// The possible websocket message types received by our client.
// {"event": "bts:subscription_succeeded", "channel": "diff_order_book_ethbtc", "data": {}}
// A diff message (timestamp omitted):
// {"data":{"microtimestamp":"1677003825236814","bids":[["0.06805441","0.00000000"]],"asks":[["0.06810436","0.00000000"]]},"channel":"diff_order_book_ethbtc","event":"data"}
#[derive(Debug, Deserialize)]
struct WsMessage {
    event: String,
    channel: String,
    data: Data,
}

/// The inner data payload of a websocket message.
#[derive(Debug, Deserialize)]
struct Data {
    microtimestamp: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// Ingest the buffered diff events, applying the ones with microtimestamps after the snapshot to the
/// initial snapshot.
/// Validates timestamps, ensuring they are increasing.
/// Returns an [Orderbook] and the latest microtimestamp which has been processed.
fn ingest_buffer(
    initial_snapshot: OrderbookSnapshot,
    message_buffer: VecDeque<WsMessage>,
) -> anyhow::Result<(Orderbook, u64)> {
    // initialize our internal orderbook from the initial snapshot
    let mut orderbook = Orderbook::from_asks_bids(initial_snapshot.asks, initial_snapshot.bids);

    // drop events where microtimestamp <= initial snapshot microtimestamp
    let mut updates = message_buffer
        .into_iter()
        .skip_while(|message| message.data.microtimestamp <= initial_snapshot.microtimestamp);
    // validate that new microtimestamp > previous microtimestamp for all events in buffer
    let mut prev_timestamp = initial_snapshot.microtimestamp;
    for message in updates {
        if message.data.microtimestamp < prev_timestamp { 
            // would be very surprising over tls
            return Err(anyhow::anyhow!("out of sequence timestamp"));
        }
        prev_timestamp = message.data.microtimestamp;
        orderbook.ingest_updates(message.data.asks, message.data.bids);
    }
    Ok((
        orderbook,
        prev_timestamp,
    ))
}

/// Add events to a buffer whilst waiting for the initial orderbook snapshot.
async fn buffer_messages(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    message_buffer: &mut VecDeque<WsMessage>,
    expected_symbol: &str,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let message: WsMessage = serde_json::from_slice(&data)?;
        println!("{:?}", message);
        // todo only add the messages you need here (not pings or ws subscription confirmations etc)
        message_buffer.push_back(message);
    }
    Ok(())
}

/// For every event message arriving in the websocket, update the local orderbook state
/// and then forward orderbook with truncated to <depth> downstream.
/// This is how websocket messages are to be processed after we already have initialized 
/// from a restful snapshot.
/// todo validation
async fn process_events(
    mut read: Pin<&mut impl Stream<Item = Result<Message, Error>>>,
    tx: mpsc::Sender<Orderbook>,
    mut orderbook: Orderbook,
    mut prev_microtimestamp: u64,
    expected_symbol: &str,
    depth: usize,
) -> anyhow::Result<()> {
    while let Some(message) = read.next().await {
        let data = message?.into_data();
        let message: WsMessage = serde_json::from_slice(&data)?;
        orderbook.ingest_updates(message.data.asks, message.data.bids);
        tx.send(orderbook.truncate(depth)).await?;
    }
    Ok(())
}

/// When using the diff delta channel, we need an initial orderbook snapshot (from rest endpoint).
async fn get_initial_snapshot(symbol: &str) -> anyhow::Result<OrderbookSnapshot> {
    Ok(
        reqwest::get(format!("https://www.bitstamp.net/api/v2/order_book/{symbol}/"))
            .await?
            .json::<OrderbookSnapshot>()
            .await?,
    )
}

/// Long-running websocket client task tracking remote orderbook state locally using a diff/delta 
/// event stream. This requires initially buffering event updates whilst we await an initial snapshot
/// from a restful endpoint, and then processing the buffer once after a successful rest response with 
/// the initial state has arrived.
/// Returns errors on disconnections or state errors. 
/// It's the responsibility of the calling client to attempt reconnection, say in a loop with a delay.
/// Forwards top-<depth> [Orderbook]s using the provided channel sender.
pub async fn run_client(

// To subscribe to bitstamp diff channel {"event": "bts:subscribe", "data": {"channel": "diff_order_book_ethbtc"}}
    
    // depth of the forwarded "partial orderbook"
    depth: usize,
    // symbol as the exchange formats it
    symbol: &str,
    // forward orderbook depth updates
    downstream_tx: mpsc::Sender<Orderbook>,
) -> anyhow::Result<()> {
    let connect_addr = WS_BASE_URL;
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
