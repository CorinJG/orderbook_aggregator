// ws docs: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
//
// A connection lasts for 24h then expect to be disconnected
// server will send ping every 3 minutes
// client may send unsolicited Pongs
// if server doesn't receive Pong within 10mins, disconnect client

// wss://stream.binance.com:9443/ws/ethbtc@depth@100ms <-- the diff stream, buffer events
// diff depth stream: {"e":"depthUpdate","E":1676819103587,"s":"BNBBTC","U":2979280130,"u":2979280131,"b":[["0.01280800","2.47500000"]],"a":[["0.01280900","1.13100000"]]}
// https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 <-- get this after 1s or so of buffering diff ws stream events
// Drop any event in the ws msg buffer where u <= lastUpdateId in the snapshot
// the first event in the buffer to process should have U <= lastUpdateId+1 AND u >= lastUpdateId+1
// While listening to the stream, each new event's U should be equal to the previous event's u+1
// Be aware there will be levels outside of the (1,000 or 5,000) initial depth snapshot
// This means you WILL receive updates for prices at levels outside of the initial snapshot
// but you won't receive updates for the prices which DO NOT change, so your local orderbook will be
// different far from spread.
// Receiving an event at a price level that is outside of your local orderbook can happen and is normal
//
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use futures_util::{pin_mut, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

use crate::orderbook::{DiffOrderbook, OrderbookSnapshot};

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

/// Whilst still awaiting an initial restful orderbook snapshot, buffer the messages.
async fn buffer_message(
    message: BinanceWsMessage,
    message_buffer: &Rc<RefCell<VecDeque<BinanceWsMessage>>>,
) {
    match message {
        Ping(_) => (), // tungstenite handles pong frame responses automatically
        DiffDepth { .. } => {
            message_buffer.borrow_mut().push_back(message);
        }
    };
}

/// Once we have obtained an initial orderbook snapshot, can process them and update internal state.
async fn process_message(
    message: BinanceWsMessage,
    tx: &mpsc::Sender<OrderbookSnapshot>
) {
    match message {
        Ping(_) => (), // tungstenite handles pong frame responses automatically
        DiffDepth { .. } => {
            // send downstream
            // tx.send(OrderbookSnapshot{asks: Vec::new(), bids: Vec::new()}).await; //todo
        }
    };
}

/// Once we have received the initial rest snapshot, flush the buffered ws diff
/// messages, processing the ones with pertinent update_ids.
fn flush_buffer(
    orderbook: &Rc<RefCell<BinanceOrderbookSnapshot>>,
    message_buffer: &Rc<RefCell<VecDeque<BinanceWsMessage>>>, 
) {

}

pub async fn run_client() -> anyhow::Result<()> {
    let (downstream_tx, _) = mpsc::channel::<OrderbookSnapshot>(4); // these will be passed in as paramters

    let connect_addr = format!("{WS_BASE_URL}/ethbtc@depth@100ms");
    let url = url::Url::parse(&connect_addr).unwrap();

    let (ws_stream, _response) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    // buffer ws messages whilst awaiting initial rest snapshot
    let message_buffer: Rc<RefCell<VecDeque<BinanceWsMessage>>> =
        Rc::new(RefCell::new(VecDeque::new()));

    // binance ws client never required to send messages to server (apart from pong frame handled by tungstenite)
    let (_, read) = ws_stream.split();

    let orderbook: Rc<RefCell<BinanceOrderbookSnapshot>> = Rc::new(RefCell::new(BinanceOrderbookSnapshot::default()));
    let initial_snapshot_received = Rc::new(Cell::new(false));

    let incoming_ws_messages = {
        read.for_each(|message| async {
            let message = message.unwrap_or_else(|e| panic!("error: {e:?}"));
            let data = message.into_data();
            let v: BinanceWsMessage = serde_json::from_slice(&data).unwrap();
            if initial_snapshot_received.get() {
                process_message(v, &downstream_tx).await;  
            } else {
                buffer_message(v, &message_buffer).await;
            }
        })
    };

    pin_mut!(incoming_ws_messages);
    // select on the incoming_ws_message receiver and the restful initial snapshot request
    tokio::select! {
        _ = &mut incoming_ws_messages => {
            panic!("websocket dropped"); // todo return error
        },
        r = reqwest::get("https://api.binance.com/api/v3/depth?symbol=ETHBTC&limit=1000") => {
            match r {
                Ok(r) => { match r.json::<BinanceOrderbookSnapshot>().await {
                    Ok(v) => {
                        // now we have an initial snapshot and no longer need to buffer ws messsages
                        initial_snapshot_received.set(true);
                        *orderbook.borrow_mut() = v;
                    },
                    Err(e) => {},
                }},
                Err(e) => {},            
            } 
        },
    }
    println!("message_buffer: {:?}", message_buffer);
    flush_buffer(&orderbook, &message_buffer);
    // after this point the ws client will not use the buffer, processing diffs immediately and forwarding downstream
    incoming_ws_messages.await;
    Ok(())
}
