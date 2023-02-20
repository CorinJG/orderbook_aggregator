use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const WS_BASE_URL: &str = "wss://stream.binance.com:443/ws";

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceWsMessage {
    // binance includes timestamp in ping frames
    Ping(u64),
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
        bids: Vec<(String, String)>,
        #[serde(rename = "a")]
        asks: Vec<(String, String)>,
    },
}

#[tokio::main]
async fn main() {
    let connect_addr = format!("{WS_BASE_URL}/bnbbtc@depth@100ms");
    let url = url::Url::parse(&connect_addr).unwrap();

    let (to_ws_tx, to_ws_rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(message_producer(to_ws_tx));

    let (ws_stream, response) = connect_async(url).await.expect("Failed to connect");
    println!("{response:?}");
    println!("WebSocket handshake has been successfully completed");

    let (write, read) = ws_stream.split();

    let send_to_ws = ReceiverStream::new(to_ws_rx).map(Ok).forward(write);
    let incoming_ws_messages = {
        read.for_each(|message| async {
            let data = message.unwrap().into_data();
            // let text = std::str::from_utf8(&data).unwrap();
            // println!("{text:?}");
            let v: BinanceWsMessage = serde_json::from_slice(&data).unwrap();
            use BinanceWsMessage::*;
            match v {
                Ping(_) => {
                    // tungstenite handles pong frame responses to ping frames automatically
                }
                DiffDepth {
                    event,
                    timestamp,
                    symbol,
                    first_update_id,
                    last_update_id,
                    bids,
                    asks,
                } => {}
            };
        })
    };

    tokio::select! {
        _ = send_to_ws => {},
        _ = incoming_ws_messages => {},
    }
}

/// Task to produce the messages to be sent to the websocket server.
async fn message_producer(tx: tokio::sync::mpsc::Sender<Message>) {
    loop {
        // just an example which periodically sends some message to the server
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(Message::binary("<json-string>")).await.unwrap(); //todo handle
    }
}
