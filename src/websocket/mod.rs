//! This module contains the [WebsocketClient] trait and client implementations for various channels
//! (delta / snapshot).
//!
//! Some of the trait's methods are conceived with a delta client in mind, so they would be no-ops
//! for a snapshot client.
//!
//! ## Delta channels
//! When feasible, client implementations may prefer a delta channel to a snapshot channel for
//! order books to reduce network and processing load and ultimately latency.
//!
//! Note that when using a delta stream, synchronization may be necessary. This typically involves
//! buffering messages for a short time and then requesting a snapshot (either on the websocket or
//! rest). Then, conditional on having adequate delta messages buffered, an internal state for the
//! order book can be initialized.
//!
//! Immediately following successful synchronization, the remaining messages in the buffer should
//! be applied to the internal order book state. But it may be appropriate to delay sending the
//! first update downstream for a brief time, as any message which wasn't processed immediately
//! whilst awaiting the initial snapshot has become "stale".

pub mod binance;
pub mod bitstamp;

use std::{collections::BTreeMap, pin::Pin, time::Duration};

use anyhow::bail;
use async_trait::async_trait;
use futures_util::{stream::SplitSink, Stream, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::{error::Error, http, protocol::Message};

use crate::{
    config::Exchange,
    messages::{
        OrderbookSnapshot,
        OrderbookUpdateMessage::{self, *},
    },
    utils::Millis,
};

// how long to wait before attempting reconnection
const RECONNECT_DELAY: Millis = 1_000;

#[derive(Debug, Error)]
pub enum WebsocketClientError {
    #[error("{0:?} websocket client disconnected unexpectedly")]
    Disconnect(Exchange),
    #[error("{0:?} synchronziation failed")]
    Synchronization(Exchange),
    #[error("{0:?} synchronization expired")]
    SynchronizationExpired(Exchange),
    #[error("{0:?} assumed invariant of the websocket API has been violated: {1}")]
    InvariantViolation(Exchange, String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
use WebsocketClientError::*;

type WebsocketConnectionResult = Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        http::Response<Option<Vec<u8>>>,
    ),
    tungstenite::Error,
>;

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

/// Trait for websocket clients. Agnostic to any downstream message forwarding.
#[async_trait]
pub trait WebsocketClient {
    /// Connect to the websocket.
    async fn connect(&self) -> WebsocketConnectionResult;

    /// Subscribe to websocket channels by sending subscription messages (if any).
    async fn subscribe(&self, write: Pin<&mut WsWriter>) -> anyhow::Result<()>;

    /// For clients which reconcile an initial snapshot with buffered messages to attach to a delta stream.
    async fn synchronize(
        &self,
        read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError>;

    /// Long-running task following any necessary synchronization to process all messages arriving on the websocket.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError>;

    /// Manage the connection. Attempt reconnection after a short delay.
    /// Periodic re-synchronize is available, if client returns process_messages with SynchronizationExpired.
    async fn manage_connection(&self) -> anyhow::Result<()> {
        'connect: loop {
            let (ws_stream, _response) = self.connect().await?;
            let (write, read) = ws_stream.split();
            tokio::pin!(write, read);

            self.subscribe(write.as_mut()).await?;

            'synchronize: loop {
                if let Err(e) = self.synchronize(read.as_mut()).await {
                    eprintln!("{e}");
                    match e {
                        Disconnect(exchange) => {
                            tokio::time::sleep(Duration::from_millis(RECONNECT_DELAY)).await;
                            println!("{exchange:?} attempting reconnection");
                            continue 'connect;
                        }
                        Synchronization(exchange) => {
                            println!("{exchange:?} attempting synchronization");
                            continue 'synchronize;
                        }
                        other => bail!(other),
                    }
                }

                match self.process_messages(read.as_mut()).await {
                    Ok(_) => unreachable!("process_messages should error on completion"),
                    Err(e) => {
                        eprintln!("{e}");
                        match e {
                            Disconnect(exchange) => {
                                tokio::time::sleep(Duration::from_millis(RECONNECT_DELAY)).await;
                                println!("{exchange:?} attempting reconnection");
                                continue 'connect;
                            }
                            SynchronizationExpired(_) => {
                                continue 'synchronize;
                            }
                            other => bail!(other),
                        }
                    }
                }
            }
        }
    }
}

/// A type for efficiently updating an order book using delta events.
/// Maps price to quantity, maintaining price order.
#[derive(Debug, Default)]
struct BTreeBook {
    asks: BTreeMap<Decimal, Decimal>,
    bids: BTreeMap<Decimal, Decimal>,
}

impl BTreeBook {
    pub(crate) fn apply_deltas(
        &mut self,
        asks: Vec<(Decimal, Decimal)>,
        bids: Vec<(Decimal, Decimal)>,
    ) {
        for (p, q) in asks {
            if q == dec!(0) {
                self.asks.remove(&p);
            } else {
                self.asks.insert(p, q);
            }
        }
        for (p, q) in bids {
            if q == dec!(0) {
                self.bids.remove(&p);
            } else {
                self.bids.insert(p, q);
            }
        }
    }

    /// Construct a [OrderbookUpdateMessage::DepthSnapshot] from the state.
    pub(crate) fn to_depth_snapshot(
        &self,
        exchange: Exchange,
        depth: usize,
    ) -> OrderbookUpdateMessage {
        DepthSnapshot {
            exchange,
            orderbook: OrderbookSnapshot {
                // top asks in ascending order
                asks: self
                    .asks
                    .iter()
                    .take(depth)
                    .map(|(p, q)| (*p, *q))
                    .collect(),
                // top bids in descending order
                bids: self
                    .bids
                    .iter()
                    .rev()
                    .take(depth)
                    .map(|(p, q)| (*p, *q))
                    .collect(),
            },
        }
    }
}
