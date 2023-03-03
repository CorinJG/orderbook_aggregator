//! This module contains websocket client implementations to convert snapshots and/or deltas
//! into an internal standard message protocol.
//!
//! Running clients forward [crate::messages::OrderbookUpdateMessage]s downstream using a channel provided.
//!
//! When available, client implementations should prefer to subscribe to an orderbook's
//! diff/delta channel, forwarding deltas whenever updates are received. Otherwise
//! if an exchange only supports a channel which sends the client orderbook snapshots at
//! fixed intervals, downstream subscribers will receive latest snapshots at fixed
//! intervals.

pub mod binance;
pub mod bitstamp;

use std::pin::Pin;
use std::time::Duration;

use anyhow::bail;
use async_trait::async_trait;
use futures_util::{stream::SplitSink, Stream, StreamExt};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::{error::Error, http, protocol::Message};

use crate::{config::Exchange, utils::Millis};

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

/// Trait for orderbook websocket clients.
#[async_trait]
pub trait OrderbookWebsocketClient {
    /// Connect to the websocket.
    async fn connect(&self) -> WebsocketConnectionResult;

    /// Subscribe to websocket channel(s) by sending subscription messages (if any).
    async fn subscribe(&self, write: Pin<&mut WsWriter>) -> anyhow::Result<()>;

    /// For websocket clients which must reconcile an initial snapshot with buffered websocket messages to
    /// attach to a delta stream.
    /// Any pertinent websocket events taken from the read buffer during synchronization should be processed
    /// (e.g. sent downstream).
    async fn synchronize(
        &self,
        read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError>;

    /// Long-running task to process all messages arriving on the websocket, including those buffered
    /// before obtaining an initial snapshot to sychronize with.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> Result<(), WebsocketClientError>;

    /// Manage the websocket connection. If the synchronization fails or the client becomes disconnected,
    /// attempt reconnection after a short delay.
    /// Periodically re-synchronize, depending on when the exchange client implementation chooses to return
    /// processing messages early with SynchronizationExpired.
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
                        Disconnect(exchange) | Synchronization(exchange) => {
                            tokio::time::sleep(Duration::from_millis(RECONNECT_DELAY)).await;
                            println!("{exchange:?} attempting reconnection");
                            continue 'connect;
                        }
                        other => bail!(other),
                    }
                }

                match self.process_messages(read.as_mut()).await {
                    Ok(_) => unreachable!("client process_messages should error on completion"),
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
