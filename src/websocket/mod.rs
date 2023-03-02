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
use futures_util::{pin_mut, stream::SplitSink, Stream, StreamExt};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::{error::Error, http, protocol::Message};

use crate::config::Exchange;

// The two websocket client errors which warrant reconnection attempt.
#[derive(Debug, Error)]
enum WebsocketClientError {
    #[error("{0:?} websocket client disconnected unexpectedly")]
    DisconnectError(Exchange),
    #[error("{0:?} synchronization error")]
    SynchronizationError(Exchange),
}

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
    /// The deserialization target type for the rest snapshot;
    type RawOrderbookSnapshot: DeserializeOwned + Send;

    /// Connect to the websocket.
    async fn connect(&self) -> WebsocketConnectionResult;

    /// Subscribe to websocket channel(s) by sending subscription messages (if any).
    async fn subscribe(&self, write: Pin<&mut WsWriter>) -> anyhow::Result<()>;

    /// Sleep for a short time without reading the websocket to allow the messages to buffer.
    async fn buffer_messages(&self);

    /// Get an orderbook snapshot via a rest request, if required for synchronizing with a ws channel.
    async fn request_snapshot(&self) -> anyhow::Result<Option<Self::RawOrderbookSnapshot>>;

    /// For websocket clients which must reconcile an initial rest snapshot with buffered websocket messages.
    async fn synchronize(
        &self,
        read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        initial_snapshot: Option<Self::RawOrderbookSnapshot>,
    ) -> anyhow::Result<()>;

    /// Long-running task to process all messages arriving on the websocket, including those buffered
    /// before obtaining an initial snapshot to sychronize with.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
    ) -> anyhow::Result<()>;

    /// Manage the websocket connection. If the synchronization fails or the client becomes disconnected,
    /// attempt reconnection after a short delay.
    async fn manage_connection(&self) -> anyhow::Result<()> {
        loop {
            let (ws_stream, _response) = self.connect().await?;
            let (write, read) = ws_stream.split();
            pin_mut!(write);

            self.subscribe(write.as_mut()).await?;
            self.buffer_messages().await;

            // wrap the rest request in a timer so we aren't buffering indefinitely
            let initial_snapshot: Option<Self::RawOrderbookSnapshot> =
                timeout(Duration::from_millis(3_000), self.request_snapshot()).await??;

            tokio::pin!(read);
            if let Err(e) = self.synchronize(read.as_mut(), initial_snapshot).await {
                eprintln!("{e}");
                if e.downcast_ref::<WebsocketClientError>().is_some() {
                    // attempt reconnection after a short delay
                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                    continue;
                } else {
                    bail!(e)
                }
            }

            match self.process_messages(read.as_mut()).await {
                Ok(_) => unreachable!("client process_messages loop should error on completion"),
                Err(e) => {
                    eprintln!("{e}");
                    if e.downcast_ref::<WebsocketClientError>().is_some() {
                        // attempt reconnection after a short delay
                        tokio::time::sleep(Duration::from_millis(1_000)).await;
                        continue;
                    } else {
                        bail!(e)
                    }
                }
            }
        }
    }
}
