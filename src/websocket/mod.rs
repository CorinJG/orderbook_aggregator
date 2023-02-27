//! Websocket client implementations to track the state of orderbooks using the exchange's
//! websocket API.
//!
//! Running clients forward orderbook snapshots truncated to a specified depth downstream
//! using a channel provided by users.
//!
//! When available, client implementations should prefer to subscribe to an orderbook's
//! diff/delta channel, forwarding snapshots whenever update events are received. Otherwise
//! if an exchange only supports a channel which sends the client orderbook snapshots at
//! fixed intervals, downstream subscribers will receive latest snapshots at fixed
//! intervals.

pub mod binance;
pub mod bitstamp;

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{pin_mut, stream::SplitSink, Stream, StreamExt};
use serde::de::DeserializeOwned;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::{error::Error, http, protocol::Message};

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
    /// The deserialization target for the rest snapshot;
    type OrderbookSnapshot: DeserializeOwned + Send;

    /// Connect to the websocket.
    async fn connect(&self) -> WebsocketConnectionResult;

    /// Subscribe to websocket channel(s) by sending subscription messages (if any).
    async fn subscribe(&self, write: Pin<&mut WsWriter>) -> anyhow::Result<()>;

    /// Sleep for a short time without reading the websocket to allow the messages to buffer.
    async fn buffer_messages(&self);

    /// Get the initial orderbook snapshot via a rest request. If the initial snapshot is sent as a first
    /// message in the websocket channel, this is a no-op.
    async fn request_snapshot(&self) -> anyhow::Result<Option<Self::OrderbookSnapshot>>;

    /// Long-running task to process all messages arriving on the websocket, including those buffered
    /// before obtaining an initial snapshot to sychronize with.
    async fn process_messages(
        &self,
        mut read: Pin<&mut (impl Stream<Item = Result<Message, Error>> + Send)>,
        initial_snapshot: Option<Self::OrderbookSnapshot>,
    ) -> anyhow::Result<()>;

    /// Manage the websocket connection. If the client becomes disconnected from the websocket, attempt
    /// reconnection after 1s delay.
    async fn manage_connection(&self) -> anyhow::Result<()> {
        // todo handle the disconnect error and do a loop for this async fn
        let (ws_stream, _response) = self.connect().await?;
        let (write, read) = ws_stream.split();
        pin_mut!(write);

        self.subscribe(write.as_mut()).await?;
        self.buffer_messages().await;

        // wrap the rest request in a timer so we aren't buffering indefinitely
        let initial_snapshot: Option<Self::OrderbookSnapshot> =
            timeout(Duration::from_millis(3_000), self.request_snapshot()).await??;

        tokio::pin!(read);
        self.process_messages(read.as_mut(), initial_snapshot).await
    }
}
