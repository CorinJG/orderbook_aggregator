//! Websocket client implementations to track the state of orderbooks using the exchange's
//! websocket API.
//!
//! Running clients forward orderbook snapshots truncated to a specified depth downstream
//! using a channel provided by users.
//!
//! When available, client implementations will subscribe to an exchange's orderbook
//! diff/delta channel, forwarding snapshots whenever update events are received. Otherwise
//! exchanges will only support a channel which sends the client orderbook snapshots at
//! fixed intervals, so downstream subscribers will receive latest snapshots at fixed
//! intervals.

pub mod binance;
pub mod bitstamp;
