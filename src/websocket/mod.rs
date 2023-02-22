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

pub(crate) mod utils {
    //! Utility functions required by websocket implementations.

    use serde::{Deserialize, Deserializer};
    use std::fmt::Display;
    use std::str::FromStr;

    pub(crate) fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: FromStr + serde::Deserialize<'de>,
        <T as FromStr>::Err: Display,
    {
        String::deserialize(deserializer)?
            .parse::<T>()
            .map_err(serde::de::Error::custom)
    }
}
