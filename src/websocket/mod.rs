//! Exchange API websocket client implementations for orderbook datafeeds.
//!
//! Running clients forward orderbook snapshots truncated to a specified depth downstream
//! using a channel provided by callers.
//!
//! When available, client implementations will use a diff/delta channel. Otherwise exchanges
//! will just send periodic orderbook snapshots.

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
