pub mod aggregator;
pub mod config;
pub mod grpc_server;
pub mod orderbook;
pub mod proto;
pub mod websocket;

pub(crate) mod utils {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer};

    pub fn deserialize_using_parse<'de, T, D>(deserializer: D) -> Result<T, D::Error>
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
