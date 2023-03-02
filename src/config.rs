//! Types and functions for parsing and validating configuration from a YAML file.

use anyhow::bail;
use regex::Regex;
use serde::Deserialize;
use std::net::SocketAddr;

use crate::utils::deserialize_using_parse;

/// The supported exchanges.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum Exchange {
    Binance,
    Bitstamp,
}
use Exchange::*;

impl Default for Exchange {
    fn default() -> Self {
        Self::Binance
    }
}

impl From<&Exchange> for String {
    fn from(value: &Exchange) -> Self {
        match &value {
            Binance => "binance".into(),
            Bitstamp => "bitstamp".into(),
        }
    }
}

impl std::str::FromStr for Exchange {
    type Err = anyhow::Error;
    fn from_str(exchange: &str) -> Result<Self, Self::Err> {
        match exchange.trim().to_lowercase().as_ref() {
            "binance" => Ok(Binance),
            "bitstamp" => Ok(Bitstamp),
            _ => bail!("exchange not implemented: {exchange}"),
        }
    }
}

/// Currency pair for reasoning about how exchanges present symbols at various
/// places in their API.
/// Also allows API client implementations to specify whether it's "ethbtc",
/// "eth_btc" or "BTC-ETH" etc. We'll use lowercase internally and the type will
/// enforce this.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct CurrencyPair {
    base: String,
    quote: String,
}

impl Default for CurrencyPair {
    fn default() -> Self {
        Self {
            base: "eth".to_owned(),
            quote: "btc".to_owned(),
        }
    }
}

impl CurrencyPair {
    pub fn base(&self) -> &str {
        self.base.as_str()
    }
    pub fn quote(&self) -> &str {
        self.quote.as_str()
    }
}

impl std::str::FromStr for CurrencyPair {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = Regex::new(r"^[a-z]{3,4}_[a-z]{3,4}$").unwrap();
        if !re.is_match(s) {
            bail!("invalid currency_pair format: {s}");
        }
        let mut assets = s.splitn(2, '_');
        Ok(Self {
            // unwraps never panic due to regex check
            base: assets.next().unwrap().to_owned(),
            quote: assets.next().unwrap().to_owned(),
        })
    }
}

/// A target structure for deserializing the YAML config file.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub addr: SocketAddr,
    #[serde(deserialize_with = "deserialize_using_parse")]
    pub currency_pair: CurrencyPair,
    pub depth: usize,
    pub exchanges: Vec<Exchange>,
    pub ws_buffer_time_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:50051".parse().unwrap(),
            currency_pair: "eth_btc".parse().unwrap(),
            depth: 10,
            exchanges: vec![Binance, Bitstamp],
            ws_buffer_time_ms: 3_000,
        }
    }
}

impl Config {
    /// Validate the configuration.
    fn validate(self) -> anyhow::Result<Self> {
        if self.depth > 100 {
            bail!("depth too large, some ws channels limit snapshots to 100")
        } else if self.depth < 1 {
            bail!("depth must be greater than 0")
        }
        if self.exchanges.is_empty() {
            bail!("number of exchanges must be > 0")
        }
        Ok(self)
    }
}

/// Parse the config file and validate it.
///
/// # Panics
/// Will panic on invalid config, for example an unsupported exchange, invalid
/// currency_pair formatting or currency_pair not supported by an exchange.
pub fn read_config() -> Config {
    let config_path = env!("CARGO_MANIFEST_DIR");
    let f = std::fs::File::open(format!("{config_path}/config.yml"))
        .expect("failed to open config file");
    let config: Config = serde_yaml::from_reader(f).expect("failed to parse config file");
    config.validate().expect("invalid config")
}
