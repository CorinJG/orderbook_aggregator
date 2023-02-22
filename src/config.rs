//! Types and functions for parsing and validating configuration from a YAML file.

use anyhow::anyhow;
use regex::Regex;
use serde::Deserialize;
use std::net::SocketAddr;

use crate::utils::deserialize_using_parse;

/// The supported exchanges.
#[derive(Debug, Clone, Copy)]
pub enum Exchange {
    Binance,
    Bitstamp,
}
use Exchange::*;

impl std::str::FromStr for Exchange {
    type Err = anyhow::Error;
    fn from_str(exchange: &str) -> Result<Self, Self::Err> {
        match exchange.trim().to_lowercase().as_ref() {
            "binance" => Ok(Binance),
            "bitstamp" => Ok(Bitstamp),
            _ => Err(anyhow!("exchange not implemented: {exchange}")),
        }
    }
}

/// Currency pair for reasoning about how exchanges present symbols at various
/// places in their API.
/// Also allows API client implementations to specify whether it's "ethbtc",
/// "eth_btc" or "BTC-ETH" etc. We'll use lowercase internally.
#[derive(Debug, Deserialize)]
pub struct CurrencyPair {
    base: String,
    quote: String,
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
            return Err(anyhow!("invalid currency_pair format: {s}"));
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
    pub exchanges: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:50051".parse().unwrap(),
            currency_pair: "eth_btc".parse().unwrap(),
            depth: 10,
            exchanges: vec!["binance".into(), "bitstamp".into()],
        }
    }
}

impl Config {
    /// Validate the configuration.
    fn validate(self) -> anyhow::Result<Self> {
        for exchange in &self.exchanges {
            exchange.parse::<Exchange>()?;
        }
        Ok(self)
    }
}

/// Parse the config file and validate it.
pub fn read_config() -> Config {
    let config_path = env!("CARGO_MANIFEST_DIR");
    let f = std::fs::File::open(format!("{config_path}/config.yml"))
        .expect("failed to open config file");
    let config: Config = serde_yaml::from_reader(f).expect("failed to parse config file");
    config.validate().expect("invalid config")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that valid configs are validating and invalid ones are erroring.
    #[test]
    fn config() {
        read_config();
    }
}
