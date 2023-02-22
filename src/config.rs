use serde::Deserialize;
use std::net::SocketAddr;

/// A target structure for deserializing the yaml config file.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub addr: SocketAddr,
    pub currency_pair: String,
    pub depth: u32,
    pub exchanges: Vec<String>,
}

impl Default for Config {
    /// Defaults for testing purposes.
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:50051".parse().unwrap(),
            currency_pair: "eth_btc".into(),
            depth: 10,
            exchanges: vec!["binance".into(), "bitstamp".into()],
        }
    }
}

/// Parse the config file and validate it.
pub fn read_config() -> Config {
    let f = std::fs::File::open("../config.yml").expect("failed to open config file");
    serde_yaml::from_reader(f).expect("failed to parse config file")
    // todo validation
}
