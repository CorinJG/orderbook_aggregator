use serde::Deserialize;

/// A target structure for deserializing the yaml config file.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub addr: String, // todo, use Url crate
    pub currency_pair: String,
    pub depth: u32,
    pub exchanges: Vec<String>,
}

/// Parse the config file and validate it.
pub fn read_config() -> Config {
    let f = std::fs::File::open("../config.yml").expect("failed to open config file");
    serde_yaml::from_reader(f).expect("failed to parse config file")
}