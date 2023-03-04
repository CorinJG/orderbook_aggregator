pub mod aggregator;
pub mod config;
pub mod grpc_server;
pub mod messages;
pub mod websocket;

pub mod proto {
    pub mod orderbook {
        tonic::include_proto!("orderbook");
    }
}

pub(crate) mod utils {
    use std::fmt::{self, Display};
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use serde::{
        de::{self, IgnoredAny, SeqAccess, Visitor},
        Deserialize, Deserializer,
    };

    use crate::config::CONFIG;

    pub(crate) type Seconds = u64;
    pub(crate) type Millis = u64;

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

    /// Target deserialization type for a single order in a list of asks or bids in a JSON snapshot
    /// having custom Visitor which skips asks/bids after first <depth>.
    #[derive(Debug, Default)]
    pub struct TruncatedOrders(pub Vec<(Decimal, Decimal)>);

    /// For sequences, deserialize the first <depth> and skip the remaining.
    impl<'de> de::Deserialize<'de> for TruncatedOrders {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct MyTupleVisitor;

            impl<'de> Visitor<'de> for MyTupleVisitor {
                type Value = TruncatedOrders;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("struct TruncatedOrders")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
                where
                    V: SeqAccess<'de>,
                {
                    let mut inner = Vec::new();
                    for i in 0..CONFIG.depth {
                        inner.push(
                            seq.next_element()?
                                .ok_or_else(|| de::Error::invalid_length(i, &self))?,
                        );
                    }

                    while let Some(IgnoredAny) = seq.next_element()? {
                        // Skip all subsequent items
                    }

                    Ok(TruncatedOrders(inner))
                }
            }

            deserializer.deserialize_seq(MyTupleVisitor)
        }
    }
}
