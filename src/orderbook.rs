//! Module for defining types which are used to internally maintain orderbook state.

use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// A representation of an orderbook for a currency pair, designed for updating using
/// diffs/deltas.
///
/// When updating from a diff channel, [BTreeMap] is suitable for the two halves of the
/// orderbook as it facilitates O(log(n)) insert and remove.
///
/// If an exchange only provides a snapshot API and no diff channel, a [Vec] may be more suitable
/// for each half of the orderbook instead.
#[derive(Eq, PartialEq)]
pub struct Orderbook {
    asks: BTreeMap<Decimal, Decimal>,
    bids: BTreeMap<Decimal, Decimal>,
}

impl Debug for Orderbook {
    /// Display all levels with asks ordered by increasing price and bids by decreasing price.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Orderbook {{ asks: {{ ")?;
        for (p, q) in self.asks.iter() {
            write!(f, "[{p}, {q}], ")?;
        }
        write!(f, "... }},\nbids: {{ ")?;
        for (p, q) in self.bids.iter().rev() {
            write!(f, "[{p}, {q}], ")?;
        }
        write!(f, "... }} }}")
    }
}

impl Display for Orderbook {
    /// Display only the top 10 levels with asks ordered by increasing price and bids by decreasing price.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Orderbook {{ asks: {{ ")?;
        for (p, q) in self.asks.iter().take(10) {
            write!(f, "[{p}, {q}], ")?;
        }
        write!(f, "... }},\nbids: {{ ")?;
        for (p, q) in self.bids.iter().rev().take(10) {
            write!(f, "[{p}, {q}], ")?;
        }
        write!(f, "... }} }}")
    }
}

impl Orderbook {
    /// Construct an instance from a type typically available directly when deserializing from exchange API.
    pub fn from_asks_bids(asks: Vec<(Decimal, Decimal)>, bids: Vec<(Decimal, Decimal)>) -> Self {
        Self {
            asks: BTreeMap::from_iter(asks.into_iter().map(|(price, quantity)| (price, quantity))),
            bids: BTreeMap::from_iter(bids.into_iter().map(|(price, quantity)| (price, quantity))),
        }
    }

    /// Update the orderbook with the new prices/quantities (when using websocket diff/delta channel).
    pub fn apply_updates(
        &mut self,
        ask_updates: Vec<(Decimal, Decimal)>,
        bid_updates: Vec<(Decimal, Decimal)>,
    ) {
        for (price, quantity) in ask_updates {
            if quantity == dec!(0) {
                self.asks.remove(&price); // None here may be because order outside initial snapshot depth
            } else {
                self.asks.insert(price, quantity);
            }
        }
        for (price, quantity) in bid_updates {
            if quantity == dec!(0) {
                self.bids.remove(&price); // None here may be because order outside initial snapshot depth
            } else {
                self.bids.insert(price, quantity);
            }
        }
    }

    /// Return a new Orderbook with ask and bid halves truncated to given depth.
    pub fn to_truncated(&self, depth: usize) -> Self {
        Self {
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(&price, &quantity)| (price, quantity))
                .collect(),
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(&price, &quantity)| (price, quantity))
                .collect(),
        }
    }

    /// Consumes the Orderbook, returning the asks and bids for Aggregator to process.
    pub fn into_asks_bids(self) -> (BTreeMap<Decimal, Decimal>, BTreeMap<Decimal, Decimal>) {
        (self.asks, self.bids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_orderbook_updates() {
        #[rustfmt::skip]
        let mut dob = Orderbook::from_asks_bids(
            vec![
                (dec!(3), dec!(1)),
                (dec!(5), dec!(1)),
                (dec!(4), dec!(1)),
                (dec!(2), dec!(2)),
                (dec!(1), dec!(3)),
            ],
            vec![
                (dec!(1), dec!(2)),
                (dec!(5), dec!(1)),
                (dec!(3), dec!(1)),
                (dec!(4), dec!(2)),
                (dec!(2), dec!(3)),
            ],
        );

        #[rustfmt::skip]
        let ask_updates = vec!(
            (dec!(1), dec!(21)),
            (dec!(2), dec!(22)),
            (dec!(3), dec!(0)),
            (dec!(14), dec!(24)),
            (dec!(15), dec!(0)),
        );
        #[rustfmt::skip]
        let bid_updates = vec!(
            (dec!(1), dec!(2)),
            (dec!(2), dec!(0)),
            (dec!(3), dec!(2)),
            (dec!(4), dec!(2)),
            (dec!(6), dec!(2)),
        );
        dob.apply_updates(ask_updates, bid_updates);

        #[rustfmt::skip]
        let target = Orderbook::from_asks_bids(
            vec![
                (dec!(14), dec!(24)),
                (dec!(5), dec!(1)),
                (dec!(4), dec!(1)),
                (dec!(2), dec!(22)),
                (dec!(1), dec!(21)),
            ],
            vec![
                (dec!(1), dec!(2)),
                (dec!(5), dec!(1)),
                (dec!(3), dec!(2)),
                (dec!(4), dec!(2)),
                (dec!(6), dec!(2)),
            ]
        );
        assert_eq!(dob, target);
    }

    #[test]
    fn truncate_orderbook() {
        #[rustfmt::skip]
        let ob = Orderbook::from_asks_bids(
            vec![
                (dec!(14), dec!(24)),
                (dec!(5), dec!(1)),
                (dec!(4), dec!(1)),
                (dec!(2), dec!(22)),
                (dec!(1), dec!(21)),
            ],
            vec![
                (dec!(1), dec!(2)),
                (dec!(5), dec!(1)),
                (dec!(3), dec!(2)),
                (dec!(4), dec!(2)),
                (dec!(6), dec!(2)),
            ]
        );
        let target = Orderbook::from_asks_bids(
            vec![(dec!(4), dec!(1)), (dec!(2), dec!(22)), (dec!(1), dec!(21))],
            vec![(dec!(5), dec!(1)), (dec!(4), dec!(2)), (dec!(6), dec!(2))],
        );
        assert_eq!(target, ob.to_truncated(3));
    }
}
