use std::cmp::Ordering;
use std::collections::BTreeMap;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// A representation of an exchange orderbook for a currency pair, designed
/// for updating using diffs/deltas. If an exchange provides no such API, use
/// OrderbookSnapshot instead.
/// May be missing some orders far from the spread, as is the case when updating
/// from an initial snapshot with a limited depth.
/// BTreeMap facilitates O(log(n)) insert and remove and significantly simplifies
/// the implementation.
#[derive(Debug, Eq, PartialEq)]
pub struct DiffOrderbook {
    asks: BTreeMap<Decimal, Decimal>,
    bids: BTreeMap<Decimal, Decimal>,
}

impl DiffOrderbook {
    /// Create a new instance from a snapshot.
    pub fn from_snapshot(snapshot: OrderbookSnapshot) -> Self {
        Self {
            asks: BTreeMap::from_iter(snapshot.asks.iter().map(|ask| (ask.price, ask.quantity))),
            bids: BTreeMap::from_iter(snapshot.bids.iter().map(|bid| (bid.price, bid.quantity))),
        }
    }

    /// Update the orderbook with the new prices/quantities.
    fn import_updates(
        &mut self,
        ask_updates: Vec<OrderbookLevel>,
        bid_updates: Vec<OrderbookLevel>,
    ) {
        for update in ask_updates {
            if update.quantity == dec!(0) {
                self.asks.remove(&update.price); // None here may be because order outside initial snapshot depth
            } else {
                self.asks.insert(update.price, update.quantity);
            }
        }
        for update in bid_updates {
            if update.quantity == dec!(0) {
                self.bids.remove(&update.price); // None here may be because order outside initial snapshot depth
            } else {
                self.bids.insert(update.price, update.quantity);
            }
        }
    }
}

/// A representation of en exchange orderbook for a currency pair.
/// The API guarantees it is always sorted, in that asks are ascending in price, bids are descending in price.
/// This struct is useful for rest or websocket APIs sending snapshots, as opposed to diffs/deltas.
#[derive(Debug, Eq, PartialEq)]
pub struct OrderbookSnapshot {
    asks: Vec<OrderbookLevel>,
    bids: Vec<OrderbookLevel>,
}

impl OrderbookSnapshot {
    pub fn new() -> Self {
        Self {
            asks: Vec::new(),
            bids: Vec::new(),
        }
    }

    /// Sort the orderbook, so that asks are ascending and bids are descending.
    fn sort(&mut self) {
        self.asks.sort_by(|a, b| a.cmp(b));
        self.bids.sort_by(|a, b| b.cmp(a));
    }
}

/// A single level in an orderbook.
#[derive(Debug)]
struct OrderbookLevel {
    price: Decimal,
    quantity: Decimal,
}

impl PartialOrd for OrderbookLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.price.partial_cmp(&other.price)
    }
}

impl PartialEq for OrderbookLevel {
    fn eq(&self, other: &Self) -> bool {
        self.price == other.price
    }
}

impl Eq for OrderbookLevel {}

impl Ord for OrderbookLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price.cmp(&other.price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orderbook_sort() {
        #[rustfmt::skip]
        let mut ob = OrderbookSnapshot {
            asks: vec![
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(1), },
                OrderbookLevel { price: dec!(2), quantity: dec!(2), },
                OrderbookLevel { price: dec!(1), quantity: dec!(3), },
            ],
            bids: vec![
                OrderbookLevel { price: dec!(1), quantity: dec!(2), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(2), },
                OrderbookLevel { price: dec!(2), quantity: dec!(3), },
            ],
        };
        #[rustfmt::skip]
        let target = OrderbookSnapshot {
            asks: vec![
                OrderbookLevel { price: dec!(1), quantity: dec!(3), },
                OrderbookLevel { price: dec!(2), quantity: dec!(2), },
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(1), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
            ],
            bids: vec![
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(2), },
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(2), quantity: dec!(3), },
                OrderbookLevel { price: dec!(1), quantity: dec!(2), },
            ],
        };
        ob.sort();
        assert_eq!(ob, target);
    }

    #[test]
    fn diff_orderbook_update() {
        #[rustfmt::skip]
        let ob = OrderbookSnapshot {
            asks: vec![
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(1), },
                OrderbookLevel { price: dec!(2), quantity: dec!(2), },
                OrderbookLevel { price: dec!(1), quantity: dec!(3), },
            ],
            bids: vec![
                OrderbookLevel { price: dec!(1), quantity: dec!(2), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(3), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(2), },
                OrderbookLevel { price: dec!(2), quantity: dec!(3), },
            ],
        };

        let mut dob = DiffOrderbook::from_snapshot(ob);

        #[rustfmt::skip]
        let ask_updates = vec!(
            OrderbookLevel { price: dec!(1), quantity: dec!(21) },
            OrderbookLevel { price: dec!(2), quantity: dec!(22) },
            OrderbookLevel { price: dec!(3), quantity: dec!(0) },
            OrderbookLevel { price: dec!(14), quantity: dec!(24) },
            OrderbookLevel { price: dec!(15), quantity: dec!(0) },
        );
        #[rustfmt::skip]
        let bid_updates = vec!(
            OrderbookLevel { price: dec!(1), quantity: dec!(2) },
            OrderbookLevel { price: dec!(2), quantity: dec!(0) },
            OrderbookLevel { price: dec!(3), quantity: dec!(2) },
            OrderbookLevel { price: dec!(4), quantity: dec!(2) },
            OrderbookLevel { price: dec!(6), quantity: dec!(2) },
        );
        dob.import_updates(ask_updates, bid_updates);

        #[rustfmt::skip]
        let target_snapshot = OrderbookSnapshot {
            asks: vec![
                OrderbookLevel { price: dec!(14), quantity: dec!(24) },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(4), quantity: dec!(1), },
                OrderbookLevel { price: dec!(2), quantity: dec!(22), },
                OrderbookLevel { price: dec!(1), quantity: dec!(21), },
            ],
            bids: vec![
                OrderbookLevel { price: dec!(1), quantity: dec!(2), },
                OrderbookLevel { price: dec!(5), quantity: dec!(1), },
                OrderbookLevel { price: dec!(3), quantity: dec!(2), },
                OrderbookLevel { price: dec!(4), quantity: dec!(2), },
                OrderbookLevel { price: dec!(6), quantity: dec!(2), },
            ]
        };
        let target = DiffOrderbook::from_snapshot(target_snapshot);
        assert_eq!(dob, target);
    }
}
