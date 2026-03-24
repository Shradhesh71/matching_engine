use std::collections::{BTreeMap, VecDeque};

use shared::{Fill, Order, PriceLevel, Side};

/// each side is a `BTreeMap<u64, VecDeque<Order>>`:
pub struct OrderBook {
    pub bids: BTreeMap<u64, VecDeque<Order>>,
    pub asks: BTreeMap<u64, VecDeque<Order>>,
    next_id:  u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids:    BTreeMap::new(),
            asks:    BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// insert a taker order, run the matching loop, return every fill produced
    pub fn submit(&mut self, mut taker: Order) -> Vec<Fill> {
        let mut fills = Vec::new();

        match taker.side {
            Side::Buy => {
                // match against asks (ascending price)
                loop {
                    let best_ask_price = match self.asks.keys().next().copied() {
                        Some(p) if p <= taker.price => p,
                        _ => break,
                    };

                    let level = self.asks.get_mut(&best_ask_price).unwrap();

                    let maker = level.front_mut().unwrap();

                    let fill_qty = taker.qty.min(maker.qty);
                    fills.push(Fill {
                        maker_order_id: maker.id,
                        taker_order_id: taker.id,
                        price:          maker.price, // fill at maker price
                        qty:            fill_qty,
                    });

                    maker.qty -= fill_qty;
                    taker.qty -= fill_qty;

                    if maker.qty == 0 {
                        level.pop_front();
                    }
                    if level.is_empty() {
                        self.asks.remove(&best_ask_price);
                    }
                    if taker.qty == 0 {
                        break;
                    }
                }

                // rest unfilled taker qty on the bid side
                if taker.qty > 0 {
                    self.bids
                        .entry(taker.price)
                        .or_default()
                        .push_back(taker);
                }
            }

            Side::Sell => {
                // match against bids (descending price)
                loop {
                    let best_bid_price = match self.bids.keys().next_back().copied() {
                        Some(p) if p >= taker.price => p,
                        _ => break,
                    };

                    let level = self.bids.get_mut(&best_bid_price).unwrap();
                    let maker = level.front_mut().unwrap();

                    let fill_qty = taker.qty.min(maker.qty);
                    fills.push(Fill {
                        maker_order_id: maker.id,
                        taker_order_id: taker.id,
                        price:          maker.price, // fill at maker price
                        qty:            fill_qty,
                    });

                    maker.qty -= fill_qty;
                    taker.qty -= fill_qty;

                    if maker.qty == 0 {
                        level.pop_front();
                    }
                    if level.is_empty() {
                        self.bids.remove(&best_bid_price);
                    }
                    if taker.qty == 0 {
                        break;
                    }
                }

                // rest unfilled taker qty on the ask side
                if taker.qty > 0 {
                    self.asks
                        .entry(taker.price)
                        .or_default()
                        .push_back(taker);
                }
            }
        }

        fills
    }

    /// snapshot bids and asks for GET /orderbook
    pub fn snapshot(&self) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let bids = self
            .bids
            .iter()
            .rev() // highest bid first
            .map(|(&price, queue)| PriceLevel {
                price,
                qty: queue.iter().map(|o| o.qty).sum(),
            })
            .collect();

        let asks = self
            .asks
            .iter() // lowest ask first
            .map(|(&price, queue)| PriceLevel {
                price,
                qty: queue.iter().map(|o| o.qty).sum(),
            })
            .collect();

        (bids, asks)
    }
}