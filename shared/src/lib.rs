use serde::{Deserialize, Serialize};

// domain types 

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id:    u64,
    pub side:  Side,
    pub price: u64, // integer ticks — no floats
    pub qty:   u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price:          u64,
    pub qty:            u64,
}

// wire protocol 

/// API server → Engine
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineRequest {
    SubmitOrder { side: Side, price: u64, qty: u64 },
    GetOrderBook,
}

/// engine → API server (response to a request)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EngineResponse {
    Accepted { order_id: u64, fills: Vec<Fill> },
    OrderBook { bids: Vec<PriceLevel>, asks: Vec<PriceLevel> },
    Error { message: String },
}

/// engine → API server (unsolicited push, same TCP stream)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EnginePush {
    Fill(Fill),
}

/// one price level in the order book snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: u64,
    pub qty:   u64, // total resting qty at this price
}