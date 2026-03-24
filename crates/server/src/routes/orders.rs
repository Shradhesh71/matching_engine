use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use shared::{EngineRequest, EngineResponse, Side};
use tracing::error;

use crate::state::AppState;

// POST /orders

#[derive(Debug, Deserialize)]
pub struct OrderRequest {
    pub side:  Side,
    pub price: u64,
    pub qty:   u64,
}

#[derive(Serialize)]
struct OrderSubmitted {
    order_id: u64,
    fills:    Vec<shared::Fill>,
}

pub async fn post_order(
    state: web::Data<AppState>,
    body:  web::Json<OrderRequest>,
) -> HttpResponse {
    let req = EngineRequest::SubmitOrder {
        side:  body.side,
        price: body.price,
        qty:   body.qty,
    };

    match state.engine.send(req).await {
        Ok(EngineResponse::Accepted { order_id, fills }) => {
            HttpResponse::Ok().json(OrderSubmitted { order_id, fills })
        }
        Ok(EngineResponse::Error { message }) => {
            HttpResponse::BadRequest().json(serde_json::json!({ "error": message }))
        }
        Ok(other) => {
            error!("unexpected engine response: {other:?}");
            HttpResponse::InternalServerError().finish()
        }
        Err(e) => {
            error!("engine unreachable: {e}");
            HttpResponse::ServiceUnavailable().finish()
        }
    }
}

// GET /orderbook

#[derive(Serialize)]
struct OrderBookSnapshot {
    bids: Vec<shared::PriceLevel>,
    asks: Vec<shared::PriceLevel>,
}

pub async fn get_orderbook(state: web::Data<AppState>) -> HttpResponse {
    match state.engine.send(EngineRequest::GetOrderBook).await {
        Ok(EngineResponse::OrderBook { bids, asks }) => {
            HttpResponse::Ok().json(OrderBookSnapshot { bids, asks })
        }
        Ok(EngineResponse::Error { message }) => {
            HttpResponse::InternalServerError().json(serde_json::json!({ "error": message }))
        }
        Ok(other) => {
            error!("unexpected engine response: {other:?}");
            HttpResponse::InternalServerError().finish()
        }
        Err(e) => {
            error!("engine unreachable: {e}");
            HttpResponse::ServiceUnavailable().finish()
        }
    }
}