use actix_web::{middleware, web, App, HttpServer};
use anyhow::Result;
use state::AppState;
use tracing::info;

mod engine_client;
mod routes;
mod state;

#[actix_web::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "server=info,actix_web=info".into()),
        )
        .init();
 
    let http_addr   = std::env::var("HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let engine_addr = std::env::var("ENGINE_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".into());
 
    info!("connecting to engine at {engine_addr}");
    let engine = engine_client::EngineClient::connect(&engine_addr).await?;
    info!("connected to engine");
 
    let state = web::Data::new(AppState { engine });
 
    info!("HTTP server listening on {http_addr}");
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(middleware::Logger::default())
            // HTTP routes
            .route("/orders",    web::post().to(routes::orders::post_order))
            .route("/orderbook", web::get().to(routes::orders::get_orderbook))
            // // WebSocket feed
            .route("/ws",        web::get().to(routes::ws::ws_handler))
    })
    .bind(&http_addr)?
    .run()
    .await?;
 
    Ok(())
}
 