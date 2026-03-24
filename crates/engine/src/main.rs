mod order_book;

use std::sync::Arc;

use anyhow::Result;
use order_book::OrderBook;
use shared::{EngineRequest, EngineResponse, EnginePush, Order};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{broadcast, Mutex},
};
use tracing::{error, info, warn};

/// shared state: one order book + broadcast channel for fill events
struct EngineState {
    book:          Mutex<OrderBook>,
    fill_tx:       broadcast::Sender<shared::Fill>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "engine=info".into()),
        )
        .init();

    let addr = std::env::var("ENGINE_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".into());
    let listener = TcpListener::bind(&addr).await?;
    info!("engine listening on {addr}");

    let (fill_tx, _) = broadcast::channel::<shared::Fill>(1024);

    let state = Arc::new(EngineState {
        book:    Mutex::new(OrderBook::new()),
        fill_tx,
    });

    loop {
        let (stream, peer) = listener.accept().await?;
        info!("API server connected: {peer}");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
                warn!("connection {peer} closed: {e}");
            }
        });
    }
}

/// handle one persistent TCP connection from an API server
async fn handle_connection(stream: TcpStream, state: Arc<EngineState>) -> Result<()> {
    let (reader, writer) = stream.into_split();

    let writer = Arc::new(Mutex::new(writer));

    let push_writer = Arc::clone(&writer);
    let mut fill_rx = state.fill_tx.subscribe();

    tokio::spawn(async move {
        loop {
            match fill_rx.recv().await {
                Ok(fill) => {
                    let push = EnginePush::Fill(fill);
                    let mut line = serde_json::to_string(&push).unwrap();
                    line.push('\n');
                    let mut w = push_writer.lock().await;
                    if w.write_all(line.as_bytes()).await.is_err() {
                        break; // connection closed
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("fill broadcast lagged, dropped {n} fills");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_owned();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<EngineRequest>(&line) {
            Ok(req) => process_request(req, &state).await,
            Err(e) => {
                error!("parse error: {e}  raw={line}");
                EngineResponse::Error { message: e.to_string() }
            }
        };

        let mut out = serde_json::to_string(&response)?;
        out.push('\n');
        writer.lock().await.write_all(out.as_bytes()).await?;
    }

    Ok(())
}

/// process a single request under book mutex, broadcast fills, return response
async fn process_request(req: EngineRequest, state: &EngineState) -> EngineResponse {
    match req {
        EngineRequest::SubmitOrder { side, price, qty } => {
            let (order_id, fills) = {
                let mut book = state.book.lock().await;
                let id = book.next_id();
                let order = Order { id, side, price, qty };
                let fills = book.submit(order);
                (id, fills)
            }; // lock released here

            for fill in &fills {
                let _ = state.fill_tx.send(fill.clone());
            }

            EngineResponse::Accepted { order_id, fills }
        }

        EngineRequest::GetOrderBook => {
            let book = state.book.lock().await;
            let (bids, asks) = book.snapshot();
            EngineResponse::OrderBook { bids, asks }
        }
    }
}