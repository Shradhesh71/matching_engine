use std::sync::Arc;

use anyhow::{Context, Result};
use shared::{EngineRequest, EngineResponse, EnginePush};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{broadcast, Mutex},
};
use tracing::{error, warn};

/// A handle http handlers use to talk to engine process
#[derive(Clone)]
pub struct EngineClient {
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    resp_rx: Arc<Mutex<tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>>>,
    pub fill_tx: broadcast::Sender<shared::Fill>,
}

impl EngineClient {
    /// connect to engine and spawn push listener task
    /// returns EngineClient
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("could not connect to engine at {addr}"))?;

        let (reader, writer) = stream.into_split();
        let lines = BufReader::new(reader).lines();

        let (fill_tx, _) = broadcast::channel::<shared::Fill>(1024);

        let client = EngineClient {
            writer:  Arc::new(Mutex::new(writer)),
            resp_rx: Arc::new(Mutex::new(lines)),
            fill_tx: fill_tx.clone(),
        };

        let resp_rx_push = Arc::clone(&client.resp_rx);
        tokio::spawn(async move {
            loop {
                let line = {
                    let mut lines = resp_rx_push.lock().await;
                    match lines.next_line().await {
                        Ok(Some(l)) => l,
                        Ok(None) => {
                            warn!("engine disconnected");
                            break;
                        }
                        Err(e) => {
                            error!("engine read error: {e}");
                            break;
                        }
                    }
                };

                if let Ok(push) = serde_json::from_str::<EnginePush>(&line) {
                    match push {
                        EnginePush::Fill(fill) => {
                            let _ = fill_tx.send(fill);
                        }
                    }
                }
                // request handler directly, they won't appear here because
                // handler holds lock while awaiting its response
            }
        });

        Ok(client)
    }

    /// send request to engine and return response
    pub async fn send(&self, req: EngineRequest) -> Result<EngineResponse> {
        let mut line = serde_json::to_string(&req)?;
        line.push('\n');

        let mut writer = self.writer.lock().await;
        let mut resp_rx = self.resp_rx.lock().await;

        writer.write_all(line.as_bytes()).await?;

        loop {
            let raw = resp_rx
                .next_line()
                .await?
                .context("engine closed connection")?;

            if let Ok(resp) = serde_json::from_str::<EngineResponse>(&raw) {
                return Ok(resp);
            }

            if let Ok(EnginePush::Fill(fill)) = serde_json::from_str::<EnginePush>(&raw) {
                let _ = self.fill_tx.send(fill);
            }
        }
    }
}