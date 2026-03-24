use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::{Context, Result};
use shared::{EnginePush, EngineRequest, EngineResponse};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{broadcast, oneshot, Mutex},
};
use tracing::{error, warn};

/// A handle the http handlers use to talk to the engine process.
#[derive(Clone)]
pub struct EngineClient {
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    /// queue of oneshot senders, one per inflight request
    pending: Arc<Mutex<VecDeque<oneshot::Sender<EngineResponse>>>>,
    pub fill_tx: broadcast::Sender<shared::Fill>,
}

impl EngineClient {
    /// connect to the engine at `addr` and start the background reader task
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("could not connect to engine at {addr}"))?;

        let (reader, writer) = stream.into_split();

        let (fill_tx, _) = broadcast::channel::<shared::Fill>(1024);
        let fill_tx_bg = fill_tx.clone();

        let pending: Arc<Mutex<VecDeque<oneshot::Sender<EngineResponse>>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        let pending_bg = Arc::clone(&pending);

        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();

            loop {
                let line = match lines.next_line().await {
                    Ok(Some(l)) => l,
                    Ok(None) => {
                        warn!("engine disconnected");
                        break;
                    }
                    Err(e) => {
                        error!("engine read error: {e}");
                        break;
                    }
                };

                let line = line.trim().to_owned();
                if line.is_empty() {
                    continue;
                }

                // try to parse as a response (Accepted / OrderBook / Error)
                if let Ok(resp) = serde_json::from_str::<EngineResponse>(&line) {
                    let sender = pending_bg.lock().await.pop_front();
                    match sender {
                        Some(tx) => {
                            let _ = tx.send(resp);
                        }
                        None => {
                            warn!("received response with no pending waiter: {line}");
                        }
                    }
                    continue;
                }

                // try to parse as an unsolicited push (Fill event)
                if let Ok(EnginePush::Fill(fill)) = serde_json::from_str::<EnginePush>(&line) {
                    let _ = fill_tx_bg.send(fill);
                    continue;
                }

                warn!("unrecognised frame from engine: {line}");
            }
        });

        Ok(EngineClient {
            writer: Arc::new(Mutex::new(writer)),
            pending,
            fill_tx,
        })
    }

    /// send a request to engine and return response
    pub async fn send(&self, req: EngineRequest) -> Result<EngineResponse> {
        let (tx, rx) = oneshot::channel::<EngineResponse>();

        let mut line = serde_json::to_string(&req)?;
        line.push('\n');

        {
            let mut writer  = self.writer.lock().await;
            let mut pending = self.pending.lock().await;
            pending.push_back(tx);
            writer.write_all(line.as_bytes()).await?;
        }

        rx.await.context("engine closed connection before responding")
    }
}