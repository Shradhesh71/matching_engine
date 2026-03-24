use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::Message;
use tracing::{info, warn};

use crate::state::AppState;

/// websocket endpoint clients connect here to receive fill events in real time
/// fan out design
pub async fn ws_handler(
    req:   HttpRequest,
    body:  web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;

    let mut fill_rx = state.engine.fill_tx.subscribe();

    actix_web::rt::spawn(async move {
        info!("websocket client connected");

        loop {
            tokio::select! {
                // receive a fill from engine broadcast and forward to client
                result = fill_rx.recv() => {
                    match result {
                        Ok(fill) => {
                            let json = match serde_json::to_string(&fill) {
                                Ok(j) => j,
                                Err(e) => {
                                    warn!("fill serialize error: {e}");
                                    continue;
                                }
                            };
                            if session.text(json).await.is_err() {
                                break; // client disconnected
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("ws client lagged, dropped {n} fills");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }

                msg = msg_stream.recv() => {
                    match msg {
                        Some(Ok(Message::Ping(bytes))) => {
                            if session.pong(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(reason))) => {
                            let _ = session.close(reason).await;
                            break;
                        }
                        Some(Err(e)) => {
                            warn!("WS error: {e}");
                            break;
                        }
                        None => break, 
                        _ => {} 
                    }
                }
            }
        }

        info!("websocket client disconnected");
    });

    Ok(response)
}