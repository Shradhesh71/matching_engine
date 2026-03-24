use crate::engine_client::EngineClient;

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineClient,
}