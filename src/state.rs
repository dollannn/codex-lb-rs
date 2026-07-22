use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::watch;

use crate::{config::Config, crypto::TokenCrypto};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub pool: SqlitePool,
    pub crypto: TokenCrypto,
    pub http: reqwest::Client,
    shutdown: watch::Sender<bool>,
}

impl AppState {
    pub fn new(config: Config, pool: SqlitePool, crypto: TokenCrypto) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            config: Arc::new(config),
            pool,
            crypto,
            http: reqwest::Client::new(),
            shutdown,
        }
    }

    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn signal_shutdown(&self) {
        self.shutdown.send_replace(true);
    }
}
