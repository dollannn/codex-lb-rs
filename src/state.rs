use std::sync::Arc;

use sqlx::PgPool;

use crate::{config::Config, crypto::TokenCrypto};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub pool: PgPool,
    pub crypto: TokenCrypto,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(config: Config, pool: PgPool, crypto: TokenCrypto) -> Self {
        Self {
            config: Arc::new(config),
            pool,
            crypto,
            http: reqwest::Client::new(),
        }
    }
}
