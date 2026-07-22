use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use codex_lb_rs::{
    build_app,
    cli::{self, Cli, Command, MigrateCommand},
    config::Config,
    crypto::TokenCrypto,
    db,
    state::AppState,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();

    match &cli.command {
        Command::Serve(args) => {
            let mut config = Config::from_env()?;
            if let Some(host) = args.host.clone() {
                config.host = host;
            }
            if let Some(port) = args.port {
                config.port = port;
            }
            serve(config).await
        }
        Command::Migrate(args) => match args.command {
            MigrateCommand::Up => migrate().await,
        },
        _ => cli::run_api_command(&cli).await,
    }
}

async fn migrate() -> Result<()> {
    let config = Config::from_env()?;
    let pool = db::connect(&config.database_url).await?;
    db::run_migrations(&pool).await?;
    db::reset_inflight(&pool).await?;
    println!("migrations applied");
    Ok(())
}

async fn serve(config: Config) -> Result<()> {
    let addr = config.socket_addr()?;
    let crypto = TokenCrypto::load_or_create(&config.encryption_key_file).await?;
    let pool = db::connect(&config.database_url).await?;
    db::run_migrations(&pool).await?;
    db::reset_inflight(&pool).await?;

    if !addr.ip().is_loopback()
        && (config.admin_token.is_none() || config.proxy_api_token.is_none())
    {
        anyhow::bail!(
            "refusing a non-loopback listener without both CODEX_LB_ADMIN_TOKEN and CODEX_LB_PROXY_API_TOKEN"
        );
    }

    let state = AppState::new(config, pool, crypto);
    let _scheduler = codex_lb_rs::scheduler::spawn(state.clone());
    let shutdown_state = state.clone();
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "codex-lb-rs listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_state))
        .await?;
    Ok(())
}

async fn shutdown_signal(state: AppState) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        signal.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    state.signal_shutdown();
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("codex_lb_rs=info,tower_http=info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
