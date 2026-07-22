use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior};

use crate::{db, state::AppState, upstream};

pub fn spawn(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(2),
            state.config.usage_refresh_interval,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut refresh_count = 0_u64;

        loop {
            ticker.tick().await;
            match upstream::refresh_all_usage(
                &state.pool,
                &state.crypto,
                &state.http,
                &state.config,
            )
            .await
            {
                Ok(snapshots) if !snapshots.is_empty() => {
                    tracing::debug!(accounts = snapshots.len(), "usage snapshots refreshed");
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "usage refresh cycle failed"),
            }

            refresh_count = refresh_count.wrapping_add(1);
            if refresh_count == 1 || refresh_count.is_multiple_of(360) {
                match db::runtime_settings(&state.pool).await {
                    Ok(settings) => {
                        if let Err(error) =
                            db::prune_history(&state.pool, settings.usage_sample_retention_days)
                                .await
                        {
                            tracing::warn!(%error, "history pruning failed");
                        }
                    }
                    Err(error) => tracing::warn!(%error, "could not load retention settings"),
                }
            }
        }
    })
}

pub fn spawn_api_cost_backfill(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = state.subscribe_shutdown();
        let mut total = 0_u64;
        loop {
            let result = tokio::select! {
                _ = shutdown.changed() => return,
                result = db::backfill_api_costs_batch(
                    &state.pool,
                    db::API_COST_BACKFILL_BATCH_SIZE,
                ) => result,
            };
            match result {
                Ok(batch) if batch.selected == 0 => {
                    if total > 0 {
                        tracing::info!(requests = total, "historical API cost backfill completed");
                    }
                    return;
                }
                Ok(batch) => total = total.saturating_add(batch.updated),
                Err(error) => {
                    tracing::warn!(%error, "historical API cost backfill batch failed; retrying");
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
}
