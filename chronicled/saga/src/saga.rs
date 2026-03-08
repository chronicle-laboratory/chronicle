use crate::config::SagaConfig;
use crate::pipeline::source::Source;
use crate::storage::flusher::Flusher;
use crate::storage::lifecycle::Lifecycle;
use crate::storage::memtable::Memtable;
use crate::storage::merger::Merger;
use crate::storage::saga_catalog::SagaCatalog;
use crate::server::http::{self, AppState};
use chronicle_lexicon_client::LexiconClient;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

/// Main Saga orchestrator that owns all background tasks.
pub struct Saga {
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Saga {
    pub async fn new(
        _oxia_catalog: Arc<dyn catalog::Catalog>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let config = SagaConfig::default();
        Self::with_config(config).await
    }

    pub async fn with_config(
        config: SagaConfig,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Connect to Lexicon for schema resolution.
        let catalog = match LexiconClient::connect(&config.lexicon_endpoint).await {
            Ok(client) => {
                info!(endpoint = %config.lexicon_endpoint, "connected to lexicon");
                Arc::new(SagaCatalog::with_lexicon(client))
            }
            Err(e) => {
                warn!(
                    endpoint = %config.lexicon_endpoint,
                    error = %e,
                    "failed to connect to lexicon, starting without schema registry"
                );
                Arc::new(SagaCatalog::new())
            }
        };

        let memtables: Arc<parking_lot::RwLock<HashMap<String, Arc<Memtable>>>> =
            Arc::new(parking_lot::RwLock::new(HashMap::new()));

        let lifecycle = Arc::new(
            Lifecycle::recover(std::path::PathBuf::from(&config.checkpoint_path)).await?,
        );

        let store: Arc<dyn ObjectStore> = if config.use_local_fs {
            Arc::new(
                LocalFileSystem::new_with_prefix(&config.storage_root)
                    .unwrap_or_else(|_| LocalFileSystem::new()),
            )
        } else {
            Arc::new(LocalFileSystem::new()) // TODO: S3 store
        };

        let mut handles = Vec::new();

        // Consumer task.
        {
            let source = Source::new(
                config.clone(),
                catalog.clone(),
                memtables.clone(),
                lifecycle.clone(),
            );
            let rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = source.run(rx).await {
                    error!(error = %e, "source task failed");
                }
            }));
        }

        // Flush scheduler.
        {
            let cfg = config.clone();
            let cat = catalog.clone();
            let mts = memtables.clone();
            let lc = lifecycle.clone();
            let mut rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                let flusher = Flusher::new(cfg.clone(), cat);
                let flush_interval = Duration::from_secs(cfg.flush_age_threshold_secs.max(5));
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            info!("flush scheduler shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(flush_interval) => {
                            flush_eligible(&flusher, &mts, &lc, &cfg).await;
                        }
                    }
                }
            }));
        }

        // Merge scheduler.
        {
            let cfg = config.clone();
            let cat = catalog.clone();
            let st = store.clone();
            let lc = lifecycle.clone();
            let mut rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                let merger = Merger::new(cfg.clone(), cat, st);
                let merge_interval = Duration::from_secs(cfg.merge_check_interval_secs);
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            info!("merge scheduler shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(merge_interval) => {
                            match merger.check_and_merge().await {
                                Ok(Some(result)) => {
                                    info!(
                                        subject = %result.topic,
                                        files_merged = result.l0_files_merged,
                                        rows = result.rows_total,
                                        "merge completed"
                                    );
                                    lc.advance_persisted(0); // TODO: track merge sequence
                                }
                                Ok(None) => {}
                                Err(e) => error!(error = %e, "merge check failed"),
                            }
                        }
                    }
                }
            }));
        }

        // Checkpoint scheduler.
        {
            let lc = lifecycle.clone();
            let mut rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.changed() => {
                            let _ = lc.checkpoint().await;
                            info!("checkpoint scheduler shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {
                            if let Err(e) = lc.checkpoint().await {
                                error!(error = %e, "checkpoint failed");
                            }
                        }
                    }
                }
            }));
        }

        // HTTP server.
        {
            let state = AppState {
                catalog: catalog.clone(),
                memtables: memtables.clone(),
                config: config.clone(),
            };
            let port = config.http_port;
            let mut rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                let app = http::build_router(state);
                let addr = format!("0.0.0.0:{}", port);
                info!(addr = %addr, "starting HTTP server");
                let listener = match tokio::net::TcpListener::bind(&addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        error!(error = %e, "failed to bind HTTP server");
                        return;
                    }
                };
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = rx.changed().await;
                    })
                    .await
                    .ok();
                info!("HTTP server stopped");
            }));
        }

        info!(
            http_port = config.http_port,
            wal_endpoint = %config.wal_endpoint,
            lexicon_endpoint = %config.lexicon_endpoint,
            "Saga started"
        );

        Ok(Self {
            shutdown_tx,
            handles,
        })
    }

    pub async fn stop(self) {
        info!("stopping Saga");
        let _ = self.shutdown_tx.send(true);
        for handle in self.handles {
            let _ = handle.await;
        }
        info!("Saga stopped");
    }
}

async fn flush_eligible(
    flusher: &Flusher,
    memtables: &parking_lot::RwLock<HashMap<String, Arc<Memtable>>>,
    lifecycle: &Lifecycle,
    config: &SagaConfig,
) {
    let flush_duration = Duration::from_secs(config.flush_age_threshold_secs);
    let topics: Vec<(String, Arc<Memtable>)> = {
        let tables = memtables.read();
        tables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    for (topic, mt) in topics {
        if mt.should_flush(config.flush_size_threshold, flush_duration) {
            if let Some(frozen) = mt.freeze(&topic) {
                let max_seq = frozen.max_sequence_id;
                match flusher.flush(frozen).await {
                    Ok(meta) => {
                        info!(
                            subject = %topic,
                            rows = meta.row_count,
                            path = %meta.path,
                            "memtable flushed"
                        );
                        lifecycle.advance_flushed(max_seq);
                    }
                    Err(e) => error!(error = %e, subject = %topic, "flush failed"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saga_lifecycle() {
        let config = SagaConfig {
            http_port: 0,
            wal_endpoint: "http://127.0.0.1:19999".into(),
            lexicon_endpoint: "http://127.0.0.1:19998".into(),
            checkpoint_path: "/tmp/saga-test-checkpoint.json".into(),
            ..Default::default()
        };
        let saga = Saga::with_config(config).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        saga.stop().await;
    }
}
