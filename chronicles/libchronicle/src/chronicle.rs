use crate::conn::{ConnOptions, ConnPool};
use crate::error::ChronicleError;
use crate::observability::ClientMetrics;
use crate::timeline::Timeline;
use catalog::Catalog;
use opentelemetry::metrics::Meter;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const DEFAULT_REPLICATION_FACTOR: usize = 3;

pub struct ChronicleOptions {
    pub(crate) replication_factor: usize,
    pub(crate) conn_opts: ConnOptions,
    meter: Option<Meter>,
}

impl Default for ChronicleOptions {
    fn default() -> Self {
        Self {
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            conn_opts: ConnOptions::default(),
            meter: None,
        }
    }
}

impl ChronicleOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replication_factor(mut self, rf: usize) -> Self {
        self.replication_factor = rf;
        self
    }

    pub fn conns_per_unit(mut self, n: usize) -> Self {
        self.conn_opts.conns_per_unit = n.max(1);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.request_timeout = timeout;
        self
    }

    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.conn_opts.keep_alive_interval = interval;
        self
    }

    pub fn keep_alive_timeout(mut self, timeout: Duration) -> Self {
        self.conn_opts.keep_alive_timeout = timeout;
        self
    }

    pub fn meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }
}

pub struct Chronicle {
    catalog: Arc<Catalog>,
    pool: Arc<ConnPool>,
    options: ChronicleOptions,
    #[allow(dead_code)]
    metrics: Arc<ClientMetrics>,
}

impl Chronicle {
    pub fn new(
        catalog: Arc<Catalog>,
        options: ChronicleOptions,
    ) -> Self {
        let metrics = match &options.meter {
            Some(meter) => Arc::new(ClientMetrics::new(meter)),
            None => Arc::new(ClientMetrics::noop()),
        };

        Self {
            pool: Arc::new(ConnPool::new(options.conn_opts.clone())),
            catalog,
            options,
            metrics,
        }
    }

    pub async fn open_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        Timeline::open(
            self.catalog.clone(),
            self.pool.clone(),
            name,
            self.options.replication_factor,
        )
        .await
    }

    pub async fn drop_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        self.catalog.delete_timeline(name).await?;
        info!(name, "timeline dropped");
        Ok(())
    }
}
