use crate::conn::ConnPool;
use crate::cursor::TimelineCursor;
use crate::error::ChronicleError;
use crate::observability::ClientMetrics;
use crate::timeline::{self, Timeline};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{TimelineStatus, UnitStatus};
use opentelemetry::metrics::Meter;
use std::sync::Arc;
use tracing::info;

const DEFAULT_REPLICATION_FACTOR: usize = 3;
const DEFAULT_CONNS_PER_UNIT: usize = 4;

pub struct ChronicleOptions {
    pub(crate) replication_factor: usize,
    pub(crate) conns_per_unit: usize,
    meter: Option<Meter>,
}

impl Default for ChronicleOptions {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(DEFAULT_CONNS_PER_UNIT);
        Self {
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            conns_per_unit: cpus.max(1),
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
        self.conns_per_unit = n.max(1);
        self
    }

    pub fn meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }
}

pub struct Chronicle {
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) pool: Arc<ConnPool>,
    pub(crate) options: ChronicleOptions,
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
            pool: Arc::new(ConnPool::new(options.conns_per_unit)),
            catalog,
            options,
            metrics,
        }
    }

    async fn resolve_unit_conns(&self) -> Result<std::collections::HashMap<String, crate::conn::Conn>, ChronicleError> {
        let registrations = self.catalog.list_units().await?;
        let mut conns = std::collections::HashMap::new();
        for reg in &registrations {
            if reg.status() == UnitStatus::Writable {
                conns.insert(reg.address.clone(), self.pool.get_or_connect(&reg.address)?);
            }
        }
        Ok(conns)
    }

    pub async fn create_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        let conns = self.resolve_unit_conns().await?;
        Timeline::create(
            &*self.catalog,
            &conns,
            name,
            self.options.replication_factor,
        )
        .await
    }

    pub async fn open_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        if tc.status() == TimelineStatus::Sealed {
            return Err(ChronicleError::Sealed(name.to_string()));
        }
        let conns = self.resolve_unit_conns().await?;
        let reconciled =
            timeline::reconcile(&*self.catalog, &conns, name).await?;

        Timeline::open(
            reconciled,
            &conns,
            name,
            tc.timeline_id,
            self.options.replication_factor,
        )
        .await
    }

    pub async fn seal_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        let mut updated = tc.clone();
        updated.status = TimelineStatus::Sealed.into();
        self.catalog.put_timeline(&updated, tc.version).await?;
        info!(name, "timeline sealed");
        Ok(())
    }

    pub async fn delete_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        self.catalog.delete_timeline(name).await?;
        info!(name, "timeline deleted");
        Ok(())
    }

    pub async fn open_cursor(
        &self,
        name: &str,
    ) -> Result<TimelineCursor, ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        let conns = self.resolve_unit_conns().await?;
        Ok(TimelineCursor::new(
            tc.timeline_id,
            tc.segments,
            &conns,
        ))
    }

    pub async fn open_tail_cursor(
        &self,
        name: &str,
    ) -> Result<TimelineCursor, ChronicleError> {
        let cursor = self.open_cursor(name).await?;
        Ok(cursor.with_tail())
    }
}
