use crate::client::unit_client::UnitClient;
use crate::error::ChronicleError;
use crate::cursor::{TailCursor, TimelineCursor};
use crate::observability::ClientMetrics;
use crate::writer::reconciliation;
use crate::writer::timeline::Timeline;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{TimelineStatus, UnitStatus};
use opentelemetry::metrics::Meter;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Configuration for the top-level [`Chronicle`] client.
pub struct ChronicleConfig {
    pub replication_factor: usize,
    /// Optional OpenTelemetry meter for client-side metrics.
    /// If None, a no-op meter is used (zero overhead).
    pub meter: Option<Meter>,
    /// Interval for refreshing the unit list from catalog.
    /// Defaults to 30 seconds. Set to None to disable.
    pub refresh_interval: Option<Duration>,
    /// Pre-seed unit addresses to connect to directly.
    /// If set, these are used instead of catalog discovery.
    pub unit_addresses: Vec<String>,
}

/// Top-level client factory.
///
/// Owns the catalog handle and a pool of pre-connected unit clients.
/// Provides methods to create, open, and read timelines.
/// Periodically refreshes the unit list from the catalog.
pub struct Chronicle {
    catalog: Arc<dyn Catalog>,
    unit_clients: Arc<RwLock<HashMap<String, UnitClient>>>,
    config: ChronicleConfig,
    metrics: Arc<ClientMetrics>,
    _cancel: watch::Sender<()>,
    _refresh_handle: Option<JoinHandle<()>>,
}

impl Chronicle {
    /// Discover writable units from the catalog and connect to them.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        config: ChronicleConfig,
    ) -> Result<Self, ChronicleError> {
        let metrics = match &config.meter {
            Some(meter) => Arc::new(ClientMetrics::new(meter)),
            None => Arc::new(ClientMetrics::noop()),
        };

        let mut unit_clients = HashMap::new();
        if !config.unit_addresses.is_empty() {
            for addr in &config.unit_addresses {
                let client = UnitClient::connect(addr).await?;
                unit_clients.insert(addr.clone(), client);
                info!(address = %addr, "connected to unit (pre-configured)");
            }
        } else {
            let registrations = catalog.list_units().await?;
            for reg in &registrations {
                if reg.status() == UnitStatus::Writable {
                    let client = UnitClient::connect(&reg.address).await?;
                    unit_clients.insert(reg.address.clone(), client);
                    info!(address = %reg.address, "connected to unit");
                }
            }
        }

        let unit_clients = Arc::new(RwLock::new(unit_clients));
        let (cancel_tx, cancel_rx) = watch::channel(());

        // Skip background refresh when using pre-configured unit addresses,
        // since catalog discovery is bypassed and would incorrectly remove them.
        let refresh_handle = if config.unit_addresses.is_empty() {
            let interval = config.refresh_interval.unwrap_or(DEFAULT_REFRESH_INTERVAL);
            let catalog = catalog.clone();
            let clients = unit_clients.clone();
            Some(tokio::spawn(async move {
                bg_refresh_units(catalog, clients, cancel_rx, interval).await;
            }))
        } else {
            None
        };

        Ok(Self {
            catalog,
            unit_clients,
            config,
            metrics,
            _cancel: cancel_tx,
            _refresh_handle: refresh_handle,
        })
    }

    /// Create a brand-new timeline and return a writer.
    pub async fn create_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        let clients = self.unit_clients.read().await;
        Timeline::create(
            self.catalog.as_ref(),
            &clients,
            name,
            self.config.replication_factor,
        )
        .await
    }

    /// Open an existing timeline for writing.
    ///
    /// Runs the full reconciliation protocol (fence all units, truncate dirty
    /// entries) before returning a writer.
    pub async fn open_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        if tc.status() == TimelineStatus::Sealed {
            return Err(ChronicleError::Sealed(name.to_string()));
        }
        let clients = self.unit_clients.read().await;
        let reconciled =
            reconciliation::reconcile(self.catalog.as_ref(), &clients, name).await?;

        Timeline::open(
            reconciled,
            &clients,
            name,
            tc.timeline_id,
            self.config.replication_factor,
        )
        .await
    }

    /// Seal a timeline so no more events can be written.
    pub async fn seal_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        let mut updated = tc.clone();
        updated.status = TimelineStatus::Sealed.into();
        self.catalog.put_timeline(&updated, tc.version).await?;
        info!(name, "timeline sealed");
        Ok(())
    }

    /// Delete a timeline from the catalog.
    pub async fn delete_timeline(&self, name: &str) -> Result<(), ChronicleError> {
        self.catalog.delete_timeline(name).await?;
        info!(name, "timeline deleted");
        Ok(())
    }

    /// Check all connected units and return the addresses of any that are unresponsive.
    pub async fn detect_failed_units(&self) -> Vec<String> {
        let clients = self.unit_clients.read().await;
        let mut failed = Vec::new();
        for (addr, client) in clients.iter() {
            if !client.check_alive().await {
                failed.push(addr.clone());
            }
        }
        if !failed.is_empty() {
            warn!(failed = ?failed, "detected unresponsive units");
        }
        failed
    }

    /// Open a cursor for streaming events from a timeline.
    pub async fn open_cursor(
        &self,
        name: &str,
    ) -> Result<TimelineCursor, ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        let clients = self.unit_clients.read().await;
        Ok(TimelineCursor::new(
            tc.timeline_id,
            tc.segments,
            &clients,
        ))
    }

    /// Open a tail cursor that polls for new events instead of returning None.
    pub async fn open_tail_cursor(
        &self,
        name: &str,
    ) -> Result<TailCursor<TimelineCursor>, ChronicleError> {
        let cursor = self.open_cursor(name).await?;
        Ok(TailCursor::new(cursor))
    }
}

/// Periodically polls the catalog for unit changes and connects/disconnects
/// unit clients as needed.
async fn bg_refresh_units(
    catalog: Arc<dyn Catalog>,
    clients: Arc<RwLock<HashMap<String, UnitClient>>>,
    mut cancel: watch::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = cancel.changed() => break,
            _ = ticker.tick() => {
                let registrations = match catalog.list_units().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(error = %e, "failed to refresh unit list");
                        continue;
                    }
                };

                let writable: HashMap<String, _> = registrations
                    .into_iter()
                    .filter(|r| r.status() == UnitStatus::Writable)
                    .map(|r| (r.address.clone(), r))
                    .collect();

                let mut map = clients.write().await;

                // Remove units no longer in catalog.
                let to_remove: Vec<String> = map
                    .keys()
                    .filter(|addr| !writable.contains_key(*addr))
                    .cloned()
                    .collect();
                for addr in &to_remove {
                    map.remove(addr);
                    info!(address = %addr, "removed unit (no longer in catalog)");
                }

                // Connect to new units.
                for addr in writable.keys() {
                    if !map.contains_key(addr) {
                        match UnitClient::connect(addr).await {
                            Ok(client) => {
                                map.insert(addr.clone(), client);
                                info!(address = %addr, "connected to new unit");
                            }
                            Err(e) => {
                                warn!(address = %addr, error = %e, "failed to connect to new unit");
                            }
                        }
                    }
                }
            }
        }
    }
}
