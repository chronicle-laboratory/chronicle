use crate::conn::UnitClient;
use crate::error::ChronicleError;
use crate::observability::ClientMetrics;
use crate::cursor::TimelineCursor;
use crate::timeline::{self, Timeline};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{TimelineStatus, UnitStatus};
use opentelemetry::metrics::Meter;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const DEFAULT_REPLICATION_FACTOR: usize = 3;
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

pub struct ChronicleOptions {
    pub(crate) replication_factor: usize,
    meter: Option<Meter>,
    refresh_interval: Duration,
    unit_addresses: Vec<String>,
}

impl Default for ChronicleOptions {
    fn default() -> Self {
        Self {
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            meter: None,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            unit_addresses: Vec::new(),
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

    pub fn meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }

    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    pub fn unit_addresses(mut self, addresses: Vec<String>) -> Self {
        self.unit_addresses = addresses;
        self
    }
}

pub struct Chronicle {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) unit_clients: Arc<RwLock<HashMap<String, UnitClient>>>,
    pub(crate) options: ChronicleOptions,
    #[allow(dead_code)]
    metrics: Arc<ClientMetrics>,
    _cancel: watch::Sender<()>,
    _refresh_handle: Option<JoinHandle<()>>,
}

impl Chronicle {
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        options: ChronicleOptions,
    ) -> Result<Self, ChronicleError> {
        let metrics = match &options.meter {
            Some(meter) => Arc::new(ClientMetrics::new(meter)),
            None => Arc::new(ClientMetrics::noop()),
        };

        let mut unit_clients = HashMap::new();
        if !options.unit_addresses.is_empty() {
            for addr in &options.unit_addresses {
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

        let refresh_handle = if options.unit_addresses.is_empty() {
            let interval = options.refresh_interval;
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
            options,
            metrics,
            _cancel: cancel_tx,
            _refresh_handle: refresh_handle,
        })
    }

    pub async fn create_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        let clients = self.unit_clients.read().await;
        Timeline::create(
            self.catalog.as_ref(),
            &clients,
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
        let clients = self.unit_clients.read().await;
        let reconciled =
            timeline::reconcile(self.catalog.as_ref(), &clients, name).await?;

        Timeline::open(
            reconciled,
            &clients,
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
        let clients = self.unit_clients.read().await;
        Ok(TimelineCursor::new(
            tc.timeline_id,
            tc.segments,
            &clients,
        ))
    }

    pub async fn open_tail_cursor(
        &self,
        name: &str,
    ) -> Result<TimelineCursor, ChronicleError> {
        let cursor = self.open_cursor(name).await?;
        Ok(cursor.with_tail())
    }

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
}

async fn bg_refresh_units(
    catalog: Arc<dyn Catalog>,
    clients: Arc<RwLock<HashMap<String, UnitClient>>>,
    mut cancel: watch::Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;

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

                let to_remove: Vec<String> = map
                    .keys()
                    .filter(|addr| !writable.contains_key(*addr))
                    .cloned()
                    .collect();
                for addr in &to_remove {
                    map.remove(addr);
                    info!(address = %addr, "removed unit (no longer in catalog)");
                }

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
