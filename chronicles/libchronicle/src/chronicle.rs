use crate::client::unit_client::UnitClient;
use crate::error::ChronicleError;
use crate::cursor::TimelineCursor;
use crate::writer::reconciliation;
use crate::writer::timeline::Timeline;
use catalog::Catalog;
use chronicle_proto::pb_catalog::UnitStatus;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Configuration for the top-level [`Chronicle`] client.
pub struct ChronicleConfig {
    pub replication_factor: usize,
}

/// Top-level client factory.
///
/// Owns the catalog handle and a pool of pre-connected unit clients.
/// Provides methods to create, open, and read timelines.
pub struct Chronicle {
    catalog: Arc<dyn Catalog>,
    unit_clients: HashMap<String, UnitClient>,
    config: ChronicleConfig,
}

impl Chronicle {
    /// Discover writable units from the catalog and connect to them.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        config: ChronicleConfig,
    ) -> Result<Self, ChronicleError> {
        let registrations = catalog.list_units().await?;

        let mut unit_clients = HashMap::new();
        for reg in &registrations {
            if reg.status() == UnitStatus::Writable {
                let client = UnitClient::connect(&reg.address).await?;
                unit_clients.insert(reg.address.clone(), client);
                info!(address = %reg.address, "connected to unit");
            }
        }
        Ok(Self {
            catalog,
            unit_clients,
            config,
        })
    }

    /// Create a brand-new timeline and return a writer.
    pub async fn create_timeline(&self, name: &str) -> Result<Timeline, ChronicleError> {
        Timeline::create(
            self.catalog.as_ref(),
            &self.unit_clients,
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
        let reconciled =
            reconciliation::reconcile(self.catalog.as_ref(), &self.unit_clients, name).await?;

        Timeline::open(
            reconciled,
            &self.unit_clients,
            name,
            tc.timeline_id,
            self.config.replication_factor,
        )
        .await
    }

    /// Open a cursor for streaming events from a timeline.
    pub async fn open_cursor(
        &self,
        name: &str,
    ) -> Result<TimelineCursor, ChronicleError> {
        let tc = self.catalog.get_timeline(name).await?;
        Ok(TimelineCursor::new(
            tc.timeline_id,
            tc.segments,
            &self.unit_clients,
        ))
    }
}
