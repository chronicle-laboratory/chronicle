use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration, UnitRegistry};
use liboxia::client::{GetOption, OxiaClient, PutOption};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use prost::Message;
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::{debug, info, warn};

use crate::error::CatalogError;

fn hex_preview(data: &[u8]) -> String {
    data.iter().take(32).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ")
}

const KEY_PREFIX: &str = "/chronicle/timelines/";
const UNITS_KEY: &str = "/chronicle/units";

pub struct OxiaCatalog {
    client: OxiaClient,
    next_timeline_id: AtomicI64,
}

impl OxiaCatalog {
    pub async fn new(service_address: String, namespace: String) -> Result<Self, CatalogError> {
        let client = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            OxiaClientBuilder::new()
                .service_address(service_address)
                .namespace(namespace)
                .build(),
        )
        .await
        .map_err(|_| CatalogError::Transport("oxia client build timed out after 30s".into()))?
        .map_err(|e| CatalogError::Transport(e.to_string()))?;
        let catalog = Self {
            client,
            next_timeline_id: AtomicI64::new(1),
        };
        if let Ok(timelines) = catalog.list_timelines().await {
            let max_id = timelines.iter().map(|t| t.timeline_id).max().unwrap_or(0);
            catalog
                .next_timeline_id
                .store(max_id + 1, Ordering::SeqCst);
        }
        Ok(catalog)
    }

    fn timeline_key(name: &str) -> String {
        format!("{}{}", KEY_PREFIX, name)
    }

    fn decode_timeline(value: &[u8], version_id: i64) -> Result<TimelineCatalog, CatalogError> {
        let mut catalog = TimelineCatalog::decode(value)
            .map_err(|e| CatalogError::Internal(format!("failed to decode timeline: {}", e)))?;
        catalog.version = version_id;
        Ok(catalog)
    }

    pub async fn shutdown(self) -> Result<(), CatalogError> {
        self.client
            .shutdown()
            .await
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    pub async fn read_unit_registry(&self) -> Result<(UnitRegistry, i64), CatalogError> {
        match self.client.get_with_options(UNITS_KEY.to_string(), vec![GetOption::IncludeValue()]).await {
            Ok(result) => {
                let value = result.value.unwrap_or_default();
                info!(
                    value_len = value.len(),
                    value_hex = %hex_preview(&value),
                    version = result.version.version_id,
                    "read_unit_registry: got value"
                );
                let registry = UnitRegistry::decode(value.as_slice())
                    .map_err(|e| CatalogError::Internal(format!("failed to decode unit registry: {}", e)))?;
                Ok((registry, result.version.version_id))
            }
            Err(OxiaError::KeyNotFound()) => {
                Ok((UnitRegistry { units: vec![] }, -1))
            }
            Err(e) => Err(CatalogError::from(e)),
        }
    }

    pub async fn write_unit_registry(&self, registry: &UnitRegistry, expected_version: i64) -> Result<(), CatalogError> {
        let value = registry.encode_to_vec();
        self.client
            .put_with_options(
                UNITS_KEY.to_string(),
                value,
                vec![PutOption::ExpectVersionId(expected_version)],
            )
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;
        Ok(())
    }
}

impl OxiaCatalog {
    pub async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        let key = Self::timeline_key(name);
        debug!("get_timeline: key={}", key);

        let result = self
            .client
            .get_with_options(key, vec![GetOption::IncludeValue()])
            .await
            .map_err(|e| match e {
                OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
                other => CatalogError::from(other),
            })?;

        let value = result
            .value
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        Self::decode_timeline(&value, result.version.version_id)
    }

    pub async fn put_timeline(
        &self,
        catalog: &TimelineCatalog,
        expected_version: i64,
    ) -> Result<TimelineCatalog, CatalogError> {
        let key = Self::timeline_key(&catalog.name);
        let value = catalog.encode_to_vec();
        debug!(
            "put_timeline: key={}, expected_version={}",
            key, expected_version
        );

        let result = self
            .client
            .put_with_options(
                key,
                value,
                vec![PutOption::ExpectVersionId(expected_version)],
            )
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;

        let mut updated = catalog.clone();
        updated.version = result.version.version_id;
        Ok(updated)
    }

    pub async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        let timeline_id = self.next_timeline_id.fetch_add(1, Ordering::SeqCst);
        let catalog = TimelineCatalog {
            name: name.to_string(),
            timeline_id,
            ..Default::default()
        };
        let key = Self::timeline_key(name);
        let value = catalog.encode_to_vec();
        debug!("create_timeline: key={}, timeline_id={}", key, timeline_id);

        let result = self
            .client
            .put_with_options(key, value, vec![PutOption::ExpectVersionId(-1)])
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => {
                    CatalogError::AlreadyExists(name.to_string())
                }
                other => CatalogError::from(other),
            })?;

        let mut created = catalog;
        created.version = result.version.version_id;
        Ok(created)
    }

    pub async fn delete_timeline(&self, name: &str) -> Result<(), CatalogError> {
        let key = Self::timeline_key(name);
        debug!("delete_timeline: key={}", key);

        self.client.delete(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
            other => CatalogError::from(other),
        })?;
        Ok(())
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError> {
        let min_key = KEY_PREFIX.to_string();
        let max_key = format!("{}\x7f", KEY_PREFIX);
        debug!("list_timelines: range=[{}, {})", min_key, max_key);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut timelines = Vec::with_capacity(result.records.len());
        for record in &result.records {
            if let Some(ref value) = record.value {
                let timeline = Self::decode_timeline(value, record.version.version_id)?;
                timelines.push(timeline);
            }
        }
        Ok(timelines)
    }

    pub async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        info!(address = %registration.address, "register_unit: starting");

        for attempt in 0..10 {
            let (mut registry, version) = self.read_unit_registry().await?;
            info!(
                attempt,
                existing_count = registry.units.len(),
                version,
                "register_unit: read registry"
            );

            registry.units.retain(|u| u.address != registration.address);
            registry.units.push(registration.clone());

            info!(
                new_count = registry.units.len(),
                "register_unit: writing registry"
            );

            match self.write_unit_registry(&registry, version).await {
                Ok(()) => {
                    info!(address = %registration.address, "register_unit: success");
                    return Ok(());
                }
                Err(CatalogError::VersionConflict { .. }) => {
                    warn!("register_unit: CAS conflict on attempt {}, retrying", attempt);
                    if attempt >= 5 {
                        let value = registry.encode_to_vec();
                        info!("register_unit: falling back to unconditional put");
                        self.client
                            .put(UNITS_KEY.to_string(), value)
                            .await
                            .map_err(CatalogError::from)?;
                        info!(address = %registration.address, "register_unit: success (unconditional)");
                        return Ok(());
                    }
                    continue;
                }
                Err(e) => {
                    warn!(error = %e, "register_unit: write failed");
                    return Err(e);
                }
            }
        }
        Err(CatalogError::Internal("register_unit: too many CAS retries".into()))
    }

    pub async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        debug!("unregister_unit: address={}", address);

        for _ in 0..10 {
            let (mut registry, version) = self.read_unit_registry().await?;

            let before = registry.units.len();
            registry.units.retain(|u| u.address != address);
            if registry.units.len() == before {
                return Err(CatalogError::NotFound(address.to_string()));
            }

            match self.write_unit_registry(&registry, version).await {
                Ok(()) => return Ok(()),
                Err(CatalogError::VersionConflict { .. }) => {
                    debug!("unregister_unit: CAS conflict, retrying");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(CatalogError::Internal("unregister_unit: too many CAS retries".into()))
    }

    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        debug!("list_units");
        let (registry, _version) = self.read_unit_registry().await?;
        Ok(registry.units)
    }
}
