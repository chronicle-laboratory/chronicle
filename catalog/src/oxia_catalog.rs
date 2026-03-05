use async_trait::async_trait;
use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration};
use liboxia::client::OxiaClient;
use liboxia::client::PutOption;
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use prost::Message;
use tracing::debug;

use crate::Catalog;
use crate::error::CatalogError;

const KEY_PREFIX: &str = "/chronicle/timelines/";
const UNIT_PREFIX: &str = "/chronicle/units/";

pub struct OxiaCatalog {
    client: OxiaClient,
}

impl OxiaCatalog {
    pub async fn new(service_address: String, namespace: String) -> Result<Self, CatalogError> {
        let client = OxiaClientBuilder::new()
            .service_address(service_address)
            .namespace(namespace)
            .build()
            .await
            .map_err(|e| CatalogError::Transport(e.to_string()))?;
        Ok(Self { client })
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
}

#[async_trait]
impl Catalog for OxiaCatalog {
    async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        let key = Self::timeline_key(name);
        debug!("get_timeline: key={}", key);

        let result = self.client.get(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
            other => CatalogError::from(other),
        })?;

        let value = result
            .value
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        Self::decode_timeline(&value, result.version.version_id)
    }

    async fn put_timeline(
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

    async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        let catalog = TimelineCatalog {
            name: name.to_string(),
            ..Default::default()
        };
        let key = Self::timeline_key(name);
        let value = catalog.encode_to_vec();
        debug!("create_timeline: key={}", key);

        let result = self
            .client
            .put(key, value)
            .await
            .map_err(CatalogError::from)?;

        let mut created = catalog;
        created.version = result.version.version_id;
        Ok(created)
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError> {
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

    // ── Unit operations ──────────────────────────────────────────────────

    async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        let key = format!("{}{}", UNIT_PREFIX, registration.address);
        let value = registration.encode_to_vec();
        debug!("register_unit: key={}", key);

        self.client
            .put(key, value)
            .await
            .map_err(CatalogError::from)?;
        Ok(())
    }

    async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        let key = format!("{}{}", UNIT_PREFIX, address);
        debug!("unregister_unit: key={}", key);

        self.client.delete(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(address.to_string()),
            other => CatalogError::from(other),
        })?;
        Ok(())
    }

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        let min_key = UNIT_PREFIX.to_string();
        let max_key = format!("{}\x7f", UNIT_PREFIX);
        debug!("list_units: range=[{}, {})", min_key, max_key);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut units = Vec::with_capacity(result.records.len());
        for record in &result.records {
            if let Some(ref value) = record.value {
                let reg = UnitRegistration::decode(value.as_slice()).map_err(|e| {
                    CatalogError::Internal(format!("failed to decode unit registration: {}", e))
                })?;
                units.push(reg);
            }
        }
        Ok(units)
    }
}
