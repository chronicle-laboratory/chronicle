use async_trait::async_trait;
use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration};
use chronicle_proto::pb_logic::Schema;
use liboxia::client::OxiaClient;
use liboxia::client::PutOption;
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use prost::Message;
use tracing::debug;

use crate::Catalog;
use crate::error::CatalogError;

const KEY_PREFIX: &str = "/chronicle/timelines/";
const SCHEMA_PREFIX: &str = "/chronicle/schemas/";
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

    fn meta_key(name: &str) -> String {
        format!("{}{}/_meta", SCHEMA_PREFIX, name)
    }

    fn schema_version_key(name: &str, version: i32) -> String {
        format!("{}{}/versions/{:010}", SCHEMA_PREFIX, name, version)
    }

    fn decode_schema(value: &[u8]) -> Result<Schema, CatalogError> {
        Schema::decode(value)
            .map_err(|e| CatalogError::Internal(format!("failed to decode schema: {}", e)))
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

    async fn create_schema(&self, schema: &Schema) -> Result<Schema, CatalogError> {
        let key = Self::meta_key(&schema.name);
        debug!("create schema: key={}", key);

        // Check if already exists
        match self.client.get(key.clone()).await {
            Ok(_) => return Err(CatalogError::AlreadyExists(schema.name.clone())),
            Err(OxiaError::KeyNotFound()) => {} // expected
            Err(e) => return Err(CatalogError::from(e)),
        }

        let mut created = schema.clone();
        created.version = 1;
        created.created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let value = created.encode_to_vec();

        // Store meta
        self.client
            .put(key, value.clone())
            .await
            .map_err(CatalogError::from)?;

        // Store version 1
        let vkey = Self::schema_version_key(&created.name, 1);
        self.client
            .put(vkey, value)
            .await
            .map_err(CatalogError::from)?;

        Ok(created)
    }

    async fn get_schema(&self, name: &str, version: Option<i32>) -> Result<Schema, CatalogError> {
        let key = match version {
            Some(v) => Self::schema_version_key(name, v),
            None => Self::meta_key(name),
        };
        debug!("get schema: key={}", key);

        let result = self.client.get(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
            other => CatalogError::from(other),
        })?;

        let value = result
            .value
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        Self::decode_schema(&value)
    }

    async fn update_schema(&self, schema: &Schema) -> Result<Schema, CatalogError> {
        let key = Self::meta_key(&schema.name);
        debug!("update schema: key={}", key);

        // Get current to find version
        let current_result = self.client.get(key.clone()).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(schema.name.clone()),
            other => CatalogError::from(other),
        })?;

        let current_value = current_result
            .value
            .ok_or_else(|| CatalogError::NotFound(schema.name.clone()))?;
        let current = Self::decode_schema(&current_value)?;

        let mut updated = schema.clone();
        updated.version = current.version + 1;
        updated.created_at = current.created_at;

        let value = updated.encode_to_vec();

        // Update meta with version check
        self.client
            .put_with_options(
                key,
                value.clone(),
                vec![PutOption::ExpectVersionId(
                    current_result.version.version_id,
                )],
            )
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: current_result.version.version_id,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;

        // Store new version
        let vkey = Self::schema_version_key(&updated.name, updated.version);
        self.client
            .put(vkey, value)
            .await
            .map_err(CatalogError::from)?;

        Ok(updated)
    }

    async fn delete_schema(&self, name: &str) -> Result<(), CatalogError> {
        let key = Self::meta_key(name);
        debug!("delete schema: key={}", key);

        // Delete meta key
        self.client.delete(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
            other => CatalogError::from(other),
        })?;

        // Delete all version keys (best-effort scan and delete)
        let prefix = format!("{}{}/versions/", SCHEMA_PREFIX, name);
        let end = format!("{}\x7f", prefix);
        if let Ok(result) = self.client.range_scan(prefix, end).await {
            for record in &result.records {
                let _ = self.client.delete(record.key.clone()).await;
            }
        }

        Ok(())
    }

    async fn list_schemas(&self) -> Result<Vec<Schema>, CatalogError> {
        let min_key = SCHEMA_PREFIX.to_string();
        let max_key = format!("{}\x7f", SCHEMA_PREFIX);
        debug!("list schemas: range=[{}, {})", min_key, max_key);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut schemas = Vec::new();
        for record in &result.records {
            // Only include _meta keys to avoid duplicates from version keys
            if record.key.ends_with("/_meta") {
                if let Some(ref value) = record.value {
                    let schema = Self::decode_schema(value)?;
                    schemas.push(schema);
                }
            }
        }
        Ok(schemas)
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
