use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitRegistration, UnitRegistry};
use liboxia::client::{GetOption, OxiaClient, PutOption};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use prost::Message;
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::{debug, info, warn};

use crate::Versioned;
use crate::error::CatalogError;

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

    fn meta_key(name: &str) -> String {
        format!("{}{}", KEY_PREFIX, name)
    }

    fn decode_meta(value: &[u8], version_id: i64) -> Result<TimelineMeta, CatalogError> {
        let mut meta = TimelineMeta::decode(value)
            .map_err(|e| CatalogError::Internal(format!("failed to decode timeline: {}", e)))?;
        meta.version = version_id;
        Ok(meta)
    }

    async fn read_unit_registry(&self) -> Result<(UnitRegistry, i64), CatalogError> {
        match self.client.get_with_options(UNITS_KEY.to_string(), vec![GetOption::IncludeValue()]).await {
            Ok(result) => {
                let value = result.value.unwrap_or_default();
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

    async fn write_unit_registry(&self, registry: &UnitRegistry, expected_version: i64) -> Result<(), CatalogError> {
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
    pub async fn get_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let key = Self::meta_key(name);
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
        Self::decode_meta(&value, result.version.version_id)
    }

    pub async fn put_timeline(
        &self,
        meta: &TimelineMeta,
        expected_version: i64,
    ) -> Result<TimelineMeta, CatalogError> {
        let key = Self::meta_key(&meta.name);
        let value = meta.encode_to_vec();

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

        let mut updated = meta.clone();
        updated.version = result.version.version_id;
        Ok(updated)
    }

    pub async fn create_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let timeline_id = self.next_timeline_id.fetch_add(1, Ordering::SeqCst);
        let meta = TimelineMeta {
            name: name.to_string(),
            timeline_id,
            status: chronicle_proto::pb_catalog::TimelineStatus::Active as i32,
            term: 0,
            lra: 0,
            version: 0,
        };
        let key = Self::meta_key(name);
        let value = meta.encode_to_vec();

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

        let mut created = meta;
        created.version = result.version.version_id;
        Ok(created)
    }

    pub async fn delete_timeline(&self, name: &str, expected_version: i64) -> Result<(), CatalogError> {
        let key = Self::meta_key(name);
        self.client
            .delete_with_options(
                key,
                vec![liboxia::client::DeleteOption::ExpectVersionId(expected_version)],
            )
            .await
            .map_err(|e| match e {
                OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;
        // TODO: also delete segment keys
        Ok(())
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineMeta>, CatalogError> {
        let min_key = KEY_PREFIX.to_string();
        let max_key = format!("{}\x7f", KEY_PREFIX);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut timelines = Vec::with_capacity(result.records.len());
        for record in &result.records {
            // Skip segment keys (contain /seg/)
            if record.key.contains("/seg/") {
                continue;
            }
            if let Some(ref value) = record.value {
                let meta = Self::decode_meta(value, record.version.version_id)?;
                timelines.push(meta);
            }
        }
        Ok(timelines)
    }

    pub async fn put_segment(
        &self,
        timeline_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, CatalogError> {
        let key = crate::segment_key(timeline_name, segment.start_offset);
        let value = segment.encode_to_vec();
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

        Ok(Versioned::new(segment.clone(), result.version.version_id))
    }

    pub async fn list_segments(&self, timeline_name: &str) -> Result<Vec<Versioned<Segment>>, CatalogError> {
        let min_key = crate::segment_key_prefix(timeline_name);
        let max_key = crate::segment_key_max(timeline_name);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut segments = Vec::with_capacity(result.records.len());
        for record in &result.records {
            if let Some(ref value) = record.value {
                let seg = Segment::decode(value.as_slice())
                    .map_err(|e| CatalogError::Internal(format!("failed to decode segment: {}", e)))?;
                segments.push(Versioned::new(seg, record.version.version_id));
            }
        }
        Ok(segments)
    }

    pub async fn get_last_segment(&self, timeline_name: &str) -> Result<Option<Versioned<Segment>>, CatalogError> {
        let segments = self.list_segments(timeline_name).await?;
        Ok(segments.into_iter().last())
    }

    pub async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        info!(address = %registration.address, "register_unit: starting");

        for attempt in 0..10 {
            let (mut registry, version) = self.read_unit_registry().await?;

            registry.units.retain(|u| u.address != registration.address);
            registry.units.push(registration.clone());

            match self.write_unit_registry(&registry, version).await {
                Ok(()) => {
                    info!(address = %registration.address, "register_unit: success");
                    return Ok(());
                }
                Err(CatalogError::VersionConflict { .. }) => {
                    warn!("register_unit: CAS conflict on attempt {}, retrying", attempt);
                    if attempt >= 5 {
                        let value = registry.encode_to_vec();
                        self.client
                            .put(UNITS_KEY.to_string(), value)
                            .await
                            .map_err(CatalogError::from)?;
                        return Ok(());
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(CatalogError::Internal("register_unit: too many CAS retries".into()))
    }

    pub async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        for _ in 0..10 {
            let (mut registry, version) = self.read_unit_registry().await?;

            let before = registry.units.len();
            registry.units.retain(|u| u.address != address);
            if registry.units.len() == before {
                return Err(CatalogError::NotFound(address.to_string()));
            }

            match self.write_unit_registry(&registry, version).await {
                Ok(()) => return Ok(()),
                Err(CatalogError::VersionConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(CatalogError::Internal("unregister_unit: too many CAS retries".into()))
    }

    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        let (registry, _version) = self.read_unit_registry().await?;
        Ok(registry.units)
    }
}
