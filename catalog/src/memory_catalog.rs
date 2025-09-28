use async_trait::async_trait;
use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration};
use chronicle_proto::pb_logic::Schema;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use crate::Catalog;
use crate::error::CatalogError;

/// In-memory [`Catalog`] implementation.
///
/// Useful for testing and examples where an Oxia cluster is not available.
/// All state lives in process memory and is lost on drop.
pub struct MemoryCatalog {
    timelines: Mutex<HashMap<String, TimelineCatalog>>,
    schemas: Mutex<HashMap<String, Vec<Schema>>>,
    units: DashMap<String, UnitRegistration>,
    next_timeline_id: AtomicI64,
    next_version: AtomicI64,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            timelines: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            units: DashMap::new(),
            next_timeline_id: AtomicI64::new(1),
            next_version: AtomicI64::new(1),
        }
    }

    fn next_version(&self) -> i64 {
        self.next_version.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    // ── Timeline operations ─────────────────────────────────────────────

    async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        self.timelines
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    async fn put_timeline(
        &self,
        catalog: &TimelineCatalog,
        expected_version: i64,
    ) -> Result<TimelineCatalog, CatalogError> {
        let mut store = self.timelines.lock().unwrap();
        let existing = store
            .get(&catalog.name)
            .ok_or_else(|| CatalogError::NotFound(catalog.name.clone()))?;
        if existing.version != expected_version {
            return Err(CatalogError::VersionConflict {
                expected: expected_version,
                actual: existing.version,
            });
        }
        let mut updated = catalog.clone();
        updated.version = self.next_version();
        store.insert(catalog.name.clone(), updated.clone());
        Ok(updated)
    }

    async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        let mut store = self.timelines.lock().unwrap();
        if store.contains_key(name) {
            return Err(CatalogError::AlreadyExists(name.to_string()));
        }
        let tc = TimelineCatalog {
            name: name.to_string(),
            timeline_id: self.next_timeline_id.fetch_add(1, Ordering::SeqCst),
            version: self.next_version(),
            ..Default::default()
        };
        store.insert(name.to_string(), tc.clone());
        Ok(tc)
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError> {
        Ok(self.timelines.lock().unwrap().values().cloned().collect())
    }

    // ── Schema operations ───────────────────────────────────────────────

    async fn create_schema(&self, schema: &Schema) -> Result<Schema, CatalogError> {
        let mut store = self.schemas.lock().unwrap();
        if store.contains_key(&schema.name) {
            return Err(CatalogError::AlreadyExists(schema.name.clone()));
        }
        let mut created = schema.clone();
        created.version = 1;
        created.created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        store.insert(schema.name.clone(), vec![created.clone()]);
        Ok(created)
    }

    async fn get_schema(&self, name: &str, version: Option<i32>) -> Result<Schema, CatalogError> {
        let store = self.schemas.lock().unwrap();
        let versions = store
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        match version {
            Some(v) => versions
                .iter()
                .find(|s| s.version == v)
                .cloned()
                .ok_or_else(|| CatalogError::NotFound(format!("{}@v{}", name, v))),
            None => versions
                .last()
                .cloned()
                .ok_or_else(|| CatalogError::NotFound(name.to_string())),
        }
    }

    async fn update_schema(&self, schema: &Schema) -> Result<Schema, CatalogError> {
        let mut store = self.schemas.lock().unwrap();
        let versions = store
            .get_mut(&schema.name)
            .ok_or_else(|| CatalogError::NotFound(schema.name.clone()))?;
        let current = versions
            .last()
            .ok_or_else(|| CatalogError::NotFound(schema.name.clone()))?;
        let mut updated = schema.clone();
        updated.version = current.version + 1;
        updated.created_at = current.created_at;
        versions.push(updated.clone());
        Ok(updated)
    }

    async fn delete_schema(&self, name: &str) -> Result<(), CatalogError> {
        let mut store = self.schemas.lock().unwrap();
        store
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    async fn list_schemas(&self) -> Result<Vec<Schema>, CatalogError> {
        let store = self.schemas.lock().unwrap();
        Ok(store
            .values()
            .filter_map(|versions| versions.last().cloned())
            .collect())
    }

    // ── Unit operations ──────────────────────────────────────────────────

    async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        self.units
            .insert(registration.address.clone(), registration.clone());
        Ok(())
    }

    async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        self.units
            .remove(address)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(address.to_string()))
    }

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        Ok(self.units.iter().map(|r| r.value().clone()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeline_create_get_put() {
        let catalog = MemoryCatalog::new();

        let tc = catalog.create_timeline("t1").await.unwrap();
        assert_eq!(tc.name, "t1");
        assert_eq!(tc.timeline_id, 1);

        let fetched = catalog.get_timeline("t1").await.unwrap();
        assert_eq!(fetched.version, tc.version);

        let mut updated = fetched.clone();
        updated.term = 5;
        let result = catalog.put_timeline(&updated, fetched.version).await.unwrap();
        assert_eq!(result.term, 5);
        assert_ne!(result.version, fetched.version);
    }

    #[tokio::test]
    async fn timeline_version_conflict() {
        let catalog = MemoryCatalog::new();
        let tc = catalog.create_timeline("t1").await.unwrap();

        let err = catalog.put_timeline(&tc, tc.version + 999).await.unwrap_err();
        assert!(matches!(err, CatalogError::VersionConflict { .. }));
    }

    #[tokio::test]
    async fn timeline_already_exists() {
        let catalog = MemoryCatalog::new();
        catalog.create_timeline("t1").await.unwrap();

        let err = catalog.create_timeline("t1").await.unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn unit_register_list_unregister() {
        use chronicle_proto::pb_catalog::UnitStatus;

        let catalog = MemoryCatalog::new();

        let reg = UnitRegistration {
            address: "http://127.0.0.1:7070".into(),
            status: UnitStatus::Writable.into(),
        };
        catalog.register_unit(&reg).await.unwrap();

        let units = catalog.list_units().await.unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].address, "http://127.0.0.1:7070");

        catalog
            .unregister_unit("http://127.0.0.1:7070")
            .await
            .unwrap();
        let units = catalog.list_units().await.unwrap();
        assert!(units.is_empty());
    }

    #[tokio::test]
    async fn unit_unregister_not_found() {
        let catalog = MemoryCatalog::new();
        let err = catalog.unregister_unit("http://missing:1234").await.unwrap_err();
        assert!(matches!(err, CatalogError::NotFound(_)));
    }

    #[tokio::test]
    async fn schema_lifecycle() {
        let catalog = MemoryCatalog::new();

        let schema = Schema {
            name: "s1".into(),
            definition: "{}".into(),
            ..Default::default()
        };
        let created = catalog.create_schema(&schema).await.unwrap();
        assert_eq!(created.version, 1);

        let fetched = catalog.get_schema("s1", None).await.unwrap();
        assert_eq!(fetched.version, 1);

        let updated = catalog.update_schema(&schema).await.unwrap();
        assert_eq!(updated.version, 2);

        let v1 = catalog.get_schema("s1", Some(1)).await.unwrap();
        assert_eq!(v1.version, 1);

        let all = catalog.list_schemas().await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].version, 2);

        catalog.delete_schema("s1").await.unwrap();
        assert!(catalog.get_schema("s1", None).await.is_err());
    }
}
