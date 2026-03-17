use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitRegistration};
use dashmap::DashMap;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use crate::error::CatalogError;

pub struct MemoryCatalog {
    timelines: Mutex<HashMap<String, TimelineMeta>>,
    segments: Mutex<HashMap<String, BTreeMap<i64, Segment>>>,
    units: DashMap<String, UnitRegistration>,
    next_timeline_id: AtomicI64,
    next_version: AtomicI64,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            timelines: Mutex::new(HashMap::new()),
            segments: Mutex::new(HashMap::new()),
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

impl MemoryCatalog {
    pub async fn get_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        self.timelines
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    pub async fn put_timeline(
        &self,
        meta: &TimelineMeta,
        expected_version: i64,
    ) -> Result<TimelineMeta, CatalogError> {
        let mut store = self.timelines.lock().unwrap();
        let existing = store
            .get(&meta.name)
            .ok_or_else(|| CatalogError::NotFound(meta.name.clone()))?;
        if existing.version != expected_version {
            return Err(CatalogError::VersionConflict {
                expected: expected_version,
                actual: existing.version,
            });
        }
        let mut updated = meta.clone();
        updated.version = self.next_version();
        store.insert(meta.name.clone(), updated.clone());
        Ok(updated)
    }

    pub async fn create_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let mut store = self.timelines.lock().unwrap();
        if store.contains_key(name) {
            return Err(CatalogError::AlreadyExists(name.to_string()));
        }
        let meta = TimelineMeta {
            name: name.to_string(),
            timeline_id: self.next_timeline_id.fetch_add(1, Ordering::SeqCst),
            version: self.next_version(),
            ..Default::default()
        };
        store.insert(name.to_string(), meta.clone());
        Ok(meta)
    }

    pub async fn delete_timeline(&self, name: &str) -> Result<(), CatalogError> {
        let mut store = self.timelines.lock().unwrap();
        store
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        self.segments.lock().unwrap().remove(name);
        Ok(())
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineMeta>, CatalogError> {
        Ok(self.timelines.lock().unwrap().values().cloned().collect())
    }

    pub async fn put_segment(
        &self,
        timeline_name: &str,
        segment: &Segment,
    ) -> Result<(), CatalogError> {
        let mut store = self.segments.lock().unwrap();
        store
            .entry(timeline_name.to_string())
            .or_default()
            .insert(segment.start_offset, segment.clone());
        Ok(())
    }

    pub async fn list_segments(&self, timeline_name: &str) -> Result<Vec<Segment>, CatalogError> {
        let store = self.segments.lock().unwrap();
        Ok(store
            .get(timeline_name)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default())
    }

    pub async fn get_last_segment(&self, timeline_name: &str) -> Result<Option<Segment>, CatalogError> {
        let store = self.segments.lock().unwrap();
        Ok(store
            .get(timeline_name)
            .and_then(|m| m.values().next_back().cloned()))
    }

    pub async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        self.units
            .insert(registration.address.clone(), registration.clone());
        Ok(())
    }

    pub async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        self.units
            .remove(address)
            .map(|_| ())
            .ok_or_else(|| CatalogError::NotFound(address.to_string()))
    }

    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
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
    async fn timeline_delete() {
        let catalog = MemoryCatalog::new();
        catalog.create_timeline("t1").await.unwrap();

        catalog.delete_timeline("t1").await.unwrap();
        let err = catalog.get_timeline("t1").await.unwrap_err();
        assert!(matches!(err, CatalogError::NotFound(_)));
    }

    #[tokio::test]
    async fn timeline_delete_not_found() {
        let catalog = MemoryCatalog::new();
        let err = catalog.delete_timeline("missing").await.unwrap_err();
        assert!(matches!(err, CatalogError::NotFound(_)));
    }

    #[tokio::test]
    async fn segments_crud() {
        let catalog = MemoryCatalog::new();
        catalog.create_timeline("t1").await.unwrap();

        let seg1 = Segment {
            ensemble: vec!["a".into(), "b".into()],
            start_offset: 1,
        };
        let seg2 = Segment {
            ensemble: vec!["a".into(), "c".into()],
            start_offset: 100,
        };

        catalog.put_segment("t1", &seg1).await.unwrap();
        catalog.put_segment("t1", &seg2).await.unwrap();

        let segments = catalog.list_segments("t1").await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_offset, 1);
        assert_eq!(segments[1].start_offset, 100);

        let last = catalog.get_last_segment("t1").await.unwrap().unwrap();
        assert_eq!(last.start_offset, 100);
    }

    #[tokio::test]
    async fn segments_deleted_with_timeline() {
        let catalog = MemoryCatalog::new();
        catalog.create_timeline("t1").await.unwrap();
        catalog.put_segment("t1", &Segment {
            ensemble: vec!["a".into()],
            start_offset: 1,
        }).await.unwrap();

        catalog.delete_timeline("t1").await.unwrap();
        let segments = catalog.list_segments("t1").await.unwrap();
        assert!(segments.is_empty());
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
}
