use crate::catalog::{
    Catalog, ChronicleResources, TimelineId, TimelineVerseCursorResources,
    ParallelTimelineResources, UnitId, UnitResources,
};
use crate::errors::CatalogError;
use crate::CatalogOptions;
use chronicle_proto::pb_catalog::{Timeline, TimelineVerse, Unit, UnitMode};
use std::collections::HashMap;
use tokio::sync::RwLock;

// In-memory catalog implementation
pub struct MemoryCatalog {
    chronicle_id: RwLock<Option<String>>,
    units: RwLock<HashMap<UnitId, Unit>>,
    timeline_verses: RwLock<HashMap<String, TimelineVerse>>,
    // Main Timelines storage, verse_name -> timeline_id -> Timeline
    timelines: RwLock<HashMap<String, HashMap<TimelineId, Timeline>>>,
    // Cursors for each verse, verse_name -> cursor
    cursors: RwLock<HashMap<String, String>>,
}

impl MemoryCatalog {
    pub async fn new(_options: CatalogOptions) -> Result<Self, CatalogError> {
        Ok(MemoryCatalog {
            chronicle_id: RwLock::new(None),
            units: RwLock::new(HashMap::new()),
            timeline_verses: RwLock::new(HashMap::new()),
            timelines: RwLock::new(HashMap::new()),
            cursors: RwLock::new(HashMap::new()),
        })
    }
}

impl Catalog for MemoryCatalog {}

#[async_trait::async_trait]
impl TimelineVerseCursorResources for MemoryCatalog {
    async fn create_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError> {
        self.cursors.write().await.insert(verse, String::new());
        Ok(())
    }

    async fn update_timeline_verse_cursor(
        &self,
        verse: String,
        cursor: String,
    ) -> Result<(), CatalogError> {
        self.cursors.write().await.insert(verse, cursor);
        Ok(())
    }

    async fn delete_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError> {
        self.cursors.write().await.remove(&verse);
        Ok(())
    }

    async fn advance_commit_offset(&self, _offsets: HashMap<u64, u64>) {
        // Not applicable for in-memory model, but required by trait.
    }
}

#[async_trait::async_trait]
impl ParallelTimelineResources for MemoryCatalog {
    async fn create_parallel_timeline(
        &self,
        verse_name: String,
        verse: TimelineVerse,
    ) -> Result<(), CatalogError> {
        self.timeline_verses
            .write()
            .await
            .insert(verse_name.clone(), verse);
        self.timelines
            .write()
            .await
            .insert(verse_name, HashMap::new());
        Ok(())
    }

    async fn delete_parallel_timeline(
        &self,
        verse_name: String,
        _verse: TimelineVerse,
    ) -> Result<(), CatalogError> {
        self.timeline_verses.write().await.remove(&verse_name);
        self.timelines.write().await.remove(&verse_name);
        Ok(())
    }

    async fn get_parallel_timeline(&self, verse_name: String) -> Result<TimelineVerse, CatalogError> {
        self.timeline_verses
            .read()
            .await
            .get(&verse_name)
            .cloned()
            .ok_or_else(|| CatalogError::VerseNotFound {
                verse: verse_name.clone(),
            })
    }

    async fn open_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
        term: crate::catalog::TimelineTerm,
    ) {
        let mut timelines = self.timelines.write().await;
        let verse_timelines = timelines.entry(verse_name).or_insert_with(HashMap::new);
        verse_timelines.entry(timeline_id).or_insert(Timeline {
            id: timeline_id,
            term,
            ..Default::default()
        });
    }

    async fn get_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
    ) -> Result<Timeline, CatalogError> {
        self.timelines
            .read()
            .await
            .get(&verse_name)
            .and_then(|verse_timelines| verse_timelines.get(&timeline_id).cloned())
            .ok_or_else(|| CatalogError::TimelineNotFound {
                id: timeline_id,
                verse: verse_name,
            })
    }

    async fn destroy_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
    ) -> Result<(), CatalogError> {
        if let Some(verse_timelines) = self.timelines.write().await.get_mut(&verse_name) {
            verse_timelines.remove(&timeline_id);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ChronicleResources for MemoryCatalog {
    async fn create_chronicle(&self, id: String) -> Result<(), CatalogError> {
        *self.chronicle_id.write().await = Some(id);
        Ok(())
    }

    async fn get_chronicle(&self) -> Result<String, CatalogError> {
        self.chronicle_id
            .read()
            .await
            .clone()
            .ok_or(CatalogError::ChronicleNotFound)
    }
}

#[async_trait::async_trait]
impl UnitResources for MemoryCatalog {
    async fn create_unit(&self, unit_id: UnitId, unit: Unit) -> Result<(), CatalogError> {
        self.units.write().await.insert(unit_id, unit);
        Ok(())
    }

    async fn scan_units(
        &self,
        mode: UnitMode,
    ) -> Result<Vec<(crate::catalog::UnitId, Unit)>, CatalogError> {
        let units = self.units.read().await;
        let result = units
            .iter()
            .filter(|(_, unit)| {
                unit.mode == i32::from(mode)
                    || mode == UnitMode::All
                    || (mode == UnitMode::WriteRead && unit.mode == i32::from(UnitMode::WriteRead))
                    || (mode == UnitMode::Readonly && unit.mode == i32::from(UnitMode::Readonly))
            })
            .map(|(id, unit)| (id.clone(), unit.clone()))
            .collect();
        Ok(result)
    }

    async fn get_unit(&self, unit_id: UnitId) -> Result<Unit, CatalogError> {
        self.units
            .read()
            .await
            .get(&unit_id)
            .cloned()
            .ok_or(CatalogError::UnitNotFound)
    }

    async fn unit_online(&self, unit_id: UnitId) -> Result<(), CatalogError> {
        if let Some(unit) = self.units.write().await.get_mut(&unit_id) {
            unit.online = true;
            Ok(())
        } else {
            Err(CatalogError::UnitNotFound)
        }
    }

    async fn unit_offline(&self, unit_id: UnitId) -> Result<(), CatalogError> {
        if let Some(unit) = self.units.write().await.get_mut(&unit_id) {
            unit.online = false;
            Ok(())
        } else {
            Err(CatalogError::UnitNotFound)
        }
    }
}
