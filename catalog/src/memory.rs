use crate::CatalogOptions;
use crate::catalog::{
    Catalog, ChronicleResources, TimelineVerseCursorResources, TimelineVerseResources,
    UnitResources,
};
use crate::errors::CatalogError;
use chronicle_proto::pb_catalog::{Timeline, TimelineVerse, Unit, UnitMode};
use std::collections::{BTreeMap, HashMap};

pub struct MemoryCatalog {
    store: BTreeMap<String, Vec<u8>>,
}

impl MemoryCatalog {
    pub async fn new(options: CatalogOptions) -> Result<Self, CatalogError> {
        todo!()
    }
}

impl Catalog for MemoryCatalog {}

#[async_trait::async_trait]
impl TimelineVerseCursorResources for MemoryCatalog {
    async fn create_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError> {
        todo!()
    }

    async fn update_timeline_verse_cursor(
        &self,
        verse: String,
        cursor: String,
    ) -> Result<(), CatalogError> {
        todo!()
    }

    async fn delete_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError> {
        todo!()
    }

    async fn advance_commit_offset(&self, offsets: HashMap<u64, u64>) {
        todo!()
    }
}

#[async_trait::async_trait]
impl TimelineVerseResources for MemoryCatalog {
    async fn create_timeline_verse(
        &self,
        verse_name: String,
        verse: TimelineVerse,
    ) -> Result<(), CatalogError> {
        todo!()
    }

    async fn delete_timeline_verse(
        &self,
        verse_name: String,
        verse: TimelineVerse,
    ) -> Result<(), CatalogError> {
        todo!()
    }

    async fn get_timeline_verse(&self, verse_name: String) -> Result<TimelineVerse, CatalogError> {
        todo!()
    }

    async fn open_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
        term: crate::catalog::TimelineTerm,
    ) {
        todo!()
    }

    async fn get_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
    ) -> Result<Timeline, CatalogError> {
        todo!()
    }

    async fn destroy_timeline(
        &self,
        verse_name: String,
        timeline_id: crate::catalog::TimelineId,
    ) -> Result<(), CatalogError> {
        todo!()
    }
}

#[async_trait::async_trait]
impl ChronicleResources for MemoryCatalog {
    async fn create_chronicle(&self, id: String) -> Result<(), CatalogError> {
        todo!()
    }

    async fn get_chronicle(&self) -> Result<String, CatalogError> {
        todo!()
    }
}

#[async_trait::async_trait]
impl UnitResources for MemoryCatalog {
    async fn create_unit(
        &self,
        unit_id: crate::catalog::UnitId,
        unit: Unit,
    ) -> Result<(), CatalogError> {
        todo!()
    }

    async fn scan_units(
        &self,
        mode: UnitMode,
    ) -> Result<Vec<(crate::catalog::UnitId, Unit)>, CatalogError> {
        todo!()
    }

    async fn get_unit(&self, unit_id: crate::catalog::UnitId) -> Result<Unit, CatalogError> {
        todo!()
    }

    async fn unit_online(&self, unit_id: crate::catalog::UnitId) -> Result<(), CatalogError> {
        todo!()
    }

    async fn unit_offline(&self, unit_id: crate::catalog::UnitId) -> Result<(), CatalogError> {
        todo!()
    }
}
