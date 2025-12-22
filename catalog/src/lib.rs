use crate::errors::CatalogError;
use chronicle_proto::pb_catalog::{Timeline, TimelineVerse, Unit, UnitMode};
use std::collections::HashMap;

mod errors;
mod memory;

type UnitId = String;
const CHRONICLE_ID_KEY: &str = "/chronicle/id";
const UNIT_COOKIE_PREFIX: &str = "/chronicle/units/meta/";
const UNIT_MODE_WRITE_READ_PREFIX: &str = "/chronicle/units/status/write_read/";
const UNIT_MODE_READONLY_PREFIX: &str = "/chronicle/units/status/readonly/";

#[async_trait::async_trait]
trait ChronicleResources {
    async fn create_chronicle(&self, id: String) -> Result<(), CatalogError>;
    async fn get_chronicle(&self) -> Result<String, CatalogError>;
}

#[async_trait::async_trait]
trait UnitResources {
    async fn create_unit(&self, unit_id: UnitId, unit: Unit) -> Result<(), CatalogError>;
    async fn scan_units(&self, mode: UnitMode) -> Result<Vec<(UnitId, Unit)>, CatalogError>;
    async fn get_unit(&self, unit_id: UnitId) -> Result<Unit, CatalogError>;
    async fn unit_online(&self, unit_id: UnitId) -> Result<(), CatalogError>;
    async fn unit_offline(&self, unit_id: UnitId) -> Result<(), CatalogError>;
}

type TimelineId = u64;
type TimelineTerm = u64;
const TIMELINE_NEW_TERM: TimelineTerm = 1;

#[async_trait::async_trait]
trait TimelineVerseResources {
    async fn create_timeline_verse(
        &self,
        verse_name: String,
        verse: TimelineVerse,
    ) -> Result<(), CatalogError>;
    async fn delete_timeline_verse(
        &self,
        verse_name: String,
        verse: TimelineVerse,
    ) -> Result<(), CatalogError>;

    async fn get_timeline_verse(&self, verse_name: String) -> Result<TimelineVerse, CatalogError>;
    async fn open_timeline(&self, verse_name: String, timeline_id: TimelineId, term: TimelineTerm);
    async fn get_timeline(
        &self,
        verse_name: String,
        timeline_id: TimelineId,
    ) -> Result<Timeline, CatalogError>;
    async fn destroy_timeline(
        &self,
        verse_name: String,
        timeline_id: TimelineId,
    ) -> Result<(), CatalogError>;
}

#[async_trait::async_trait]
trait TimelineVerseCursorResources {
    async fn create_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError>;
    async fn update_timeline_verse_cursor(
        &self,
        verse: String,
        cursor: String,
    ) -> Result<(), CatalogError>;
    async fn delete_timeline_verse_cursor(&self, verse: String) -> Result<(), CatalogError>;
    async fn advance_commit_offset(&self, offsets: HashMap<u64, u64>);
}
