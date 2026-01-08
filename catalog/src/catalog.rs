use crate::errors::CatalogError;
use chronicle_proto::pb_catalog::{
    ParallelTimeline, ParallelTimelineCursor, Timeline, Unit, UnitMode,
};
use std::collections::HashMap;

pub type UnitId = String;
pub const CHRONICLE_ID_KEY: &str = "/chronicle/id";
pub const UNIT_COOKIE_PREFIX: &str = "/chronicle/units/meta/";
pub const UNIT_MODE_WRITE_READ_PREFIX: &str = "/chronicle/units/status/write_read/";
pub const UNIT_MODE_READONLY_PREFIX: &str = "/chronicle/units/status/readonly/";

#[async_trait::async_trait]
pub trait Catalog:
    ChronicleResources
    + UnitResources
    + ParallelTimelineResources
    + TimelineResources
    + TransactionResources
    + ParallelTimelineCursorResources
    + Send
    + Sync
{
}

#[async_trait::async_trait]
pub trait ChronicleResources {
    async fn create_chronicle(&self, id: String) -> Result<(), CatalogError>;
    async fn get_chronicle(&self) -> Result<String, CatalogError>;
}

#[async_trait::async_trait]
pub trait UnitResources {
    async fn create_unit(&self, unit_id: UnitId, unit: Unit) -> Result<(), CatalogError>;
    async fn scan_units(&self, mode: UnitMode) -> Result<Vec<(UnitId, Unit)>, CatalogError>;
    async fn get_unit(&self, unit_id: UnitId) -> Result<Unit, CatalogError>;
    async fn unit_online(&self, unit_id: UnitId) -> Result<(), CatalogError>;
    async fn unit_offline(&self, unit_id: UnitId) -> Result<(), CatalogError>;
}

pub type TimelineId = u64;
pub type TimelineTerm = u64;
pub const TIMELINE_NEW_TERM: TimelineTerm = 1;

#[async_trait::async_trait]
pub trait ParallelTimelineResources {
    async fn create_parallel_timeline(
        &self,
        chron_name: String,
        pt_name: String,
        pt: ParallelTimeline,
    ) -> Result<(), CatalogError>;
    async fn delete_parallel_timeline(
        &self,
        chron_name: String,
        pt_name: String,
    ) -> Result<(), CatalogError>;
    async fn get_parallel_timeline(
        &self,
        chron_name: String,
        pt_name: String,
    ) -> Result<ParallelTimeline, CatalogError>;
    async fn list_parallel_timelines(&self, chron_name: String) -> Result<Vec<String>, CatalogError>;
}

#[async_trait::async_trait]
pub trait TimelineResources {
    async fn create_timeline(
        &self,
        chron_name: String,
        pt_name: String,
        tl_name: String,
        timeline: Timeline,
    ) -> Result<(), CatalogError>;
    async fn get_timeline(
        &self,
        chron_name: String,
        pt_name: String,
        tl_name: String,
    ) -> Result<Timeline, CatalogError>;
    async fn list_timelines(
        &self,
        chron_name: String,
        pt_name: String,
    ) -> Result<Vec<String>, CatalogError>;
    async fn delete_timeline(
        &self,
        chron_name: String,
        pt_name: String,
        tl_name: String,
    ) -> Result<(), CatalogError>;
}

#[async_trait::async_trait]
pub trait TransactionResources {
    async fn create_transaction(
        &self,
        chron_name: String,
        txn_id: String,
        metadata: HashMap<String, String>,
    ) -> Result<(), CatalogError>;
    async fn get_transaction(
        &self,
        chron_name: String,
        txn_id: String,
    ) -> Result<HashMap<String, String>, CatalogError>;
    async fn list_transactions(&self, chron_name: String) -> Result<Vec<String>, CatalogError>;
    async fn delete_transaction(
        &self,
        chron_name: String,
        txn_id: String,
    ) -> Result<(), CatalogError>;
}

#[async_trait::async_trait]
pub trait ParallelTimelineCursorResources {
    async fn create_cursor(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
        cursor: ParallelTimelineCursor,
    ) -> Result<(), CatalogError>;
    async fn get_cursor(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
    ) -> Result<ParallelTimelineCursor, CatalogError>;
    async fn list_cursors(
        &self,
        chron_name: String,
        pt_name: String,
    ) -> Result<Vec<String>, CatalogError>;
    async fn delete_cursor(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
    ) -> Result<(), CatalogError>;
    async fn get_read_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
    ) -> Result<i64, CatalogError>;
    async fn set_read_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
        pos: i64,
    ) -> Result<(), CatalogError>;
    async fn get_ack_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
    ) -> Result<i64, CatalogError>;
    async fn set_ack_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
        pos: i64,
    ) -> Result<(), CatalogError>;
    async fn get_commit_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
    ) -> Result<i64, CatalogError>;
    async fn set_commit_pos(
        &self,
        chron_name: String,
        pt_name: String,
        cursor_name: String,
        pos: i64,
    ) -> Result<(), CatalogError>;
}
