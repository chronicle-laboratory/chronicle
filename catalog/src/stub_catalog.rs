use crate::catalog::*;
use crate::errors::CatalogError;
use chronicle_proto::pb_catalog::{ParallelTimeline, ParallelTimelineCursor, Timeline, Unit, UnitMode};
use std::collections::HashMap;

/// Stub implementation of Catalog for development
pub struct StubCatalog;

impl Catalog for StubCatalog {}

#[async_trait::async_trait]
impl ChronicleResources for StubCatalog {
    async fn create_chronicle(&self, _id: String) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_chronicle(&self) -> Result<String, CatalogError> {
        Ok("stub-chronicle".to_string())
    }
}

#[async_trait::async_trait]
impl UnitResources for StubCatalog {
    async fn create_unit(&self, _unit_id: UnitId, _unit: Unit) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn scan_units(&self, _mode: UnitMode) -> Result<Vec<(UnitId, Unit)>, CatalogError> {
        Ok(vec![])
    }
    
    async fn get_unit(&self, _unit_id: UnitId) -> Result<Unit, CatalogError> {
        Err(CatalogError::UnitNotFound)
    }
    
    async fn unit_online(&self, _unit_id: UnitId) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn unit_offline(&self, _unit_id: UnitId) -> Result<(), CatalogError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ParallelTimelineResources for StubCatalog {
    async fn create_parallel_timeline(
        &self,
        _chron_name: String,
        _pt_name: String,
        _pt: ParallelTimeline,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn delete_parallel_timeline(
        &self,
        _chron_name: String,
        _pt_name: String,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_parallel_timeline(
        &self,
        _chron_name: String,
        pt_name: String,
    ) -> Result<ParallelTimeline, CatalogError> {
        Err(CatalogError::VerseNotFound { verse: pt_name })
    }
    
    async fn list_parallel_timelines(&self, _chron_name: String) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl TimelineResources for StubCatalog {
    async fn create_timeline(
        &self,
        _chron_name: String,
        _pt_name: String,
        _tl_name: String,
        _timeline: Timeline,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_timeline(
        &self,
        _chron_name: String,
        pt_name: String,
        _tl_name: String,
    ) -> Result<Timeline, CatalogError> {
        Err(CatalogError::TimelineNotFound { id: 0, verse: pt_name })
    }
    
    async fn list_timelines(
        &self,
        _chron_name: String,
        _pt_name: String,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    
    async fn delete_timeline(
        &self,
        _chron_name: String,
        _pt_name: String,
        _tl_name: String,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl TransactionResources for StubCatalog {
    async fn create_transaction(
        &self,
        _chron_name: String,
        _txn_id: String,
        _metadata: HashMap<String, String>,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_transaction(
        &self,
        _chron_name: String,
        _txn_id: String,
    ) -> Result<HashMap<String, String>, CatalogError> {
        Ok(HashMap::new())
    }
    
    async fn list_transactions(&self, _chron_name: String) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    
    async fn delete_transaction(
        &self,
        _chron_name: String,
        _txn_id: String,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ParallelTimelineCursorResources for StubCatalog {
    async fn create_cursor(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
        _cursor: ParallelTimelineCursor,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_cursor(
        &self,
        _chron_name: String,
        pt_name: String,
        _cursor_name: String,
    ) -> Result<ParallelTimelineCursor, CatalogError> {
        Err(CatalogError::VerseNotFound { verse: pt_name })
    }
    
    async fn list_cursors(
        &self,
        _chron_name: String,
        _pt_name: String,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    
    async fn delete_cursor(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_read_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
    ) -> Result<i64, CatalogError> {
        Ok(0)
    }
    
    async fn set_read_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
        _pos: i64,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_ack_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
    ) -> Result<i64, CatalogError> {
        Ok(0)
    }
    
    async fn set_ack_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
        _pos: i64,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
    
    async fn get_commit_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
    ) -> Result<i64, CatalogError> {
        Ok(0)
    }
    
    async fn set_commit_pos(
        &self,
        _chron_name: String,
        _pt_name: String,
        _cursor_name: String,
        _pos: i64,
    ) -> Result<(), CatalogError> {
        Ok(())
    }
}
