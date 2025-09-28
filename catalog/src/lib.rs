pub mod error;

use chronicle_proto::pb_catalog::TimelineCatalog;
use error::CatalogError;

pub struct OxiaCatalog {}

impl OxiaCatalog {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_timeline(&self, _name: &str) -> Result<TimelineCatalog, CatalogError> {
        todo!()
    }

    pub async fn put_timeline(
        &self,
        _catalog: &TimelineCatalog,
        _expected_version: i64,
    ) -> Result<TimelineCatalog, CatalogError> {
        todo!()
    }

    pub async fn create_timeline(&self, _name: &str) -> Result<TimelineCatalog, CatalogError> {
        todo!()
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError> {
        todo!()
    }
}
