use crate::error::ChronicleError;
use async_trait::async_trait;
use chronicle_proto::pb_catalog::TimelineCatalog;

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, ChronicleError>;
    async fn put_timeline(
        &self,
        catalog: &TimelineCatalog,
        expected_version: i64,
    ) -> Result<TimelineCatalog, ChronicleError>;
    async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, ChronicleError>;
    async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, ChronicleError>;
}
