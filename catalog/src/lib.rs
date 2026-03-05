pub mod error;
pub mod memory_catalog;
pub mod oxia_catalog;

use async_trait::async_trait;
use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration};
use error::CatalogError;
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

#[async_trait]
pub trait Catalog: Send + Sync {
    // Timeline operations
    async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError>;
    async fn put_timeline(
        &self,
        catalog: &TimelineCatalog,
        expected_version: i64,
    ) -> Result<TimelineCatalog, CatalogError>;
    async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError>;
    async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError>;

    // Unit operations
    async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError>;
    async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError>;
    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError>;
}

#[derive(Debug, Deserialize, Clone)]
pub struct CatalogOptions {
    #[serde(default = "default_catalog_backend")]
    pub backend: String,
    #[serde(default)]
    pub service_address: Option<String>,
    #[serde(default = "default_catalog_namespace")]
    pub namespace: String,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            backend: default_catalog_backend(),
            service_address: None,
            namespace: default_catalog_namespace(),
        }
    }
}

fn default_catalog_backend() -> String {
    "memory".to_string()
}

fn default_catalog_namespace() -> String {
    "default".to_string()
}

/// Build a [`Catalog`] implementation from the given options.
pub async fn build_catalog(
    options: &CatalogOptions,
) -> Result<Arc<dyn Catalog>, CatalogError> {
    match options.backend.as_str() {
        "memory" => {
            info!("using memory catalog");
            Ok(Arc::new(memory_catalog::MemoryCatalog::new()))
        }
        "oxia" => {
            let address = options
                .service_address
                .as_deref()
                .ok_or_else(|| {
                    CatalogError::Internal(
                        "catalog.service_address is required when backend = \"oxia\"".to_string(),
                    )
                })?;
            info!(address, namespace = %options.namespace, "connecting to oxia catalog");
            let catalog =
                oxia_catalog::OxiaCatalog::new(address.to_string(), options.namespace.clone())
                    .await?;
            Ok(Arc::new(catalog))
        }
        other => Err(CatalogError::Internal(format!(
            "unknown catalog backend: {}",
            other
        ))),
    }
}
