pub mod error;
pub mod memory_catalog;
pub mod oxia_catalog;

use chronicle_proto::pb_catalog::{TimelineCatalog, UnitRegistration};
use error::CatalogError;
use memory_catalog::MemoryCatalog;
use oxia_catalog::OxiaCatalog;
use serde::Deserialize;
use tracing::info;

pub enum Catalog {
    Memory(MemoryCatalog),
    Oxia(OxiaCatalog),
}

impl Catalog {
    pub async fn get_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        match self {
            Catalog::Memory(c) => c.get_timeline(name).await,
            Catalog::Oxia(c) => c.get_timeline(name).await,
        }
    }

    pub async fn put_timeline(
        &self,
        catalog: &TimelineCatalog,
        expected_version: i64,
    ) -> Result<TimelineCatalog, CatalogError> {
        match self {
            Catalog::Memory(c) => c.put_timeline(catalog, expected_version).await,
            Catalog::Oxia(c) => c.put_timeline(catalog, expected_version).await,
        }
    }

    pub async fn create_timeline(&self, name: &str) -> Result<TimelineCatalog, CatalogError> {
        match self {
            Catalog::Memory(c) => c.create_timeline(name).await,
            Catalog::Oxia(c) => c.create_timeline(name).await,
        }
    }

    pub async fn delete_timeline(&self, name: &str) -> Result<(), CatalogError> {
        match self {
            Catalog::Memory(c) => c.delete_timeline(name).await,
            Catalog::Oxia(c) => c.delete_timeline(name).await,
        }
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineCatalog>, CatalogError> {
        match self {
            Catalog::Memory(c) => c.list_timelines().await,
            Catalog::Oxia(c) => c.list_timelines().await,
        }
    }

    pub async fn register_unit(
        &self,
        registration: &UnitRegistration,
    ) -> Result<(), CatalogError> {
        match self {
            Catalog::Memory(c) => c.register_unit(registration).await,
            Catalog::Oxia(c) => c.register_unit(registration).await,
        }
    }

    pub async fn unregister_unit(&self, address: &str) -> Result<(), CatalogError> {
        match self {
            Catalog::Memory(c) => c.unregister_unit(address).await,
            Catalog::Oxia(c) => c.unregister_unit(address).await,
        }
    }

    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        match self {
            Catalog::Memory(c) => c.list_units().await,
            Catalog::Oxia(c) => c.list_units().await,
        }
    }
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

pub async fn build_catalog(
    options: &CatalogOptions,
) -> Result<Catalog, CatalogError> {
    match options.backend.as_str() {
        "memory" => {
            info!("using memory catalog");
            Ok(Catalog::Memory(MemoryCatalog::new()))
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
                OxiaCatalog::new(address.to_string(), options.namespace.clone()).await?;
            Ok(Catalog::Oxia(catalog))
        }
        other => Err(CatalogError::Internal(format!(
            "unknown catalog backend: {}",
            other
        ))),
    }
}
