use crate::catalog::Catalog;
use crate::errors::CatalogError;
use crate::stub_catalog::StubCatalog;

pub mod catalog;
pub mod errors;
mod stub_catalog;

pub struct CatalogOptions {
    pub provider: String,
    pub address: String,
}

pub async fn new_catalog(_options: CatalogOptions) -> Result<Box<dyn Catalog>, CatalogError> {
    Ok(Box::new(StubCatalog))
}
