use crate::catalog::Catalog;
use crate::errors::CatalogError;
use crate::memory::MemoryCatalog;

pub mod catalog;
pub mod errors;
mod memory;

pub struct CatalogOptions {
    pub provider: String,
    pub address: String,
}



pub async fn new_catalog(options: CatalogOptions) -> Result<Box<dyn Catalog>, CatalogError> {
    let catalog = MemoryCatalog::new(options).await?;
    Ok(Box::new(catalog))
}
