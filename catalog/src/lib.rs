use crate::catalog::Catalog;
use crate::errors::CatalogError;

pub mod catalog;
pub mod errors;

pub struct CatalogOptions {
    pub provider: String,
    pub address: String,
}

pub async fn new_catalog(options: CatalogOptions) -> Result<Box<dyn Catalog>, CatalogError> {
    Ok(Box::new(catalog))
}
