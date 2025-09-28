use crate::catalog::Catalog;
use crate::unit_client::UnitClientPool;
use std::sync::Arc;

pub struct ChronicleOptions {
    pub catalog_address: String,
}

pub struct Chronicle {
    catalog: Arc<dyn Catalog>,
    unit_pool: Arc<UnitClientPool>,
}

impl Chronicle {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            unit_pool: Arc::new(UnitClientPool::new()),
        }
    }
}
