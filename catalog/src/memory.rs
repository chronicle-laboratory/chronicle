use crate::ChronicleResources;
use crate::errors::CatalogError;
use std::collections::{BTreeMap, HashMap};

struct MemoryCatalog {
    store: BTreeMap<String, Vec<u8>>,
}

#[async_trait::async_trait]
impl ChronicleResources for MemoryCatalog {
    async fn create_chronicle(&self, id: String) -> Result<(), CatalogError> {

    }

    async fn get_chronicle(&self) -> Result<String, CatalogError> {
        todo!()
    }
}
