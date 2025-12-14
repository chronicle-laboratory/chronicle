use crate::error::unit_error::UnitError;
use std::sync::Arc;

struct Inner {}

#[derive(Clone)]
pub struct Wal {
    inner: Arc<Inner>,
}

pub struct WalOptions {
    pub dir: String,
}

impl Wal {
    pub fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let inner = Inner {};
        Ok(Wal {
            inner: Arc::new(inner),
        })
    }
}
