use std::sync::Arc;

struct Inner {}

#[derive(Clone)]
pub struct Log {
    inner: Arc<Inner>,
}

impl Log {
    
}
