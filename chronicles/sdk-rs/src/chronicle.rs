use crate::errors::ChronicleError;
use crate::parallel_timeline::{ParallelTimeline, ParallelTimelineOptions};
use std::sync::Arc;

pub struct ChronicleOptions {
    pub catalog_address: String,
}

struct ChronicleInner {
    options: ChronicleOptions,
}
#[derive(Clone)]
pub struct Chronicle {
    inner: Arc<ChronicleInner>,
}

impl Chronicle {
    pub fn new(options: ChronicleOptions) -> Self {
        Chronicle {
            inner: Arc::new(ChronicleInner { options }),
        }
    }

    pub async fn open_parallel_timeline(
        &self,
        name: String,
        parallel_key: String,
        options: ParallelTimelineOptions,
    ) -> Result<ParallelTimeline, ChronicleError> {
        todo!()
    }

    pub async fn drop_timeline(&self, name: String) -> Result<(), ChronicleError> {
        todo!()
    }

    pub async fn close(&self) {}
}
