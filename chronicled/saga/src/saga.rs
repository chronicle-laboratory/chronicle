use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct Saga {
    context: CancellationToken,
    catalog: Arc<dyn catalog::Catalog>,
}

impl Saga {
    pub async fn new(
        catalog: Arc<dyn catalog::Catalog>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let context = CancellationToken::new();

        info!("saga initializing");

        Ok(Self { context, catalog })
    }

    pub async fn stop(self) {
        info!("saga shutting down");
        self.context.cancel();
        info!("saga stopped");
    }
}
