use crate::config::SagaConfig;
use crate::pipeline::decoder;
use crate::error::{Result, SagaError};
use crate::storage::lifecycle::Lifecycle;
use crate::storage::memtable::Memtable;
use crate::storage::saga_catalog::SagaCatalog;
use chronicle_proto::pb_saga::segment_discovery_client::SegmentDiscoveryClient;
use chronicle_proto::pb_saga::segment_reader_client::SegmentReaderClient;
use chronicle_proto::pb_saga::{AckSegmentRequest, ListPendingRequest, ReadSegmentRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// WAL source that pulls data via gRPC and writes into per-subject Memtables.
///
/// Schemas are resolved from Lexicon (schema registry) on demand via `SagaCatalog`.
pub struct Source {
    config: SagaConfig,
    catalog: Arc<SagaCatalog>,
    memtables: Arc<parking_lot::RwLock<HashMap<String, Arc<Memtable>>>>,
    lifecycle: Arc<Lifecycle>,
}

impl Source {
    pub fn new(
        config: SagaConfig,
        catalog: Arc<SagaCatalog>,
        memtables: Arc<parking_lot::RwLock<HashMap<String, Arc<Memtable>>>>,
        lifecycle: Arc<Lifecycle>,
    ) -> Self {
        Self {
            config,
            catalog,
            memtables,
            lifecycle,
        }
    }

    /// Run the source loop until shutdown signal.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let endpoint = self.config.wal_endpoint.clone();
        info!(endpoint = %endpoint, "starting WAL source");

        let poll_interval =
            tokio::time::Duration::from_millis(self.config.wal_poll_interval_ms);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("source received shutdown signal");
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {
                    if let Err(e) = self.poll_once(&endpoint).await {
                        warn!(error = %e, "source poll failed");
                    }
                }
            }
        }
    }

    async fn poll_once(&self, endpoint: &str) -> Result<()> {
        let mut discovery = SegmentDiscoveryClient::connect(endpoint.to_string())
            .await
            .map_err(|e| SagaError::Internal(format!("connect discovery: {}", e)))?;

        let watermark = self.lifecycle.watermarks().ingested;

        let resp = discovery
            .list_pending_segments(ListPendingRequest {
                after_sequence_id: watermark,
                limit: 100,
            })
            .await?
            .into_inner();

        if resp.segments.is_empty() {
            debug!("no pending segments");
            return Ok(());
        }

        let mut reader = SegmentReaderClient::connect(endpoint.to_string())
            .await
            .map_err(|e| SagaError::Internal(format!("connect reader: {}", e)))?;

        for segment in &resp.segments {
            self.consume_segment(&mut reader, &mut discovery, segment)
                .await?;
        }

        Ok(())
    }

    async fn consume_segment(
        &self,
        reader: &mut SegmentReaderClient<tonic::transport::Channel>,
        discovery: &mut SegmentDiscoveryClient<tonic::transport::Channel>,
        segment: &chronicle_proto::pb_saga::SegmentMeta,
    ) -> Result<()> {
        let mut stream = reader
            .read_segment(ReadSegmentRequest {
                sequence_id: segment.sequence_id,
                batch_size: 10_000,
            })
            .await?
            .into_inner();

        // schema_id maps to a Lexicon subject.
        let subject = &segment.schema_id;

        // Resolve schema from Lexicon (cached after first lookup).
        let arrow_schema = match self.catalog.resolve_schema(subject).await {
            Ok(schema) => schema,
            Err(e) => {
                warn!(subject = %subject, error = %e, "failed to resolve schema, skipping segment");
                return Ok(());
            }
        };

        let memtable = self.get_or_create_memtable(subject)?;

        while let Some(batch_resp) = futures_util::StreamExt::next(&mut stream).await {
            let batch_resp = batch_resp?;
            if batch_resp.rows.is_empty() {
                continue;
            }

            let record_batch = decoder::decode_rows(&batch_resp.rows, &arrow_schema)?;
            memtable.ingest(record_batch, segment.sequence_id)?;
        }

        // Ack the segment.
        discovery
            .ack_segment(AckSegmentRequest {
                sequence_id: segment.sequence_id,
            })
            .await?;

        self.lifecycle.advance_ingested(segment.sequence_id);

        info!(
            subject = %subject,
            sequence_id = segment.sequence_id,
            "segment consumed"
        );

        Ok(())
    }

    fn get_or_create_memtable(&self, subject: &str) -> Result<Arc<Memtable>> {
        {
            let tables = self.memtables.read();
            if let Some(mt) = tables.get(subject) {
                return Ok(mt.clone());
            }
        }

        let schema = self
            .catalog
            .schema(subject)
            .map_err(|_| SagaError::Internal(format!(
                "subject '{}' must be resolved before memtable creation", subject
            )))?;

        let mt = Arc::new(Memtable::new(schema));
        let mut tables = self.memtables.write();
        Ok(tables.entry(subject.to_string()).or_insert(mt).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_new() {
        let config = SagaConfig::default();
        let catalog = Arc::new(SagaCatalog::new());
        let memtables = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let lifecycle = Arc::new(Lifecycle::new(std::path::PathBuf::from("/tmp/test.json")));
        let _source = Source::new(config, catalog, memtables, lifecycle);
    }
}
