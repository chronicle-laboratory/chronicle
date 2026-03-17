use crate::conn::ConnPool;
use crate::cursor::EventStream;
use crate::ensemble;
use crate::error::ChronicleError;
use crate::replicator::Replicator;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus, UnitStatus};
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_LINGER: Duration = Duration::from_millis(5);

struct PendingEvent {
    event: UserEvent,
    tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

pub struct Timeline {
    timeline_id: i64,
    segments: Vec<Segment>,
    conns: HashMap<String, crate::conn::Conn>,
    event_tx: mpsc::Sender<PendingEvent>,
    _group_task: tokio::task::JoinHandle<()>,
    _replicator: Arc<Replicator>,
}

impl Drop for Timeline {
    fn drop(&mut self) {
        self._group_task.abort();
    }
}

impl Timeline {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        let conns = resolve_conns(&catalog, &pool).await?;

        match catalog.get_timeline(name).await {
            Ok(tc) => {
                let replicator = Arc::new(
                    Replicator::open(&catalog, &conns, name, replication_factor).await?,
                );
                let segments = catalog.get_timeline(name).await?.segments;
                Self::start(tc.timeline_id, segments, conns, replicator)
            }
            Err(catalog::error::CatalogError::NotFound(_)) => {
                let tc = catalog.create_timeline(name).await?;

                let available: Vec<String> = conns.keys().cloned().collect();
                let ens = ensemble::select(&available, &[], &[], replication_factor)
                    .ok_or_else(|| {
                        ChronicleError::EnsembleUnavailable(format!(
                            "need {} units, have {}",
                            replication_factor,
                            available.len()
                        ))
                    })?;

                let first_segment = Segment {
                    id: 1,
                    ensemble: ens.clone(),
                    start_offset: 1,
                };

                let mut updated = tc.clone();
                updated.status = TimelineStatus::Active as i32;
                updated.segments = vec![first_segment.clone()];
                updated.term = 1;
                let updated = catalog.put_timeline(&updated, tc.version).await?;

                info!(
                    timeline = name,
                    timeline_id = updated.timeline_id,
                    ensemble = ?ens,
                    "timeline created"
                );

                let replicator = Arc::new(
                    Replicator::create(updated.timeline_id, replication_factor, &first_segment, &conns).await?,
                );
                Self::start(updated.timeline_id, updated.segments, conns, replicator)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn start(
        timeline_id: i64,
        segments: Vec<Segment>,
        conns: HashMap<String, crate::conn::Conn>,
        replicator: Arc<Replicator>,
    ) -> Result<Self, ChronicleError> {
        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(DEFAULT_BATCH_SIZE * 2);

        let replicator_clone = replicator.clone();
        let group_task = tokio::spawn(async move {
            event_group_loop(timeline_id, replicator_clone, event_rx).await;
        });

        Ok(Self {
            timeline_id,
            segments,
            conns,
            event_tx,
            _group_task: group_task,
            _replicator: replicator,
        })
    }

    pub fn fetch(&self, opts: FetchOptions) -> EventStream {
        let start_offset = match opts.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => {
                let s = self._replicator.state().lock().unwrap();
                s.lra + 1
            }
            StartPosition::Offset(o) => o,
            StartPosition::Index { .. } => 1, // TODO: index lookup
        };

        let mut stream = EventStream::new(
            self.timeline_id,
            self.segments.clone(),
            &self.conns,
            start_offset,
        );

        if let Some(limit) = opts.limit {
            stream = stream.with_limit(limit);
        }
        if let Some(timeout) = opts.timeout {
            stream = stream.with_timeout(timeout);
        }
        if opts.limit.is_none() && opts.timeout.is_none() {
            stream = stream.with_tail();
        }

        stream
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("ack channel dropped".into()))?
    }
}

async fn resolve_conns(
    catalog: &Catalog,
    pool: &ConnPool,
) -> Result<HashMap<String, crate::conn::Conn>, ChronicleError> {
    let registrations = catalog.list_units().await?;
    let mut conns = HashMap::new();
    for reg in &registrations {
        if reg.status() == UnitStatus::Writable {
            conns.insert(reg.address.clone(), pool.get_or_connect(&reg.address)?);
        }
    }
    Ok(conns)
}

async fn event_group_loop(
    timeline_id: i64,
    replicator: Arc<Replicator>,
    mut event_rx: mpsc::Receiver<PendingEvent>,
) {
    let mut batch: Vec<PendingEvent> = Vec::with_capacity(DEFAULT_BATCH_SIZE);

    loop {
        batch.clear();

        match event_rx.recv().await {
            Some(first) => batch.push(first),
            None => return,
        }

        let deadline = tokio::time::sleep(DEFAULT_LINGER);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                event = event_rx.recv() => {
                    match event {
                        Some(e) => {
                            batch.push(e);
                            if batch.len() >= DEFAULT_BATCH_SIZE {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = &mut deadline => break,
            }
        }

        if batch.is_empty() {
            continue;
        }

        let items = {
            let mut s = replicator.state().lock().unwrap();
            let mut items = Vec::with_capacity(batch.len());

            for pending in batch.drain(..) {
                let offset = s.lrs + 1;
                s.lrs = offset;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let proto_event = Event {
                    timeline_id,
                    term: s.term,
                    offset,
                    payload: Some(pending.event.payload.into()),
                    crc32: None,
                    timestamp: now,
                    schema_id: pending.event.schema_id.unwrap_or(0),
                };

                let item = RecordEventsRequestItem {
                    event: Some(proto_event),
                    trunc: s.needs_trunc,
                    lra: s.lra,
                };
                s.needs_trunc = false;

                s.acked.insert(offset, std::collections::HashSet::new());
                s.waiters.insert(offset, pending.tx);

                items.push(item);
            }

            items
        };

        if let Err(e) = replicator.replicate(items).await {
            tracing::warn!(error = %e, "failed to replicate batch");
        }
    }
}
