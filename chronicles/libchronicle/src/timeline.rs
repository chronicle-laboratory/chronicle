use crate::conn::ConnPool;
use crate::cursor::EventStream;
use crate::ensemble;
use crate::error::ChronicleError;
use crate::event_group::{self, PendingEvent};
use crate::state_machine::StateMachine;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

struct TimelineInner {
    event_tx: Option<mpsc::Sender<PendingEvent>>,
    group_task: Option<tokio::task::JoinHandle<()>>,
}

pub struct Timeline {
    timeline_id: i64,
    #[allow(dead_code)]
    options: TimelineOptions,
    pool: Arc<ConnPool>,
    sm: Arc<StateMachine>,
    inner: Mutex<TimelineInner>,
}

impl Timeline {
    pub async fn open(
        catalog: Arc<Catalog>,
        pool: Arc<ConnPool>,
        name: &str,
        options: TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let rf = options.replication_factor;

        let tc = match catalog.get_timeline(name).await {
            Ok(tc) => tc,
            Err(catalog::error::CatalogError::NotFound(_)) => {
                let tc = catalog.create_timeline(name).await?;

                let registrations = catalog.list_units().await?;
                let available: Vec<String> = registrations
                    .iter()
                    .filter(|r| r.status() == chronicle_proto::pb_catalog::UnitStatus::Writable)
                    .map(|r| r.address.clone())
                    .collect();

                let ens = ensemble::select(&available, &[], &[], rf)
                    .ok_or_else(|| {
                        ChronicleError::EnsembleUnavailable(format!(
                            "need {} units, have {}",
                            rf, available.len()
                        ))
                    })?;

                let first_segment = Segment {
                    id: 1,
                    ensemble: ens.clone(),
                    start_offset: 1,
                };

                let mut updated = tc.clone();
                updated.status = TimelineStatus::Active as i32;
                updated.segments = vec![first_segment];
                updated.term = 0;
                let updated = catalog.put_timeline(&updated, tc.version).await?;

                info!(
                    timeline = name,
                    timeline_id = updated.timeline_id,
                    ensemble = ?ens,
                    "timeline created"
                );

                updated
            }
            Err(e) => return Err(e.into()),
        };

        let sm = Arc::new(
            StateMachine::open(&catalog, &pool, &tc, rf, options.schema_id.clone()).await?
        );

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(options.max_batch_size * 2);

        let sm_clone = sm.clone();
        let batch_size = options.max_batch_size;
        let linger = options.linger;
        let timeline_id = tc.timeline_id;
        let group_task = tokio::spawn(async move {
            event_group::run(timeline_id, sm_clone, event_rx, batch_size, linger).await;
        });

        Ok(Self {
            timeline_id: tc.timeline_id,
            options,
            pool,
            sm,
            inner: Mutex::new(TimelineInner {
                event_tx: Some(event_tx),
                group_task: Some(group_task),
            }),
        })
    }

    pub fn fetch(&self, opts: FetchOptions) -> EventStream {
        let start_offset = match opts.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => self.sm.lra() + 1,
            StartPosition::Offset(o) => o,
            StartPosition::Index { .. } => 1,
        };

        let segments = self.sm.segments.clone();
        let mut conns = std::collections::HashMap::new();
        for seg in &segments {
            for ep in &seg.ensemble {
                if !conns.contains_key(ep) {
                    if let Ok(conn) = self.pool.get_or_connect(ep) {
                        conns.insert(ep.clone(), conn);
                    }
                }
            }
        }

        let mut stream = EventStream::new(
            self.timeline_id,
            segments,
            &conns,
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

    pub async fn close(&self) {
        let task = {
            let mut inner = self.inner.lock().unwrap();
            inner.event_tx.take();
            inner.group_task.take()
        };
        if let Some(task) = task {
            let _ = task.await;
        }
        self.sm.close().await;
        info!(timeline_id = self.timeline_id, "timeline closed");
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sender = {
            let inner = self.inner.lock().unwrap();
            inner.event_tx.clone()
                .ok_or_else(|| ChronicleError::Internal("timeline closed".into()))?
        };
        let (tx, rx) = oneshot::channel();
        sender
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("ack channel dropped".into()))?
    }
}
