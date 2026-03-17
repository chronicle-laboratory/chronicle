use crate::conn::{Conn, ConnPool};
use crate::cursor::EventStream;
use crate::ensemble;
use crate::error::ChronicleError;
use crate::event_group::{self, PendingEvent};
use crate::state_machine::StateMachine;
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus, UnitStatus};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

struct TimelineInner {
    event_tx: Option<mpsc::Sender<PendingEvent>>,
    group_task: Option<tokio::task::JoinHandle<()>>,
}

pub struct Timeline {
    timeline_id: i64,
    name: String,
    conns: HashMap<String, Conn>,
    #[allow(dead_code)]
    options: TimelineOptions,
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
        let conns = resolve_conns(&catalog, &pool).await?;
        let rf = options.replication_factor;

        let tc = match catalog.get_timeline(name).await {
            Ok(tc) => tc,
            Err(catalog::error::CatalogError::NotFound(_)) => {
                let tc = catalog.create_timeline(name).await?;

                let available: Vec<String> = conns.keys().cloned().collect();
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
            StateMachine::open(&catalog, &conns, name, &tc, rf).await?
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
            name: name.to_string(),
            conns,
            options,
            sm,
            inner: Mutex::new(TimelineInner {
                event_tx: Some(event_tx),
                group_task: Some(group_task),
            }),
        })
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

    pub fn fetch(&self, opts: FetchOptions) -> EventStream {
        let start_offset = match opts.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => self.sm.lra() + 1,
            StartPosition::Offset(o) => o,
            StartPosition::Index { .. } => 1,
        };

        let mut stream = EventStream::new(
            self.timeline_id,
            self.sm.segments.clone(),
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

async fn resolve_conns(
    catalog: &Catalog,
    pool: &ConnPool,
) -> Result<HashMap<String, Conn>, ChronicleError> {
    let registrations = catalog.list_units().await?;
    let mut conns = HashMap::new();
    for reg in &registrations {
        if reg.status() == UnitStatus::Writable {
            conns.insert(reg.address.clone(), pool.get_or_connect(&reg.address)?);
        }
    }
    Ok(conns)
}
