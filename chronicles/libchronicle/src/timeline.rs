use crate::conn::{Conn, ConnPool};
use crate::cursor::EventStream;
use crate::ensemble;
use crate::error::ChronicleError;
use crate::event_group::{self, PendingEvent};
use crate::state_machine::{self, ReplicationState};
use crate::{Event as UserEvent, FetchOptions, Offset, StartPosition, TimelineOptions, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus, UnitStatus};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub struct Timeline {
    timeline_id: i64,
    segments: Vec<Segment>,
    conns: HashMap<String, Conn>,
    #[allow(dead_code)]
    options: TimelineOptions,
    state: Arc<Mutex<ReplicationState>>,
    event_tx: mpsc::Sender<PendingEvent>,
    _group_task: tokio::task::JoinHandle<()>,
    _ack_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Timeline {
    fn drop(&mut self) {
        self._group_task.abort();
        for task in &self._ack_tasks {
            task.abort();
        }
    }
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

        let (timeline_id, term, lrs, lra, needs_trunc, segments, writable_segment) =
            match catalog.get_timeline(name).await {
                Ok(tc) => {
                    let (term, lra, segments, writable_segment) =
                        state_machine::new_term(&catalog, &conns, name, &tc).await?;
                    (tc.timeline_id, term, lra, lra, true, segments, writable_segment)
                }
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
                    updated.segments = vec![first_segment.clone()];
                    updated.term = 1;
                    let updated = catalog.put_timeline(&updated, tc.version).await?;

                    info!(
                        timeline = name,
                        timeline_id = updated.timeline_id,
                        ensemble = ?ens,
                        "timeline created"
                    );

                    (updated.timeline_id, 1, 0, 0, false, updated.segments, first_segment)
                }
                Err(e) => return Err(e.into()),
            };

        let (state, ack_tasks) = state_machine::init(
            timeline_id, term, lrs, lra, rf, needs_trunc,
            &writable_segment, &conns,
        ).await?;

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(options.max_batch_size * 2);

        let state_clone = state.clone();
        let batch_size = options.max_batch_size;
        let linger = options.linger;
        let group_task = tokio::spawn(async move {
            event_group::run(timeline_id, state_clone, event_rx, batch_size, linger).await;
        });

        Ok(Self {
            timeline_id,
            segments,
            conns,
            options,
            state,
            event_tx,
            _group_task: group_task,
            _ack_tasks: ack_tasks,
        })
    }

    pub fn fetch(&self, opts: FetchOptions) -> EventStream {
        let start_offset = match opts.start {
            StartPosition::Earliest => 1,
            StartPosition::Latest => {
                let s = self.state.lock().unwrap();
                s.lra + 1
            }
            StartPosition::Offset(o) => o,
            StartPosition::Index { .. } => 1,
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
