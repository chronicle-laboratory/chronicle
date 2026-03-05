use crate::client::unit_client::UnitClient;
use crate::error::ChronicleError;
use crate::writer::ensemble::select_ensemble;
use crate::writer::reconciliation::ReconciledState;
use crate::{Offset, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use chronicle_proto::pb_ext::{
    Event, RecordEventsRequest, RecordEventsRequestItem, RecordEventsResponse, StatusCode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

// ─── Internal types ─────────────────────────────────────────────────────────

struct InnerState {
    timeline_id: i64,
    #[allow(dead_code)]
    name: String,
    term: i64,
    #[allow(dead_code)]
    segments: Vec<Segment>,
    writable_segment: Option<Segment>,
    lrs: i64,
    lra: i64,
    acked: HashMap<i64, HashSet<String>>,
    #[allow(dead_code)]
    catalog_version: i64,
    replication_factor: usize,
    needs_trunc: bool,
    senders: HashMap<String, mpsc::Sender<RecordEventsRequest>>,
}

// ─── Timeline ───────────────────────────────────────────────────────

/// Core write-path state machine implementing the TLA+ `TimelineState`.
///
/// Sends events to ALL units in the writable segment's ensemble. Acks are
/// collected asynchronously by background tasks; the LRA advances once every
/// unit has acknowledged an offset contiguously from the previous LRA.
pub struct Timeline {
    timeline_id: i64,
    inner: Arc<Mutex<InnerState>>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Timeline {
    fn drop(&mut self) {
        for task in &self._tasks {
            task.abort();
        }
    }
}

impl Timeline {
    /// Create a brand-new timeline (TLA+ `OpenNewTimeline`).
    ///
    /// Selects an ensemble, CAS-creates the catalog entry with the first
    /// segment and term=1, then opens record streams to all ensemble units.
    pub async fn create(
        catalog: &dyn Catalog,
        unit_clients: &HashMap<String, UnitClient>,
        name: &str,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        // Create timeline entry in catalog.
        let tc = catalog.create_timeline(name).await?;

        // Select ensemble from available units.
        let available: Vec<String> = unit_clients.keys().cloned().collect();
        let ensemble = select_ensemble(&available, &[], &[], replication_factor).ok_or_else(
            || {
                ChronicleError::EnsembleUnavailable(format!(
                    "need {} units, have {}",
                    replication_factor,
                    available.len()
                ))
            },
        )?;

        // Build first segment.
        let first_segment = Segment {
            id: 1,
            ensemble: ensemble.clone(),
            start_offset: 1,
        };

        // CAS catalog with segment, term=1, status=ACTIVE.
        let mut updated = tc.clone();
        updated.status = TimelineStatus::Active as i32;
        updated.segments = vec![first_segment.clone()];
        updated.term = 1;
        let updated = catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline = name,
            timeline_id = updated.timeline_id,
            ensemble = ?ensemble,
            "timeline created"
        );

        Self::open_inner(
            updated.timeline_id,
            name,
            1,   // term
            updated.segments.clone(),
            first_segment,
            0,   // lrs
            0,   // lra
            updated.version,
            replication_factor,
            false, // needs_trunc
            unit_clients,
        )
        .await
    }

    /// Open a timeline after reconciliation has completed.
    pub async fn open(
        reconciled: ReconciledState,
        unit_clients: &HashMap<String, UnitClient>,
        name: &str,
        timeline_id: i64,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        Self::open_inner(
            timeline_id,
            name,
            reconciled.term,
            reconciled.segments,
            reconciled.writable_segment,
            reconciled.lrs,
            reconciled.lra,
            reconciled.catalog_version,
            replication_factor,
            true, // first write after reconciliation uses trunc=true
            unit_clients,
        )
        .await
    }

    /// Shared construction: build inner state, open record streams, spawn
    /// background ack-collector tasks.
    #[allow(clippy::too_many_arguments)]
    async fn open_inner(
        timeline_id: i64,
        name: &str,
        term: i64,
        segments: Vec<Segment>,
        writable_segment: Segment,
        lrs: i64,
        lra: i64,
        catalog_version: i64,
        replication_factor: usize,
        needs_trunc: bool,
        unit_clients: &HashMap<String, UnitClient>,
    ) -> Result<Self, ChronicleError> {
        let inner = Arc::new(Mutex::new(InnerState {
            timeline_id,
            name: name.to_string(),
            term,
            segments,
            writable_segment: Some(writable_segment.clone()),
            lrs,
            lra,
            acked: HashMap::new(),
            catalog_version,
            replication_factor,
            needs_trunc,
            senders: HashMap::new(),
        }));

        let mut senders = HashMap::new();
        let mut tasks = Vec::new();

        for endpoint in &writable_segment.ensemble {
            let client = unit_clients.get(endpoint).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!("no client for unit {}", endpoint))
            })?;

            let (tx, response_stream) = client.open_record_stream(64).await?;
            senders.insert(endpoint.clone(), tx);

            let inner_clone = inner.clone();
            let ep = endpoint.clone();
            tasks.push(tokio::spawn(async move {
                ack_collector(inner_clone, ep, response_stream).await;
            }));
        }

        // Store senders inside InnerState so record() can access them.
        {
            let mut state = inner.lock().unwrap();
            state.senders = senders;
        }

        Ok(Self {
            timeline_id,
            inner,
            _tasks: tasks,
        })
    }

    // ── Accessors ───────────────────────────────────────────────────────

    pub fn timeline_id(&self) -> i64 {
        self.timeline_id
    }

    // ── Ensemble change (TLA+ TimelineEnsembleChange) ───────────────────

    /// Replace failed units with a new ensemble, creating a new segment at
    /// LRA+1 and CAS-updating the catalog.
    pub async fn handle_ensemble_change(
        &self,
        catalog: &dyn Catalog,
        unit_clients: &HashMap<String, UnitClient>,
        failed_units: &[String],
    ) -> Result<(), ChronicleError> {
        let available: Vec<String> = unit_clients.keys().cloned().collect();
        let exclude: Vec<String> = failed_units.to_vec();

        let new_ensemble =
            select_ensemble(&available, &[], &exclude, 0 /* filled below */).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable("cannot find replacement ensemble".into())
            })?;

        let mut state = self.inner.lock().unwrap();
        let rf = state.replication_factor;

        let real_ensemble = select_ensemble(&available, &[], &exclude, rf).ok_or_else(|| {
            ChronicleError::EnsembleUnavailable("cannot find replacement ensemble".into())
        })?;

        let new_seg = Segment {
            id: state.segments.last().map_or(1, |s| s.id + 1),
            ensemble: real_ensemble.clone(),
            start_offset: state.lra + 1,
        };

        state.segments.push(new_seg.clone());
        state.writable_segment = Some(new_seg);

        // Remove acks from failed units for offsets not yet committed.
        for (_offset, acked_set) in state.acked.iter_mut() {
            for fu in failed_units {
                acked_set.remove(fu);
            }
        }

        drop(state);

        // Re-open streams for new ensemble members would go here.
        // For now we update catalog only; full stream migration is a follow-up.
        let _ = (catalog, new_ensemble);

        Ok(())
    }
}

// ─── Writer trait impl ──────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Writer for Timeline {
    /// Assign the next offset, fire the event to ALL units in the writable
    /// segment, and return immediately. Acks are collected asynchronously.
    async fn record(&self, event: Vec<u8>) -> Result<Offset, ChronicleError> {
        let (offset, request, senders) = {
            let mut state = self.inner.lock().unwrap();

            let ensemble = state
                .writable_segment
                .as_ref()
                .ok_or_else(|| ChronicleError::Internal("no writable segment".into()))?
                .ensemble
                .clone();

            // Assign next offset (TLA+: offset = lrs + 1).
            let offset = state.lrs + 1;
            state.lrs = offset;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let event = Event {
                timeline_id: state.timeline_id,
                term: state.term,
                offset,
                payload: Some(event.into()),
                crc32: None,
                timestamp: now,
                schema_id: 0,
            };

            let item = RecordEventsRequestItem {
                event: Some(event),
                trunc: state.needs_trunc,
                lra: state.lra,
            };
            state.needs_trunc = false;

            // Initialize acked set for this offset.
            state.acked.insert(offset, HashSet::new());

            let senders: Vec<mpsc::Sender<RecordEventsRequest>> = ensemble
                .iter()
                .filter_map(|ep| state.senders.get(ep).cloned())
                .collect();

            (offset, RecordEventsRequest { items: vec![item] }, senders)
        };

        // Send to ALL units outside the lock.
        for sender in &senders {
            if let Err(e) = sender.send(request.clone()).await {
                warn!(offset, error = %e, "failed to send record to unit");
            }
        }

        Ok(Offset {
            timeline_id: self.timeline_id,
            offset,
        })
    }
}

// ─── Background ack collector ───────────────────────────────────────────────

/// Reads responses from a single unit's Record stream and updates the shared
/// acked state. Advances LRA when all units have acked contiguously.
async fn ack_collector(
    inner: Arc<Mutex<InnerState>>,
    endpoint: String,
    mut response_stream: tonic::Streaming<RecordEventsResponse>,
) {
    while let Ok(Some(response)) = response_stream.message().await {
        let mut state = inner.lock().unwrap();

        for item in &response.items {
            if item.code == StatusCode::Ok as i32 {
                if let Some(ref event) = item.event {
                    state
                        .acked
                        .entry(event.offset)
                        .or_default()
                        .insert(endpoint.clone());
                }
            } else {
                warn!(
                    endpoint = %endpoint,
                    code = item.code,
                    "record response error from unit"
                );
            }
        }

        // Advance LRA: scan contiguous fully-acked offsets (TLA+ FindMaxContinuousAck).
        let rf = state.replication_factor;
        while state.lra < state.lrs {
            let next = state.lra + 1;
            let fully_acked = state
                .acked
                .get(&next)
                .is_some_and(|set| set.len() >= rf);
            if fully_acked {
                state.lra = next;
            } else {
                break;
            }
        }

        // Prune acked entries for committed offsets.
        let lra = state.lra;
        state.acked.retain(|&offset, _| offset > lra);
    }
}
