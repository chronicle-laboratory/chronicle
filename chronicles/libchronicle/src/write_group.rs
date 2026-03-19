use crate::error::ChronicleError;
use crate::state_machine::SharedState;
use crate::{Event as UserEvent, Offset};
use chronicle_proto::pb_ext::{RecordEventsRequest, RecordEventsRequestItem};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

struct PendingEvent {
    event: UserEvent,
    tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

/// A write shard — owns a batch loop that feeds into the shared state machine.
pub(crate) struct WriteGroup {
    event_tx: Mutex<Option<mpsc::Sender<PendingEvent>>>,
    group_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl WriteGroup {
    pub fn start(
        shared: Arc<Mutex<SharedState>>,
        max_batch_size: usize,
        linger: Duration,
    ) -> Self {
        let timeline_id = shared.lock().unwrap().timeline_id;

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(max_batch_size * 2);

        let shared_clone = shared.clone();
        let group_task = tokio::spawn(async move {
            batch_loop(timeline_id, shared_clone, event_rx, max_batch_size, linger).await;
        });

        Self {
            event_tx: Mutex::new(Some(event_tx)),
            group_task: Mutex::new(Some(group_task)),
        }
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let sender = {
            let tx = self.event_tx.lock().unwrap();
            tx.clone()
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

    pub async fn close(&self) {
        let task = {
            self.event_tx.lock().unwrap().take();
            self.group_task.lock().unwrap().take()
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

// --- batching loop ---

async fn batch_loop(
    timeline_id: i64,
    shared: Arc<Mutex<SharedState>>,
    mut event_rx: mpsc::Receiver<PendingEvent>,
    max_batch_size: usize,
    linger: Duration,
) {
    let mut batch: Vec<PendingEvent> = Vec::with_capacity(max_batch_size);

    loop {
        batch.clear();

        match event_rx.recv().await {
            Some(first) => batch.push(first),
            None => return,
        }

        let deadline = tokio::time::sleep(linger);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                event = event_rx.recv() => {
                    match event {
                        Some(e) => {
                            batch.push(e);
                            if batch.len() >= max_batch_size {
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

        let events: Vec<_> = batch
            .drain(..)
            .map(|p| (p.event, p.tx))
            .collect();

        let items = prepare_batch(&shared, timeline_id, events);

        if let Err(e) = replicate(&shared, items).await {
            warn!(error = %e, "failed to replicate batch");
        }
    }
}

// --- prepare + replicate ---

fn prepare_batch(
    shared: &Arc<Mutex<SharedState>>,
    timeline_id: i64,
    events: Vec<(UserEvent, oneshot::Sender<Result<Offset, ChronicleError>>)>,
) -> Vec<RecordEventsRequestItem> {
    let mut s = shared.lock().unwrap();
    let mut items = Vec::with_capacity(events.len());

    for (event, tx) in events {
        let offset = s.lrs + 1;
        s.lrs = offset;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let proto_event = chronicle_proto::pb_ext::Event {
            timeline_id,
            term: s.term,
            offset,
            payload: Some(event.payload.into()),
            crc32: None,
            timestamp: now,
            schema_id: 0,
        };

        let item = RecordEventsRequestItem {
            event: Some(proto_event),
            trunc: s.needs_trunc,
            lra: s.lra,
        };
        s.needs_trunc = false;

        s.acked.insert(offset, std::collections::HashSet::new());
        s.waiters.insert(offset, tx);

        items.push(item);
    }

    items
}

async fn replicate(
    shared: &Arc<Mutex<SharedState>>,
    items: Vec<RecordEventsRequestItem>,
) -> Result<(), ChronicleError> {
    let senders = {
        let s = shared.lock().unwrap();
        s.senders.values().cloned().collect::<Vec<_>>()
    };

    if senders.is_empty() {
        return Err(ChronicleError::EnsembleUnavailable("no connected units".into()));
    }

    let request = RecordEventsRequest { items };
    for sender in &senders {
        if let Err(e) = sender.send(request.clone()).await {
            warn!(error = %e, "failed to send batch to unit");
        }
    }

    Ok(())
}
