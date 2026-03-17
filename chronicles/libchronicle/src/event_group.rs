use crate::error::ChronicleError;
use crate::state_machine::{self, ReplicationState};
use crate::{Event as UserEvent, Offset};
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub(crate) struct PendingEvent {
    pub event: UserEvent,
    pub tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

pub(crate) async fn run(
    timeline_id: i64,
    state: Arc<Mutex<ReplicationState>>,
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

        let items = {
            let mut s = state.lock().unwrap();
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

                s.acked.insert(offset, HashSet::new());
                s.waiters.insert(offset, pending.tx);

                items.push(item);
            }

            items
        };

        if let Err(e) = state_machine::replicate(&state, items).await {
            warn!(error = %e, "failed to replicate batch");
        }
    }
}
