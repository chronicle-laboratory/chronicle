use crate::error::ChronicleError;
use crate::state_machine::StateMachine;
use crate::{Event as UserEvent, Offset};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub(crate) struct PendingEvent {
    pub event: UserEvent,
    pub tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

pub(crate) async fn run(
    timeline_id: i64,
    sm: Arc<StateMachine>,
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

        let items = sm.prepare_batch(timeline_id, events);

        if let Err(e) = sm.replicate(items).await {
            warn!(error = %e, "failed to replicate batch");
        }
    }
}
