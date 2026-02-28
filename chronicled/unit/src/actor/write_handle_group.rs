use crate::actor::write_handle_actor::WriteActor;
use crate::actor::Envelope;
use crate::storage::storage::Storage;
use crate::unit::timeline_state::TimelineStateManager;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use std::sync::Arc;
use tonic::Status;

pub struct WriteActorGroup {
    actors: Vec<WriteActor>,
    actor_id_mask: i64,
}

impl WriteActorGroup {
    pub fn new(
        actor_num: usize,
        inflight_num: usize,
        wal: Wal,
        storage: Storage,
        timeline_state: Arc<TimelineStateManager>,
    ) -> Self {
        let mut actors = Vec::with_capacity(actor_num);
        let inflight_per_actor = inflight_num / actor_num;

        for index in 0..actor_num {
            let actor = WriteActor::new(
                index as i32,
                inflight_per_actor,
                wal.clone(),
                storage.clone(),
                timeline_state.clone(),
            );
            actors.push(actor);
        }

        Self {
            actors,
            actor_id_mask: (actor_num - 1) as i64,
        }
    }

    #[inline]
    pub async fn dispatch(
        &self,
        envelope: Envelope<RecordEventsRequestItem, Event, Status>,
    ) -> Result<(), Status> {
        match &envelope.request.event {
            Some(event) => {
                let idx = (event.timeline_id & self.actor_id_mask) as usize;
                self.actors[idx]
                    .send(envelope)
                    .await
                    .map_err(|_| Status::unavailable("write handle actor is unavailable."))?
            }
            _ => return Err(Status::invalid_argument("unexpected empty event.")),
        }
        Ok(())
    }
}
