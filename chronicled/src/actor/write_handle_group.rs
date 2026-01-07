use crate::actor::{Envelope, write_handle_actor};
use crate::storage::storage::Storage;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use tonic::Status;
use write_handle_actor::WriteActor;

struct WriteActorGroup {
    actors: Vec<WriteActor>,
    actor_id_mask: i64,
}

impl WriteActorGroup {
    pub fn new(actor_num: usize, inflight_num: usize, wal: Wal, storage: Storage) -> Self {
        let mut actors = Vec::with_capacity(actor_num);
        let inflight_per_actor = inflight_num / actor_num;

        // init actors container
        for index in 0..actor_num {
            let actor = WriteActor::new(
                index as i32,
                inflight_per_actor,
                wal.clone(),
                storage.clone(),
            );
            actors[index] = actor
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
            _ => return Err(Status::invalid_argument("unexpect empty event.")),
        }
        Ok(())
    }
}
