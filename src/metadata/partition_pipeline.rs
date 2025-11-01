use crate::metadata::node_aware::NodeAware;
use crate::metadata::partition_pipeline::Stage::{Snapshot, State};
use crate::metadata::partition_store::PartitionStore;
use crate::pb_metadata::{MailPostChunk, SnapshotData};
use crate::provider::internal_provider_manager::InternalProviderManager;
use Stage::{
    AdvanceApply, AdvanceReady, CommitIndex, CommittedEntries, Entries, HardState, PersistentState,
};
use base64::engine::Engine;
use log::{debug, warn};
use once_cell::sync::Lazy;
use prost::{DecodeError, Message};
use protobuf::Message;
use raft::{LightReady, RawNode, Ready};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

trait PipelineStage {
    fn is_ready(&self, state_machine: &RawNode<PartitionStore>, pipeline: &Pipeline) -> bool;

    fn process(&self, state_machine: &mut RawNode<PartitionStore>, pipeline: &mut Pipeline)
    -> bool;
}

struct StageState;
struct StageSnapshot;
struct StageCommittedEntries;
struct StageEntries;
struct StageHardState;
struct StagePersistentState;
struct StageAdvancedReady;
struct StageCommitIndex;
struct StageAdvanceApply;

impl PipelineStage for StageState {
    fn is_ready(&self, state_machine: &RawNode<PartitionStore>, pipeline: &Pipeline) -> bool {
        !pipeline.ready.messages().is_empty()
    }

    fn process(
        &self,
        _state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        let partition_id = pipeline.partition_id;
        let messages = pipeline.ready.take_messages();
        let mut batches: HashMap<SocketAddr, Vec<Vec<u8>>> = HashMap::new();
        for message in messages {
            let mut buf = Vec::with_capacity(message.compute_size() as usize);
            if let Err(err) = message.write_to_vec(&mut buf) {
                warn!(
                    "Failed to encode the state machine message. partition_id={:?} error: {:?}",
                    partition_id, err
                );
                return false;
            };
            match pipeline.node_aware.get_node_address(message.to) {
                None => {
                    warn!(
                        "Target address can't be found. partition_id={:?} target_node={:?}.",
                        partition_id, message.to
                    );
                    return false;
                }
                Some(address) => {
                    batches.entry(address).or_default().push(buf);
                }
            }
        }
        for (target, batch) in batches {
            match pipeline
                .provider_manager
                .try_get_mail_post_stream(partition_id, target)
            {
                None => {
                    debug!(
                        "Target mail post stream does not init yet. partition_id={:?} target={:?}",
                        partition_id, target
                    );
                    return false;
                }
                Some(sender) => {
                    if let Err(err) = sender.try_send(MailPostChunk {
                        partition_id,
                        payloads: batch,
                    }) {
                        warn!(
                            "Failed to send mail to the stream. partition_id={:?} target={:?} error={:?}",
                            partition_id, target, err
                        );
                        return false;
                    }
                }
            }
        }
        true
    }
}

impl PipelineStage for StageSnapshot {
    fn is_ready(&self, _state_machine: &RawNode<PartitionStore>, _pipeline: &Pipeline) -> bool {
        true
    }

    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        let snapshot = pipeline.ready.snapshot();
        let snapshot_data = match SnapshotData::decode(snapshot.get_data()) {
            Ok(data) => data,
            Err(err) => {
                warn!(
                    "Load snapshot data failed. unexpected behaviour need admin to investigate!  error:{:?}",
                    err
                );
                return false;
            }
        };
        let snapshot_id = snapshot_data.id;
        match pipeline.snapshot_barrier {
            None => {}
            Some(barrier) => if barrier.is_finished() {},
        }
        return false;
    }
}
impl PipelineStage for StageCommittedEntries {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}
impl PipelineStage for StageEntries {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}

impl PipelineStage for StageHardState {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}

impl PipelineStage for StagePersistentState {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}
impl PipelineStage for StageAdvancedReady {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}

impl PipelineStage for StageCommitIndex {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}
impl PipelineStage for StageAdvanceApply {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}

pub enum Stage {
    State(StageState),
    Snapshot(StageSnapshot),
    CommittedEntries(StageCommittedEntries),
    Entries(StageEntries),
    HardState(StageHardState),
    PersistentState(StagePersistentState),
    AdvanceReady(StageAdvancedReady),
    CommitIndex(StageCommitIndex),
    AdvanceApply(StageAdvanceApply),
}

impl PipelineStage for Stage {
    fn is_ready(&self, _state_machine: &RawNode<PartitionStore>, _pipeline: &Pipeline) -> bool {
        true
    }

    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        match self {
            State(stagMessage) => stagMessage.process(state_machine, pipeline),
            Snapshot(stageSnapshot) => stageSnapshot.process(state_machine, pipeline),
            CommittedEntries(commit_entries) => commit_entries.process(state_machine, pipeline),
            Entries(entries) => entries.process(state_machine, pipeline),
            HardState(hard_state) => hard_state.process(state_machine, pipeline),
            PersistentState(persistent_messages) => {
                persistent_messages.process(state_machine, pipeline)
            }
            AdvanceReady(advance_ready) => advance_ready.process(state_machine, pipeline),
            CommitIndex(commit_index) => commit_index.process(state_machine, pipeline),
            AdvanceApply(advance_apply) => advance_apply.process(state_machine, pipeline),
        }
    }
}

static PIPELINE_WORKERS: Lazy<Vec<Stage>> = Lazy::new(|| {
    vec![
        State(StageState),
        Snapshot(StageSnapshot),
        CommittedEntries(StageCommittedEntries),
        Entries(StageEntries),
        HardState(StageHardState),
        PersistentState(StagePersistentState),
        AdvanceReady(StageAdvancedReady),
        CommitIndex(StageCommitIndex),
        AdvanceApply(StageAdvanceApply),
    ]
});

pub struct Pipeline {
    partition_id: u64,
    node_aware: NodeAware,
    provider_manager: InternalProviderManager,
    partition_store: PartitionStore,
    ready: Ready,
    light_ready: Option<LightReady>,
    snapshot_barrier: Option<JoinHandle<(i64, ())>>,
}

impl Pipeline {
    pub fn new(
        partition_id: u64,
        node_aware: NodeAware,
        provider_manager: InternalProviderManager,
        partition_store: PartitionStore,
        ready: Ready,
    ) -> Self {
        Self {
            partition_id,
            node_aware,
            provider_manager,
            partition_store,
            ready,
            light_ready: None,
            snapshot_barrier: None,
        }
    }

    pub fn run(&mut self, state_machine: &mut RawNode<PartitionStore>) {
        for stage in PIPELINE_WORKERS.iter() {
            let keep_going = stage.process(state_machine, self);
            if !keep_going {
                return;
            }
        }
    }
}
