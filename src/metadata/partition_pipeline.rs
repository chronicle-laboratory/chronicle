use crate::metadata::node_aware::NodeAware;
use crate::metadata::partition_pipeline::Stage::{Message, Snapshot};
use crate::metadata::partition_store::PartitionStore;
use crate::provider::internal_provider_manager::InternalProviderManager;
use once_cell::sync::Lazy;
use raft::{LightReady, RawNode, Ready};

trait PipelineStage {
    fn process(&self, state_machine: &mut RawNode<PartitionStore>, pipeline: &mut Pipeline)
    -> bool;
}

struct StageMessage;
struct StageSnapshot;
struct StageCommittedEntries;
struct StageEntries;
struct StageHardState;
struct StagePersistentMessages;
struct StageAdvancedReady;
struct StageCommitIndex;
struct StageAdvanceApply;

pub enum Stage {
    Message(StageMessage),
    Snapshot(StageSnapshot),
    CommittedEntries(StageCommittedEntries),
    Entries(StageEntries),
    HardState(StageHardState),
    PersistentMessages(StagePersistentMessages),
    AdvanceReady(StageAdvancedReady),
    CommitIndex(StageCommitIndex),
    AdvanceApply(StageAdvanceApply),
}

impl PipelineStage for Stage {
    fn process(
        &self,
        state_machine: &mut RawNode<PartitionStore>,
        pipeline: &mut Pipeline,
    ) -> bool {
        todo!()
    }
}

static PIPELINE_WORKERS: Lazy<Vec<Stage>> =
    Lazy::new(|| vec![Message(StageMessage), Snapshot(StageSnapshot)]);

pub struct Pipeline {
    partition_id: u64,
    node_aware: NodeAware,
    provider_manager: InternalProviderManager,
    partition_store: PartitionStore,
    ready: Ready,
    light_ready: Option<LightReady>,
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
