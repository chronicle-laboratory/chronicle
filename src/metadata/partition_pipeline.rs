use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::MetadataPartition;
use crate::metadata::node_aware::NodeAware;
use crate::metadata::partition_pipeline::Stage::{
    LightReadyCommittedEntries, LightReadyStates, Snapshot, States,
};
use crate::pb_metadata::{MailPostChunk, SnapshotData, SnapshotRequest};
use crate::provider::internal_provider_manager::InternalProviderManager;
use crate::storage::storage::Storage;
use Stage::{
    AdvanceApply, AdvanceReady, CommitIndex, CommittedEntries, Entries, HardState, PersistentStates,
};
use backoff::Error;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use prost::Message as ProstMessage;
use protobuf::Message;
use raft::LightReady;
use raft::RawNode;
use raft::Ready;
use raft::eraftpb;
use raft::eraftpb::ConfChangeV2;
use raft::eraftpb::EntryType;
use raft::prelude::EntryType::EntryConfChangeV2;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::StreamExt;

trait PipelineStage {
    async fn process(&self, state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool;
}
/**

todo:

1. asynchronous persistent-store operations.
2. backpressure
3. gating
4. metrics
**/

struct StageStates;
struct StageSnapshot;
struct StageCommittedEntries;
struct StageEntries;
struct StageHardState;
struct StagePersistentStates;
struct StageAdvancedReady;
struct StageCommitIndex;

struct StageLightReadyStates;
struct StageLightCommittedEntries;
struct StageAdvanceApply;

#[inline]
fn handle_committed_entries(state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool {
    let committed_entries = match &mut pipeline.ready {
        None => match &mut pipeline.light_ready {
            None => return true,
            Some(ready) => ready.take_committed_entries(),
        },
        Some(ready) => ready.take_committed_entries(),
    };
    for entry in committed_entries {
        if entry.data.is_empty() {
            // Empty entry, when the peer becomes Leader it will send an empty entry.
            continue;
        }
        match EntryType::from_i32(entry.entry_type) {
            None => {
                warn!("Unknown entry type: {}", entry.entry_type);
                continue;
            }
            Some(entry_type) => match entry_type {
                EntryType::EntryNormal => pipeline.partition_store.apply_entry(entry),
                EntryConfChangeV2 => {
                    let mut cc = ConfChangeV2::default();
                    if let Err(err) = cc.merge_from_bytes(&entry.data) {
                        error!("Decode a malformed config data. error:{:?}", err);
                        return false;
                    }
                    match state_machine.apply_conf_change(&cc) {
                        Ok(cc) => pipeline.partition_store.apply_conf_state(cc),
                        Err(err) => {
                            error!("apply config data failed. error:{:?}", err);
                            return false;
                        }
                    }
                }
                _ => {
                    warn!(
                        "Receive unsupported entry type. type:{:?}",
                        entry.entry_type
                    );
                }
            },
        }
    }
    true
}

#[inline]
fn broadcast_states(pipeline: &mut Pipeline, messages: Vec<eraftpb::Message>) -> bool {
    let mut batches: HashMap<SocketAddr, Vec<Vec<u8>>> = HashMap::new();
    for message in messages {
        let mut buf = Vec::with_capacity(message.compute_size() as usize);
        if let Err(err) = message.write_to_vec(&mut buf) {
            warn!(
                "Failed to encode the state machine message. partition_id={:?} error: {:?}",
                pipeline.partition_id, err
            );
            return false;
        };
        match pipeline.node_aware.try_get_node_address(message.to) {
            None => {
                warn!(
                    "Target address can't be found. partition_id={:?} target_node={:?}.",
                    pipeline.partition_id, message.to
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
            .try_get_mail_post_stream(pipeline.partition_id, target)
        {
            None => {
                debug!(
                    "Target mail post stream does not init yet. partition_id={:?} target={:?}",
                    pipeline.partition_id, target
                );
                return false;
            }
            Some(sender) => {
                if let Err(err) = sender.try_send(MailPostChunk {
                    partition_id: pipeline.partition_id,
                    payloads: batch,
                }) {
                    warn!(
                        "Failed to send mail to the stream. partition_id={:?} target={:?} error={:?}",
                        pipeline.partition_id, target, err
                    );
                    return false;
                }
            }
        }
    }
    true
}

impl PipelineStage for StageStates {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.messages().is_empty() {
                return true;
            }
            let states = ready.take_messages();
            return broadcast_states(pipeline, states);
        }
        false
    }
}

impl PipelineStage for StageSnapshot {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.snapshot().is_empty() && pipeline.snapshot_barrier.is_none() {
                return true;
            }
            let snapshot = ready.snapshot();
            // todo: should we check if the metadata is empty?
            if snapshot.is_empty() {
                if let Some((_, handler)) = &pipeline.snapshot_barrier {
                    if !handler.is_finished() {
                        return false;
                    }
                    pipeline.snapshot_barrier.take();
                    return true;
                }
            }
            let snapshot_metadata = snapshot.metadata.clone().unwrap();
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
            match &pipeline.snapshot_barrier {
                None => {}
                Some((barrier_snapshot_id, handler)) => {
                    let bsi = barrier_snapshot_id.clone();
                    // same snapshot
                    if bsi == snapshot_id {
                        if !handler.is_finished() {
                            // continue waiting for the current async snapshot load
                            return false;
                        }
                        // load finished
                        pipeline.snapshot_barrier.take();
                        return true;
                    }
                    let (_, handler) = pipeline.snapshot_barrier.take().unwrap();
                    handler.abort();
                    let _ = handler.await; // wait for previous background canceled; maybe we can consider two-phrase validation?
                }
            };

            let from = snapshot_data.from;
            // loading
            info!("Prepare to load snapshot from node: {}", from);
            let provider_manager = pipeline.provider_manager.clone();
            let node_aware = pipeline.node_aware.clone();
            let partition_id = pipeline.partition_id;
            let partition_store = pipeline.partition_store.clone();
            let handle = tokio::spawn(async move {
                info!("Loading snapshot from node: {}", snapshot_id);
                let retryable = || async {
                    let node_addr = &node_aware.get_node_address(from)?;
                    let provider = &mut provider_manager.get_provider(node_addr.clone())?;
                    let request = SnapshotRequest {
                        partition_id,
                        snapshot_id,
                    };
                    let response = provider
                        .load_snapshot(request)
                        .await
                        .map_err(|err| MetadataPartition(err.to_string()))?;
                    let mut streaming = response.into_inner();
                    partition_store.prepare_snapshot(snapshot_id);
                    loop {
                        let option_result = streaming.next().await;
                        match option_result {
                            None => {
                                info!("Finish snapshot loading stream.");
                                break;
                            }
                            Some(chunk_result) => {
                                let snapshot_chunk = chunk_result
                                    .map_err(|err| MetadataPartition(err.to_string()))?;
                                let chunk_name = snapshot_chunk.name.clone();
                                let chunk_index = snapshot_chunk.index;
                                let chunk_total = snapshot_chunk.total;
                                &partition_store.apply_snapshot_chunk(snapshot_chunk)?;
                                info!(
                                    "Loaded snapshot chunk. name={}  index={} total={} ",
                                    chunk_name, chunk_index, chunk_total
                                );
                            }
                        }
                    }
                    Ok::<(), Error<UnitError>>(())
                };
                // notice: this backoff should never have permanent error
                let _ = backoff::future::retry_notify(
                    backoff::ExponentialBackoff::default(),
                    retryable,
                    |error, duration| {
                        warn!(
                            "Loading snapshot failed. error: {:?}, retry-after: {:?}",
                            error, duration
                        )
                    },
                )
                .await;
                partition_store.apply_snapshot(snapshot_metadata, snapshot_data);
                ()
            });
            pipeline.snapshot_barrier = Some((snapshot_id, handle));
        }
        false
    }
}
impl PipelineStage for StageCommittedEntries {
    async fn process(&self, state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.committed_entries().is_empty() {
                return true;
            }
            return handle_committed_entries(state_machine, pipeline);
        }
        false
    }
}

impl PipelineStage for StageEntries {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.entries().is_empty() {
                return true;
            }
            pipeline
                .partition_store
                .append_entries(ready.take_entries());
            return true;
        }
        false
    }
}

impl PipelineStage for StageHardState {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.hs().is_none() {
                return true;
            }
            pipeline
                .partition_store
                .apply_hard_state(ready.hs().unwrap());
            return true;
        }
        false
    }
}

impl PipelineStage for StagePersistentStates {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(ready) = &mut pipeline.ready {
            if ready.persisted_messages().is_empty() {
                return true;
            }
            let states = ready.take_persisted_messages();
            return broadcast_states(pipeline, states);
        }
        false
    }
}
impl PipelineStage for StageAdvancedReady {
    async fn process(&self, state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool {
        if let Some(ready) = pipeline.ready.take() {
            let light_ready = state_machine.advance(ready);
            pipeline.light_ready = Some(light_ready);
        }
        true
    }
}

impl PipelineStage for StageCommitIndex {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(light_ready) = &mut pipeline.light_ready {
            pipeline
                .partition_store
                .apply_commit_offset(light_ready.commit_index().unwrap());
            return true;
        }
        false
    }
}

impl PipelineStage for StageLightReadyStates {
    async fn process(
        &self,
        _state_machine: &mut RawNode<Storage>,
        pipeline: &mut Pipeline,
    ) -> bool {
        if let Some(light_ready) = &mut pipeline.light_ready {
            let states = light_ready.take_messages();
            return broadcast_states(pipeline, states);
        }
        false
    }
}

impl PipelineStage for StageLightCommittedEntries {
    async fn process(&self, state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool {
        if let Some(ready) = &mut pipeline.light_ready {
            if ready.committed_entries().is_empty() {
                return true;
            }
            return handle_committed_entries(state_machine, pipeline);
        }
        false
    }
}

impl PipelineStage for StageAdvanceApply {
    async fn process(
        &self,
        state_machine: &mut RawNode<Storage>,
        _pipeline: &mut Pipeline,
    ) -> bool {
        state_machine.advance_apply();
        false
    }
}

pub enum Stage {
    States(StageStates),
    Snapshot(StageSnapshot),
    CommittedEntries(StageCommittedEntries),
    Entries(StageEntries),
    HardState(StageHardState),
    PersistentStates(StagePersistentStates),
    AdvanceReady(StageAdvancedReady),
    CommitIndex(StageCommitIndex),
    LightReadyStates(StageLightReadyStates),
    LightReadyCommittedEntries(StageLightCommittedEntries),
    AdvanceApply(StageAdvanceApply),
}

impl PipelineStage for Stage {
    async fn process(&self, state_machine: &mut RawNode<Storage>, pipeline: &mut Pipeline) -> bool {
        match self {
            States(stage) => stage.process(state_machine, pipeline).await,
            Snapshot(stage) => stage.process(state_machine, pipeline).await,
            CommittedEntries(stage) => stage.process(state_machine, pipeline).await,
            Entries(stage) => stage.process(state_machine, pipeline).await,
            HardState(stage) => stage.process(state_machine, pipeline).await,
            PersistentStates(stage) => stage.process(state_machine, pipeline).await,
            AdvanceReady(stage) => stage.process(state_machine, pipeline).await,
            CommitIndex(stage) => stage.process(state_machine, pipeline).await,
            LightReadyStates(stage) => stage.process(state_machine, pipeline).await,
            LightReadyCommittedEntries(stage) => stage.process(state_machine, pipeline).await,
            AdvanceApply(stage) => stage.process(state_machine, pipeline).await,
        }
    }
}

static PIPELINE_WORKERS: Lazy<Vec<Stage>> = Lazy::new(|| {
    vec![
        States(StageStates),
        Snapshot(StageSnapshot),
        CommittedEntries(StageCommittedEntries),
        Entries(StageEntries),
        HardState(StageHardState),
        PersistentStates(StagePersistentStates),
        AdvanceReady(StageAdvancedReady),
        CommitIndex(StageCommitIndex),
        LightReadyStates(StageLightReadyStates),
        LightReadyCommittedEntries(StageLightCommittedEntries),
        AdvanceApply(StageAdvanceApply),
    ]
});

pub struct Pipeline {
    partition_id: u64,
    node_aware: NodeAware,
    provider_manager: InternalProviderManager,
    partition_store: Storage,
    ready: Option<Ready>,
    light_ready: Option<LightReady>,
    snapshot_barrier: Option<(u64, JoinHandle<()>)>,
}

impl Pipeline {
    pub fn new(
        partition_id: u64,
        node_aware: NodeAware,
        provider_manager: InternalProviderManager,
        partition_store: Storage,
        ready: Ready,
    ) -> Self {
        Self {
            partition_id,
            node_aware,
            provider_manager,
            partition_store,
            ready: Some(ready),
            light_ready: None,
            snapshot_barrier: None,
        }
    }

    pub async fn run(&mut self, state_machine: &mut RawNode<Storage>) {
        for stage in PIPELINE_WORKERS.iter() {
            let keep_going = stage.process(state_machine, self).await;
            if !keep_going {
                return;
            }
        }
    }
}
