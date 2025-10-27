use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::{MetadataPartition, MetadataPartitionNotLeader};
use crate::metadata::node_aware::NodeAware;
use crate::metadata::partition::Proposal::ConfigChange;
use crate::metadata::partition_pipeline::Pipeline;
use crate::metadata::partition_store::PartitionStore;
use crate::pb_metadata::{Entry, MailPostChunk, SnapshotData};
use crate::provider::internal_provider_manager::InternalProviderManager;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded};
use log::{debug, error, warn};
use memberlist::futures::{AsyncReadExt, TryFutureExt};
use prost::{DecodeError, Message as _};
use protobuf::Message as _;
use raft::eraftpb::EntryType::EntryNormal;
use raft::eraftpb::{ConfChange, Snapshot};
use raft::eraftpb::{Message, SnapshotMetadata};
use raft::{Config, GetEntriesContext, RaftState, RawNode, Ready, StateRole, Storage};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::task::ready;
use std::time::Duration;
use sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{spawn, sync};
use tracing::field::debug;

struct PartitionOptions {
    id: u64,
    provider_manager: InternalProviderManager,
}

impl Into<Config> for PartitionOptions {
    fn into(self) -> Config {
        Config::default()
    }
}

pub enum Proposal {
    DataEntry(Entry),
    ConfigChange(ConfChange),
    LeaderTransfer(u64),
}

struct InflightProposal {
    proposal: Proposal,
    callback: Sender<UnitError>,
}

pub enum Mail {
    Proposal(InflightProposal),
    PartitionState(Message),
}

struct Inner {
    mailbox: Sender<Mail>,
    followers: HashMap<u64, Sender<Message>>,
}

#[derive(Clone)]
pub struct Partition {
    id: u64,
    inner: Arc<Inner>,
    node_aware: NodeAware,
    state_machine_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl Partition {
    fn new(
        options: PartitionOptions,
        node_aware: NodeAware,
        provider_manager: InternalProviderManager,
    ) -> Result<Self, UnitError> {
        let partition_id = options.id;
        let raft_config: Config = options.into();
        let partition_store = PartitionStore {};
        let state_machine = RawNode::with_default_logger(&raft_config, partition_store.clone())
            .map_err(|err| MetadataPartition(err.to_string()))?;
        let (mailbox_tx, mailbox_rx) = bounded::<Mail>(1024);
        let state_machine_handle = bg_start_state_machine(
            partition_id,
            node_aware.clone(),
            state_machine,
            mailbox_rx,
            provider_manager,
            partition_store,
        );
        Ok(Self {
            id: partition_id,
            inner: Arc::new(Inner {
                mailbox: mailbox_tx,
                followers: Default::default(),
            }),
            node_aware,
            state_machine_handle: Arc::new(Mutex::new(state_machine_handle)),
        })
    }

    async fn get_follower_stream(&self, follower_id: u64) -> Sender<Message> {
        todo!()
    }
}

fn bg_start_state_machine(
    partition_id: u64,
    node_aware: NodeAware,
    mut state_machine: RawNode<PartitionStore>,
    mailbox: Receiver<Mail>,
    provider_manager: InternalProviderManager,
    partition_store: PartitionStore,
) -> JoinHandle<()> {
    spawn(async move {
        let mut t = Instant::now();
        let timeout = Duration::from_millis(100);
        loop {
            loop {
                match mailbox.recv_timeout(timeout) {
                    Ok(mail) => propose_mail(&mut state_machine, mail),
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
            if t.elapsed() > timeout {
                state_machine.tick();
                t = Instant::now();
            }
            if !state_machine.has_ready() {
                continue;
            }
            let ready = state_machine.ready();
            let mut pipeline = Pipeline::new(
                partition_id,
                node_aware.clone(),
                provider_manager.clone(),
                partition_store.clone(),
                ready,
            );
            pipeline.run(&mut state_machine)

            // let mut ready_status = state_machine.ready();
            // if !ready_status.messages().is_empty() {
            //     if !process_message(
            //         partition_id,
            //         &node_aware,
            //         &mut state_machine,
            //         &provider_manager,
            //         ready_status.take_messages(),
            //     ) {
            //         continue;
            //     }
            // }
            // if !ready_status.snapshot().is_empty() {
            //     if !process_snapshot(&node_aware, &provider_manager, &mut ready_status) {
            //         continue;
            //     }
            // }
            // if !is_snapshot_applied(&mut snapshot_load_handle) {
            //     continue;
            // }
            // if !ready_status.committed_entries().is_empty() {
            //     let committed_entries = ready_status.take_committed_entries();
            //     process_committed_entries(&mut state_machine, committed_entries).await
            // }
            // if ready_status.entries().is_empty() {
            //     let entries = ready_status.take_entries();
            //     if !process_entries() {
            //         continue;
            //     }
            // }
            // if let Some(hs) = ready_status.hs() {
            //     if !process_hard_status() {
            //         continue;
            //     }
            // }
            // if !ready_status.persisted_messages().is_empty() {
            //     let vec = ready_status.take_persisted_messages();
            //     if !process_persisted_messages() {
            //         continue;
            //     }
            // }
            // let mut light_ready = state_machine.advance(ready_status);
            // if let Some(commit_index) = light_ready.commit_index() {
            //     if !process_commit_index() {}
            // }
            // if !light_ready.messages().is_empty() {
            //     if !process_message(
            //         partition_id,
            //         &node_aware,
            //         &mut state_machine,
            //         &provider_manager,
            //         light_ready.take_messages(),
            //     ) {
            //         continue;
            //     }
            // }
            // if !light_ready.committed_entries().is_empty() {
            //     let committed_entries = light_ready.take_committed_entries();
            //     if !process_committed_entries(&mut state_machine, committed_entries) {
            //         continue;
            //     }
            // }
            // state_machine.advance_apply();
        }
    })
}

fn process_commit_index() -> bool {}

fn process_entries() -> bool {}

fn process_hard_status() -> bool {}

fn process_persisted_messages() -> bool {}

fn is_snapshot_applied(load_handle: &mut Option<(u64, JoinHandle<Result<(), UnitError>>)>) -> bool {
}

fn process_snapshot(
    node_aware: &NodeAware,
    provider_manager: &InternalProviderManager,
    mut ready_status: &mut Ready,
) -> bool {
    if !ready_status.snapshot().is_empty() {
        let snapshot = ready_status.snapshot();
        match SnapshotData::decode(snapshot.get_data()) {
            Ok(snapshot_data) => {
                match &snapshot_load_handle {
                    None => {}
                    Some((snapshot_id, handle)) => {
                        let own_snapshot_id = snapshot_id.clone();
                        if own_snapshot_id != snapshot_data.id {
                            if !handle.is_finished() {
                                handle.abort_handle().abort();
                            } else {
                                snapshot_load_handle = Some((
                                    own_snapshot_id,
                                    spawn(load_snapshot(
                                        &node_aware,
                                        &provider_manager,
                                        snapshot.get_metadata(),
                                        &snapshot_data,
                                    )),
                                ))
                            }
                            return true; // skip to the next round
                        }
                    }
                }
            }
            Err(err) => {
                warn!(
                    "Failed to decode snapshot data, break and retry in the next round. Error: {}",
                    err
                );
                return true;
            }
        };
    }
    // waiting for snapshot apply first
    if let Some((_, handle)) = &snapshot_load_handle {
        if !handle.is_finished() {
            return true;
        }
        let (_, handle) = snapshot_load_handle.take().unwrap();
        match handle.await {
            Ok(result) => {
                if let Err(err) = result {
                    warn!(
                        "Failed to load snapshot, break and retry in the next round. Error: {}",
                        err
                    );
                    return true;
                }
            }
            Err(join_error) => {
                warn!(
                    "Failed to load snapshot, break and retry in the next round. Error: {}",
                    join_error
                );
                return true;
            }
        }
    }
    false
}

#[inline]
async fn load_snapshot(
    node_aware: &NodeAware,
    provider_manager: &InternalProviderManager,
    snapshot_meta: &SnapshotMetadata,
    snapshot_data: &SnapshotData,
) -> Result<(), UnitError> {
    // let option = node_aware.get_node_address(snapshot_data.from);
    // provider_manager.try_get_provider(snapshot_data.from)
    todo!()
}

#[inline]
fn process_committed_entries(
    state_machine: &mut RawNode<PartitionStore>,
    committed_entries: Vec<raft::eraftpb::Entry>,
) -> bool {
    for committed_entry in committed_entries {}
    todo!()
}

#[inline]
fn process_message(
    partition_id: u64,
    node_aware: &NodeAware,
    _: &mut RawNode<PartitionStore>,
    provider_manager: &InternalProviderManager,
    states: Vec<Message>,
) -> bool {
    let mut batches: HashMap<SocketAddr, Vec<Vec<u8>>> = HashMap::new();
    for state in states {
        let mut buf = Vec::with_capacity(state.compute_size() as usize);
        if let Err(err) = state.write_to_vec(&mut buf) {
            warn!(
                "Failed to encode the state machine message. partition_id={:?} error: {:?}",
                partition_id, err
            );
            return false;
        };
        match node_aware.get_node_address(state.to) {
            None => {
                warn!(
                    "Target address can't be found. partition_id={:?} target_node={:?}.",
                    partition_id, state.to
                );
                return false;
            }
            Some(address) => {
                batches.entry(address).or_default().push(buf);
            }
        }
    }
    for (target, batch) in batches {
        match provider_manager.try_get_mail_post_stream(partition_id, target) {
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

#[inline]
fn propose_mail(state_machine: &mut RawNode<PartitionStore>, mail: Mail) {
    match mail {
        Mail::Proposal(proposal) => {
            let soft_state = state_machine.raft.soft_state();
            let result = match soft_state.raft_state {
                StateRole::Leader => match proposal.proposal {
                    Proposal::DataEntry(entry) => {
                        let buf = entry.encode_to_vec();
                        if let Err(err) = state_machine.propose(vec![], buf) {
                            error!("Partition propose data entry state machine error. {}", err);
                            Err(MetadataPartition(err.to_string()))
                        } else {
                            Ok(())
                        }
                    }
                    Proposal::ConfigChange(config_change) => {
                        if let Err(err) = state_machine.propose_conf_change(vec![], config_change) {
                            error!(
                                "Partition propose config change state machine error. {}",
                                err
                            );
                            Err(MetadataPartition(err.to_string()))
                        } else {
                            Ok(())
                        }
                    }
                    Proposal::LeaderTransfer(transfer) => {
                        state_machine.transfer_leader(transfer);
                        Ok(())
                    }
                },
                StateRole::Follower => Err(MetadataPartitionNotLeader()),
                StateRole::Candidate => Err(MetadataPartitionNotLeader()),
                StateRole::PreCandidate => Err(MetadataPartitionNotLeader()),
            };
            if let Err(err) = result {
                if let Err(err) = proposal.callback.send(err) {
                    warn!("Send callback to proposal failed: {} ", err);
                }
            }
        }
        Mail::PartitionState(state) => {
            if let Err(err) = state_machine.step(state) {
                error!("Partition step state machine error. {}", err)
            }
        }
    }
}
