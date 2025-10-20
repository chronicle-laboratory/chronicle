use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::MetadataPartition;
use crate::metadata::provider_manager::ProviderManager;
use crate::pb_metadata::Entry;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use dashmap::DashMap;
use log::error;
use prost::Message;
use raft::eraftpb::{ConfChange, Snapshot};
use raft::{Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Clone)]
struct PartitionStore {}

impl PartitionStore {
    pub(crate) fn apply_snapshot(&self, p0: &Snapshot) {
        todo!()
    }
}

impl Storage for PartitionStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<String> {
        todo!()
    }
}

struct PartitionOptions {
    id: u64,
    provider_manager: ProviderManager,
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
    PartitionState(raft::eraftpb::Message),
}

struct Inner {}

pub struct Partition {
    mailbox: Sender<Mail>,
}

impl Partition {
    fn new(
        options: PartitionOptions,
        provider_manager: ProviderManager,
    ) -> Result<Self, UnitError> {
        let raft_config: Config = options.into();
        let partition_store = PartitionStore {};

        let state_machine = RawNode::with_default_logger(&raft_config, partition_store.clone())
            .map_err(|err| MetadataPartition(err.to_string()))?;
        let (mailbox_tx, mailbox_rx) = unbounded::<Mail>();
        let handle =
            bg_start_state_machine(state_machine, mailbox_rx, provider_manager, partition_store);
        Ok(Self {
            mailbox: mailbox_tx,
        })
    }
}

fn bg_start_state_machine(
    mut state_machine: RawNode<PartitionStore>,
    mailbox: Receiver<Mail>,
    provider_manager: ProviderManager,
    partition_store: PartitionStore,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(100);
        loop {
            match mailbox.recv_timeout(timeout) {
                Ok(mail) => propose_mail(&mut state_machine, mail),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            if t.elapsed() > timeout {
                state_machine.tick();
                t = Instant::now();
            }

            if !state_machine.has_ready() {
                continue;
            }

            let mut ready_status = state_machine.ready();
            if !ready_status.messages().is_empty() {
                let state = ready_status.take_messages();
                process_state(&mut state_machine, &provider_manager, state).await
            }

            if !ready_status.snapshot().is_empty() {
                let snapshot = ready_status.snapshot();
                load_snapshot(&provider_manager, snapshot).await;
                partition_store.apply_snapshot(snapshot)
            }

            if !ready_status.committed_entries().is_empty() {
                let committed_entries = ready_status.take_committed_entries();
                process_committed_entry(&mut state_machine, committed_entries).await
            }

            if ready_status.entries().is_empty() {
                let entries = ready_status.take_entries();
                partition_store.apply_entries(entries)
            }

            let mut light_ready = state_machine.advance(ready_status);
            if let Some(commit_index) = light_ready.commit_index() {
                // state persist
                partition_store.apply_commit_offset(commit_index);
            }

            if !light_ready.messages().is_empty() {
                let state = light_ready.take_messages();
                process_state(&mut state_machine, &provider_manager, state).await
            }

            if !light_ready.committed_entries().is_empty() {
                let committed_entries = light_ready.take_committed_entries();
                process_committed_entry(&mut state_machine, committed_entries).await
            }

            state_machine.advance_apply();
        }
    })
}

#[inline]
async fn load_snapshot(provider_manager: &ProviderManager, snapshot: &Snapshot) {}

#[inline]
async fn process_committed_entry(
    state_machine: &mut RawNode<PartitionStore>,
    committed_entries: Vec<raft::eraftpb::Entry>,
) {
    for committed_entry in committed_entries {}
}

#[inline]
async fn process_state(
    state_machine: &mut RawNode<PartitionStore>,
    provider_manager: &ProviderManager,
    states: Vec<raft::eraftpb::Message>,
) {
    for state in states {
        let target = state.to;
        let provider = provider_manager.get_provider(target);
    }
}

#[inline]
fn propose_mail(state_machine: &mut RawNode<PartitionStore>, mail: Mail) {
    match mail {
        Mail::Proposal(proposal) => {
            let soft_state = state_machine.raft.soft_state();
            match soft_state.raft_state {
                StateRole::Leader => match proposal.proposal {
                    Proposal::DataEntry(entry) => {
                        let buf = entry.encode_to_vec();
                        if let Err(err) = state_machine.propose(vec![], buf) {
                            error!("Partition propose data entry state machine error. {}", err)
                        }
                    }
                    Proposal::ConfigChange(config_change) => {
                        if let Err(err) = state_machine.propose_conf_change(vec![], config_change) {
                            error!(
                                "Partition propose config change state machine error. {}",
                                err
                            )
                        }
                    }
                    Proposal::LeaderTransfer(transfer) => state_machine.transfer_leader(transfer),
                },
                StateRole::Follower => {}
                StateRole::Candidate => {}
                StateRole::PreCandidate => {}
            }
        }
        Mail::PartitionState(state) => {
            if let Err(err) = state_machine.step(state) {
                error!("Partition step state machine error. {}", err)
            }
        }
    }
}
