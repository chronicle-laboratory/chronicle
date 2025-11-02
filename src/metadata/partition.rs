use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::{MetadataPartition, MetadataPartitionNotLeader};
use crate::metadata::node_aware::NodeAware;
use crate::metadata::partition::Proposal::ConfigChange;
use crate::metadata::partition_pipeline::Pipeline;
use crate::metadata::partition_store::PartitionStore;
use crate::pb_metadata::Entry;
use crate::provider::internal_provider_manager::InternalProviderManager;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use log::{error, warn};
use prost::Message as _;
use raft::eraftpb::ConfChangeV2;
use raft::eraftpb::Message;
use raft::{Config, RawNode, StateRole};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::Instant;

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
    ConfigChange(ConfChangeV2),
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
            pipeline.run(&mut state_machine).await
        }
    })
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
                    ConfigChange(config_change) => {
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
