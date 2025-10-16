use crate::error::unit_error::UnitError;
use crate::pb_metadata::Proposal;
use raft::{Config, GetEntriesContext, Raft, RaftLog, RaftState, Storage};
use std::collections::VecDeque;

struct PartitionStore {}

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
}

impl Into<Config> for PartitionOptions {
    fn into(self) -> Config {
        Config::default()
    }
}

struct Inner {}

pub struct Partition {
    mailbox: VecDeque<Proposal>,
}

impl Partition {
    fn new(options: PartitionOptions) -> Result<Self, UnitError> {
        let raft_config: Config = options.into();
        let partition_store = PartitionStore {};

        let mut raft = Raft::new(&raft_config, partition_store)?;
    }
}


fn bg_start_state_machine()

