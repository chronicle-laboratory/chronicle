use raft::eraftpb::Snapshot;
use raft::{GetEntriesContext, RaftState, Storage};

#[derive(Clone)]
pub struct PartitionStore {}

impl PartitionStore {
    pub(crate) fn apply_entries(&self, p0: Vec<raft::eraftpb::Entry>) {
        todo!()
    }

    pub(crate) fn apply_snapshot(&self, p0: &Snapshot) {
        todo!()
    }
    pub(crate) fn apply_commit_offset(&self, p0: u64) {
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

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        todo!()
    }
}
