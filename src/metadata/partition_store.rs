use crate::error::unit_error::UnitError;
use crate::log::log::Log;
use crate::pb_metadata::{SnapshotChunk, SnapshotData};
use crate::storage::storage::Storage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::{GetEntriesContext, RaftState};

#[derive(Clone)]
pub struct PartitionStore {
    partition_id: u64,
    storage: Storage,
    log: Log,
}

impl PartitionStore {
    pub(crate) fn apply_hard_state(&self, p0: &HardState) {
        todo!()
    }
    pub(crate) fn append_entries(&self, p0: Vec<Entry>) {
        todo!()
    }
    pub(crate) fn prepare_snapshot(&self, p0: u64) {
        todo!()
    }
    pub(crate) fn apply_snapshot_chunk(&self, chunk: SnapshotChunk) -> Result<(), UnitError> {
        todo!()
    }

    pub(crate) fn apply_snapshot(
        &self,
        snapshot_metadata: SnapshotMetadata,
        snapshot_data: SnapshotData,
    ) {
        todo!()
    }
    pub(crate) fn apply_conf_state(&self, conf_state: ConfState) {
        todo!()
    }

    pub(crate) fn apply_entry(&self, entry: Entry) {
        todo!()
    }
    pub(crate) fn apply_commit_offset(&self, p0: u64) {
        todo!()
    }
}

impl raft::Storage for PartitionStore {
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
