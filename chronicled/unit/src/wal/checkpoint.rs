use crate::storage::index::Storage;
use crate::error::unit_error::UnitError;

/// Well-known RocksDB key for the WAL checkpoint.
/// Stored alongside segment meta with 0xFE prefix to avoid collision.
const WAL_CHECKPOINT_KEY: [u8; 1] = [0xFE];

/// WAL checkpoint — tracks the last fully-flushed WAL segment.
/// All segments with ID strictly below `segment_id` can be safely trimmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCheckpoint {
    /// The segment ID up to which data has been durably flushed to L0 + indexed.
    pub segment_id: u64,
}

impl WalCheckpoint {
    pub fn new(segment_id: u64) -> Self {
        Self { segment_id }
    }

    fn encode(&self) -> [u8; 8] {
        self.segment_id.to_be_bytes()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 8 {
            let segment_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
            Some(Self { segment_id })
        } else {
            None
        }
    }
}

/// Persist the WAL checkpoint — the segment ID up to which all data
/// has been durably flushed to L0 segments and indexed.
///
/// WAL segments with ID strictly below this value can be safely deleted.
pub fn write_checkpoint(index: &Storage, checkpoint: &WalCheckpoint) -> Result<(), UnitError> {
    index.put_raw(&WAL_CHECKPOINT_KEY, &checkpoint.encode())
}

/// Read the last persisted WAL checkpoint.
/// Returns segment_id=0 if no checkpoint exists (fresh start).
pub fn read_checkpoint(index: &Storage) -> WalCheckpoint {
    index
        .get_raw(&WAL_CHECKPOINT_KEY)
        .and_then(|v| WalCheckpoint::decode(&v))
        .unwrap_or(WalCheckpoint { segment_id: 0 })
}
