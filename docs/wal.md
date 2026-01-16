# Write-Ahead Log (WAL) Implementation

## Overview

The Chronicle WAL is a batchable, standalone write-ahead log implementation inspired by [RocksDB's WAL design](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29). It provides durability guarantees for write operations with support for batching, checksums, and crash recovery.

## Architecture

### Components

1. **WAL**: Main interface for appending and recovering records
2. **Record**: Encodes/decodes individual log entries with checksums
3. **RecordBatch**: Groups multiple records for efficient writes
4. **Segment**: Manages physical log files on disk

### Record Format

Each record in the WAL uses the RocksDB-compatible format:

```
+----------+-----------+-----------+--- ... ---+
|CRC (4B)  | Size (2B) | Type (1B) | Payload   |
+----------+-----------+-----------+--- ... ---+
```

- **CRC**: CRC32 checksum computed over the type and payload (we use XXH32 for performance)
- **Size**: Length of the payload data in bytes (little-endian u16, max 65535 bytes)
- **Type**: Record type (Full=1, First=2, Middle=3, Last=4) for block-based storage
- **Payload**: The actual record data

This format is compatible with RocksDB's WAL record structure, enabling interoperability and future enhancements like block-based storage and recycling.

### Write Path

1. **Append**: Client calls `wal.append(data)` with a record
2. **Buffer**: Record is queued in an in-memory channel buffer
3. **Batch**: Background writer collects records into batches
4. **Encode**: Batch is encoded with record headers and checksums
5. **Write**: Encoded batch is written to the current segment
6. **Sync**: Background syncer fsync()s the segment to disk
7. **Notify**: Clients are notified of successful persistence

```
┌─────────┐    ┌─────────────┐    ┌───────────┐    ┌──────────┐
│ Client  │───▶│ WAL Buffer  │───▶│  Batch    │───▶│ Segment  │
└─────────┘    └─────────────┘    └───────────┘    └──────────┘
                                         │                │
                                         │                ▼
                                         │          ┌──────────┐
                                         └─────────▶│  Syncer  │
                                                    └──────────┘
```

### Recovery

On startup or after a crash, the WAL can be recovered using the `read_all()` method:

1. Read the segment file from beginning to end
2. Decode each record using the length prefix
3. Validate checksums to detect corruption
4. Return all valid records for replay

## Usage

### Basic Operations

```rust
use chronicled::wal::wal::{Wal, WalOptions};

// Create a new WAL
// Create a WAL with recycling enabled for better performance
let wal = Wal::new(WalOptions {
    dir: "/path/to/wal".to_string(),
    max_segment_size: Some(64 * 1024 * 1024), // 64MB
    recycle: true, // Enable WAL file recycling
}).await?;

// Append a single record
let offset = wal.append(b"my data".to_vec()).await?;

// Append multiple records as a batch
let offsets = wal.append_batch(vec![
    b"record1".to_vec(),
    b"record2".to_vec(),
    b"record3".to_vec(),
]).await?;

// Watch for synced offsets
let mut synced_watcher = wal.watch_synced();
while synced_watcher.changed().await.is_ok() {
    let synced_offset = *synced_watcher.borrow();
    println!("Data synced up to offset: {}", synced_offset);
}

// Recover from WAL on restart
let recovered_records = wal.read_all().await?;
for record in recovered_records {
    // Replay the record
    println!("Recovered: {:?}", record);
}
```

### Configuration Options

- **`dir`**: Directory where WAL segment files are stored
- **`max_segment_size`**: Maximum size of a segment before rotation (default: 64MB)
- **`recycle`**: Enable WAL file recycling to reuse old log files (default: false)

### WAL File Recycling

When `recycle` is enabled, the WAL will reuse existing log files by truncating them to zero length instead of creating new files. This reduces allocation overhead and can improve performance, especially on filesystems where file creation is expensive.

**Benefits:**
- Reduced allocation overhead
- Lower filesystem metadata operations
- Better performance for write-heavy workloads

**Considerations:**
- Ensure old WAL files are no longer needed before recycling
- May require additional logic to track which files can be safely recycled

### Batching Parameters

The WAL automatically batches writes for efficiency:

- **Batch Flush Interval**: 10ms (records are flushed every 10ms)
- **Max Batch Size**: 512 records (batches are flushed when they reach this size)

## Performance Characteristics

### Write Amplification

The batching mechanism significantly reduces write amplification:

- Individual writes are grouped into batches
- Each batch is written as a single I/O operation
- Checksums and headers add minimal overhead (7 bytes per record)

### Latency

- **Write Latency**: Controlled by batch flush interval (default 10ms)
- **Sync Latency**: Depends on `fsync()` performance
- **Recovery Time**: Linear in the size of the WAL (O(n) where n is the number of records)

### Throughput

The WAL is designed for high write throughput:

- Async I/O with Tokio for non-blocking operations
- Background threads for writing and syncing
- Batching reduces syscall overhead

## Durability Guarantees

The WAL provides the following durability guarantees:

1. **Write-ahead Logging**: All writes go to the WAL before being applied to storage
2. **Fsync on Commit**: Data is fsync'd to disk before clients are notified
3. **Checksum Validation**: Corruption is detected during recovery
4. **Crash Recovery**: All committed records can be recovered after a crash

## Limitations

### Current Implementation

- **Single Segment**: No automatic segment rotation yet (but infrastructure is in place)
- **No Compression**: Records are stored uncompressed
- **Sequential Reads Only**: No random access to specific offsets
- **No Truncation**: Cannot remove old records after they're persisted

### Future Enhancements

- **Segment Rotation**: Automatic creation of new segments when size limits are reached
- **Compression**: Optional compression of record payloads (e.g., Snappy, LZ4)
- **Archive Management**: Automatic archival and deletion of old segments
- **Parallel Recovery**: Multi-threaded recovery for faster startup
- **CRC32C**: Use hardware-accelerated CRC32C instead of XXH32

## Testing

The WAL includes comprehensive tests for:

- Record encoding/decoding
- Checksum validation
- Batch operations
- Corruption detection
- Empty and large records

Run tests with:

```bash
cargo test --test wal_record_test
```

## References

- [RocksDB Write-Ahead Log](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29)
- [Write-Ahead Log File Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)
- [WAL Performance](https://github.com/facebook/rocksdb/wiki/WAL-Performance)
