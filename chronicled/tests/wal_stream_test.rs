// Integration test for WAL streaming functionality

use chronicled::wal::wal::{Wal, WalOptions};
use futures_util::stream::StreamExt;
use std::path::PathBuf;

#[tokio::test]
async fn test_wal_stream_read_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal_stream_empty");
    
    let wal = Wal::new(WalOptions {
        dir: wal_dir.to_str().unwrap().to_string(),
        max_segment_size: None,
        recycle: false,
    })
    .await
    .expect("Failed to create WAL");
    
    // Read from empty WAL should produce no records
    let mut stream = wal.read_stream().await;
    let mut count = 0;
    
    while let Some(result) = stream.next().await {
        result.expect("Should not have errors");
        count += 1;
    }
    
    assert_eq!(count, 0, "Empty WAL should produce no records");
}

#[tokio::test]
async fn test_wal_stream_read_single_record() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal_stream_single");
    
    let wal = Wal::new(WalOptions {
        dir: wal_dir.to_str().unwrap().to_string(),
        max_segment_size: None,
        recycle: false,
    })
    .await
    .expect("Failed to create WAL");
    
    // Append a single record
    let test_data = b"test record".to_vec();
    wal.append(test_data.clone())
        .await
        .expect("Failed to append");
    
    // Wait for sync
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Read via stream
    let mut stream = wal.read_stream().await;
    let mut records = Vec::new();
    
    while let Some(result) = stream.next().await {
        let data = result.expect("Should not have errors");
        records.push(data);
    }
    
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], test_data);
}

#[tokio::test]
async fn test_wal_stream_read_multiple_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal_stream_multiple");
    
    let wal = Wal::new(WalOptions {
        dir: wal_dir.to_str().unwrap().to_string(),
        max_segment_size: None,
        recycle: false,
    })
    .await
    .expect("Failed to create WAL");
    
    // Append multiple records using individual append calls
    let test_records = vec![
        b"record 1".to_vec(),
        b"record 2".to_vec(),
        b"record 3".to_vec(),
        b"record 4".to_vec(),
        b"record 5".to_vec(),
    ];
    
    for data in &test_records {
        wal.append(data.clone())
            .await
            .expect("Failed to append");
    }
    
    // Wait for sync
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Read via stream
    let mut stream = wal.read_stream().await;
    let mut records = Vec::new();
    
    while let Some(result) = stream.next().await {
        let data = result.expect("Should not have errors");
        records.push(data);
    }
    
    assert_eq!(records.len(), test_records.len());
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record, &test_records[i]);
    }
}

#[tokio::test]
async fn test_wal_background_batching() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal_batching");
    
    let wal = Wal::new(WalOptions {
        dir: wal_dir.to_str().unwrap().to_string(),
        max_segment_size: None,
        recycle: false,
    })
    .await
    .expect("Failed to create WAL");
    
    // Append many records quickly to trigger batching
    let num_records = 100;
    let mut expected_records = Vec::new();
    
    for i in 0..num_records {
        let data = format!("record {}", i).into_bytes();
        expected_records.push(data.clone());
        wal.append(data)
            .await
            .expect("Failed to append");
    }
    
    // Wait for all records to be flushed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Read via stream and verify all records are present
    let mut stream = wal.read_stream().await;
    let mut records = Vec::new();
    
    while let Some(result) = stream.next().await {
        let data = result.expect("Should not have errors");
        records.push(data);
    }
    
    assert_eq!(records.len(), num_records);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record, &expected_records[i]);
    }
}

#[tokio::test]
async fn test_wal_stream_large_records() {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal_dir = temp_dir.path().join("wal_stream_large");
    
    let wal = Wal::new(WalOptions {
        dir: wal_dir.to_str().unwrap().to_string(),
        max_segment_size: None,
        recycle: false,
    })
    .await
    .expect("Failed to create WAL");
    
    // Append large records (but within the 64KB limit)
    let large_data1 = vec![0xAB; 32000];
    let large_data2 = vec![0xCD; 32000];
    
    wal.append(large_data1.clone())
        .await
        .expect("Failed to append");
    wal.append(large_data2.clone())
        .await
        .expect("Failed to append");
    
    // Wait for sync
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    // Read via stream
    let mut stream = wal.read_stream().await;
    let mut records = Vec::new();
    
    while let Some(result) = stream.next().await {
        let data = result.expect("Should not have errors");
        records.push(data);
    }
    
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], large_data1);
    assert_eq!(records[1], large_data2);
}
