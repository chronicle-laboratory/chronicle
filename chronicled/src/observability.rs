use std::sync::OnceLock;
use std::sync::Arc;

use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider, ObservableGauge, UpDownCounter};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream};
use prometheus::Registry;
use tracing::info;

static GLOBAL_METRICS: OnceLock<Arc<ServerMetrics>> = OnceLock::new();

/// Set the global metrics instance (called once during unit init).
pub fn set_global_metrics(metrics: Arc<ServerMetrics>) {
    let _ = GLOBAL_METRICS.set(metrics);
}

/// Get the global metrics instance. Returns None if not yet initialized.
pub fn global_metrics() -> Option<&'static Arc<ServerMetrics>> {
    GLOBAL_METRICS.get()
}

/// Server-side metrics for chronicled.
#[derive(Clone)]
pub struct ServerMetrics {
    // Write path
    pub write_requests: Counter<u64>,
    pub write_errors: Counter<u64>,
    pub write_latency: Histogram<f64>,
    pub write_bytes: Counter<u64>,

    // Read path
    pub read_requests: Counter<u64>,
    pub read_errors: Counter<u64>,
    pub read_latency: Histogram<f64>,
    pub read_events: Counter<u64>,

    // WAL
    pub wal_writes: Counter<u64>,
    pub wal_sync_latency: Histogram<f64>,
    pub wal_bytes: Counter<u64>,

    // Write cache
    pub write_cache_entries: UpDownCounter<i64>,
    pub write_cache_seals: Counter<u64>,

    // Compaction
    pub compaction_runs: Counter<u64>,
    pub compaction_latency: Histogram<f64>,
    pub compaction_bytes_read: Counter<u64>,
    pub compaction_bytes_written: Counter<u64>,

    // Segments
    pub segment_count: UpDownCounter<i64>,
    pub segment_bytes: UpDownCounter<i64>,

    // Index (RocksDB)
    pub index_reads: Counter<u64>,
    pub index_writes: Counter<u64>,
    pub index_read_latency: Histogram<f64>,
    pub index_write_latency: Histogram<f64>,

    // Admin
    pub admin_requests: Counter<u64>,
    pub admin_latency: Histogram<f64>,

    // Catalog
    pub catalog_operations: Counter<u64>,
    pub catalog_errors: Counter<u64>,
    pub catalog_latency: Histogram<f64>,

    // Queue / inflight
    pub write_queue_depth: UpDownCounter<i64>,
    pub read_queue_depth: UpDownCounter<i64>,
    pub failed_requests: Counter<u64>,
}

impl ServerMetrics {
    pub fn new(meter: &Meter) -> Self {
        // Sub-millisecond buckets for request latency: 50us to 10s
        let latency_buckets = vec![
            0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
            0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        // Compaction can take longer: 1ms to 60s
        let compaction_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
            1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        ];

        Self {
            write_requests: meter
                .u64_counter("chronicle.server.write.requests")
                .with_description("Total write requests received")
                .build(),
            write_errors: meter
                .u64_counter("chronicle.server.write.errors")
                .with_description("Total write errors")
                .build(),
            write_latency: meter
                .f64_histogram("chronicle.server.write.latency")
                .with_description("Write request latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),
            write_bytes: meter
                .u64_counter("chronicle.server.write.bytes")
                .with_description("Total bytes written")
                .build(),

            read_requests: meter
                .u64_counter("chronicle.server.read.requests")
                .with_description("Total read requests received")
                .build(),
            read_errors: meter
                .u64_counter("chronicle.server.read.errors")
                .with_description("Total read errors")
                .build(),
            read_latency: meter
                .f64_histogram("chronicle.server.read.latency")
                .with_description("Read request latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),
            read_events: meter
                .u64_counter("chronicle.server.read.events")
                .with_description("Total events read")
                .build(),

            wal_writes: meter
                .u64_counter("chronicle.server.wal.writes")
                .with_description("Total WAL writes")
                .build(),
            wal_sync_latency: meter
                .f64_histogram("chronicle.server.wal.sync_latency")
                .with_description("WAL sync latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),
            wal_bytes: meter
                .u64_counter("chronicle.server.wal.bytes")
                .with_description("Total WAL bytes written")
                .build(),

            write_cache_entries: meter
                .i64_up_down_counter("chronicle.server.write_cache.entries")
                .with_description("Current entries in write cache")
                .build(),
            write_cache_seals: meter
                .u64_counter("chronicle.server.write_cache.seals")
                .with_description("Total write cache seal operations")
                .build(),

            compaction_runs: meter
                .u64_counter("chronicle.server.compaction.runs")
                .with_description("Total compaction runs")
                .build(),
            compaction_latency: meter
                .f64_histogram("chronicle.server.compaction.latency")
                .with_description("Compaction latency in seconds")
                .with_unit("s")
                .with_boundaries(compaction_buckets)
                .build(),
            compaction_bytes_read: meter
                .u64_counter("chronicle.server.compaction.bytes_read")
                .with_description("Total bytes read during compaction")
                .build(),
            compaction_bytes_written: meter
                .u64_counter("chronicle.server.compaction.bytes_written")
                .with_description("Total bytes written during compaction")
                .build(),

            segment_count: meter
                .i64_up_down_counter("chronicle.server.segments.count")
                .with_description("Current segment count")
                .build(),
            segment_bytes: meter
                .i64_up_down_counter("chronicle.server.segments.bytes")
                .with_description("Current total segment bytes")
                .build(),

            index_reads: meter
                .u64_counter("chronicle.server.index.reads")
                .with_description("Total index read operations")
                .build(),
            index_writes: meter
                .u64_counter("chronicle.server.index.writes")
                .with_description("Total index write operations")
                .build(),
            index_read_latency: meter
                .f64_histogram("chronicle.server.index.read_latency")
                .with_description("Index read latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),
            index_write_latency: meter
                .f64_histogram("chronicle.server.index.write_latency")
                .with_description("Index write latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),

            admin_requests: meter
                .u64_counter("chronicle.server.admin.requests")
                .with_description("Total admin API requests")
                .build(),
            admin_latency: meter
                .f64_histogram("chronicle.server.admin.latency")
                .with_description("Admin API latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets.clone())
                .build(),

            catalog_operations: meter
                .u64_counter("chronicle.server.catalog.operations")
                .with_description("Total catalog operations")
                .build(),
            catalog_errors: meter
                .u64_counter("chronicle.server.catalog.errors")
                .with_description("Total catalog operation errors")
                .build(),
            catalog_latency: meter
                .f64_histogram("chronicle.server.catalog.latency")
                .with_description("Catalog operation latency in seconds")
                .with_unit("s")
                .with_boundaries(latency_buckets)
                .build(),

            write_queue_depth: meter
                .i64_up_down_counter("chronicle.server.write.queue_depth")
                .with_description("Current queued write requests")
                .build(),
            read_queue_depth: meter
                .i64_up_down_counter("chronicle.server.read.queue_depth")
                .with_description("Current queued read requests")
                .build(),
            failed_requests: meter
                .u64_counter("chronicle.server.failed_requests")
                .with_description("Total failed requests (dispatch errors)")
                .build(),
        }
    }

    /// Record a compaction run with level label.
    pub fn record_compaction(&self, level: u32, duration_secs: f64, bytes_read: u64, bytes_written: u64) {
        let attrs = [KeyValue::new("level", level as i64)];
        self.compaction_runs.add(1, &attrs);
        self.compaction_latency.record(duration_secs, &attrs);
        self.compaction_bytes_read.add(bytes_read, &attrs);
        self.compaction_bytes_written.add(bytes_written, &attrs);
    }

    /// Record segment count change with level label.
    pub fn record_segment_change(&self, level: u32, count_delta: i64, bytes_delta: i64) {
        let attrs = [KeyValue::new("level", level as i64)];
        self.segment_count.add(count_delta, &attrs);
        self.segment_bytes.add(bytes_delta, &attrs);
    }
}

/// Recursively compute total file size under a directory.
fn dir_size(path: &str) -> u64 {
    let mut total: u64 = 0;
    let mut stack = vec![std::path::PathBuf::from(path)];
    while let Some(dir) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        stack.push(entry.path());
                    } else {
                        total += meta.len();
                    }
                }
            }
        }
    }
    total
}

/// Register disk usage gauge that reports total bytes used under data dirs.
/// The returned handle must be kept alive for the gauge to be reported.
pub fn register_disk_usage_gauge(meter: &Meter, data_dirs: Vec<String>) -> ObservableGauge<u64> {
    info!(dirs = ?data_dirs, "registering disk usage gauge");
    meter
        .u64_observable_gauge("chronicle.server.disk.usage_bytes")
        .with_description("Total disk usage of data directories in bytes")
        .with_callback(move |observer| {
            let mut total: u64 = 0;
            for dir in &data_dirs {
                total += dir_size(dir);
            }
            observer.observe(total, &[]);
        })
        .build()
}

/// Register RocksDB ticker gauges that are polled on each Prometheus scrape.
/// The returned handles must be kept alive for the gauges to be reported.
pub fn register_rocksdb_gauges(meter: &Meter, storage: crate::storage::index::Storage) -> Vec<ObservableGauge<u64>> {
    use rocksdb::statistics::Ticker;

    let tickers: Vec<(Ticker, &str, &str)> = vec![
        (Ticker::BlockCacheHit, "chronicle.rocksdb.block_cache.hits", "Block cache hits"),
        (Ticker::BlockCacheMiss, "chronicle.rocksdb.block_cache.misses", "Block cache misses"),
        (Ticker::BytesWritten, "chronicle.rocksdb.bytes_written", "Total bytes written to DB"),
        (Ticker::BytesRead, "chronicle.rocksdb.bytes_read", "Total bytes read from DB"),
        (Ticker::CompactReadBytes, "chronicle.rocksdb.compact.read_bytes", "RocksDB compaction read bytes"),
        (Ticker::CompactWriteBytes, "chronicle.rocksdb.compact.write_bytes", "RocksDB compaction write bytes"),
        (Ticker::BloomFilterUseful, "chronicle.rocksdb.bloom.useful", "Bloom filter useful (avoided reads)"),
    ];

    tickers.into_iter().map(|(ticker, name, desc)| {
        let s = storage.clone();
        meter
            .u64_observable_gauge(name)
            .with_description(desc)
            .with_callback(move |observer| {
                observer.observe(s.ticker(ticker), &[]);
            })
            .build()
    }).collect()
}

/// Initialize the OpenTelemetry meter provider with Prometheus exporter.
/// Returns the provider, meter, and prometheus registry for the HTTP endpoint.
pub fn init_meter_provider() -> (SdkMeterProvider, Meter, Registry) {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("failed to build prometheus exporter");

    // Sub-millisecond buckets for request latency: 50us to 10s
    let latency_boundaries = vec![
        0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
        0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];
    // Compaction can take longer: 1ms to 60s
    let compaction_boundaries = vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
        1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    ];

    // View: apply sub-millisecond buckets to all latency histograms except compaction.
    let lb = latency_boundaries.clone();
    let latency_view = move |inst: &Instrument| -> Option<Stream> {
        if inst.kind == Some(InstrumentKind::Histogram)
            && inst.name.contains("latency")
            && !inst.name.contains("compaction")
        {
            Some(Stream::new()
                .name(inst.name.clone())
                .description(inst.description.clone())
                .unit(inst.unit.clone())
                .aggregation(Aggregation::ExplicitBucketHistogram {
                    boundaries: lb.clone(),
                    record_min_max: true,
                }))
        } else {
            None
        }
    };

    // View: apply compaction-specific buckets to compaction latency.
    let cb = compaction_boundaries;
    let compaction_view = move |inst: &Instrument| -> Option<Stream> {
        if inst.kind == Some(InstrumentKind::Histogram)
            && inst.name.contains("compaction") && inst.name.contains("latency")
        {
            Some(Stream::new()
                .name(inst.name.clone())
                .description(inst.description.clone())
                .unit(inst.unit.clone())
                .aggregation(Aggregation::ExplicitBucketHistogram {
                    boundaries: cb.clone(),
                    record_min_max: true,
                }))
        } else {
            None
        }
    };

    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_view(latency_view)
        .with_view(compaction_view)
        .build();

    let meter = provider.meter("chronicle");

    info!("opentelemetry meter provider initialized with prometheus exporter");

    (provider, meter, registry)
}
