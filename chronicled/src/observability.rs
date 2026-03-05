use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider, UpDownCounter};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;
use tracing::info;

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
}

impl ServerMetrics {
    pub fn new(meter: &Meter) -> Self {
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

/// Initialize the OpenTelemetry meter provider with Prometheus exporter.
/// Returns the provider, meter, and prometheus registry for the HTTP endpoint.
pub fn init_meter_provider() -> (SdkMeterProvider, Meter, Registry) {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("failed to build prometheus exporter");

    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .build();

    let meter = provider.meter("chronicle");

    info!("opentelemetry meter provider initialized with prometheus exporter");

    (provider, meter, registry)
}
