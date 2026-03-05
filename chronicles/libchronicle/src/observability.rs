use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};

/// Client-side metrics for libchronicle.
///
/// Users pass an `opentelemetry::metrics::Meter` when constructing
/// the `Chronicle` client. If no meter is provided, a no-op meter
/// is used and all recording is zero-cost.
#[derive(Clone)]
pub struct ClientMetrics {
    pub write_requests: Counter<u64>,
    pub write_errors: Counter<u64>,
    pub write_latency: Histogram<f64>,

    pub read_requests: Counter<u64>,
    pub read_errors: Counter<u64>,
    pub read_latency: Histogram<f64>,
    pub read_events: Counter<u64>,

    pub reconciliation_runs: Counter<u64>,
    pub reconciliation_latency: Histogram<f64>,
}

impl ClientMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            write_requests: meter
                .u64_counter("chronicle.client.write.requests")
                .with_description("Total write requests sent")
                .build(),
            write_errors: meter
                .u64_counter("chronicle.client.write.errors")
                .with_description("Total write errors")
                .build(),
            write_latency: meter
                .f64_histogram("chronicle.client.write.latency")
                .with_description("Write request latency in seconds")
                .with_unit("s")
                .build(),

            read_requests: meter
                .u64_counter("chronicle.client.read.requests")
                .with_description("Total read requests sent")
                .build(),
            read_errors: meter
                .u64_counter("chronicle.client.read.errors")
                .with_description("Total read errors")
                .build(),
            read_latency: meter
                .f64_histogram("chronicle.client.read.latency")
                .with_description("Read request latency in seconds")
                .with_unit("s")
                .build(),
            read_events: meter
                .u64_counter("chronicle.client.read.events")
                .with_description("Total events read")
                .build(),

            reconciliation_runs: meter
                .u64_counter("chronicle.client.reconciliation.runs")
                .with_description("Total reconciliation runs")
                .build(),
            reconciliation_latency: meter
                .f64_histogram("chronicle.client.reconciliation.latency")
                .with_description("Reconciliation latency in seconds")
                .with_unit("s")
                .build(),
        }
    }

    /// Create a no-op metrics instance (zero-cost when no meter is provided).
    pub fn noop() -> Self {
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let meter = provider.meter("noop");
        Self::new(&meter)
    }
}
