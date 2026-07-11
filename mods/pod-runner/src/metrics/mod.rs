// Copyright 2026 Tabsdata Inc.

//! Prometheus metrics for the pod runner.
//!
//! Provides [`Metrics`] — a shared metrics registry with counters,
//! gauges, and histograms for pod/container lifecycle, memory usage,
//! HTTP latency, and uptime.

pub mod memory;
pub mod middleware;
pub mod server;

use std::sync::Arc;
use std::time::Instant;

use prometheus::{Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntGauge, Registry};

/// Default metrics port.
pub const DEFAULT_METRICS_PORT: u16 = 3001;

/// Default metrics URL (HTTP only).
pub const DEFAULT_METRICS_URL: &str = "http://127.0.0.1:3001";

/// Shared metrics for the pod runner.
pub struct Metrics {
    pub registry: Registry,

    // -- gauges (current state) --
    pub pods_running: IntGauge,
    pub containers_running: IntGauge,
    pub tokio_tasks: IntGauge,
    pub memory_physical_bytes: IntGauge,
    pub memory_virtual_bytes: IntGauge,
    pub uptime_seconds: Gauge,

    // -- counters (totals since startup) --
    pub pods_created_total: IntCounter,
    pub pods_started_total: IntCounter,
    pub pods_deleted_total: IntCounter,
    pub pods_failed_total: IntCounter,
    pub containers_started_total: IntCounter,
    pub containers_failed_total: IntCounter,

    // -- histograms (latency) --
    pub pod_creation_seconds: Histogram,
    pub pod_deletion_seconds: Histogram,
    pub http_request_duration: HistogramVec,

    start_time: Instant,
}

impl Metrics {
    /// Create and register all metrics.
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let pods_running =
            IntGauge::new("pr_pods_running", "Number of currently running pods").unwrap();
        let containers_running = IntGauge::new(
            "pr_containers_running",
            "Number of currently running containers",
        )
        .unwrap();
        let tokio_tasks =
            IntGauge::new("pr_tokio_tasks_active", "Number of active tokio tasks").unwrap();
        let memory_physical_bytes =
            IntGauge::new("pr_memory_physical_bytes", "Physical memory (RSS) in bytes").unwrap();
        let memory_virtual_bytes =
            IntGauge::new("pr_memory_virtual_bytes", "Virtual memory in bytes").unwrap();
        let uptime_seconds =
            Gauge::new("pr_uptime_seconds", "Seconds since the pod runner started").unwrap();

        let pods_created_total =
            IntCounter::new("pr_pods_created_total", "Total pods created since startup").unwrap();
        let pods_started_total = IntCounter::new(
            "pr_pods_started_total",
            "Total pods that reached Running phase",
        )
        .unwrap();
        let pods_deleted_total =
            IntCounter::new("pr_pods_deleted_total", "Total pods deleted since startup").unwrap();
        let pods_failed_total = IntCounter::new(
            "pr_pods_failed_total",
            "Total pods that reached Failed phase",
        )
        .unwrap();
        let containers_started_total = IntCounter::new(
            "pr_containers_started_total",
            "Total containers started since startup",
        )
        .unwrap();
        let containers_failed_total = IntCounter::new(
            "pr_containers_failed_total",
            "Total containers that failed since startup",
        )
        .unwrap();

        let latency_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        let pod_creation_seconds = Histogram::with_opts(
            HistogramOpts::new("pr_pod_creation_seconds", "Pod creation latency in seconds")
                .buckets(latency_buckets.clone()),
        )
        .unwrap();
        let pod_deletion_seconds = Histogram::with_opts(
            HistogramOpts::new("pr_pod_deletion_seconds", "Pod deletion latency in seconds")
                .buckets(latency_buckets.clone()),
        )
        .unwrap();
        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "pr_http_request_duration_seconds",
                "HTTP request duration in seconds",
            )
            .buckets(latency_buckets),
            &["method", "path", "status"],
        )
        .unwrap();

        // Register all metrics
        for g in [
            &pods_running,
            &containers_running,
            &tokio_tasks,
            &memory_physical_bytes,
            &memory_virtual_bytes,
        ] {
            registry.register(Box::new(g.clone())).unwrap();
        }
        registry.register(Box::new(uptime_seconds.clone())).unwrap();

        for c in [
            &pods_created_total,
            &pods_started_total,
            &pods_deleted_total,
            &pods_failed_total,
            &containers_started_total,
            &containers_failed_total,
        ] {
            registry.register(Box::new(c.clone())).unwrap();
        }

        registry
            .register(Box::new(pod_creation_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(pod_deletion_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(http_request_duration.clone()))
            .unwrap();

        Arc::new(Self {
            registry,
            pods_running,
            containers_running,
            tokio_tasks,
            memory_physical_bytes,
            memory_virtual_bytes,
            uptime_seconds,
            pods_created_total,
            pods_started_total,
            pods_deleted_total,
            pods_failed_total,
            containers_started_total,
            containers_failed_total,
            pod_creation_seconds,
            pod_deletion_seconds,
            http_request_duration,
            start_time: Instant::now(),
        })
    }

    /// Refresh gauges that are sampled on scrape (uptime, memory, tokio tasks).
    pub fn update_gauges(&self) {
        self.uptime_seconds
            .set(self.start_time.elapsed().as_secs_f64());

        if let Some(mem) = memory::get_memory_usage() {
            self.memory_physical_bytes.set(mem.physical_bytes as i64);
            self.memory_virtual_bytes.set(mem.virtual_bytes as i64);
        }

        let handle = tokio::runtime::Handle::current();
        self.tokio_tasks
            .set(handle.metrics().num_alive_tasks() as i64);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_new_creates_all() {
        let m = Metrics::new();
        // Gauges start at 0
        assert_eq!(m.pods_running.get(), 0);
        assert_eq!(m.containers_running.get(), 0);
        // Counters start at 0
        assert_eq!(m.pods_created_total.get(), 0);
    }

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.pods_created_total.inc();
        m.pods_created_total.inc();
        assert_eq!(m.pods_created_total.get(), 2);
    }

    #[test]
    fn histogram_observes() {
        let m = Metrics::new();
        m.pod_creation_seconds.observe(0.05);
        m.pod_creation_seconds.observe(0.1);
        assert_eq!(m.pod_creation_seconds.get_sample_count(), 2);
    }

    #[tokio::test]
    async fn update_gauges_sets_uptime() {
        let m = Metrics::new();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        m.update_gauges();
        assert!(m.uptime_seconds.get() > 0.0);
    }

    #[tokio::test]
    async fn update_gauges_sets_memory() {
        let m = Metrics::new();
        m.update_gauges();
        assert!(m.memory_physical_bytes.get() > 0, "RSS should be > 0");
    }

    #[tokio::test]
    async fn update_gauges_sets_tokio_tasks() {
        let m = Metrics::new();
        m.update_gauges();
        // Just verify it doesn't panic and sets a non-negative value
        assert!(m.tokio_tasks.get() >= 0);
    }

    #[test]
    fn containers_running_gauge_inc_dec() {
        let m = Metrics::new();
        m.containers_running.inc();
        m.containers_running.inc();
        assert_eq!(m.containers_running.get(), 2);
        m.containers_running.dec();
        assert_eq!(m.containers_running.get(), 1);
    }

    #[test]
    fn http_histogram_records_with_labels() {
        let m = Metrics::new();
        m.http_request_duration
            .with_label_values(&["GET", "/getPods", "200"])
            .observe(0.042);
        m.http_request_duration
            .with_label_values(&["POST", "/createPod", "201"])
            .observe(0.15);

        let count = m
            .http_request_duration
            .with_label_values(&["GET", "/getPods", "200"])
            .get_sample_count();
        assert_eq!(count, 1);
    }

    #[test]
    fn registry_renders_prometheus_text() {
        use prometheus::Encoder;
        let m = Metrics::new();
        m.pods_created_total.inc();
        let families = m.registry.gather();
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&families, &mut buf)
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("pr_pods_created_total 1"));
        assert!(text.contains("pr_uptime_seconds"));
    }
}
