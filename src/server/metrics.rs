use crate::admission;
use axum::{
    extract::{Request as HttpRequest, State},
    middleware::Next,
    response::Response,
};
use serde_json::{json, Value};
use std::{
    fmt::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

const ENDPOINTS: [&str; 8] = [
    "/healthz",
    "/metrics",
    "/v1/status",
    "/v1/write",
    "/v1/query",
    "/v1/points/{id}",
    "/v1/requests/{boundary}/{nonce}",
    "unmatched",
];
const BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

struct EndpointMetrics {
    requests: [AtomicU64; 5],
    buckets: [AtomicU64; 12],
    latency_micros: AtomicU64,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_micros: AtomicU64::new(0),
        }
    }
}

pub(super) struct HttpMetrics {
    endpoints: [EndpointMetrics; ENDPOINTS.len()],
}

impl HttpMetrics {
    pub(super) fn new() -> Self {
        Self {
            endpoints: std::array::from_fn(|_| EndpointMetrics::new()),
        }
    }
}

fn endpoint(path: &str) -> usize {
    match path {
        "/healthz" => 0,
        "/metrics" => 1,
        "/v1/status" => 2,
        "/v1/write" => 3,
        "/v1/query" => 4,
        path if path.starts_with("/v1/points/") => 5,
        path if path.starts_with("/v1/requests/") => 6,
        _ => 7,
    }
}

pub(super) async fn record_metrics(
    State(metrics): State<Arc<HttpMetrics>>,
    request: HttpRequest,
    next: Next,
) -> Response {
    let endpoint = endpoint(request.uri().path());
    let start = Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed();
    let counters = &metrics.endpoints[endpoint];
    let class = usize::from(response.status().as_u16() / 100);
    if (1..=5).contains(&class) {
        counters.requests[class - 1].fetch_add(1, Ordering::Relaxed);
    }
    let seconds = elapsed.as_secs_f64();
    for (index, bound) in BUCKETS.iter().enumerate() {
        if seconds <= *bound {
            counters.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }
    counters.buckets[BUCKETS.len()].fetch_add(1, Ordering::Relaxed);
    counters.latency_micros.fetch_add(
        elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    response
}

fn sample(engine: &admission::EngineMetrics, name: &str) -> u64 {
    engine
        .samples
        .iter()
        .find(|(sample, _)| *sample == name)
        .map_or(0, |&(_, value)| value)
}

/// Clustered-view state from the engine's samples: `none` (per-seal
/// routing), `converting` (an automatic or explicit conversion is staged;
/// queries use the previous root until it publishes) or `clustered`.
/// `auto_cluster_rows` is the automatic conversion threshold and
/// `auto_recluster_factor` the growth that rebuilds a view as a new epoch
/// (0 disables either); `progress` is the running conversion's phase,
/// counters and the epoch it builds.
pub(super) fn clustering_status(engine: &admission::EngineMetrics) -> Value {
    let sample = |name: &str| sample(engine, name);
    let progress = match sample("glider_clustered_state") {
        1 => Some(json!({
            "phase": match sample("glider_conversion_phase") {
                1 => "sample",
                2 => "assign",
                3 => "gather",
                4 => "write",
                5 => "catalog",
                6 => "root",
                _ => "unknown",
            },
            "sources": sample("glider_conversion_sources"),
            "sources_done": sample("glider_conversion_sources_done"),
            "pass": sample("glider_conversion_pass"),
            "passes": sample("glider_conversion_passes"),
            "posting_packs": sample("glider_conversion_posting_packs"),
            "rows": sample("glider_conversion_rows"),
            "epoch": sample("glider_conversion_epoch"),
            "centroids": sample("glider_conversion_centroids"),
        })),
        _ => None,
    };
    json!({
        "state": match sample("glider_clustered_state") {
            1 => "converting",
            2 => "clustered",
            _ => "none",
        },
        "epoch": sample("glider_clustered_epoch"),
        "centroids": sample("glider_clustered_centroids"),
        "auto_cluster_rows": sample("glider_auto_cluster_rows"),
        "auto_recluster_factor": sample("glider_auto_recluster_factor"),
        "reclusters": sample("glider_recluster_starts_total"),
        "progress": progress,
        "conversions": sample("glider_conversions_total"),
        "conversion_failures": sample("glider_conversion_failures_total"),
    })
}

/// NVMe warm-up state from the engine's cache samples: `disabled` without an
/// NVMe tier, `cold` before the first warm-up unit, `warming` during a pass,
/// `warm` when the tier holds every block of the selected root, and
/// `partial` when a pass ended with part of the root uncached (the limit is
/// below `namespace_bytes`). Queries never depend on it for correctness.
pub(super) fn cache_status(engine: &admission::EngineMetrics) -> Value {
    let sample = |name: &str| sample(engine, name);
    let (limit, namespace, warm) = (
        sample("glider_cache_nvme_limit_bytes"),
        sample("glider_cache_namespace_bytes"),
        sample("glider_cache_warm_bytes"),
    );
    let state = match (limit, sample("glider_cache_warm_complete"), namespace) {
        (0, _, _) => "disabled",
        (_, 0, 0) => "cold",
        (_, 0, _) => "warming",
        _ if warm >= namespace => "warm",
        _ => "partial",
    };
    json!({
        "state": state,
        "nvme_bytes": sample("glider_cache_nvme_bytes"),
        "nvme_limit_bytes": limit,
        "namespace_bytes": namespace,
        "warm_bytes": warm,
    })
}

pub(super) fn render_metrics(
    http: &HttpMetrics,
    queue: admission::Status,
    engine: admission::EngineMetrics,
) -> String {
    let mut body = String::new();
    body.push_str("# TYPE glider_http_requests_total counter\n");
    for (name, metrics) in ENDPOINTS.iter().zip(&http.endpoints) {
        for (index, count) in metrics.requests.iter().enumerate() {
            writeln!(
                body,
                "glider_http_requests_total{{endpoint=\"{name}\",status_class=\"{}xx\"}} {}",
                index + 1,
                count.load(Ordering::Relaxed)
            )
            .unwrap();
        }
    }
    body.push_str("# TYPE glider_http_request_duration_seconds histogram\n");
    for (name, metrics) in ENDPOINTS.iter().zip(&http.endpoints) {
        for (index, bound) in BUCKETS.iter().enumerate() {
            writeln!(body, "glider_http_request_duration_seconds_bucket{{endpoint=\"{name}\",le=\"{bound}\"}} {}", metrics.buckets[index].load(Ordering::Relaxed)).unwrap();
        }
        writeln!(
            body,
            "glider_http_request_duration_seconds_bucket{{endpoint=\"{name}\",le=\"+Inf\"}} {}",
            metrics.buckets[BUCKETS.len()].load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            body,
            "glider_http_request_duration_seconds_sum{{endpoint=\"{name}\"}} {}",
            metrics.latency_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(
            body,
            "glider_http_request_duration_seconds_count{{endpoint=\"{name}\"}} {}",
            metrics.buckets[BUCKETS.len()].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    for (name, value, kind) in [
        ("glider_admission_commands", queue.commands as u64, "gauge"),
        ("glider_admission_bytes", queue.bytes as u64, "gauge"),
        ("glider_worker_failed", u64::from(queue.failed), "gauge"),
        ("glider_worker_closed", u64::from(queue.closed), "gauge"),
        (
            "glider_maintenance_errors_total",
            queue.maintenance_errors,
            "counter",
        ),
        ("glider_committed_sequence", engine.sequence, "gauge"),
    ] {
        writeln!(body, "# TYPE {name} {kind}\n{name} {value}").unwrap();
    }
    for (name, value) in engine.samples {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        writeln!(body, "# TYPE {name} {kind}\n{name} {value}").unwrap();
    }
    body
}
