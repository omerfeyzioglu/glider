#[path = "../benches/support/metrics.rs"]
mod metrics;
use metrics::{metadata, resources, timing, usage_delta, Usage};

#[test]
fn percentiles_throughput_and_missing_metrics() {
    let samples: Vec<_> = (1..=100).map(f64::from).rev().collect();
    let t = timing(&samples, 2);
    assert_eq!(t["p50_sample_ns"], 50.0);
    assert_eq!(t["p95_sample_ns"], 95.0);
    assert_eq!(t["p99_sample_ns"], 99.0);
    assert_eq!(t["max_sample_ns"], 100.0);
    assert_eq!(t["p50_amortized_ns_per_operation"], 25.0);
    assert_eq!(t["operations_per_timed_second"], 200.0 * 1e9 / 5050.0);
    assert_eq!(timing(&[7.], 1)["p99_sample_ns"], 7.0);
    for t in [
        timing(&[], 1),
        timing(&[1.], 0),
        timing(&[-1.], 1),
        timing(&[f64::NAN], 1),
    ] {
        assert!(t["p50_sample_ns"].is_null());
        assert!(t["operations_per_timed_second"].is_null());
    }
    let zero = timing(&[0.], 1);
    assert_eq!(zero["p99_sample_ns"], 0.0);
    assert!(zero["operations_per_timed_second"].is_null());
}

#[test]
fn cpu_deltas_and_absolute_peak_rss() {
    let before = Usage {
        user_ns: 20,
        system_ns: 4,
        max_rss_bytes: 1000,
    };
    let after = Usage {
        user_ns: 25,
        system_ns: 6,
        max_rss_bytes: 1200,
    };
    let sample = usage_delta(Some(before), Some(after));
    assert_eq!(sample["user_cpu_ns"], 5);
    assert_eq!(sample["system_cpu_ns"], 2);
    assert_eq!(sample["process_max_rss_bytes"], 1200);
    let aggregate = resources(vec![sample.clone(), sample], "test");
    assert_eq!(aggregate["user_cpu_ns"], 10);
    assert_eq!(aggregate["process_max_rss_bytes"], 1200);
    let missing = resources(vec![usage_delta(None, None)], "test");
    assert!(missing["user_cpu_ns"].is_null());
    assert!(missing["process_max_rss_bytes"].is_null());
    assert!(usage_delta(Some(after), Some(before))["user_cpu_ns"].is_null());
    // Exercise the platform probe without a timing or resource threshold.
    if let Some(usage) = Usage::capture() {
        assert_eq!(usage_delta(Some(usage), Some(usage))["system_cpu_ns"], 0);
    }
}

#[test]
fn comparison_metadata_is_explicit_and_validated() {
    for phase in ["baseline", "before", "after"] {
        let m = metadata("segments", phase, "segments-small").unwrap();
        assert_eq!(m["feature"], "segments");
        assert_eq!(m["phase"], phase);
        assert_eq!(m["comparison_group"], "segments-small");
    }
    assert!(metadata("", "baseline", "group").is_err());
    assert!(metadata("feature", "unknown", "group").is_err());
    assert!(metadata("feature", "before", " ").is_err());
}
