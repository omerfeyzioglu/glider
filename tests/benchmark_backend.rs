// These modules are also compiled by the bench binary; individual tests use
// only the relevant helpers, while the smoke runner exercises report assembly.
#[allow(dead_code)]
#[path = "../benches/support/backend.rs"]
mod backend;
#[allow(dead_code)]
#[path = "../benches/support/metrics.rs"]
mod metrics;
#[allow(dead_code)]
#[path = "../benches/support/options.rs"]
mod options;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use backend::{attach_http, http_delta, LocalNamespace, Namespace};
use options::{Backend, Options};
use serde_json::json;

#[test]
fn default_options_keep_local_json_and_explicit_backend_parses() {
    let default = Options::parse_from(Vec::<String>::new()).unwrap().unwrap();
    assert_eq!(default.backend, Backend::Local);
    let serialized = serde_json::to_value(&default).unwrap();
    assert!(serialized.get("backend").is_none());
    assert_eq!(serialized["comparison_group"], "local-v2");
    assert_eq!(serialized["feature"], "m1");
    assert_eq!(serialized["rows"], 1000);
    let explicit = Options::parse_from(["--backend", "local"].map(String::from))
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(explicit).unwrap(), serialized);
    for input in [
        vec!["--backend"],
        vec!["--backend", "unknown"],
        vec!["--backend", "S3"],
    ] {
        assert!(Options::parse_from(input.into_iter().map(String::from)).is_err());
    }
    assert_eq!("s3".parse::<Backend>().unwrap(), Backend::S3);
    let s3 = Options::parse_from(["--backend", "s3"].map(String::from));
    #[cfg(feature = "s3")]
    {
        let s3 = serde_json::to_value(s3.unwrap().unwrap()).unwrap();
        assert_eq!(s3["backend"], "s3");
        assert_eq!(s3["comparison_group"], "s3-v1");
    }
    #[cfg(not(feature = "s3"))]
    assert!(s3.err().unwrap().to_string().contains("--features s3"));
}

#[test]
fn request_metrics_are_deltas_and_do_not_invent_local_http_counts() {
    let before = json!({"get": 4, "list": 2, "put": 1, "delete": 1, "other": 0,
        "request_body_bytes": 100, "http_errors": 1, "transport_errors": 0});
    let after = json!({"get": 6, "list": 5, "put": 4, "delete": 4, "other": 1,
        "request_body_bytes": 200, "http_errors": 2, "transport_errors": 3});
    let delta = http_delta(&before, &after);
    assert_eq!(
        delta,
        json!({"get": 2, "list": 3, "put": 3, "delete": 3, "other": 1,
        "request_body_bytes": 100, "http_errors": 1, "transport_errors": 3})
    );
    assert!(http_delta(&after, &before)["get"].is_null());
    assert!(http_delta(&json!({}), &after)["get"].is_null());
    let mut local = json!({"backend": "local"});
    attach_http(&mut local, "measured_http_requests", None);
    assert_eq!(local, json!({"backend": "local"}));
    attach_http(&mut local, "measured_http_requests", Some(delta.clone()));
    assert_eq!(local["measured_http_requests"], delta);
}

#[test]
fn local_namespace_keeps_original_inventory_and_removes_only_its_own_files() {
    use glider::store::ObjectStore;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("unrelated"), b"keep").unwrap();
    {
        let namespace = LocalNamespace::new(temp.path(), "search").unwrap();
        let mut store = namespace.open().unwrap();
        store.create("object", b"payload").unwrap();
        assert_eq!(
            namespace.inventory().unwrap(),
            json!({"logical_objects": 1,
            "physical_files": 2, "file_length_bytes": 7 + 48 + 8})
        );
        assert!(LocalNamespace::observe(&store).snapshot().is_none());
    }
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[cfg(feature = "s3")]
#[test]
fn s3_environment_is_explicit_validated_and_never_serializes_credentials() {
    use backend::S3Config;
    assert_eq!(<backend::S3Namespace as Namespace>::NAME, "s3");
    use std::collections::BTreeMap;
    let vars = BTreeMap::from([
        ("GLIDER_S3_ENDPOINT", "https://storage.invalid"),
        ("GLIDER_S3_BUCKET", "bench"),
        ("GLIDER_S3_REGION", "test-region"),
        ("GLIDER_S3_NAMESPACE", "benchmarks/run"),
        ("AWS_ACCESS_KEY_ID", "test-access-marker"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret-marker"),
        ("AWS_SESSION_TOKEN", "test-token-marker"),
    ]);
    let config = S3Config::read(|k| vars.get(k).map(|v| v.to_string())).unwrap();
    let output = config.metadata().to_string();
    for secret in [
        "test-access-marker",
        "test-secret-marker",
        "test-token-marker",
    ] {
        assert!(!output.contains(secret));
    }
    assert_eq!(config.metadata()["region"], "test-region");
    for missing in [
        "GLIDER_S3_ENDPOINT",
        "GLIDER_S3_BUCKET",
        "GLIDER_S3_REGION",
        "GLIDER_S3_NAMESPACE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ] {
        let error = S3Config::read(|k| {
            if k == missing {
                None
            } else {
                vars.get(k).map(|v| v.to_string())
            }
        })
        .err()
        .unwrap();
        assert!(error.to_string().contains(missing));
    }
    for endpoint in [
        "",
        "ftp://host",
        "https://secret@host",
        "https://host?token=secret",
        "https://host/#secret",
        "https:///path",
    ] {
        assert!(S3Config::read(|k| if k == "GLIDER_S3_ENDPOINT" {
            Some(endpoint.into())
        } else {
            vars.get(k).map(|v| v.to_string())
        })
        .is_err());
    }
    for namespace in ["", "/bench", "../bench", "bench//run", "bench/"] {
        assert!(S3Config::read(|k| if k == "GLIDER_S3_NAMESPACE" {
            Some(namespace.into())
        } else {
            vars.get(k).map(|v| v.to_string())
        })
        .is_err());
    }
}

#[test]
fn checkpoint_benchmark_option_is_explicit_and_validated() {
    let default = Options::parse_from(Vec::<String>::new()).unwrap().unwrap();
    assert!(serde_json::to_value(default)
        .unwrap()
        .get("checkpoint_at")
        .is_none());
    let options =
        Options::parse_from(["--scenario", "recovery", "--checkpoint-at", "900"].map(String::from))
            .unwrap()
            .unwrap();
    assert_eq!(serde_json::to_value(options).unwrap()["checkpoint_at"], 900);
    for args in [
        vec!["--scenario", "search", "--checkpoint-at", "1"],
        vec!["--checkpoint-at", "5001"],
        vec!["--checkpoint-at", "-1"],
    ] {
        assert!(Options::parse_from(args.into_iter().map(String::from)).is_err());
    }
}

#[test]
fn compaction_benchmark_option_is_explicit_and_validated() {
    let default = Options::parse_from(Vec::<String>::new()).unwrap().unwrap();
    assert!(serde_json::to_value(default)
        .unwrap()
        .get("compact_at")
        .is_none());
    let options =
        Options::parse_from(["--scenario", "recovery", "--compact-at", "900"].map(String::from))
            .unwrap()
            .unwrap();
    assert_eq!(serde_json::to_value(options).unwrap()["compact_at"], 900);
    for args in [
        vec!["--scenario", "search", "--compact-at", "1"],
        vec!["--compact-at", "5001"],
        vec!["--compact-at", "-1"],
    ] {
        assert!(Options::parse_from(args.into_iter().map(String::from)).is_err());
    }
}

#[test]
fn diagnostic_profiling_is_opt_in_and_search_only() {
    let default = Options::parse_from(Vec::<String>::new()).unwrap().unwrap();
    assert!(serde_json::to_value(default)
        .unwrap()
        .get("profile_seconds")
        .is_none());
    let options =
        Options::parse_from(["--scenario", "search", "--profile-seconds", "8"].map(String::from))
            .unwrap()
            .unwrap();
    assert_eq!(serde_json::to_value(options).unwrap()["profile_seconds"], 8);
    for args in [
        vec!["--profile-seconds", "8"],
        vec!["--scenario", "recovery", "--profile-seconds", "8"],
        vec!["--profile-seconds", "-1"],
    ] {
        assert!(Options::parse_from(args.into_iter().map(String::from)).is_err());
    }
}
