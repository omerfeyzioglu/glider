#![cfg(feature = "server")]

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use glider::{
    admission::{Limits, Shutdown},
    segmented::SegmentedServingOptions,
    server::{multi_router_with_console, router_with_console, Multi, ServerConfig, StoreConfig},
    Config, Metric,
};
use std::{path::Path, time::Duration};
use tower::ServiceExt;

fn config(path: &Path, multi: bool) -> ServerConfig {
    let mut serving = SegmentedServingOptions::m21(path.join("cache"));
    serving.cache = None;
    serving.warm_unit_bytes = 0;
    serving.auto_cluster_rows = 0;
    ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        store: StoreConfig::Local(path.join("data")),
        collection: Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        },
        options: Default::default(),
        serving,
        limits: Limits::default(),
        token: None,
        console: true,
        embedding: Default::default(),
        lease: Duration::from_millis(100),
        multi,
        max_open_collections: 2,
        collection_idle: std::time::Duration::ZERO,
    }
}

async fn get(app: &Router, path: &str) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, 256 * 1024).await.unwrap();
    (
        parts.status,
        parts.headers,
        String::from_utf8(bytes.to_vec()).unwrap(),
    )
}

async fn assert_console(app: &Router) {
    let (status, headers, body) = get(app, "/console").await;
    // API paths are relative so the console works under a path prefix.
    assert!(!body.contains("'/v1"), "no root-relative API path");
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    let csp = headers[header::CONTENT_SECURITY_POLICY].to_str().unwrap();
    assert!(csp.contains("default-src 'none'"));
    assert!(csp.contains("connect-src 'self'"));
    assert!(body.contains("Glider"));
    assert!(body.contains("data-query-mode=\"text\""));
    assert!(body.contains("Search in plain language…"));
    assert!(body.contains("GLIDER_EMBED_PROVIDER"));
    for id in [
        "collection-select",
        "query-form",
        "query-k",
        "query-exact",
        "query-metadata",
        "query-filter",
        "query-profile",
        "scan-form",
        "scan-filter",
        "scan-next",
        "point-detail",
        "use-vector",
        "create-form",
        "delete-form",
        "auth-form",
    ] {
        assert!(
            body.contains(&format!("id=\"{id}\"")),
            "missing control {id}"
        );
    }
    for view in ["search", "browse", "collections"] {
        assert!(body.contains(&format!("data-view=\"{view}\"")));
    }
    assert!(body.contains("prefers-color-scheme: dark"));
    assert!(body.contains("role=\"tab\""));
    assert!(body.contains("sessionStorage"));
    assert!(!body.contains("src=\"//"));
    assert!(!body.contains("href=\"//"));
    assert!(csp.contains("frame-ancestors 'none'"));
    assert!(csp.contains("base-uri 'none'"));
    assert!(!body.contains("http://"));
    assert!(!body.contains("https://"));
    let (status, headers, _) = get(app, "/").await;
    assert!(status.is_redirection());
    assert_eq!(headers[header::LOCATION], "console");
}

#[tokio::test(flavor = "multi_thread")]
async fn single_console_is_public_and_can_be_disabled_without_changing_api() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path(), false);
    let running = tokio::task::block_in_place(|| config.start().unwrap());
    let app = router_with_console(running.client(), Some("secret".into()), true);
    assert_console(&app).await;
    assert_eq!(get(&app, "/v1/status").await.0, StatusCode::UNAUTHORIZED);
    let disabled = router_with_console(running.client(), None, false);
    assert_eq!(get(&disabled, "/console").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get(&disabled, "/").await.0, StatusCode::NOT_FOUND);
    let (status, _, body) = get(&disabled, "/v1/status").await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["configuration"]["dimensions"], 2);
    assert_eq!(body["configuration"]["metric"], "squared_euclidean");
    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain).unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_console_is_public_and_can_be_disabled_without_changing_api() {
    let temp = tempfile::tempdir().unwrap();
    let multi = Multi::new(config(temp.path(), true)).unwrap();
    let app = multi_router_with_console(multi.clone(), Some("secret".into()), true);
    assert_console(&app).await;
    assert_eq!(
        get(&app, "/v1/collections").await.0,
        StatusCode::UNAUTHORIZED
    );
    let disabled = multi_router_with_console(multi.clone(), None, false);
    assert_eq!(get(&disabled, "/console").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get(&disabled, "/").await.0, StatusCode::NOT_FOUND);
    assert_eq!(get(&disabled, "/v1/collections").await.0, StatusCode::OK);
    multi.shutdown().await.unwrap();
}
