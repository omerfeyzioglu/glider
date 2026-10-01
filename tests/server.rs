#![cfg(feature = "server")]
//! HTTP router: write, query, get, delete, then restart on the same directory.

use glider::server::{router, ServerConfig, StoreConfig};
use glider::{
    retry::{Request, RequestId},
    segmented::{ReadBudget, SegmentedDatabase},
    store::LocalStore,
    Mutation,
};
use tower::ServiceExt;

fn config(directory: &std::path::Path) -> ServerConfig {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _lock = ENV_LOCK.lock().unwrap();
    std::env::set_var("GLIDER_DIMENSIONS", "3");
    std::env::set_var("GLIDER_RESIDENT_FILTER", "color=red");
    std::env::set_var("GLIDER_CACHE_DIR", directory.join("cache"));
    std::env::set_var("GLIDER_DATA_DIR", directory.join("data"));
    let mut config = ServerConfig::from_env().unwrap();
    assert!(matches!(config.store, StoreConfig::Local(_)));
    config.token = Some("secret".into());
    config
}

struct TestHttp {
    app: axum::Router,
    runtime: tokio::runtime::Runtime,
}

fn call(http: &TestHttp, method: &str, path: &str, body: &str, token: &str) -> (u16, String) {
    http.runtime.block_on(async {
        let request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_owned()))
            .unwrap();
        let response = http.app.clone().oneshot(request).await.unwrap();
        let status = response.status().as_u16();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    })
}

fn serve(
    config: &ServerConfig,
) -> (
    glider::admission::Service<glider::segmented::SegmentedServing<glider::server::Store>>,
    TestHttp,
) {
    let service = config.start().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let app = router(service.client(), config.token.clone());
    (service, TestHttp { app, runtime })
}

#[test]
fn query_can_include_fields_for_reranked_routed_and_resident_hits() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path());
    config.options.routed_keys = vec!["route".into()];
    config.serving.read_budget = ReadBudget::uniform(32);
    let path = match &config.store {
        StoreConfig::Local(path) => path,
        _ => unreachable!(),
    };
    let mut db = SegmentedDatabase::open_with_options(
        LocalStore::open(path).unwrap(),
        config.collection,
        config.options.clone(),
    )
    .unwrap();
    db.apply_request(Request {
        id: RequestId {
            boundary: 0,
            nonce: [1; 16],
        },
        conditions: Vec::new(),
        mutations: vec![
            Mutation::Put {
                id: 1,
                vector: vec![0., 0., 0.],
                metadata: [
                    ("color".into(), "red".into()),
                    ("route".into(), "yes".into()),
                ]
                .into(),
            },
            Mutation::Put {
                id: 2,
                vector: vec![2., 0., 0.],
                metadata: [
                    ("color".into(), "blue".into()),
                    ("route".into(), "yes".into()),
                ]
                .into(),
            },
            Mutation::Put {
                id: 3,
                vector: vec![4., 0., 0.],
                metadata: [
                    ("color".into(), "red".into()),
                    ("route".into(), "no".into()),
                ]
                .into(),
            },
        ],
    })
    .unwrap();
    db.seal_delta().unwrap();
    drop(db);
    let (service, http) = serve(&config);
    let query = |body: &str| {
        let (status, response) = call(&http, "POST", "/v1/query", body, "secret");
        assert_eq!(status, 200, "{response}");
        response
    };
    let plain = query(r#"{"vector":[0,0,0],"k":1}"#);
    assert_eq!(
        plain,
        r#"{"results":[{"distance":0.0,"id":1}],"sequence":1}"#
    );
    assert_eq!(
        plain,
        query(r#"{"vector":[0,0,0],"k":1,"include_metadata":false,"include_vector":false}"#)
    );
    let check = |body: &str, id: u64, color: &str, expected: Vec<f32>| {
        let response: serde_json::Value = serde_json::from_str(&query(body)).unwrap();
        let hit = &response["results"][0];
        assert_eq!(hit["id"], id);
        assert_eq!(hit["metadata"]["color"], color);
        assert_eq!(hit["vector"], serde_json::json!(expected));
    };
    check(
        r#"{"vector":[0,0,0],"k":1,"include_metadata":true,"include_vector":true}"#,
        1,
        "red",
        vec![0., 0., 0.],
    );
    check(
        r#"{"vector":[2,0,0],"k":1,"filter":{"route":"yes","color":"blue"},"include_metadata":true,"include_vector":true}"#,
        2,
        "blue",
        vec![2., 0., 0.],
    );
    check(
        r#"{"vector":[4,0,0],"k":1,"filter":{"color":"red"},"include_metadata":true,"include_vector":true}"#,
        3,
        "red",
        vec![4., 0., 0.],
    );
    let metadata_only: serde_json::Value = serde_json::from_str(&query(
        r#"{"vector":[0,0,0],"k":1,"include_metadata":true}"#,
    ))
    .unwrap();
    assert!(metadata_only["results"][0].get("vector").is_none());
    let vector_only: serde_json::Value = serde_json::from_str(&query(
        r#"{"vector":[0,0,0],"k":1,"filter":{"color":"red"},"include_vector":true}"#,
    ))
    .unwrap();
    assert!(vector_only["results"][0].get("metadata").is_none());
    drop(http);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
}

#[test]
fn http_service_writes_queries_and_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let (service, http) = serve(&config);
    assert_eq!(call(&http, "GET", "/v1/status", "", "wrong").0, 401);
    let (status, body) = call(
        &http,
        "POST",
        "/v1/write",
        r#"{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]},{"id":3,"vector":[5,5,5],"metadata":{"color":"red"}}]}"#,
        "secret",
    );
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(r#""sequence":1"#), "{body}");
    let (status, body) = call(
        &http,
        "POST",
        "/v1/query",
        r#"{"vector":[1,1,0.9],"k":1}"#,
        "secret",
    );
    assert_eq!(status, 200);
    assert!(body.contains(r#""id":2"#), "{body}");
    let (_, body) = call(
        &http,
        "POST",
        "/v1/query",
        r#"{"vector":[4,4,4],"k":5,"filter":{"color":"red"}}"#,
        "secret",
    );
    assert!(
        body.contains(r#""id":3"#) && !body.contains(r#""id":2"#),
        "{body}"
    );
    assert_eq!(
        call(
            &http,
            "POST",
            "/v1/query",
            r#"{"vector":[1,1],"k":1}"#,
            "secret"
        )
        .0,
        400
    );
    // A retried write with the same request ID returns the original outcome.
    let retry =
        r#"{"delete":[2],"request_id":{"boundary":1,"nonce":"0123456789abcdef0123456789abcdef"}}"#;
    let first = call(&http, "POST", "/v1/write", retry, "secret").1;
    assert_eq!(call(&http, "POST", "/v1/write", retry, "secret").1, first);
    assert_eq!(call(&http, "GET", "/v1/points/2", "", "secret").0, 404);
    let (_, body) = call(
        &http,
        "GET",
        "/v1/requests/1/0123456789abcdef0123456789abcdef",
        "",
        "secret",
    );
    assert!(body.contains("retained"), "{body}");
    let (status, body) = call(&http, "GET", "/metrics", "", "wrong");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("glider_committed_sequence 2\n"), "{body}");
    assert!(
        body.contains(
            "glider_http_requests_total{endpoint=\"/v1/write\",status_class=\"2xx\"} 3\n"
        ),
        "{body}"
    );
    assert!(
        body.contains(
            "glider_http_requests_total{endpoint=\"/v1/query\",status_class=\"4xx\"} 1\n"
        ),
        "{body}"
    );
    assert!(
        body.contains(
            "glider_http_request_duration_seconds_bucket{endpoint=\"/v1/query\",le=\"+Inf\"}"
        ),
        "{body}"
    );
    assert!(
        body.contains("glider_segmented_seal_starts_total "),
        "{body}"
    );
    assert!(body.contains("glider_cache_nvme_entries "), "{body}");
    assert!(body.contains("glider_sketch_index_bytes "), "{body}");
    drop(http);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();

    let (service, http) = serve(&config);
    let (status, body) = call(&http, "GET", "/v1/points/3", "", "secret");
    assert_eq!(status, 200);
    assert!(body.contains(r#""color":"red""#), "{body}");
    assert!(call(&http, "GET", "/v1/status", "", "secret")
        .1
        .contains(r#""sequence":2"#));
    drop(http);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
}
