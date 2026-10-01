#![cfg(feature = "server")]
//! End to end over HTTP: write, query, get, delete, then a restart on the
//! same directory after graceful shutdown released the writer lease.

use glider::server::{router, ServerConfig, StoreConfig};
use glider::{
    retry::{Request, RequestId},
    segmented::{ReadBudget, SegmentedDatabase},
    store::LocalStore,
    Mutation,
};
use std::io::{Read, Write};

fn config(directory: &std::path::Path) -> ServerConfig {
    // Tests run in parallel: the shared environment holds only common values.
    std::env::set_var("GLIDER_DIMENSIONS", "3");
    std::env::set_var("GLIDER_RESIDENT_FILTER", "color=red");
    std::env::set_var("GLIDER_DATA_DIR", "unused");
    std::env::set_var("GLIDER_CACHE_BYTES", "1048576");
    std::env::set_var("GLIDER_LOCAL_BLOCKS", "32");
    let mut config = ServerConfig::from_env().unwrap();
    assert!(matches!(config.store, StoreConfig::Local(_)));
    assert_eq!(config.serving.read_budget.local_blocks, 32);
    assert_eq!(config.serving.cache.as_ref().unwrap().2, 1_048_576);
    config.store = StoreConfig::Local(directory.join("data"));
    config.serving.cache = Some((directory.join("cache"), 0, 1_048_576));
    config.token = Some("secret".into());
    config.lease = std::time::Duration::from_secs(60);
    config
}

/// Minimal HTTP/1.1 client: returns status and body.
fn call(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    token: &str,
) -> (u16, String) {
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1.to_owned();
    (status, body)
}

fn serve(
    config: &ServerConfig,
) -> (
    glider::server::Running,
    std::net::SocketAddr,
    tokio::runtime::Runtime,
) {
    let service = config.start().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(service.client(), config.token.clone());
    runtime.spawn(async move { axum::serve(listener, app).await });
    (service, address, runtime)
}

#[test]
fn http_service_writes_queries_and_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let (service, address, runtime) = serve(&config);
    assert_eq!(call(address, "GET", "/v1/status", "", "wrong").0, 401);
    let (status, body) = call(
        address,
        "POST",
        "/v1/write",
        r#"{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]},{"id":3,"vector":[5,5,5],"metadata":{"color":"red"}}]}"#,
        "secret",
    );
    assert_eq!(status, 200, "{body}");
    // Sequence 1 is the first start's takeover record.
    assert!(body.contains(r#""sequence":2"#), "{body}");
    let (status, body) = call(
        address,
        "POST",
        "/v1/query",
        r#"{"vector":[1,1,0.9],"k":1}"#,
        "secret",
    );
    assert_eq!(status, 200);
    assert!(body.contains(r#""id":2"#), "{body}");
    let (_, body) = call(
        address,
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
            address,
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
    let first = call(address, "POST", "/v1/write", retry, "secret").1;
    assert_eq!(call(address, "POST", "/v1/write", retry, "secret").1, first);
    assert_eq!(call(address, "GET", "/v1/points/2", "", "secret").0, 404);
    let (_, body) = call(
        address,
        "GET",
        "/v1/requests/1/0123456789abcdef0123456789abcdef",
        "",
        "secret",
    );
    assert!(body.contains("retained"), "{body}");
    let (status, body) = call(address, "GET", "/metrics", "", "wrong");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("glider_committed_sequence 3\n"), "{body}");
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
    assert!(body.contains("glider_cache_warm_complete "), "{body}");
    assert!(
        body.contains("glider_cache_nvme_limit_bytes 1048576\n"),
        "{body}"
    );
    let (_, body) = call(address, "GET", "/v1/status", "", "secret");
    assert!(body.contains(r#""nvme_limit_bytes":1048576"#), "{body}");
    assert!(body.contains(r#""state":""#), "{body}");
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();

    // Graceful shutdown released the lease: the restart does not wait for
    // it to expire. The takeover record is sequence 4 and the new epoch.
    let restarted = std::time::Instant::now();
    let (service, address, runtime) = serve(&config);
    assert!(restarted.elapsed() < config.lease / 2);
    let (status, body) = call(address, "GET", "/v1/points/3", "", "secret");
    assert_eq!(status, 200);
    assert!(body.contains(r#""color":"red""#), "{body}");
    assert!(call(address, "GET", "/v1/status", "", "secret")
        .1
        .contains(r#""sequence":4"#));
    assert!(call(address, "GET", "/metrics", "", "secret")
        .1
        .contains("glider_writer_epoch 4\n"));
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
}

#[test]
fn killed_server_restarts_on_the_same_directory_without_an_operator() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path());
    config.lease = std::time::Duration::from_millis(300);
    let (service, address, runtime) = serve(&config);
    let (status, body) = call(
        address,
        "POST",
        "/v1/write",
        r#"{"upsert":[{"id":4,"vector":[4,4,4],"metadata":{"color":"red"}}]}"#,
        "secret",
    );
    assert_eq!(status, 200, "{body}");
    // A second process finds the lease renewed and refuses to depose it.
    assert!(matches!(config.start(), Err(glider::Error::Busy(_))));
    // Killed: no drain and no lease release.
    drop(runtime);
    drop(service);

    let restarted = std::time::Instant::now();
    let (service, address, runtime) = serve(&config);
    assert!(restarted.elapsed() >= config.lease, "waited out the lease");
    let (status, body) = call(address, "GET", "/v1/points/4", "", "secret");
    assert_eq!(status, 200, "{body}");
    // Sequence 1 and 2 are the first takeover and the write.
    assert!(call(address, "GET", "/metrics", "", "secret")
        .1
        .contains("glider_writer_epoch 3\n"));
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
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
    let (service, address, runtime) = serve(&config);
    let query = |body: &str| {
        let (status, response) = call(address, "POST", "/v1/query", body, "secret");
        assert_eq!(status, 200, "{response}");
        response
    };
    let plain = query(r#"{"vector":[0,0,0],"k":1}"#);
    // The sequence includes the server's takeover record.
    assert!(
        plain.starts_with(r#"{"results":[{"distance":0.0,"id":1}],"sequence":"#),
        "{plain}"
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
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
}
