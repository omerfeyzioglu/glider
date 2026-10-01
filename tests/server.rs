#![cfg(feature = "server")]
//! End to end over HTTP: write, query, get, delete, then a restart on the
//! same directory after graceful shutdown released ownership.

use glider::server::{router, ServerConfig, StoreConfig};
use std::io::{Read, Write};

fn config(directory: &std::path::Path) -> ServerConfig {
    std::env::set_var("GLIDER_DIMENSIONS", "3");
    std::env::set_var("GLIDER_RESIDENT_FILTER", "color=red");
    std::env::set_var("GLIDER_CACHE_DIR", directory.join("cache"));
    std::env::set_var("GLIDER_DATA_DIR", directory.join("data"));
    let mut config = ServerConfig::from_env().unwrap();
    assert!(matches!(config.store, StoreConfig::Local(_)));
    config.token = Some("secret".into());
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
    glider::admission::Service<glider::segmented::SegmentedServing<glider::server::Store>>,
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
    assert!(body.contains(r#""sequence":1"#), "{body}");
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
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();

    let (service, address, runtime) = serve(&config);
    let (status, body) = call(address, "GET", "/v1/points/3", "", "secret");
    assert_eq!(status, 200);
    assert!(body.contains(r#""color":"red""#), "{body}");
    assert!(call(address, "GET", "/v1/status", "", "secret")
        .1
        .contains(r#""sequence":2"#));
    drop(runtime);
    service
        .shutdown(glider::admission::Shutdown::Drain)
        .unwrap();
}
