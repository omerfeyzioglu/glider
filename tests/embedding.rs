#![cfg(feature = "server")]
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
#[cfg(any(feature = "embed-local", feature = "embed-openai"))]
use glider::server::EmbedConfig;
use glider::{
    admission::{Limits, Shutdown},
    segmented::SegmentedServingOptions,
    server::{router_with_embedder, Embedder, ServerConfig, StoreConfig},
    Config, Metric,
};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc, time::Duration};
use tower::ServiceExt;

fn config(path: &Path) -> ServerConfig {
    let mut serving = SegmentedServingOptions::m21(path.join("cache"));
    serving.cache = None;
    serving.warm_unit_bytes = 0;
    serving.auto_cluster_rows = 0;
    ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        store: StoreConfig::Local(path.join("data")),
        collection: Config {
            dimensions: 2,
            metric: Metric::Cosine,
        },
        options: Default::default(),
        serving,
        limits: Limits::default(),
        token: Some("server-secret".into()),
        console: true,
        embedding: Default::default(),
        lease: Duration::from_millis(100),
        multi: false,
        max_open_collections: 2,
        collection_idle: Duration::ZERO,
    }
}
async fn call(
    app: &Router,
    method: &str,
    path: &str,
    body: Value,
    token: bool,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if token {
        request = request.header("authorization", "Bearer server-secret");
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test(flavor = "multi_thread")]
async fn disabled_embedding_auth_and_query_exclusivity() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let running = tokio::task::block_in_place(|| config.start().unwrap());
    let app = router_with_embedder(
        running.client(),
        config.token,
        true,
        Arc::new(Embedder::disabled()),
    );
    assert_eq!(
        call(&app, "POST", "/v1/embed", json!({"input":["hello"]}), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "POST", "/v1/query", json!({"text":"hello"}), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for (path, body) in [
        ("/v1/embed", json!({"input":["hello"]})),
        ("/v1/query", json!({"text":"hello"})),
    ] {
        let (status, body) = call(&app, "POST", path, body, true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("disabled"));
    }
    for body in [json!({}), json!({"vector":[1,0],"text":"hello"})] {
        let (status, reply) = call(&app, "POST", "/v1/query", body, true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(reply["error"].as_str().unwrap().contains("exactly one"));
    }
    let (_, status) = call(&app, "GET", "/v1/status", json!(null), true).await;
    assert!(status.get("embedding").is_none());
    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain).unwrap());
}

#[cfg(feature = "embed-openai")]
#[tokio::test(flavor = "multi_thread")]
async fn fake_openai_embed_text_query_limits_and_dimension_mismatch() {
    use axum::{extract::State, http::HeaderMap, routing::post, Json};
    use std::sync::Mutex;
    type Calls = Arc<Mutex<Vec<Value>>>;
    async fn endpoint(
        State(calls): State<Calls>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        assert_eq!(headers["authorization"], "Bearer provider-secret");
        assert_eq!(body["model"], "fake-model");
        calls.lock().unwrap().push(body.clone());
        let texts = body["input"].as_array().unwrap();
        if texts[0] == "remote-error" {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"provider-secret"})),
            );
        }
        let data: Vec<_> = texts
            .iter()
            .enumerate()
            .rev()
            .map(|(index, text)| {
                let vector = if text == "wrong-dim" {
                    vec![1.0, 0.0, 0.0]
                } else if text == "dog" {
                    vec![0.0, 1.0]
                } else {
                    vec![1.0, 0.0]
                };
                json!({"index":index,"embedding":vector})
            })
            .collect();
        (StatusCode::OK, Json(json!({"data":data})))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls: Calls = Default::default();
    let fake = Router::new()
        .route("/v1/embeddings", post(endpoint))
        .with_state(calls.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, fake).await.unwrap();
    });
    let embedder = Arc::new(
        Embedder::new(EmbedConfig::OpenAi {
            model: "fake-model".into(),
            base_url: format!("http://{address}/v1/"),
            api_key: Some("provider-secret".into()),
        })
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let running = tokio::task::block_in_place(|| config.start().unwrap());
    let app = router_with_embedder(
        running.client(),
        config.token.clone(),
        true,
        embedder.clone(),
    );
    let (_, before) = call(&app, "GET", "/v1/status", json!(null), true).await;
    assert_eq!(
        before["embedding"],
        json!({"provider":"openai","model":"fake-model","dimensions":null})
    );
    let (status, embedded) = call(
        &app,
        "POST",
        "/v1/embed",
        json!({"input":["cat","dog"]}),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        embedded,
        json!({"model":"fake-model","dimensions":2,"vectors":[[1.0,0.0],[0.0,1.0]]})
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/embed",
            json!({"input":["cat"],"kind":"query"}),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(call(&app,"POST","/v1/write",json!({"upsert":[{"id":1,"vector":embedded["vectors"][0],"metadata":{"text":"cat"}},{"id":2,"vector":embedded["vectors"][1],"metadata":{"text":"dog"}}]}),true).await.0,StatusCode::OK);
    for exact in [false, true] {
        let (status,text)=call(&app,"POST","/v1/query",json!({"text":"cat","k":2,"exact":exact,"include_metadata":true,"include_vector":true,"profile":true,"filter":{"text":{"$ne":"mouse"}}}),true).await;
        let (_,vector)=call(&app,"POST","/v1/query",json!({"vector":[1,0],"k":2,"exact":exact,"profile":exact,"include_metadata":true,"include_vector":true,"filter":{"text":{"$ne":"mouse"}}}),true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text["results"], vector["results"]);
        assert_eq!(text["results"][0]["id"], 1);
        assert!(text.get("profile").is_some());
        let embedding_ms = text["embedding_ms"].as_f64().unwrap();
        assert!(embedding_ms.is_finite() && embedding_ms >= 0.0);
        assert!(vector.get("embedding_ms").is_none());
    }
    let (_, after) = call(&app, "GET", "/v1/status", json!(null), true).await;
    assert_eq!(after["embedding"]["dimensions"], 2);
    assert!(!after.to_string().contains("secret"));
    assert!(!after.to_string().contains("http"));
    for input in [
        json!([]),
        json!([" "]),
        json!(["a".repeat(32769)]),
        json!(vec!["a"; 65]),
        json!(vec!["a".repeat(32768); 9]),
    ] {
        assert_eq!(
            call(&app, "POST", "/v1/embed", json!({"input":input}), true)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/query",
            json!({"text":"a".repeat(32769)}),
            true
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, error) = call(
        &app,
        "POST",
        "/v1/embed",
        json!({"input":["remote-error"]}),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!error.to_string().contains("provider-secret"));
    assert_eq!(calls.lock().unwrap()[0]["encoding_format"], "float");
    // Fresh server-wide provider has no inferred dimension yet; collection mismatch must be a 400.
    let fresh = Arc::new(
        Embedder::new(EmbedConfig::OpenAi {
            model: "fake-model".into(),
            base_url: format!("http://{address}/v1"),
            api_key: Some("provider-secret".into()),
        })
        .unwrap(),
    );
    let app = router_with_embedder(running.client(), config.token, true, fresh);
    let (status, error) = call(&app, "POST", "/v1/query", json!({"text":"wrong-dim"}), true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error["error"],
        "embedding dimension 3 differs from collection dimension 2"
    );
    // Same provider and API paths work in multi-collection mode, including before any collection exists.
    let mut multi_config = config_for_multi(temp.path());
    multi_config.embedding = Default::default();
    let multi = glider::server::Multi::new(multi_config).unwrap();
    let app = glider::server::multi_router_with_embedder(
        multi.clone(),
        Some("server-secret".into()),
        true,
        embedder,
    );
    let (_, listing) = call(&app, "GET", "/v1/collections", json!(null), true).await;
    assert_eq!(listing["embedding"]["dimensions"], 2);
    assert_eq!(
        call(&app, "POST", "/v1/embed", json!({"input":["cat"]}), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/collections",
            json!({"name":"docs","dimensions":2,"metric":"cosine"}),
            true
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/collections/docs/write",
            json!({"upsert":[{"id":1,"vector":[1,0]}]}),
            true
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, hits) = call(
        &app,
        "POST",
        "/v1/collections/docs/query",
        json!({"text":"cat"}),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hits["results"][0]["id"], 1);
    assert!(hits["embedding_ms"].as_f64().unwrap() >= 0.0);
    let (status, description) = call(&app, "GET", "/v1/collections/docs", json!(null), true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        description["status"]["configuration"],
        json!({"dimensions": 2, "metric": "cosine"})
    );
    multi.shutdown().await.unwrap();
    task.abort();
    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain).unwrap());
}
#[cfg(feature = "embed-openai")]
fn config_for_multi(path: &Path) -> ServerConfig {
    let mut config = config(&path.join("multi"));
    config.multi = true;
    config
}

#[cfg(feature = "embed-local")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads the ONNX model; run explicitly with network access"]
async fn local_model_download_and_dimensions() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path());
    let embedder = Arc::new(
        Embedder::new(EmbedConfig::Local {
            model: "BAAI/bge-small-en-v1.5".into(),
            cache_dir: temp.path().join("models"),
        })
        .unwrap(),
    );
    let running = tokio::task::block_in_place(|| config.start().unwrap());
    let app = router_with_embedder(running.client(), config.token, true, embedder);
    for kind in ["query", "document"] {
        let (status, reply) = call(
            &app,
            "POST",
            "/v1/embed",
            json!({"input":["A cat sleeps on a sofa."],"kind":kind}),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        assert_eq!(reply["dimensions"], 384);
        assert_eq!(reply["vectors"][0].as_array().unwrap().len(), 384);
    }
    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain).unwrap());
}
