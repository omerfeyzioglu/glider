#![cfg(feature = "server")]

use axum::{
    body::{to_bytes, Body},
    http::{Request as HttpRequest, StatusCode},
    Router,
};
use glider::{
    admission::Shutdown,
    retry::{Request, RequestId},
    segmented::{ReadBudget, SegmentedDatabase},
    server::{router, ServerConfig, StoreConfig},
    store::LocalStore,
    Mutation,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tower::ServiceExt;

async fn call(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let request = HttpRequest::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_batch_get_and_scan_use_published_view() {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_var("GLIDER_DIMENSIONS", "3");
    std::env::set_var("GLIDER_METRIC", "squared_euclidean");
    std::env::set_var("GLIDER_DATA_DIR", "unused");
    std::env::set_var("GLIDER_RESIDENT_FILTER", "color=red");
    let mut config = ServerConfig::from_env().unwrap();
    let path = temp.path().join("data");
    config.store = StoreConfig::Local(path.clone());
    config.serving.cache = None;
    config.serving.warm_unit_bytes = 0;
    config.serving.auto_cluster_rows = 0;
    config.serving.read_budget = ReadBudget::uniform(1);
    config.token = None;

    let mut db = SegmentedDatabase::open_with_options(
        LocalStore::open(&path).unwrap(),
        config.collection,
        config.options.clone(),
    )
    .unwrap();
    for batch in 0..2_u64 {
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: [batch as u8 + 1; 16],
            },
            conditions: Vec::new(),
            mutations: (1 + batch * 60..=60 + batch * 60)
                .map(|id| Mutation::Put {
                    id,
                    vector: vec![id as f32, 1.0, 0.0],
                    metadata: BTreeMap::from([(
                        "color".into(),
                        if id % 3 == 0 { "blue" } else { "red" }.into(),
                    )]),
                })
                .collect(),
        })
        .unwrap();
        db.seal_delta().unwrap();
    }
    drop(db);

    let running = tokio::task::block_in_place(|| config.start().unwrap());
    let app = router(running.client(), None);
    let (status, write) = call(
        &app,
        "/v1/write",
        json!({"upsert":[{"id":201,"vector":[201.0,1.0,0.0],"metadata":{"color":"blue"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{write}");
    let sequence = write["sequence"].as_u64().unwrap();

    let query = json!({"vector":[201.0,1.0,0.0],"k":1000,"filter":{"color":"blue"}});
    let (status, approximate) = call(&app, "/v1/query", query.clone()).await;
    assert_eq!(status, StatusCode::OK, "{approximate}");
    let mut exact_request = query;
    exact_request["exact"] = json!(true);
    let (status, exact) = call(&app, "/v1/query", exact_request).await;
    assert_eq!(status, StatusCode::OK, "{exact}");
    assert_eq!(exact["sequence"], sequence);
    assert_eq!(exact["results"].as_array().unwrap().len(), 41);
    assert!(approximate["results"].as_array().unwrap().len() < 41);

    let (status, got) = call(
        &app,
        "/v1/points/get",
        json!({"ids":[201,999,3,201],"include_vector":false}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["sequence"], sequence);
    assert_eq!(
        got["points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![201, 3, 201]
    );
    assert_eq!(got["missing"], json!([999]));
    assert!(got["points"][0].get("vector").is_none());
    assert_eq!(got["points"][0]["metadata"]["color"], "blue");

    let mut after = None;
    let mut seen = Vec::new();
    loop {
        let (status, page) = call(
            &app,
            "/v1/scan",
            json!({"filter":{"color":"blue"},"after":after,"limit":7,"include_metadata":true}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page["sequence"], sequence);
        assert_eq!(page["matched"], 41);
        for point in page["points"].as_array().unwrap() {
            assert_eq!(point["metadata"]["color"], "blue");
            seen.push(point["id"].as_u64().unwrap());
        }
        after = page["next"].as_u64();
        if after.is_none() {
            break;
        }
    }
    assert_eq!(
        seen,
        (1..=120)
            .filter(|id| id % 3 == 0)
            .chain([201])
            .collect::<Vec<_>>()
    );
    let (_, ids) = call(
        &app,
        "/v1/scan",
        json!({"filter":{"color":"blue"},"after":119,"limit":10000}),
    )
    .await;
    assert_eq!(ids["ids"], json!([120, 201]));
    assert_eq!(ids["next"], Value::Null);
    assert!(ids.get("points").is_none());

    for (path, body, expected) in [
        (
            "/v1/query",
            json!({"vector":[1,0,0],"exact":true,"unknown":1}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        ("/v1/points/get", json!({"ids":[]}), StatusCode::BAD_REQUEST),
        (
            "/v1/points/get",
            json!({"ids":vec![1;1001]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/points/get",
            json!({"ids":[1],"unknown":1}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        ("/v1/scan", json!({"limit":0}), StatusCode::BAD_REQUEST),
        ("/v1/scan", json!({"limit":10001}), StatusCode::BAD_REQUEST),
        (
            "/v1/scan",
            json!({"unknown":1}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let (status, error) = call(&app, path, body).await;
        assert_eq!(status, expected, "{error}");
        assert!(error["error"].is_string());
    }
    let (status, thousand) = call(
        &app,
        "/v1/points/get",
        json!({"ids":vec![201;1000],"include_metadata":false,"include_vector":false}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{thousand}");
    assert_eq!(thousand["points"].as_array().unwrap().len(), 1000);
    assert_eq!(thousand["points"][0], json!({"id":201}));

    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain).unwrap());
    let (status, closed) = call(&app, "/v1/scan", json!({})).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{closed}");
    assert!(closed["error"].is_string());
    let oracle = SegmentedDatabase::open_with_options(
        LocalStore::open(&path).unwrap(),
        config.collection,
        config.options,
    )
    .unwrap()
    .search_exact(&[201.0, 1.0, 0.0], 1000, &[("color", "blue")])
    .unwrap();
    let got: Vec<_> = exact["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| {
            (
                hit["id"].as_u64().unwrap(),
                hit["distance"].as_f64().unwrap(),
            )
        })
        .collect();
    let expected: Vec<_> = oracle.iter().map(|hit| (hit.id, hit.distance)).collect();
    assert_eq!(got, expected);
}
