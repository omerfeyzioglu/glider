#![cfg(feature = "server")]
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use glider::{
    admission::Limits,
    segmented::SegmentedServingOptions,
    server::{multi_router, router, Catalog, CreateCollection, Multi, ServerConfig, StoreConfig},
    store::ObjectStore,
    Config, Metric,
};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{Arc, Barrier},
    time::Duration,
};
use tower::ServiceExt;

fn config(path: &Path, max: usize) -> ServerConfig {
    let mut serving = SegmentedServingOptions::m21(path.join("cache"));
    serving.cache = None;
    serving.warm_unit_bytes = 0;
    serving.auto_cluster_rows = 0;
    ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        store: StoreConfig::Local(path.join("base")),
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
        multi: true,
        max_open_collections: max,
        collection_idle: Duration::from_secs(60),
    }
}
fn request(name: &str, dimensions: usize) -> CreateCollection {
    serde_json::from_value(json!({"name":name,"dimensions":dimensions})).unwrap()
}
async fn call(app: &Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}
async fn create(app: &Router, name: &str, dims: usize) -> (StatusCode, Value) {
    call(
        app,
        "POST",
        "/v1/collections",
        Some(json!({"name":name,"dimensions":dims})),
    )
    .await
}

#[test]
fn catalog_create_list_conflict_and_concurrent_create() {
    let temp = tempfile::tempdir().unwrap();
    let base = StoreConfig::Local(temp.path().to_path_buf());
    let catalog = Catalog::new(base.clone());
    assert!(catalog.list().unwrap().is_empty());
    let (first, created) = catalog.create(request("alpha", 2)).unwrap();
    assert!(created);
    assert_eq!(first.version, 1);
    assert_eq!(first.generation.len(), 32);
    assert!(!catalog.create(request("alpha", 2)).unwrap().1);
    assert_eq!(
        catalog.get("alpha").unwrap().unwrap().generation,
        first.generation
    );
    assert!(matches!(
        catalog.create(request("alpha", 3)),
        Err(glider::Error::RequestConflict)
    ));
    catalog.create(request("beta", 3)).unwrap();
    assert_eq!(
        catalog
            .list()
            .unwrap()
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let base = base.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                Catalog::new(base).create(request("race", 2)).unwrap()
            })
        })
        .collect();
    let winners: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(winners.iter().filter(|(_, created)| *created).count(), 1);
    assert!(winners
        .iter()
        .all(|(record, _)| record.generation == winners[0].0.generation));
}

#[test]
fn catalog_delete_crash_window_and_recreate() {
    let temp = tempfile::tempdir().unwrap();
    let base = StoreConfig::Local(temp.path().to_path_buf());
    let catalog = Catalog::new(base.clone());
    let (old, _) = catalog.create(request("alpha", 2)).unwrap();
    let old_data = catalog.data_store(&old).open().unwrap();
    old_data.create("orphan", b"old generation").unwrap();
    // Crash after the authoritative catalog delete but before data removal.
    catalog.delete(&old).unwrap();
    assert!(catalog.get("alpha").unwrap().is_none());
    let (new, _) = catalog.create(request("alpha", 2)).unwrap();
    assert_ne!(new.generation, old.generation);
    assert!(matches!(
        catalog.delete(&old),
        Err(glider::Error::RequestConflict)
    ));
    assert!(catalog
        .data_store(&new)
        .open()
        .unwrap()
        .list()
        .unwrap()
        .is_empty());
    assert_eq!(
        old_data.get("orphan").unwrap(),
        Some(b"old generation".to_vec())
    );
    Catalog::new(base).sweep().unwrap();
    assert!(old_data.list().unwrap().is_empty());
    assert_eq!(
        catalog.get("alpha").unwrap().unwrap().generation,
        new.generation
    );
    catalog.delete(&new).unwrap();
    assert!(catalog.get("alpha").unwrap().is_none());
}

#[test]
fn catalog_list_reads_many_collections_in_name_order() {
    let temp = tempfile::tempdir().unwrap();
    let base = StoreConfig::Local(temp.path().to_path_buf());
    let catalog = Catalog::new(base.clone());
    for index in (0..200).rev() {
        catalog
            .create(request(&format!("collection-{index:03}"), 2))
            .unwrap();
    }
    let names: Vec<_> = catalog
        .list()
        .unwrap()
        .into_iter()
        .map(|collection| collection.name)
        .collect();
    let expected: Vec<_> = (0..200)
        .map(|index| format!("collection-{index:03}"))
        .collect();
    assert_eq!(names, expected);

    base.child("catalog")
        .open()
        .unwrap()
        .create("corrupt", b"not JSON")
        .unwrap();
    assert!(matches!(catalog.list(), Err(glider::Error::Corrupt(_))));
}

#[test]
fn late_catalog_deletion_is_fenced_by_immutable_history_and_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let base = StoreConfig::Local(temp.path().to_path_buf());
    let catalog = Catalog::new(base.clone());
    let (first, _) = catalog.create(request("alpha", 2)).unwrap();
    // The first deletion has read the original entry and prepared slot 1,
    // then pauses before its conditional publication.
    let delayed = br#"{"version":1,"sequence":1,"collection":null}"#;
    catalog.delete(&first).unwrap();
    let (second, _) = catalog.create(request("alpha", 3)).unwrap();
    let history = base.child("catalog-history/alpha").open().unwrap();
    assert!(matches!(
        history.create("state-00000000000000000001", delayed),
        Err(glider::Error::Exists(_))
    ));
    let restarted = Catalog::new(base.clone());
    assert_eq!(restarted.get("alpha").unwrap(), Some(second.clone()));
    assert_eq!(restarted.list().unwrap(), vec![second.clone()]);
    assert!(matches!(
        restarted.delete(&first),
        Err(glider::Error::RequestConflict)
    ));
    restarted.delete(&second).unwrap();
    assert!(Catalog::new(base.clone()).get("alpha").unwrap().is_none());
    history.remove("state-00000000000000000002").unwrap();
    assert!(matches!(
        Catalog::new(base).get("alpha"),
        Err(glider::Error::Corrupt(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_background_sweep_eventually_removes_orphan() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path(), 2);
    let catalog = Catalog::new(config.store.clone());
    let (old, _) = catalog.create(request("orphan", 2)).unwrap();
    let old_data = catalog.data_store(&old).open().unwrap();
    old_data.create("point", b"old").unwrap();
    catalog.delete(&old).unwrap();

    let multi = Multi::new(config).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if old_data.get("point").unwrap().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background sweep should remove the orphan");
    multi.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn background_sweep_preserves_a_collection_created_during_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path(), 2);
    let catalog = Catalog::new(config.store.clone());
    let (marker, _) = catalog.create(request("a-marker", 2)).unwrap();
    let (blocker, _) = catalog.create(request("m-blocker", 2)).unwrap();
    let (old, _) = catalog.create(request("z-recreated", 2)).unwrap();
    let marker_data = catalog.data_store(&marker).open().unwrap();
    let blocker_data = catalog.data_store(&blocker).open().unwrap();
    let old_data = catalog.data_store(&old).open().unwrap();
    marker_data.create("point", b"marker").unwrap();
    blocker_data.create("point", b"blocker").unwrap();
    old_data.create("point", b"old").unwrap();
    for record in [&marker, &blocker, &old] {
        catalog.delete(record).unwrap();
    }

    // The local store locks its directory for removal. Hold the middle
    // orphan until the first has been removed, so the sweep's initial catalog
    // listing is complete and it has not reached the recreated name.
    let blocker_path = match catalog.data_store(&blocker) {
        StoreConfig::Local(path) => path,
        StoreConfig::S3 { .. } => unreachable!(),
    };
    let blocker_lock = std::fs::File::open(blocker_path).unwrap();
    blocker_lock.lock().unwrap();
    let multi = Multi::new(config).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if marker_data.get("point").unwrap().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sweep should reach the blocked orphan");

    let (live, _) = multi.create(request("z-recreated", 2)).await.unwrap();
    assert_ne!(live.generation, old.generation);
    let live_data = catalog.data_store(&live).open().unwrap();
    live_data.create("point", b"live").unwrap();
    std::fs::File::unlock(&blocker_lock).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if blocker_data.get("point").unwrap().is_none()
                && old_data.get("point").unwrap().is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sweep should finish removing the old generations");
    assert_eq!(live_data.get("point").unwrap(), Some(b"live".to_vec()));
    multi.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_closes_every_open_collection_and_releases_its_lease() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 12);
    config.collection_idle = Duration::ZERO;
    let multi = Multi::new(config.clone()).unwrap();
    for index in 0..12 {
        let name = format!("open-{index:02}");
        multi.create(request(&name, 2)).await.unwrap();
        drop(multi.use_collection(&name).await.unwrap().unwrap());
    }
    assert_eq!(multi.open_count().await, 12);
    multi.shutdown().await.unwrap();
    assert_eq!(multi.open_count().await, 0);

    let reopened = Multi::new(config).unwrap();
    for index in 0..12 {
        let name = format!("open-{index:02}");
        drop(reopened.use_collection(&name).await.unwrap().unwrap());
    }
    assert_eq!(reopened.open_count().await, 12);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn collections_lifecycle_isolation_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path(), 1);
    let multi = Multi::new(config.clone()).unwrap();
    let app = multi_router(multi.clone(), None);
    assert_eq!(create(&app, "a", 2).await.0, StatusCode::CREATED);
    assert_eq!(create(&app, "a", 2).await.0, StatusCode::OK);
    assert_eq!(create(&app, "a", 3).await.0, StatusCode::CONFLICT);
    assert_eq!(create(&app, "b", 3).await.0, StatusCode::CREATED);
    assert_eq!(create(&app, "Bad", 2).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        call(&app, "GET", "/v1/collections/missing", None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", "/v1/collections/missing", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (code, listed) = call(&app, "GET", "/v1/collections", None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(listed["collections"][0]["name"], "a");
    assert_eq!(listed["collections"][1]["name"], "b");
    assert_eq!(listed["collections"][0]["open"], false);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    assert!(String::from_utf8(metrics.to_vec())
        .unwrap()
        .contains("glider_open_collections 0"));
    let (code, legacy) = call(&app, "POST", "/v1/write", Some(json!({}))).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert!(legacy["error"]
        .as_str()
        .unwrap()
        .contains("/v1/collections/{name}"));
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/collections/a/write",
            Some(json!({"upsert":[{"id":1,"vector":[1.0,2.0]}]}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/collections/b/write",
            Some(json!({"upsert":[{"id":1,"vector":[1.0,2.0,3.0]}]}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(!multi.is_open("a").await);
    assert!(multi.is_open("b").await);
    let (code, point) = call(&app, "GET", "/v1/collections/a/points/1", None).await;
    assert_eq!(code, StatusCode::OK, "{point}");
    assert_eq!(point["vector"], json!([1.0, 2.0]));
    let (code, result) = call(
        &app,
        "POST",
        "/v1/collections/b/query",
        Some(json!({"vector":[1.0,2.0,3.0],"exact":true,"profile":true})),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{result}");
    assert_eq!(result["results"][0]["id"], 1);
    assert_eq!(result["profile"]["mode"], "exact_scan");
    assert!(result["profile"]["server_ms"].as_f64().unwrap() >= 0.0);
    let (code, scan) = call(&app, "POST", "/v1/collections/a/scan", Some(json!({}))).await;
    assert_eq!(code, StatusCode::OK, "{scan}");
    assert_eq!(scan["ids"], json!([1]));
    let (code, batch) = call(
        &app,
        "POST",
        "/v1/collections/a/points/get",
        Some(json!({"ids":[1]})),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{batch}");
    let (code, description) = call(&app, "GET", "/v1/collections/a", None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(description["status"]["closed"], false);
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/collections/a/write",
            Some(json!({"upsert":[{"id":2,"vector":[1.0]}]}))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    multi.shutdown().await.unwrap();
    drop(app);
    drop(multi);
    let restarted = Multi::new(config).unwrap();
    let app = multi_router(restarted.clone(), None);
    let (code, listed) = call(&app, "GET", "/v1/collections", None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(listed["collections"][0]["open"], false);
    let (code, point) = call(&app, "GET", "/v1/collections/b/points/1", None).await;
    assert_eq!(code, StatusCode::OK, "{point}");
    assert_eq!(point["vector"], json!([1.0, 2.0, 3.0]));
    assert_eq!(
        call(&app, "DELETE", "/v1/collections/b", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "GET", "/v1/collections/b/points/1", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(create(&app, "b", 3).await.0, StatusCode::CREATED);
    assert_eq!(
        call(&app, "GET", "/v1/collections/b/points/1", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn single_collection_routes_remain_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 1);
    config.multi = false;
    let running = tokio::task::spawn_blocking(move || config.start())
        .await
        .unwrap()
        .unwrap();
    let app = router(running.client(), None);
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/write",
            Some(json!({"upsert":[{"id":7,"vector":[3.0,4.0]}]}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "GET", "/v1/points/7", None).await.1["vector"],
        json!([3.0, 4.0])
    );
    assert_eq!(
        call(&app, "GET", "/v1/status", None).await.0,
        StatusCode::OK
    );
    tokio::task::spawn_blocking(move || running.shutdown(glider::admission::Shutdown::Drain))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_use_opens_once_and_busy_limit_preserves_active_use() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(temp.path(), 1);
    let multi = Multi::new(config).unwrap();
    let (record, _) = multi.create(request("alpha", 2)).await.unwrap();
    multi.create(request("beta", 2)).await.unwrap();
    let (left, right) = tokio::join!(multi.use_collection("alpha"), multi.use_collection("alpha"));
    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();
    assert_eq!(multi.open_count().await, 1);
    let objects = multi
        .catalog()
        .data_store(&record)
        .open()
        .unwrap()
        .list()
        .unwrap();
    assert_eq!(
        objects
            .iter()
            .filter(|key| key.starts_with("sglog-"))
            .count(),
        1
    );
    assert!(matches!(
        multi.use_collection("beta").await,
        Err(glider::Error::Busy(_))
    ));
    let app = multi_router(multi.clone(), None);
    let (code, body) = call(&app, "GET", "/v1/collections/beta/points/1", None).await;
    assert_eq!(code, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(
        body["error"],
        "all open collections have requests in flight"
    );
    drop((left, right));
    assert!(multi.use_collection("beta").await.unwrap().is_some());
    multi.shutdown().await.unwrap();
}

async fn wait_until_closed(multi: &Multi, name: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while multi.is_open(name).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("idle collection should close");
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_collection_closes_and_reopens_with_acknowledged_data() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 4);
    config.collection_idle = Duration::from_millis(200);
    let multi = Multi::new(config).unwrap();
    let app = multi_router(multi.clone(), None);
    assert_eq!(create(&app, "idle", 2).await.0, StatusCode::CREATED);
    let (code, body) = call(
        &app,
        "POST",
        "/v1/collections/idle/write",
        Some(json!({"upsert":[{"id":1,"vector":[1.0,2.0]}]})),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert!(multi.is_open("idle").await);
    wait_until_closed(&multi, "idle").await;
    assert_eq!(multi.open_count().await, 0);
    let (code, body) = call(&app, "GET", "/v1/collections/idle/points/1", None).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["vector"], json!([1.0, 2.0]));
    assert!(multi.is_open("idle").await);
    multi.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_collection_keeps_in_flight_request_open() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 4);
    config.collection_idle = Duration::from_millis(200);
    let multi = Multi::new(config).unwrap();
    multi.create(request("busy", 2)).await.unwrap();
    let use_ = multi.use_collection("busy").await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(multi.is_open("busy").await);
    assert_eq!(multi.open_count().await, 1);
    drop(use_);
    wait_until_closed(&multi, "busy").await;
    multi.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_idle_duration_disables_closing() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 4);
    config.collection_idle = Duration::ZERO;
    let multi = Multi::new(config).unwrap();
    multi.create(request("kept", 2)).await.unwrap();
    drop(multi.use_collection("kept").await.unwrap().unwrap());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(multi.is_open("kept").await);
    assert_eq!(multi.open_count().await, 1);
    multi.shutdown().await.unwrap();
}

#[test]
#[ignore = "requires isolated MinIO; run with GLIDER_S3_BUCKET and GLIDER_S3_ENDPOINT"]
fn minio_catalog_conditional_create_and_sweep() {
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes).unwrap();
    let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let base = StoreConfig::S3 {
        bucket: std::env::var("GLIDER_S3_BUCKET").unwrap(),
        namespace: format!("glider-collections-test/{suffix}"),
        region: std::env::var("GLIDER_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        endpoint: Some(std::env::var("GLIDER_S3_ENDPOINT").unwrap()),
    };
    let catalog = Catalog::new(base);
    let (first, _) = catalog.create(request("alpha", 2)).unwrap();
    assert!(!catalog.create(request("alpha", 2)).unwrap().1);
    assert!(matches!(
        catalog.create(request("alpha", 3)),
        Err(glider::Error::RequestConflict)
    ));
    catalog
        .data_store(&first)
        .open()
        .unwrap()
        .create("orphan", b"orphan")
        .unwrap();
    catalog.delete(&first).unwrap();
    let (second, _) = catalog.create(request("alpha", 2)).unwrap();
    assert_ne!(first.generation, second.generation);
    catalog.sweep().unwrap();
    assert!(catalog
        .data_store(&first)
        .open()
        .unwrap()
        .list()
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_collection_removes_its_local_cache() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = config(temp.path(), 4);
    config.serving.cache = Some((temp.path().join("cache"), 64, 64 * 1024 * 1024));
    let multi = Multi::new(config).unwrap();
    let app = multi_router(multi.clone(), None);
    assert_eq!(create(&app, "cached", 2).await.0, StatusCode::CREATED);
    let (code, body) = call(
        &app,
        "POST",
        "/v1/collections/cached/write",
        Some(json!({"upsert":[{"id":1,"vector":[1.0, 2.0]}]})),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let generation = multi.get("cached").await.unwrap().unwrap().generation;
    let cache = temp
        .path()
        .join("cache")
        .join(format!("cached-{generation}"));
    assert!(
        cache.exists(),
        "the open collection uses its cache directory"
    );
    assert_eq!(
        call(&app, "DELETE", "/v1/collections/cached", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert!(!cache.exists(), "delete removes the collection's cache");
    multi.shutdown().await.unwrap();
}
