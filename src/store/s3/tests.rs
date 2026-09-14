use super::*;
use crate::{Config, Database, Metric};
use object_store::client::{HttpErrorKind, HttpResponseBody};
use std::collections::VecDeque;

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
fn builder() -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name("test-bucket")
        .with_region("us-east-1")
        .with_access_key_id("test-only")
        .with_secret_access_key("test-only")
}
fn response(status: u16, body: impl AsRef<[u8]>) -> HttpResponse {
    let bytes = body.as_ref().to_vec();
    let length = bytes.len();
    let mut response = HttpResponse::new(HttpResponseBody::from(bytes));
    *response.status_mut() = status.try_into().unwrap();
    response
        .headers_mut()
        .insert("etag", "test-etag".parse().unwrap());
    response.headers_mut().insert(
        "last-modified",
        "Mon, 14 Sep 2026 00:00:00 GMT".parse().unwrap(),
    );
    response
        .headers_mut()
        .insert("content-length", length.to_string().parse().unwrap());
    response
}

fn disconnected() -> HttpError {
    HttpError::new(
        HttpErrorKind::Request,
        std::io::Error::other("injected lost connection"),
    )
}
#[derive(Debug, Clone)]
struct Script(Arc<Mutex<VecDeque<HttpResponse>>>);
impl HttpConnector for Script {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}
#[async_trait]
impl HttpService for Script {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        if request.method() == "PUT" {
            assert_eq!(request.headers()["if-none-match"], "*");
        }
        if request
            .uri()
            .query()
            .is_some_and(|q| q.contains("continuation-token"))
        {
            assert!(request.uri().query().unwrap().contains("next-page"));
        }
        self.0.lock().unwrap().pop_front().ok_or_else(disconnected)
    }
}
fn scripted(responses: Vec<HttpResponse>) -> S3Store {
    S3Store::with_connector(
        builder(),
        "test",
        Script(Arc::new(Mutex::new(responses.into()))),
    )
    .unwrap()
}
fn page(key: &str, next: bool) -> String {
    format!("<ListBucketResult><IsTruncated>{next}</IsTruncated>{}<Contents><Key>{key}</Key><LastModified>2026-09-14T00:00:00Z</LastModified><ETag>test</ETag><Size>1</Size></Contents></ListBucketResult>",
        if next { "<NextContinuationToken>next-page</NextContinuationToken>" } else { "" })
}
#[test]
fn conditional_create_errors_poison_without_retry() {
    for status in [403, 409, 412, 500, 501] {
        let mut store = scripted(vec![response(
            status,
            "<Error><Code>Failure</Code></Error>",
        )]);
        let error = store.create("object", b"value").unwrap_err();
        if status == 412 {
            assert!(matches!(error, Error::Exists(_)));
        } else {
            assert!(matches!(error, Error::Io(_)));
        }
        assert_eq!(store.metrics().snapshot().put, 1, "status={status}");
        assert_eq!(store.metrics().snapshot().http_errors, 1);
        assert!(matches!(store.get("object"), Err(Error::RecoveryRequired)));
        assert!(matches!(store.list(), Err(Error::RecoveryRequired)));
        assert!(matches!(
            store.create("next", b"value"),
            Err(Error::RecoveryRequired)
        ));
    }
}
#[test]
fn read_errors_and_corrupt_envelopes_are_not_absence() {
    let store = scripted(vec![response(404, "<Error><Code>NoSuchKey</Code></Error>")]);
    assert_eq!(store.get("missing").unwrap(), None);
    for status in [403, 500] {
        let store = scripted(vec![response(
            status,
            "<Error><Code>Failure</Code></Error>",
        )]);
        assert!(store.get("object").is_err());
        assert_eq!(store.metrics().snapshot().get, 1);
    }
    let good = encode_envelope(b"value");
    let mut changed = good.clone();
    changed[16] ^= 1;
    for bytes in [good[..good.len() - 1].to_vec(), changed, vec![]] {
        assert!(matches!(
            scripted(vec![response(200, bytes)]).get("object"),
            Err(Error::Corrupt(_))
        ));
    }
    assert_eq!(
        scripted(vec![response(200, good)]).get("object").unwrap(),
        Some(b"value".to_vec())
    );
}
#[test]
fn listing_exhausts_pages_and_never_returns_partial_success() {
    let store = scripted(vec![
        response(200, page("test/a", true)),
        response(200, page("test/b", false)),
    ]);
    assert_eq!(store.list().unwrap(), vec!["a", "b"]);
    assert_eq!(store.metrics().snapshot().list, 2);
    for second in [
        response(500, "<Error/>"),
        response(200, "broken XML"),
        response(200, page("test-other/b", false)),
    ] {
        let store = scripted(vec![response(200, page("test/a", true)), second]);
        assert!(store.list().is_err());
        assert_eq!(store.metrics().snapshot().list, 2);
    }
    let store = scripted(vec![response(200, page("test/a", true))]);
    assert!(store.list().is_err());
    assert_eq!(store.metrics().snapshot().transport_errors, 1);
}
#[test]
fn invalid_names_fail_before_requests_and_spawn_blocking_is_supported() {
    for namespace in ["", "/test", "test/", "test//db", "../db", "test%2Fdb"] {
        assert!(matches!(
            S3Store::open(builder(), namespace),
            Err(Error::Invalid(_))
        ));
    }
    let mut store = scripted(vec![]);
    for key in ["", "../escape", "nested/key"] {
        assert!(store.create(key, b"value").is_err());
    }
    assert_eq!(store.metrics().snapshot(), RequestCounts::default());
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let listed = tokio::task::spawn_blocking(|| {
            scripted(vec![response(200, page("test/a", false))]).list()
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(listed, vec!["a"]);
        drop(store); // Dropping a handle in an async task must not panic.
    });
}

fn minio_builder() -> AmazonS3Builder {
    AmazonS3Builder::from_env()
        .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").expect("run tools/test_s3.py"))
        .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").expect("test bucket required"))
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(std::time::Duration::from_secs(5)),
        )
}
fn minio(namespace: &str) -> S3Store {
    S3Store::open(minio_builder(), namespace).unwrap()
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_replay_pagination_and_namespace_isolation() {
    let store = minio("replay");
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    // Cross the native 1000-object listing page boundary, keeping live state small.
    for id in 0..1001 {
        db.put(id % 3, vec![id as f32, 1.]).unwrap();
    }
    db.delete(1).unwrap();
    let expected = db.search(&[0., 0.], 10).unwrap();
    let before = metrics.snapshot();
    assert_eq!(before.put, 1003); // metadata + mutations; no body/seal pairs
    assert_eq!(before.list, 1);
    assert_eq!(before.get, 1); // no preflight GET on create
    assert_eq!(db.search(&[0., 0.], 10).unwrap(), expected);
    assert_eq!(metrics.snapshot(), before); // query is entirely in memory
    drop(db);
    let mut adjacent = minio("replay-other");
    adjacent.create("unrelated", b"value").unwrap();
    drop(adjacent);
    let store = minio("replay");
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    assert_eq!(db.search(&[0., 0.], 10).unwrap(), expected);
    assert_eq!(metrics.snapshot().list, 2);
    assert_eq!(metrics.snapshot().get, 1003);
    eprintln!(
        "M2 recovery: 1002 mutations, 1003 objects: {:?}",
        metrics.snapshot()
    );
    db.put(9, vec![9., 9.]).unwrap();
    drop(db);
    let db = Database::open(minio("replay"), config()).unwrap();
    assert_eq!(db.get(9), Some([9., 9.].as_slice()));
    assert_eq!(db.get(1), None);
}

#[derive(Debug, Clone, Copy)]
enum Cut {
    Before,
    After,
}
#[derive(Debug, Clone, Default)]
struct Fault(Arc<Mutex<Option<Cut>>>);
#[derive(Debug)]
struct FaultService {
    inner: HttpClient,
    fault: Fault,
}
impl HttpConnector for Fault {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(FaultService {
            inner: ReqwestConnector::default().connect(options)?,
            fault: self.clone(),
        }))
    }
}
#[async_trait]
impl HttpService for FaultService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let cut = if request.method() == "PUT" {
            self.fault.0.lock().unwrap().take()
        } else {
            None
        };
        if matches!(cut, Some(Cut::Before)) {
            return Err(disconnected());
        }
        let result = self.inner.execute(request).await?;
        if matches!(cut, Some(Cut::After)) {
            assert!(result.status().is_success());
            result.into_body().bytes().await?;
            return Err(disconnected()); // server published; client never sees success
        }
        Ok(result)
    }
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_uncertain_writes_and_initialization() {
    for (suffix, cut, committed) in [("before", Cut::Before, false), ("after", Cut::After, true)] {
        let namespace = format!("uncertain-{suffix}");
        let fault = Fault::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, fault.clone()).unwrap();
        let metrics = store.metrics();
        let mut db = Database::open(store, config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(db.delete(1).is_err());
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        assert!(matches!(
            db.put(2, vec![3., 4.]),
            Err(Error::RecoveryRequired)
        ));
        assert_eq!(metrics.snapshot().put, 3);
        assert_eq!(metrics.snapshot().transport_errors, 1);
        drop(db);
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        db.put(2, vec![3., 4.]).unwrap();
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        assert_eq!(db.get(2), Some([3., 4.].as_slice()));
        assert_eq!(db.sequence, if committed { 3 } else { 2 });
        drop(db);
        let namespace = format!("init-{suffix}");
        let store = S3Store::with_connector(minio_builder(), &namespace, fault.clone()).unwrap();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(Database::open(store, config()).is_err());
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        drop(db);
        assert_eq!(
            Database::open(minio(&namespace), config()).unwrap().get(1),
            Some([1., 2.].as_slice())
        );
    }
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_immutability_and_corruption() {
    let mut store = minio("immutable");
    store.create("object", b"original").unwrap();
    assert!(matches!(
        store.create("object", b"replacement"),
        Err(Error::Exists(_))
    ));
    assert!(matches!(store.list(), Err(Error::RecoveryRequired)));
    drop(store);
    assert_eq!(
        minio("immutable").get("object").unwrap().unwrap(),
        b"original"
    );
    let mut db = Database::open(minio("corruption"), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    drop(db);
    let store = minio("corruption");
    let path = store.path("mutation-00000000000000000001").unwrap();
    // External media/object modification outside the glider contract.
    store
        .run(store.remote.put(&path, b"broken".to_vec().into()))
        .unwrap();
    drop(store);
    assert!(matches!(
        Database::open(minio("corruption"), config()),
        Err(Error::Corrupt(_))
    ));
    let store = S3Store::open(
        minio_builder().with_secret_access_key("incorrect-test-secret"),
        "auth",
    )
    .unwrap();
    assert!(Database::open(store, config()).is_err());
    let store = S3Store::open(
        minio_builder().with_bucket_name("nonexistent-bucket"),
        "missing",
    )
    .unwrap();
    assert!(Database::open(store, config()).is_err());
}
#[test]
#[ignore = "child process only"]
fn s3_process_child() {
    let mut db = Database::open(minio("process-restart"), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    db.delete(1).unwrap();
    std::process::exit(73);
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_process_exit() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "store::s3::tests::s3_process_child", "--ignored"])
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut db = Database::open(minio("process-restart"), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    db.put(3, vec![5., 6.]).unwrap();
    drop(db);
    assert_eq!(
        Database::open(minio("process-restart"), config())
            .unwrap()
            .get(3),
        Some([5., 6.].as_slice())
    );
}
#[test]
#[ignore = "runner phase before restarting MinIO"]
fn server_restart_prepare() {
    for checkpoint in [false, true] {
        let namespace = if checkpoint {
            "server-segment-restart"
        } else {
            "server-restart"
        };
        let mut db = Database::open(minio(namespace), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        db.put(2, vec![3., 4.]).unwrap();
        if checkpoint {
            db.checkpoint().unwrap();
        }
        db.delete(1).unwrap();
    }
}
#[test]
#[ignore = "runner phase after restarting MinIO"]
fn server_restart_verify() {
    for namespace in ["server-restart", "server-segment-restart"] {
        let mut db = Database::open(minio(namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(2), Some([3., 4.].as_slice()));
        db.put(3, vec![5., 6.]).unwrap();
        drop(db);
        let db = Database::open(minio(namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(3), Some([5., 6.].as_slice()));
    }
}

#[derive(Debug, Clone, Default)]
struct DeferredPut(Arc<Mutex<Option<HttpRequest>>>);
#[derive(Debug)]
struct DeferredService {
    inner: HttpClient,
    deferred: DeferredPut,
}
impl HttpConnector for DeferredPut {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(DeferredService {
            inner: ReqwestConnector::default().connect(options)?,
            deferred: self.clone(),
        }))
    }
}
#[async_trait]
impl HttpService for DeferredService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        if request.method() == "PUT" {
            *self.deferred.0.lock().unwrap() = Some(request);
            return Err(disconnected());
        }
        self.inner.execute(request).await
    }
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_late_conditional_request_cannot_overwrite_reused_sequence() {
    for old_first in [true, false] {
        let namespace = format!("late-{old_first}");
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        drop(db);
        let deferred = DeferredPut::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, deferred.clone()).unwrap();
        let mut db = Database::open(store, config()).unwrap();
        assert!(db.put(2, vec![2., 2.]).is_err());
        drop(db);
        // Release the original signed request after recovery has already observed
        // its absence. This deterministically models a request arriving late.
        let request = deferred.0.lock().unwrap().take().unwrap();
        let client = ReqwestConnector::default()
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(2), None);
        if old_first {
            let response = rt.block_on(client.execute(request)).unwrap();
            assert_eq!(response.status().as_u16(), 200);
            assert!(matches!(db.put(3, vec![3., 3.]), Err(Error::Exists(_))));
            assert!(matches!(
                db.put(4, vec![4., 4.]),
                Err(Error::RecoveryRequired)
            ));
        } else {
            db.put(3, vec![3., 3.]).unwrap();
            let response = rt.block_on(client.execute(request)).unwrap();
            assert_eq!(response.status().as_u16(), 412);
        }
        drop(db);
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        assert_eq!(db.get(2).is_some(), old_first);
        assert_eq!(db.get(3).is_some(), !old_first);
        db.put(4, vec![4., 4.]).unwrap();
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(4), Some([4., 4.].as_slice()));
        assert_eq!(db.get(2).is_some(), old_first);
        assert_eq!(db.get(3).is_some(), !old_first);
        assert_eq!(db.sequence, 3);
    }
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_segment_recovery_and_uncertain_publication() {
    for (suffix, cut, published) in [("before", Cut::Before, false), ("after", Cut::After, true)] {
        let namespace = format!("segment-{suffix}");
        let fault = Fault::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, fault.clone()).unwrap();
        let mut db = Database::open(store, config()).unwrap();
        for i in 0..20 {
            db.put(i % 3, vec![i as f32, 0.]).unwrap();
        }
        db.checkpoint().unwrap();
        db.delete(1).unwrap();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(db.checkpoint().is_err());
        assert!(matches!(
            db.put(4, vec![4., 0.]),
            Err(Error::RecoveryRequired)
        ));
        assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
        drop(db);
        let store = minio(&namespace);
        let observer = store.metrics();
        let mut db = Database::open(store, config()).unwrap();
        assert_eq!(observer.snapshot().get, if published { 2 } else { 3 });
        assert_eq!(db.get(1), None);
        db.checkpoint().unwrap();
        db.put(4, vec![4., 0.]).unwrap();
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(4), Some([4., 0.].as_slice()));
        assert_eq!(db.sequence, 22);
    }
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_late_segment_is_a_valid_prefix_after_newer_acknowledged_writes() {
    let namespace = "late-segment";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    drop(db);
    let deferred = DeferredPut::default();
    let store = S3Store::with_connector(minio_builder(), namespace, deferred.clone()).unwrap();
    let mut db = Database::open(store, config()).unwrap();
    assert!(db.checkpoint().is_err());
    drop(db);
    let request = deferred.0.lock().unwrap().take().unwrap();
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.delete(1).unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    let client = ReqwestConnector::default()
        .connect(&ClientOptions::new().with_allow_http(true))
        .unwrap();
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    assert!(rt
        .block_on(client.execute(request))
        .unwrap()
        .status()
        .is_success());
    drop(db);
    let mut db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    assert_eq!(db.sequence, 3);
    db.checkpoint().unwrap();
    db.put(3, vec![5., 6.]).unwrap();
    drop(db);
    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(3), Some([5., 6.].as_slice()));
}
