use super::*;
use crate::{
    ownership::{claims, clear_stale_claim, OwnedDatabase},
    recovery::stage_isolated_namespace,
    streaming::StreamingDatabase,
    Config, Database, Metric, Mutation,
};
use bytes::Bytes;
use http_body::Frame;
use http_body_util::StreamBody;
use object_store::client::{HttpErrorKind, HttpResponseBody};
use std::collections::{BTreeMap, VecDeque};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_addressable_payload_range_checks_length_and_bounds() {
    let store = minio("addressable-range");
    let body: Vec<u8> = (0..1_048_576).map(|n| ((n * 17) % 251) as u8).collect();
    store.create("block-object", &body).unwrap();
    let metrics = store.metrics();
    let before = metrics.snapshot();
    assert_eq!(
        store
            .get_range("block-object", 262_144, 131_072, body.len())
            .unwrap(),
        Some(body[262_144..393_216].to_vec())
    );
    assert_eq!(metrics.snapshot().get - before.get, 1);
    assert_eq!(store.get_range("missing", 0, 1, body.len()).unwrap(), None);
    assert!(matches!(
        store.get_range("block-object", 0, 1, body.len() - 1),
        Err(crate::Error::Corrupt(_))
    ));
    let before = metrics.snapshot();
    assert!(store
        .get_range("block-object", body.len(), 1, body.len())
        .is_err());
    assert_eq!(metrics.snapshot(), before);
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_bounded_open_rejects_inventory_before_loading_snapshot() {
    let namespace = "bounded-open";
    let cfg = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let mut db = Database::open(minio(namespace), cfg).unwrap();
    db.apply_batch(
        (0..100)
            .map(|id| Mutation::Put {
                id,
                vector: vec![id as f32; 128],
                metadata: BTreeMap::new(),
            })
            .collect(),
    )
    .unwrap();
    db.compact().unwrap();
    drop(db);
    let small = ReadLimits {
        objects: 16,
        object_bytes: 4096,
        namespace_bytes: 8192,
    };
    let store = minio(namespace).with_read_limits(small).unwrap();
    let metrics = store.metrics();
    assert!(Database::open(store, cfg)
        .err()
        .unwrap()
        .to_string()
        .contains("listed object bytes"));
    assert_eq!(metrics.snapshot().get, 0);
    // Rejection is read-only. A correctly budgeted reopen still recovers exact state.
    let store = minio(namespace)
        .with_read_limits(ReadLimits {
            object_bytes: 1024 * 1024,
            namespace_bytes: 2 * 1024 * 1024,
            ..small
        })
        .unwrap();
    let db = Database::open(store, cfg).unwrap();
    for id in 0..100 {
        assert_eq!(db.get(id).unwrap(), vec![id as f32; 128]);
    }
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_owner_claim_survives_drop_and_requires_explicit_cleanup() {
    let namespace = "exclusive-owner";
    let mut first = OwnedDatabase::open(minio(namespace), config()).unwrap();
    first.put(1, vec![1., 2.]).unwrap();
    assert!(matches!(
        OwnedDatabase::open(minio(namespace), config()),
        Err(crate::Error::Busy(_))
    ));
    assert!(Database::open(minio(namespace), config()).is_err());
    drop(first);
    let mut store = minio(namespace);
    let owner = claims(&store).unwrap();
    assert_eq!(owner.len(), 1);
    clear_stale_claim(&mut store, &owner[0]).unwrap();
    let db = OwnedDatabase::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), Some([1., 2.].as_slice()));
    db.close().unwrap();
    assert!(claims(&minio(namespace)).unwrap().is_empty());
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_isolated_staging_keeps_later_old_prefix_writes_out() {
    let old = "takeover-source";
    let new = "takeover-destination";
    let mut db = OwnedDatabase::open(minio(old), config()).unwrap();
    db.put(1, vec![1., 0.]).unwrap();
    drop(db);
    stage_isolated_namespace(&minio(old), minio(new), config()).unwrap();
    let mut new_db = OwnedDatabase::open(minio(new), config()).unwrap();
    assert_eq!(new_db.get(1), Some([1., 0.].as_slice()));

    let mut old_store = minio(old);
    let owner = claims(&old_store).unwrap();
    clear_stale_claim(&mut old_store, &owner[0]).unwrap();
    let mut old_db = OwnedDatabase::open(minio(old), config()).unwrap();
    old_db.put(2, vec![2., 0.]).unwrap();
    old_db.close().unwrap();
    assert_eq!(new_db.get(2), None);
    new_db.put(3, vec![3., 0.]).unwrap();
    new_db.close().unwrap();
    let db = OwnedDatabase::open(minio(new), config()).unwrap();
    assert_eq!(db.get(1), Some([1., 0.].as_slice()));
    assert_eq!(db.get(2), None);
    assert_eq!(db.get(3), Some([3., 0.].as_slice()));
    db.close().unwrap();
}
fn builder() -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name("test-bucket")
        .with_region("us-east-1")
        .with_access_key_id("test-only")
        .with_secret_access_key("test-only")
}

#[test]
fn http_runtime_keeps_driving_tasks_between_object_calls() {
    let store = S3Store::open(builder(), "idle-runtime").unwrap();
    let runtime = store.runtime.as_ref().unwrap();
    let (started, start_wait) = futures::channel::oneshot::channel();
    let (release, release_wait) = futures::channel::oneshot::channel();
    let (finished, finish_wait) = std::sync::mpsc::channel();
    runtime.spawn(async move {
        started.send(()).unwrap();
        release_wait.await.unwrap();
        finished.send(()).unwrap();
    });
    runtime.block_on(start_wait).unwrap();
    // No object operation calls block_on after this point. HTTP pool tasks must
    // still process peer closure and idle expiration while the caller is idle.
    release.send(()).unwrap();
    assert!(finish_wait
        .recv_timeout(std::time::Duration::from_secs(1))
        .is_ok());
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
fn incomplete_message() -> HttpError {
    HttpError::new(
        HttpErrorKind::Request,
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "incomplete message"),
    )
}
#[derive(Debug, Clone)]
struct Script(Arc<Mutex<VecDeque<std::result::Result<HttpResponse, HttpError>>>>);
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
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(disconnected()))
    }
}
fn scripted(responses: Vec<HttpResponse>) -> S3Store {
    scripted_calls(responses.into_iter().map(Ok).collect())
}
fn scripted_calls(responses: Vec<std::result::Result<HttpResponse, HttpError>>) -> S3Store {
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
fn read_limits_reject_headers_and_actual_body_without_poisoning() {
    let bytes = encode_envelope(b"bounded");
    let limits = ReadLimits {
        objects: 2,
        object_bytes: bytes.len(),
        namespace_bytes: 1024,
    };
    let store = scripted(vec![response(200, &bytes)])
        .with_read_limits(limits)
        .unwrap();
    assert_eq!(store.get("object").unwrap(), Some(b"bounded".to_vec()));
    for dishonest_header in [false, true] {
        let mut reply = response(200, &bytes);
        if dishonest_header {
            reply
                .headers_mut()
                .insert("content-length", "1".parse().unwrap());
        }
        let store = scripted(vec![reply, response(200, &bytes)])
            .with_read_limits(ReadLimits {
                object_bytes: bytes.len() - 1,
                ..limits
            })
            .unwrap();
        let error = store.get("object").unwrap_err();
        let expected = if dishonest_header { "body" } else { "header" };
        assert!(error.to_string().contains(expected), "{error}");
        assert!(!store.poisoned.load(Ordering::Acquire));
        assert_eq!(store.metrics().snapshot().get, 1);
    }
    assert!(scripted(vec![])
        .with_read_limits(ReadLimits {
            objects: 0,
            ..limits
        })
        .is_err());
}

#[test]
fn read_limits_stop_listing_without_partial_results_or_more_pages() {
    for (limits, expected) in [
        (
            ReadLimits {
                objects: 1,
                object_bytes: 1,
                namespace_bytes: 8,
            },
            "count",
        ),
        (
            ReadLimits {
                objects: 8,
                object_bytes: 1,
                namespace_bytes: 1,
            },
            "namespace",
        ),
    ] {
        let store = scripted(vec![
            response(200, page("test/first", true)),
            response(200, page("test/second", true)),
        ])
        .with_read_limits(limits)
        .unwrap();
        assert!(store.list().unwrap_err().to_string().contains(expected));
        assert_eq!(store.metrics().snapshot().list, 2);
        assert!(!store.poisoned.load(Ordering::Acquire));
    }
    let limits = ReadLimits {
        objects: 1,
        object_bytes: 1,
        namespace_bytes: 1,
    };
    let store = scripted(vec![response(200, page("test/first", false))])
        .with_read_limits(limits)
        .unwrap();
    assert_eq!(store.list().unwrap(), vec!["first"]);
    let store = scripted(vec![response(
        200,
        page("test/first", false).replace("<Size>1", "<Size>2"),
    )])
    .with_read_limits(limits)
    .unwrap();
    assert!(store
        .list()
        .unwrap_err()
        .to_string()
        .contains("listed object"));
}
#[test]
fn conditional_create_errors_poison_without_retry() {
    for status in [403, 409, 412, 500, 501] {
        let store = scripted(vec![response(
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
fn read_transport_failure_retries_fresh_get_and_counts_it() {
    let store = scripted_calls(vec![
        Err(incomplete_message()),
        Ok(response(200, encode_envelope(b"value"))),
    ]);
    assert_eq!(store.get("object").unwrap(), Some(b"value".to_vec()));
    let counts = store.metrics().snapshot();
    assert_eq!(counts.get, 2);
    assert_eq!(counts.read_retries, 1);
    assert_eq!(counts.transport_errors, 1);
}

#[test]
fn read_503_retries_but_404_does_not() {
    let store = scripted(vec![
        response(503, "<Error><Code>SlowDown</Code></Error>"),
        response(200, encode_envelope(b"value")),
    ]);
    assert_eq!(store.get("object").unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.metrics().snapshot().get, 2);
    assert_eq!(store.metrics().snapshot().read_retries, 1);

    let store = scripted(vec![
        response(404, "<Error><Code>NoSuchKey</Code></Error>"),
        response(200, encode_envelope(b"unexpected")),
    ]);
    assert_eq!(store.get("missing").unwrap(), None);
    assert_eq!(store.metrics().snapshot().get, 1);
    assert_eq!(store.metrics().snapshot().read_retries, 0);
}

#[test]
fn failed_partial_body_starts_a_fresh_get() {
    let envelope = encode_envelope(b"value");
    let mut partial = response(200, &envelope);
    let chunks: Vec<std::result::Result<Frame<Bytes>, HttpError>> = vec![
        Ok(Frame::data(Bytes::copy_from_slice(&envelope[..20]))),
        Err(disconnected()),
    ];
    *partial.body_mut() = HttpResponseBody::new(StreamBody::new(futures::stream::iter(chunks)));
    let store = scripted(vec![partial, response(200, &envelope)]);
    assert_eq!(store.get("object").unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.metrics().snapshot().get, 2);
    assert_eq!(store.metrics().snapshot().read_retries, 1);
}

#[test]
fn range_and_paginated_list_retry_complete_reads() {
    let mut range = response(206, b"value");
    range
        .headers_mut()
        .insert("content-range", "bytes 16-20/53".parse().unwrap());
    let store = scripted(vec![response(503, "<Error/>"), range]);
    assert_eq!(
        store.get_range("object", 0, 5, 5).unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(store.metrics().snapshot().get, 2);
    assert_eq!(store.metrics().snapshot().read_retries, 1);

    let store = scripted(vec![
        response(200, page("test/a", true)),
        response(503, "<Error/>"),
        response(200, page("test/a", true)),
        response(200, page("test/b", false)),
    ]);
    assert_eq!(store.list().unwrap(), vec!["a", "b"]);
    assert_eq!(store.metrics().snapshot().list, 4);
    assert_eq!(store.metrics().snapshot().read_retries, 1);
}

#[test]
fn batched_reads_retry_each_request() {
    let store = scripted_calls(vec![
        Err(disconnected()),
        Ok(response(200, encode_envelope(b"value"))),
    ]);
    assert_eq!(
        store.get_many(&["object".into()]).unwrap(),
        vec![Some(b"value".to_vec())]
    );
    assert_eq!(store.metrics().snapshot().read_retries, 1);

    let mut range = response(206, b"value");
    range
        .headers_mut()
        .insert("content-range", "bytes 16-20/53".parse().unwrap());
    let store = scripted_calls(vec![Err(disconnected()), Ok(range)]);
    assert_eq!(
        store.get_ranges(&[("object", 0, 5, 5)]).unwrap(),
        vec![Some(b"value".to_vec())]
    );
    assert_eq!(store.metrics().snapshot().read_retries, 1);
}

#[test]
fn read_retries_stop_after_three_attempts() {
    let store = scripted_calls(vec![
        Err(disconnected()),
        Err(disconnected()),
        Err(disconnected()),
        Ok(response(200, encode_envelope(b"unexpected"))),
    ]);
    assert!(store.get("object").is_err());
    let counts = store.metrics().snapshot();
    assert_eq!(counts.get, 3);
    assert_eq!(counts.read_retries, 2);
    assert_eq!(counts.transport_errors, 3);
}

#[test]
fn transport_errors_on_create_and_remove_do_not_retry_and_poison() {
    let store = scripted_calls(vec![Err(disconnected()), Ok(response(200, ""))]);
    assert!(store.create("object", b"value").is_err());
    assert_eq!(store.metrics().snapshot().put, 1);
    assert_eq!(store.metrics().snapshot().read_retries, 0);
    assert!(matches!(store.get("object"), Err(Error::RecoveryRequired)));

    let store = scripted_calls(vec![Err(disconnected()), Ok(response(204, ""))]);
    assert!(store.remove("object").is_err());
    assert_eq!(store.metrics().snapshot().delete, 1);
    assert_eq!(store.metrics().snapshot().read_retries, 0);
    assert!(matches!(store.list(), Err(Error::RecoveryRequired)));
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
        assert_eq!(
            store.metrics().snapshot().get,
            if status == 500 { 3 } else { 1 }
        );
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
    for (second, attempts) in [
        (response(500, "<Error/>"), 4),
        (response(200, "broken XML"), 2),
        (response(200, page("test-other/b", false)), 2),
    ] {
        let store = scripted(vec![response(200, page("test/a", true)), second]);
        assert!(store.list().is_err());
        assert_eq!(store.metrics().snapshot().list, attempts);
    }
    let store = scripted(vec![response(200, page("test/a", true))]);
    assert!(store.list().is_err());
    assert_eq!(store.metrics().snapshot().transport_errors, 3);
}
#[test]
fn invalid_names_fail_before_requests_and_spawn_blocking_is_supported() {
    for namespace in ["", "/test", "test/", "test//db", "../db", "test%2Fdb"] {
        assert!(matches!(
            S3Store::open(builder(), namespace),
            Err(Error::Invalid(_))
        ));
    }
    let store = scripted(vec![]);
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
fn minio_streaming_exact_search_reads_snapshot_chunks_and_tail() {
    let namespace = "streaming-exact";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    for id in 0..17 {
        db.put_with_metadata(
            id,
            vec![(id % 5) as f32, (id / 5) as f32],
            BTreeMap::from([("group".into(), (id % 2).to_string())]),
        )
        .unwrap();
    }
    db.compact_chunked(240).unwrap();
    db.apply_batch(vec![
        Mutation::Delete { id: 2 },
        Mutation::Put {
            id: 30,
            vector: vec![0., 0.],
            metadata: BTreeMap::from([("group".into(), "0".into())]),
        },
    ])
    .unwrap();
    let expected = db
        .search_filtered(&[0., 0.], 18, &[("group", "0")])
        .unwrap();
    drop(db);

    let chunks = minio(namespace)
        .list()
        .unwrap()
        .iter()
        .filter(|key| key.starts_with("compactedchunk-"))
        .count();
    assert!(chunks > 1);
    let store = minio(namespace);
    let metrics = store.metrics();
    let reader = StreamingDatabase::open(store, config()).unwrap();
    let before = metrics.snapshot();
    assert_eq!(
        reader
            .search_filtered(&[0., 0.], 18, &[("group", "0")])
            .unwrap(),
        expected
    );
    let after = metrics.snapshot();
    assert_eq!(after.get - before.get, chunks as u64);
    assert_eq!(after.put - before.put, 0);
    assert_eq!(
        reader.get_with_metadata(30).unwrap().unwrap().vector,
        vec![0., 0.]
    );
    assert_eq!(metrics.snapshot().get, after.get); // tail lookup stays in memory
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_chunked_snapshots_publish_manifest_last_and_recover() {
    let namespace = "chunked-snapshots";
    let store = minio(namespace);
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    for id in 0..13 {
        db.put(id, vec![(id % 5) as f32, (id / 5) as f32]).unwrap();
    }
    let expected = db.search(&[2., 1.], 13).unwrap();
    let before = metrics.snapshot();
    db.checkpoint_chunked(160).unwrap();
    let after = metrics.snapshot();
    drop(db);
    let keys = minio(namespace).list().unwrap();
    let chunks = keys
        .iter()
        .filter(|key| key.starts_with("segmentchunk-"))
        .count();
    assert!(chunks > 1);
    assert_eq!(after.put - before.put, chunks as u64 + 1);

    let mut db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.search(&[2., 1.], 13).unwrap(), expected);
    db.compact_chunked(160).unwrap();
    drop(db);
    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.search(&[2., 1.], 13).unwrap(), expected);
    let keys = minio(namespace).list().unwrap();
    assert!(keys.iter().any(|key| key.starts_with("compactedchunk-")));
    assert!(!keys
        .iter()
        .any(|key| key.starts_with("segmentchunk-") || key.starts_with("mutation-")));
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_ivf_cache_reuses_one_immutable_object_after_restart() {
    let namespace = "ivf-cache";
    let options = crate::ivf::IvfConfig {
        partitions: 3,
        iterations: 4,
        seed: 42,
    };
    let store = minio(namespace);
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    for id in 0..12 {
        db.put(id, vec![(id % 4) as f32, (id / 4) as f32]).unwrap();
    }
    db.checkpoint().unwrap();
    let before = metrics.snapshot();
    db.load_or_build_ivf(options).unwrap();
    let after = metrics.snapshot();
    assert_eq!(after.get - before.get, 1);
    assert_eq!(after.put - before.put, 1);
    assert_eq!(
        db.search_ivf(&[1., 1.], 12, 3).unwrap().neighbors,
        db.search(&[1., 1.], 12).unwrap()
    );
    drop(db);

    let store = minio(namespace);
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    let before = metrics.snapshot();
    db.load_or_build_ivf(options).unwrap();
    let after = metrics.snapshot();
    assert_eq!(after.get - before.get, 1);
    assert_eq!(after.put - before.put, 0);
    assert_eq!(
        db.search_ivf(&[1., 1.], 12, 3).unwrap().neighbors,
        db.search(&[1., 1.], 12).unwrap()
    );
    db.put(12, vec![1., 1.]).unwrap();
    db.compact().unwrap();
    assert!(!minio(namespace)
        .list()
        .unwrap()
        .iter()
        .any(|k| k.starts_with("ivf-")));
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_metadata_filtering_survives_compaction_and_restart() {
    let namespace = "metadata-filtering";
    let store = minio(namespace);
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    db.put_with_metadata(
        1,
        vec![1., 0.],
        BTreeMap::from([("team".into(), "red".into())]),
    )
    .unwrap();
    db.put_with_metadata(
        2,
        vec![0., 1.],
        BTreeMap::from([("team".into(), "blue".into())]),
    )
    .unwrap();
    db.build_ivf(crate::ivf::IvfConfig {
        partitions: 2,
        iterations: 2,
        seed: 7,
    })
    .unwrap();
    let before = metrics.snapshot();
    let exact = db
        .search_filtered(&[0., 0.], 10, &[("team", "red")])
        .unwrap();
    assert_eq!(exact.iter().map(|n| n.id).collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        db.search_ivf_filtered(&[0., 0.], 10, 2, &[("team", "red")])
            .unwrap()
            .neighbors,
        exact
    );
    let adaptive = db
        .search_ivf_filtered_adaptive(&[0., 0.], 10, 1, &[("team", "red")])
        .unwrap();
    assert_eq!(adaptive.neighbors, exact);
    assert_eq!(adaptive.partitions_probed, 2);
    assert_eq!(metrics.snapshot(), before);
    db.checkpoint().unwrap();
    db.compact().unwrap();
    drop(db);

    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(
        db.get_metadata(1).unwrap().get("team").map(String::as_str),
        Some("red")
    );
    assert_eq!(
        db.get_metadata(2).unwrap().get("team").map(String::as_str),
        Some("blue")
    );
    assert_eq!(
        db.search_filtered(&[0., 0.], 10, &[("team", "red")])
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_batch_uses_one_put_and_recovers_all_operations() {
    let namespace = "batched-mutations";
    let store = minio(namespace);
    let metrics = store.metrics();
    let mut db = Database::open(store, config()).unwrap();
    let before = metrics.snapshot();
    db.apply_batch(vec![
        Mutation::Put {
            id: 1,
            vector: vec![1., 0.],
            metadata: BTreeMap::from([("team".into(), "red".into())]),
        },
        Mutation::Put {
            id: 2,
            vector: vec![0., 1.],
            metadata: BTreeMap::from([("team".into(), "blue".into())]),
        },
        Mutation::Delete { id: 1 },
        Mutation::Put {
            id: 1,
            vector: vec![0., 0.],
            metadata: BTreeMap::from([("team".into(), "green".into())]),
        },
    ])
    .unwrap();
    let after = metrics.snapshot();
    assert_eq!(after.put - before.put, 1);
    assert_eq!(after.get - before.get, 0);
    assert_eq!(after.list - before.list, 0);
    assert_eq!(db.get(1), Some([0., 0.].as_slice()));
    drop(db);
    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), Some([0., 0.].as_slice()));
    assert_eq!(db.get(2), Some([0., 1.].as_slice()));
    assert_eq!(
        db.get_metadata(1).unwrap().get("team").map(String::as_str),
        Some("green")
    );
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
    let adjacent = minio("replay-other");
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
    TimeoutBefore,
    TimeoutAfter,
    DeleteBefore,
    DeleteAfter,
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
        let cut = {
            let mut armed = self.fault.0.lock().unwrap();
            let method = if matches!(*armed, Some(Cut::DeleteBefore | Cut::DeleteAfter)) {
                "DELETE"
            } else {
                "PUT"
            };
            if request.method() == method {
                armed.take()
            } else {
                None
            }
        };
        if matches!(
            cut,
            Some(Cut::Before | Cut::DeleteBefore | Cut::TimeoutBefore)
        ) {
            return Err(if matches!(cut, Some(Cut::TimeoutBefore)) {
                HttpError::new(
                    HttpErrorKind::Request,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "injected timeout"),
                )
            } else {
                disconnected()
            });
        }
        let result = self.inner.execute(request).await?;
        if matches!(cut, Some(Cut::After | Cut::DeleteAfter | Cut::TimeoutAfter)) {
            assert!(result.status().is_success());
            result.into_body().bytes().await?;
            return Err(if matches!(cut, Some(Cut::TimeoutAfter)) {
                HttpError::new(
                    HttpErrorKind::Request,
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "injected timeout"),
                )
            } else {
                disconnected() // server published; client never sees success
            });
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
fn minio_batch_timeout_before_or_after_publication_is_atomic() {
    for (suffix, cut, committed) in [
        ("before", Cut::TimeoutBefore, false),
        ("after", Cut::TimeoutAfter, true),
    ] {
        let namespace = format!("batch-timeout-{suffix}");
        let fault = Fault::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, fault.clone()).unwrap();
        let metrics = store.metrics();
        let mut db = Database::open(store, config()).unwrap();
        db.put(0, vec![0., 0.]).unwrap();
        let before = metrics.snapshot();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(db
            .apply_batch(vec![
                Mutation::Delete { id: 0 },
                Mutation::Put {
                    id: 1,
                    vector: vec![1., 0.],
                    metadata: Default::default(),
                },
            ])
            .is_err());
        assert_eq!(metrics.snapshot().put - before.put, 1);
        assert_eq!(db.get(0), Some([0., 0.].as_slice()));
        assert_eq!(db.get(1), None);
        assert!(matches!(
            db.put(2, vec![2., 0.]),
            Err(Error::RecoveryRequired)
        ));
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(0).is_none(), committed);
        assert_eq!(db.get(1).is_some(), committed);
        assert_eq!(db.search(&[0., 0.], 10).unwrap().len(), 1);
    }
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_immutability_and_corruption() {
    let store = minio("immutable");
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
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_rejects_selected_root_chunk_and_tail_damage() {
    let namespace = "missing-selected-chunk";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    for id in 0..8 {
        db.put(id, vec![id as f32, 0.]).unwrap();
    }
    db.compact_chunked(240).unwrap();
    drop(db);
    let store = minio(namespace);
    let chunk = store
        .list()
        .unwrap()
        .into_iter()
        .find(|key| key.starts_with("compactedchunk-"))
        .unwrap();
    store.remove(&chunk).unwrap();
    assert!(matches!(
        Database::open(minio(namespace), config()),
        Err(Error::Corrupt(_))
    ));

    let namespace = "corrupt-selected-root";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let store = minio(namespace);
    let path = store.path("segment-00000000000000000001").unwrap();
    store
        .run(store.remote.put(&path, b"broken".to_vec().into()))
        .unwrap();
    assert!(matches!(
        Database::open(minio(namespace), config()),
        Err(Error::Corrupt(_))
    ));

    let namespace = "missing-tail-middle";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    for id in 0..3 {
        db.put(id, vec![id as f32, 0.]).unwrap();
    }
    drop(db);
    minio(namespace)
        .remove("mutation-00000000000000000002")
        .unwrap();
    assert!(matches!(
        Database::open(minio(namespace), config()),
        Err(Error::Corrupt(_))
    ));
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
    for mode in [0, 1, 2] {
        let namespace = [
            "server-restart",
            "server-segment-restart",
            "server-compacted-restart",
        ][mode];
        let mut db = Database::open(minio(namespace), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        db.put(2, vec![3., 4.]).unwrap();
        if mode == 1 {
            db.checkpoint().unwrap();
        }
        db.delete(1).unwrap();
        if mode == 2 {
            db.compact().unwrap();
        }
    }
}
#[test]
#[ignore = "runner phase after restarting MinIO"]
fn server_restart_verify() {
    for namespace in [
        "server-restart",
        "server-segment-restart",
        "server-compacted-restart",
    ] {
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

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_delayed_checkpoint_races_retry_at_same_key() {
    // Both schedules are deterministic: recovery first observes the checkpoint
    // absent, then either the original request or the retry wins publication.
    for original_first in [true, false] {
        let namespace = format!("checkpoint-retry-{original_first}");
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        drop(db);
        let deferred = DeferredPut::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, deferred.clone()).unwrap();
        let mut db = Database::open(store, config()).unwrap();
        assert!(db.checkpoint().is_err());
        assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
        drop(db);
        let request = deferred.0.lock().unwrap().take().unwrap();
        assert!(request
            .uri()
            .path()
            .ends_with("/segment-00000000000000000001"));
        assert_eq!(request.headers()["if-none-match"], "*");
        let client = ReqwestConnector::default()
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.checkpoint_sequence, None);
        assert_eq!(db.sequence, 1);
        if original_first {
            assert_eq!(
                rt.block_on(client.execute(request))
                    .unwrap()
                    .status()
                    .as_u16(),
                200
            );
            assert!(matches!(db.checkpoint(), Err(Error::Exists(_))));
            assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
            assert!(matches!(
                db.put(2, vec![3., 4.]),
                Err(Error::RecoveryRequired)
            ));
            assert!(matches!(db.delete(1), Err(Error::RecoveryRequired)));
        } else {
            db.checkpoint().unwrap();
            let bytes = db.store.get("segment-00000000000000000001").unwrap();
            assert_eq!(
                rt.block_on(client.execute(request))
                    .unwrap()
                    .status()
                    .as_u16(),
                412
            );
            assert_eq!(db.store.get("segment-00000000000000000001").unwrap(), bytes);
            db.checkpoint().unwrap(); // the retry's successful handle stays usable
        }
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        drop(db);
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.checkpoint_sequence, Some(1));
        assert_eq!(db.sequence, 1); // neither checkpoint allocated a mutation
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        db.checkpoint().unwrap(); // recovered publication is an idempotent no-op
        db.delete(1).unwrap();
        db.put(2, vec![3., 4.]).unwrap();
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(2), Some([3., 4.].as_slice()));
        assert_eq!(db.sequence, 3);
    }
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_compaction_preserves_results_with_uncertain_publication_and_deletion() {
    for (suffix, cut) in [
        ("put-before", Cut::Before),
        ("put-after", Cut::After),
        ("delete-before", Cut::DeleteBefore),
        ("delete-after", Cut::DeleteAfter),
    ] {
        let namespace = format!("compact-{suffix}");
        let fault = Fault::default();
        let store = S3Store::with_connector(minio_builder(), &namespace, fault.clone()).unwrap();
        let mut db = Database::open(store, config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        db.compact().unwrap();
        db.put(2, vec![3., 4.]).unwrap();
        db.checkpoint().unwrap();
        db.delete(1).unwrap();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(db.compact().is_err());
        assert!(fault.0.lock().unwrap().is_none());
        assert!(matches!(db.compact(), Err(Error::RecoveryRequired)));
        assert!(matches!(
            db.put(3, vec![5., 6.]),
            Err(Error::RecoveryRequired)
        ));
        assert_eq!(db.get(1), None);
        drop(db);
        let mut db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(2), Some([3., 4.].as_slice()));
        db.compact().unwrap();
        drop(db);
        let store = minio(&namespace);
        let keys = store.list().unwrap();
        assert_eq!(keys.len(), 2);
        let mut db = Database::open(store, config()).unwrap();
        db.put(3, vec![5., 6.]).unwrap();
        drop(db);
        let db = Database::open(minio(&namespace), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(3), Some([5., 6.].as_slice()));
        assert_eq!(db.sequence, 4);
    }
    let store = minio("delete-idempotence");
    let metrics = store.metrics();
    store.create("object", b"value").unwrap();
    store.remove("object").unwrap();
    store.remove("object").unwrap();
    assert_eq!(metrics.snapshot().delete, 2);
    assert_eq!(metrics.snapshot().other, 0);
    assert_eq!(store.get("object").unwrap(), None);
}

#[derive(Debug, Default, Clone)]
struct DeferredDelete(Arc<Mutex<Vec<HttpRequest>>>);
#[derive(Debug)]
struct DeferredDeleteService {
    inner: HttpClient,
    deferred: DeferredDelete,
}
impl HttpConnector for DeferredDelete {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(DeferredDeleteService {
            inner: ReqwestConnector::default().connect(options)?,
            deferred: self.clone(),
        }))
    }
}
#[async_trait]
impl HttpService for DeferredDeleteService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        if request.method() == "DELETE" {
            self.deferred.0.lock().unwrap().push(request);
            return Err(disconnected());
        }
        self.inner.execute(request).await
    }
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_delayed_delete_never_targets_a_new_authoritative_object() {
    let namespace = "late-compact-delete";
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.compact().unwrap();
    drop(db);
    let deferred = DeferredDelete::default();
    let store = S3Store::with_connector(minio_builder(), namespace, deferred.clone()).unwrap();
    let mut db = Database::open(store, config()).unwrap();
    db.delete(1).unwrap();
    assert!(db.compact().is_err());
    drop(db);
    let requests = std::mem::take(&mut *deferred.0.lock().unwrap());
    assert_eq!(requests.len(), 2);
    for key in [
        "compacted-00000000000000000001",
        "mutation-00000000000000000002",
    ] {
        assert!(requests
            .iter()
            .any(|request| request.uri().path().ends_with(key)));
    }
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.compact().unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    db.compact().unwrap();
    drop(db);
    let client = ReqwestConnector::default()
        .connect(&ClientOptions::new().with_allow_http(true))
        .unwrap();
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    for request in requests.into_iter().rev() {
        assert!(rt
            .block_on(client.execute(request))
            .unwrap()
            .status()
            .is_success());
    }
    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    assert_eq!(db.sequence, 3);
}
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_delayed_old_compaction_publication_is_ignored_and_reclaimable() {
    let namespace = "late-compact-put";
    let deferred = DeferredPut::default();
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    drop(db);
    let store = S3Store::with_connector(minio_builder(), namespace, deferred.clone()).unwrap();
    let mut db = Database::open(store, config()).unwrap();
    assert!(db.compact().is_err());
    drop(db);
    let request = deferred.0.lock().unwrap().take().unwrap();
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.delete(1).unwrap();
    db.compact().unwrap();
    drop(db);
    let client = ReqwestConnector::default()
        .connect(&ClientOptions::new().with_allow_http(true))
        .unwrap();
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    assert!(rt
        .block_on(client.execute(request))
        .unwrap()
        .status()
        .is_success());
    let mut db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(1), None);
    db.compact().unwrap();
    drop(db);
    assert_eq!(minio(namespace).list().unwrap().len(), 2);
    let mut db = Database::open(minio(namespace), config()).unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    drop(db);
    let db = Database::open(minio(namespace), config()).unwrap();
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
}

#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_retry_decision_survives_uncertain_put_takeover_and_compaction() {
    use crate::retry::{Lookup, Request, RequestId};
    for (suffix, cut, committed) in [
        ("before", Cut::TimeoutBefore, false),
        ("after", Cut::TimeoutAfter, true),
    ] {
        let source = format!("retry-{suffix}");
        let destination = format!("retry-staged-{suffix}");
        let backup = format!("retry-backup-{suffix}");
        let fault = Fault::default();
        let store = S3Store::with_connector(minio_builder(), &source, fault.clone()).unwrap();
        let metrics = store.metrics();
        let mut db = OwnedDatabase::open(store, config()).unwrap();
        let request = Request {
            id: RequestId {
                boundary: 0,
                nonce: [42; 16],
            },
            conditions: vec![db.revision(1)],
            mutations: vec![Mutation::Put {
                id: 1,
                vector: vec![1., 0.],
                metadata: BTreeMap::new(),
            }],
        };
        let before = metrics.snapshot();
        *fault.0.lock().unwrap() = Some(cut);
        assert!(db.apply_request(request.clone()).is_err());
        assert_eq!(metrics.snapshot().put - before.put, 1);
        assert!(matches!(
            db.lookup_request(request.id),
            Err(Error::RecoveryRequired)
        ));
        drop(db);
        stage_isolated_namespace(&minio(&source), minio(&destination), config()).unwrap();
        let mut db = OwnedDatabase::open(minio(&destination), config()).unwrap();
        assert_eq!(
            matches!(db.lookup_request(request.id).unwrap(), Lookup::Retained(_)),
            committed
        );
        assert_eq!(db.get(1).is_some(), committed);
        let outcome = db.apply_request(request.clone()).unwrap();
        assert_eq!(outcome.sequence, 1);
        db.put(1, vec![9., 0.]).unwrap();
        db.compact_chunked(512).unwrap();
        db.close().unwrap();
        stage_isolated_namespace(&minio(&destination), minio(&backup), config()).unwrap();
        let mut db = OwnedDatabase::open(minio(&backup), config()).unwrap();
        assert_eq!(db.apply_request(request).unwrap(), outcome);
        assert_eq!(db.get(1), Some([9., 0.].as_slice()));
        db.close().unwrap();
    }
}

#[derive(Debug, Clone)]
struct DeleteGate {
    started: std::sync::mpsc::Sender<futures::channel::oneshot::Sender<u16>>,
    active: Arc<std::sync::atomic::AtomicUsize>,
}
impl HttpConnector for DeleteGate {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}
#[async_trait]
impl HttpService for DeleteGate {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        assert_eq!(request.method(), "DELETE");
        use std::sync::atomic::Ordering;
        assert!(self.active.fetch_add(1, Ordering::SeqCst) < 4);
        let (send, wait) = futures::channel::oneshot::channel();
        self.started.send(send).unwrap();
        let status = wait.await.unwrap();
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(response(
            status,
            if status == 500 {
                "<Error><Code>InternalError</Code></Error>"
            } else {
                ""
            },
        ))
    }
}
#[test]
fn parallel_cleanup_bounds_in_flight_deletes_and_poisoning() {
    use std::{sync::mpsc, time::Duration};
    for fail in [false, true] {
        let (started, wait) = mpsc::channel();
        let store = S3Store::with_connector(
            builder(),
            "test",
            DeleteGate {
                started,
                active: Arc::default(),
            },
        )
        .unwrap();
        let worker = std::thread::spawn(move || {
            let keys: Vec<_> = (0..9).map(|i| format!("obsolete-{i}")).collect();
            let result = store.remove_many(&keys);
            (store, result)
        });
        let mut first: Vec<_> = (0..4)
            .map(|_| wait.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        // Four uncompleted requests occupy every slot; no fifth request can start.
        assert!(matches!(wait.try_recv(), Err(mpsc::TryRecvError::Empty)));
        first.remove(0).send(if fail { 500 } else { 204 }).unwrap();
        for _ in 0..5 {
            wait.recv_timeout(Duration::from_secs(5))
                .unwrap()
                .send(204)
                .unwrap();
        }
        // The public method must still wait for the original three outstanding calls.
        assert!(!worker.is_finished());
        for sender in first {
            sender.send(204).unwrap();
        }
        let (store, result) = worker.join().unwrap();
        assert_eq!(result.is_err(), fail);
        assert_eq!(store.metrics().snapshot().delete, 9);
        assert_eq!(store.poisoned.load(Ordering::Acquire), fail);
        if fail {
            assert!(matches!(
                store.remove_many(&[]),
                Err(Error::RecoveryRequired)
            ));
        } else {
            store.remove_many(&[]).unwrap();
        }
    }
    let store = scripted(vec![]);
    assert!(store
        .remove_many(&["valid".into(), "invalid/key".into()])
        .is_err());
    assert_eq!(store.metrics().snapshot().delete, 0);
    assert!(!store.poisoned.load(Ordering::Acquire));
}
