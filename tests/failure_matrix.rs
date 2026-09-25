use glider::{
    ivf::IvfConfig,
    ownership::{claims, clear_stale_claim, OwnedDatabase},
    store::{LocalStore, ObjectStore},
    Config, Metric, Mutation,
};
use std::{cell::RefCell, process::Command, rc::Rc};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Create,
    Remove,
}

struct Cut {
    operation: Operation,
    prefix: &'static str,
    after: bool,
}

struct CrashStore<S> {
    inner: S,
    cut: Rc<RefCell<Option<Cut>>>,
}

impl<S: ObjectStore> CrashStore<S> {
    fn check(&self, operation: Operation, key: &str, after: bool) {
        let hit = self.cut.borrow().as_ref().is_some_and(|cut| {
            cut.operation == operation && cut.after == after && key.starts_with(cut.prefix)
        });
        if hit {
            std::process::exit(73);
        }
    }
}

impl<S: ObjectStore> ObjectStore for CrashStore<S> {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        self.check(Operation::Create, key, false);
        self.inner.create(key, value)?;
        self.check(Operation::Create, key, true);
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.check(Operation::Remove, key, false);
        self.inner.remove(key)?;
        self.check(Operation::Remove, key, true);
        Ok(())
    }
}

fn cut(case: &str) -> Cut {
    let (phase, moment) = case.rsplit_once('-').unwrap();
    let (operation, prefix) = match phase {
        "batch" => (Operation::Create, "mutation-"),
        "chunk" => (Operation::Create, "segmentchunk-"),
        "manifest" => (Operation::Create, "segment-"),
        "index" => (Operation::Create, "ivf-"),
        "cleanup" => (Operation::Remove, "mutation-"),
        _ => panic!("unknown case: {case}"),
    };
    Cut {
        operation,
        prefix,
        after: moment == "after",
    }
}

fn child<S: ObjectStore>(store: S, case: &str) {
    let control = Rc::new(RefCell::new(None));
    let mut db = OwnedDatabase::open(
        CrashStore {
            inner: store,
            cut: control.clone(),
        },
        config(),
    )
    .unwrap();
    for id in 0..4 {
        db.put(id, vec![id as f32, 0.]).unwrap();
    }
    *control.borrow_mut() = Some(cut(case));
    match case.split('-').next().unwrap() {
        "batch" => db
            .apply_batch(vec![
                Mutation::Delete { id: 0 },
                Mutation::Put {
                    id: 10,
                    vector: vec![0., 0.],
                    metadata: Default::default(),
                },
            ])
            .unwrap(),
        "chunk" | "manifest" => db.checkpoint_chunked(240).unwrap(),
        "index" => {
            db.load_or_build_ivf(IvfConfig {
                partitions: 2,
                iterations: 2,
                seed: 7,
            })
            .unwrap();
        }
        "cleanup" => db.compact_chunked(240).unwrap(),
        _ => unreachable!(),
    }
    panic!("crash point was not reached: {case}");
}

const CASES: &[&str] = &[
    "batch-before",
    "batch-after",
    "chunk-before",
    "chunk-after",
    "manifest-before",
    "manifest-after",
    "index-before",
    "index-after",
    "cleanup-before",
    "cleanup-after",
];

fn verify<S: ObjectStore>(mut store: S, reopen: impl Fn() -> S, case: &str) {
    let owner = claims(&store).unwrap();
    assert_eq!(owner.len(), 1, "case={case}");
    clear_stale_claim(&mut store, &owner[0]).unwrap();
    let mut db = OwnedDatabase::open(reopen(), config()).unwrap();
    let expected = if case == "batch-after" {
        vec![10, 1, 2, 3]
    } else {
        vec![0, 1, 2, 3]
    };
    assert_eq!(
        db.search(&[0., 0.], 10)
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        expected,
        "case={case}"
    );
    if case.starts_with("chunk-") || case.starts_with("manifest-") {
        db.checkpoint_chunked(240).unwrap();
    } else if case.starts_with("cleanup-") {
        db.compact_chunked(240).unwrap();
    } else if case.starts_with("index-") {
        db.load_or_build_ivf(IvfConfig {
            partitions: 2,
            iterations: 2,
            seed: 7,
        })
        .unwrap();
        assert_eq!(
            db.search_ivf(&[0., 0.], 10, 2).unwrap().neighbors,
            db.search(&[0., 0.], 10).unwrap()
        );
    }
    db.close().unwrap();
    let db = OwnedDatabase::open(reopen(), config()).unwrap();
    assert_eq!(
        db.search(&[0., 0.], 10)
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        expected,
        "case={case} after maintenance"
    );
    db.close().unwrap();
}

fn run_child(backend: &str, namespace: &str, case: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--ignored"])
        .env("GLIDER_CRASH_MATRIX_BACKEND", backend)
        .env("GLIDER_CRASH_MATRIX_NAMESPACE", namespace)
        .env("GLIDER_CRASH_MATRIX_CASE", case)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "case={case}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "child process only"]
fn crash_child() {
    let Ok(backend) = std::env::var("GLIDER_CRASH_MATRIX_BACKEND") else {
        return;
    };
    let namespace = std::env::var("GLIDER_CRASH_MATRIX_NAMESPACE").unwrap();
    let case = std::env::var("GLIDER_CRASH_MATRIX_CASE").unwrap();
    match backend.as_str() {
        "local" => child(LocalStore::open(namespace).unwrap(), &case),
        #[cfg(feature = "s3")]
        "s3" => child(s3(&namespace), &case),
        _ => panic!("invalid backend: {backend}"),
    }
}

#[test]
fn local_process_crash_matrix() {
    let temp = tempfile::tempdir().unwrap();
    for case in CASES {
        let path = temp.path().join(case);
        let name = path.to_str().unwrap();
        run_child("local", name, case);
        verify(
            LocalStore::open(&path).unwrap(),
            || LocalStore::open(&path).unwrap(),
            case,
        );
    }
}

#[cfg(feature = "s3")]
fn s3(namespace: &str) -> glider::store::s3::S3Store {
    use glider::store::s3::{AmazonS3Builder, S3Store};
    let builder = AmazonS3Builder::new()
        .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").unwrap())
        .with_region(std::env::var("GLIDER_S3_REGION").unwrap())
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").unwrap())
        .with_allow_http(true);
    S3Store::open(builder, namespace).unwrap()
}

#[cfg(feature = "s3")]
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn s3_process_crash_matrix() {
    let parent = std::env::var("GLIDER_S3_NAMESPACE").unwrap();
    for case in CASES {
        let namespace = format!("{parent}/failure-matrix-{case}");
        run_child("s3", &namespace, case);
        verify(s3(&namespace), || s3(&namespace), case);
    }
}
