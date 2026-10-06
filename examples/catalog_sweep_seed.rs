//! Fixture for tools/bench_catalog_sweep.py. Uses only a local MinIO endpoint.
use glider::{server::StoreConfig, store::ObjectStore};
fn main() {
    let endpoint = std::env::var("GLIDER_S3_ENDPOINT").unwrap();
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "requires disposable local MinIO"
    );
    let base = StoreConfig::S3 {
        bucket: std::env::var("GLIDER_S3_BUCKET").unwrap(),
        namespace: std::env::var("GLIDER_S3_NAMESPACE").unwrap(),
        region: "us-east-1".into(),
        endpoint: Some(std::env::var("GLIDER_S3_ENDPOINT").unwrap()),
    };
    let catalog = base.child("catalog").open().unwrap();
    for i in 0..100 {
        let name = format!("a-active-{i:03}");
        let generation = format!("{i:032x}");
        let body=format!("{{\"version\":1,\"name\":\"{name}\",\"dimensions\":128,\"metric\":\"squared_euclidean\",\"routed_keys\":[],\"generation\":\"{generation}\"}}");
        catalog.create(&name, body.as_bytes()).unwrap();
        base.child(&format!("data/{name}/{generation}"))
            .open()
            .unwrap()
            .create("point", b"active")
            .unwrap();
        let name = format!("z-orphan-{i:03}");
        base.child(&format!("data/{name}/{generation}"))
            .open()
            .unwrap()
            .create("point", b"orphan")
            .unwrap();
    }
}
