//! Administration for one segmented collection.
//!
//! Commands that open the collection acquire its writer lease and take over
//! with fencing, exactly like a server start: they wait out a dead writer's
//! lease and fail busy while a live server renews it.
//!
//! `convert [CENTROIDS]` seals the log tail, then builds (or rebuilds, as a
//! new epoch) the M37 clustered view from the canonical runs and publishes
//! it with one root; the server must be stopped. Without CENTROIDS the count
//! targets about 4,000 live rows per cluster.
use glider::{
    segmented::ConvertOptions,
    server::{stage_segmented_namespace, ServerConfig, StoreConfig},
    store::ObjectStore,
    Error, Result,
};
use serde_json::{json, Value};
use std::{env, path::Path};

fn location(value: &str, config: &ServerConfig) -> Result<StoreConfig> {
    if let Some(remote) = value.strip_prefix("s3://") {
        let (bucket, namespace) = remote
            .split_once('/')
            .ok_or_else(|| Error::Invalid("S3 location must be s3://bucket/prefix".into()))?;
        if bucket.is_empty() || namespace.is_empty() {
            return Err(Error::Invalid(
                "S3 bucket and prefix must be nonempty".into(),
            ));
        }
        let (region, endpoint) = match &config.store {
            StoreConfig::S3 {
                region, endpoint, ..
            } => (region.clone(), endpoint.clone()),
            _ => (
                env::var("GLIDER_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
                env::var("GLIDER_S3_ENDPOINT").ok(),
            ),
        };
        Ok(StoreConfig::S3 {
            bucket: bucket.into(),
            namespace: namespace.into(),
            region,
            endpoint,
        })
    } else {
        Ok(StoreConfig::Local(value.into()))
    }
}

fn disjoint(a: &StoreConfig, b: &StoreConfig) -> Result<()> {
    let overlaps = match (a, b) {
        (StoreConfig::Local(a), StoreConfig::Local(b)) => {
            let a = std::path::absolute(a)?;
            let b = std::path::absolute(b)?;
            a.starts_with(&b) || b.starts_with(&a)
        }
        (
            StoreConfig::S3 {
                bucket: a,
                namespace: x,
                ..
            },
            StoreConfig::S3 {
                bucket: b,
                namespace: y,
                ..
            },
        ) => {
            a == b && (x == y || x.starts_with(&format!("{y}/")) || y.starts_with(&format!("{x}/")))
        }
        _ => false,
    };
    if overlaps {
        Err(Error::Invalid(
            "source and destination namespaces overlap".into(),
        ))
    } else {
        Ok(())
    }
}

const USAGE: &str =
    "usage: glider-admin status|backup <destination>|restore <backup>|convert [centroids]";

fn run() -> Result<Value> {
    let mut args = env::args().skip(1);
    let command = args.next().ok_or_else(|| Error::Invalid(USAGE.into()))?;
    let argument = args.next();
    if args.next().is_some() || (command == "status" && argument.is_some()) {
        return Err(Error::Invalid("unexpected arguments".into()));
    }
    let config = ServerConfig::from_env()?;
    match command.as_str() {
        "status" => config.with_engine(|engine| {
            let db = engine.database();
            Ok(json!({
                "command": "status",
                "sequence": db.sequence(),
                "epoch": db.epoch(),
                "revision": { "id": 0, "boundary": db.revision(0).boundary },
                "configuration": {
                    "dimensions": config.collection.dimensions,
                    "metric": config.collection.metric,
                    "resident_filter": config.options.resident_filter,
                },
                "clustered_epoch": db.clustered_epoch(),
                "clusters": db.cluster_count(),
                "runs": db.run_count(),
                "blocks": db.block_count(),
                "tail_objects": db.tail_objects(),
            }))
        }),
        "backup" => {
            let target =
                argument.ok_or_else(|| Error::Invalid("backup needs a destination".into()))?;
            let destination = location(&target, &config)?;
            disjoint(&config.store, &destination)?;
            let destination_store = destination.open()?;
            let sequence = config.with_engine(|engine| {
                engine.backup_to(destination_store)?;
                Ok(engine.database().sequence())
            })?;
            let store = destination.open()?;
            let keys = store.list()?;
            let mut bytes = 0u64;
            for key in &keys {
                bytes += store
                    .get(key)?
                    .ok_or_else(|| Error::Corrupt(format!("backup object missing: {key}")))?
                    .len() as u64;
            }
            Ok(
                json!({"command":"backup", "destination":target, "sequence":sequence,
                "objects_copied":keys.len(), "bytes_copied":bytes}),
            )
        }
        "restore" => {
            let backup = argument.ok_or_else(|| Error::Invalid("restore needs a backup".into()))?;
            let source = location(&backup, &config)?;
            disjoint(&source, &config.store)?;
            if let StoreConfig::Local(path) = &source {
                if !Path::new(path).is_dir() {
                    return Err(Error::Invalid("backup directory does not exist".into()));
                }
            }
            // Staging validates the copy by opening it; the first server
            // start then takes it over.
            let (sequence, objects, bytes) = stage_segmented_namespace(
                &source.open()?,
                config.open_store()?,
                config.collection,
                config.options.clone(),
            )?;
            Ok(
                json!({"command":"restore", "backup":backup, "sequence":sequence,
                "objects_copied":objects, "bytes_copied":bytes}),
            )
        }
        "convert" => {
            let centroids = argument
                .map(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|_| Error::Invalid("centroid count must be an integer".into()))
                })
                .transpose()?;
            let summary = config.with_database(|db| {
                // Seal the log tail first so the view covers every acknowledged row.
                db.seal_delta()?;
                db.convert_clustered(ConvertOptions {
                    centroids,
                    ..ConvertOptions::default()
                })
            })?;
            Ok(json!({"command":"convert", "summary": summary}))
        }
        _ => Err(Error::Invalid(USAGE.into())),
    }
}

fn main() {
    match run() {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("glider-admin: {error}");
            println!("{}", json!({"error":error.to_string()}));
            std::process::exit(1);
        }
    }
}
