use super::{metrics::metadata, Result};
use glider::{Config, Metric};
use serde::Serialize;
use std::{env, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    Local,
    S3,
}
impl Backend {
    pub fn is_local(&self) -> bool {
        *self == Self::Local
    }
}
impl std::str::FromStr for Backend {
    type Err = &'static str;
    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::Local),
            "s3" => Ok(Self::S3),
            _ => Err("backend must be local or s3"),
        }
    }
}

#[derive(Serialize)]
pub struct Options {
    #[serde(skip_serializing_if = "Backend::is_local")]
    pub backend: Backend,
    pub scenario: String,
    pub feature: String,
    pub phase: String,
    pub comparison_group: String,
    pub rows: usize,
    pub dimensions: usize,
    pub mutations: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub checkpoint_at: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub compact_at: usize,
    pub operations: usize,
    pub queries: usize,
    pub samples: usize,
    pub k: usize,
    pub seed: u64,
    pub root: PathBuf,
    pub label: String,
}
fn is_zero(value: &usize) -> bool {
    *value == 0
}

impl Options {
    pub fn parse() -> Result<Option<Self>> {
        Self::parse_from(env::args().skip(1))
    }
    pub fn parse_from(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut o = Self {
            backend: Backend::Local,
            scenario: "all".into(),
            feature: "m1".into(),
            phase: "baseline".into(),
            comparison_group: "local-v2".into(),
            rows: 1000,
            dimensions: 32,
            mutations: 5000,
            checkpoint_at: 0,
            compact_at: 0,
            operations: 200,
            queries: 100,
            samples: 5,
            k: 10,
            seed: 42,
            root: env::temp_dir(),
            label: "unspecified".into(),
        };
        let mut group_explicit = false;
        let mut feature_explicit = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if arg == "--bench" {
                continue;
            } // Cargo supplies this to custom harnesses.
            if arg == "--help" {
                eprintln!(
                    "glider baseline: cargo bench --locked --bench baseline -- [options]\n\
                    --feature NAME (m1) --phase baseline|before|after (baseline)\n\
                    --comparison-group NAME (local-v2)\n\
                    --backend local|s3 (local; s3 requires --features s3)\n\
                    --scenario all|search|commit|recovery (all)\n\
                    --rows N (1000; search size / recovery live IDs)\n\
                    --dimensions D (32) --mutations N (5000; recovery total puts, >= rows)\n\
                    --checkpoint-at N (0; disabled, otherwise checkpoint after N recovery puts)\n\
                    --compact-at N (0; disabled, otherwise compact after N recovery puts)\n\
                    --operations N (200; commits per insert/overwrite/delete phase)\n\
                    --queries N (100) --samples N (5; search batches / warm reopens)\n\
                    --k N (10) --seed N (42) --root EXISTING_DIRECTORY (OS temp directory)\n\
                    --label TEXT (filesystem/device/power/load notes; unspecified)\n\
                    Output: one JSON document on stdout. Run without --help to measure."
                );
                return Ok(None);
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--backend" => o.backend = value.parse()?,
                "--scenario" => o.scenario = value,
                "--feature" => {
                    o.feature = value;
                    feature_explicit = true;
                }
                "--phase" => o.phase = value,
                "--comparison-group" => {
                    o.comparison_group = value;
                    group_explicit = true;
                }
                "--rows" => o.rows = value.parse()?,
                "--dimensions" => o.dimensions = value.parse()?,
                "--mutations" => o.mutations = value.parse()?,
                "--checkpoint-at" => o.checkpoint_at = value.parse()?,
                "--compact-at" => o.compact_at = value.parse()?,
                "--operations" => o.operations = value.parse()?,
                "--queries" => o.queries = value.parse()?,
                "--samples" => o.samples = value.parse()?,
                "--k" => o.k = value.parse()?,
                "--seed" => o.seed = value.parse()?,
                "--root" => o.root = PathBuf::from(value),
                "--label" => o.label = value,
                _ => return Err(format!("unknown option: {arg}").into()),
            }
        }
        if !matches!(
            o.scenario.as_str(),
            "all" | "search" | "commit" | "recovery"
        ) {
            return Err("invalid scenario".into());
        }
        if [
            o.rows,
            o.dimensions,
            o.operations,
            o.queries,
            o.samples,
            o.k,
        ]
        .contains(&0)
        {
            return Err(
                "rows, dimensions, operations, queries, samples and k must be positive".into(),
            );
        }
        if matches!(o.scenario.as_str(), "all" | "recovery") && o.mutations < o.rows {
            return Err("recovery mutations must be >= rows".into());
        }
        if o.checkpoint_at > 0
            && (!matches!(o.scenario.as_str(), "all" | "recovery") || o.checkpoint_at > o.mutations)
        {
            return Err("checkpoint-at requires recovery and must not exceed mutations".into());
        }
        if o.compact_at > 0
            && (!matches!(o.scenario.as_str(), "all" | "recovery") || o.compact_at > o.mutations)
        {
            return Err("compact-at requires recovery and must not exceed mutations".into());
        }
        if o.backend == Backend::S3 && !cfg!(feature = "s3") {
            return Err("S3 benchmarks require cargo bench --features s3".into());
        }
        if o.backend == Backend::S3 && !group_explicit {
            o.comparison_group = "s3-v1".into();
        }
        if o.backend == Backend::S3 && !feature_explicit {
            o.feature = "m2".into();
        }
        metadata(&o.feature, &o.phase, &o.comparison_group)?;
        o.root = o.root.canonicalize()?;
        if !o.root.is_dir() {
            return Err("root must be an existing directory".into());
        }
        Ok(Some(o))
    }
    pub fn config(&self) -> Config {
        Config {
            dimensions: self.dimensions,
            metric: Metric::SquaredEuclidean,
        }
    }
}
