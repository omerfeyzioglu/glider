//! Rebuildable IVF-Flat: full-precision vectors, deterministic partition training.
use crate::{
    matches_filter, store::ObjectStore, Config, Database, Error, Metric, Neighbor, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IvfConfig {
    pub partitions: usize,
    pub iterations: usize,
    pub seed: u64,
}

#[derive(Debug)]
pub struct IvfSearch {
    pub neighbors: Vec<Neighbor>,
    pub centroid_distances: usize,
    pub vector_distances: usize,
    /// Number of partition posting lists scanned; zero for k=0 or an empty index.
    pub partitions_probed: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Index {
    centers: Vec<Vec<f32>>,
    postings: Vec<Vec<u64>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIndex {
    version: u32,
    sequence: u64,
    config: Config,
    options: IvfConfig,
    index: Index,
}

#[derive(Serialize)]
struct PersistedIndexRef<'a> {
    version: u32,
    sequence: u64,
    config: Config,
    options: IvfConfig,
    index: &'a Index,
}

fn cache_key(sequence: u64, config: Config, options: IvfConfig) -> Result<String> {
    let identity =
        serde_json::to_vec(&(1_u32, config, options)).map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(format!("ivf-{sequence:020}-{:x}", Sha256::digest(identity)))
}

pub(crate) fn cache_sequence(key: &str) -> Result<u64> {
    let (number, digest) = key
        .strip_prefix("ivf-")
        .and_then(|suffix| suffix.split_once('-'))
        .ok_or_else(|| Error::Corrupt(format!("invalid object key: {key}")))?;
    let sequence = number
        .parse::<u64>()
        .map_err(|_| Error::Corrupt(format!("invalid object key: {key}")))?;
    if number != format!("{sequence:020}")
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Error::Corrupt(format!("invalid object key: {key}")));
    }
    Ok(sequence)
}

impl Index {
    fn validate<S: ObjectStore>(&self, db: &Database<S>, options: IvfConfig) -> Result<()> {
        let count = options.partitions.min(db.documents.len());
        if self.centers.len() != count || self.postings.len() != count {
            return Err(Error::Corrupt("invalid IVF partition count".into()));
        }
        for center in &self.centers {
            db.config
                .vector(center)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
        }
        let mut seen = BTreeSet::new();
        for posting in &self.postings {
            if posting.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(Error::Corrupt("IVF postings are not ordered".into()));
            }
            for id in posting {
                if !db.documents.contains_key(id) || !seen.insert(*id) {
                    return Err(Error::Corrupt("invalid IVF posting ID".into()));
                }
            }
        }
        if seen.len() != db.documents.len() {
            return Err(Error::Corrupt("IVF postings omit documents".into()));
        }
        Ok(())
    }
}

fn closest(metric: Metric, vector: &[f32], centers: &[Vec<f32>]) -> usize {
    centers
        .iter()
        .enumerate()
        .map(|(i, center)| (i, metric.score(vector, center)))
        .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
        .unwrap()
        .0
}

impl<S: ObjectStore> Database<S> {
    /// Load a previously published index for this exact sequence and configuration,
    /// or train and publish it as one immutable derived object. A successful call
    /// makes the index searchable. A publication error leaves the trained index
    /// readable on this handle but requires reopen before further durable writes.
    pub fn load_or_build_ivf(&mut self, options: IvfConfig) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if options.partitions == 0 || options.iterations == 0 {
            return Err(Error::Invalid(
                "IVF partitions and iterations must be positive".into(),
            ));
        }
        let key = cache_key(self.sequence, self.config, options)?;
        if let Some(bytes) = self.store.get(&key)? {
            let saved: PersistedIndex = crate::decode(&bytes)?;
            if saved.version != 1
                || saved.sequence != self.sequence
                || saved.config != self.config
                || saved.options != options
            {
                return Err(Error::Corrupt("IVF cache identity mismatch".into()));
            }
            saved.index.validate(self, options)?;
            self.ivf = Some(saved.index);
            return Ok(());
        }
        self.build_ivf(options)?;
        let index = self.ivf.as_ref().expect("build_ivf installed an index");
        let bytes = crate::encode(&PersistedIndexRef {
            version: 1,
            sequence: self.sequence,
            config: self.config,
            options,
            index,
        })?;
        self.poisoned = true;
        self.tracked_create(&key, &bytes)?;
        self.poisoned = false;
        Ok(())
    }

    /// Build from the acknowledged in-memory state. No storage I/O or durable change.
    /// Every successful put/delete invalidates this index; rebuild explicitly.
    /// Empty databases build an empty index. Partitions are capped at live rows.
    pub fn build_ivf(&mut self, options: IvfConfig) -> Result<()> {
        if options.partitions == 0 || options.iterations == 0 {
            return Err(Error::Invalid(
                "IVF partitions and iterations must be positive".into(),
            ));
        }
        let points: Vec<_> = self.documents.iter().collect();
        let count = options.partitions.min(points.len());
        let mut centers = Vec::with_capacity(count);
        if count > 0 {
            // Seeded first point followed by deterministic farthest-point initialization.
            // Track the nearest existing center rather than rescanning all prior centers.
            let mut selected = (options.seed % points.len() as u64) as usize;
            let mut nearest = vec![f64::INFINITY; points.len()];
            let mut used = vec![false; points.len()];
            for _ in 0..count {
                centers.push(points[selected].1.vector.clone());
                used[selected] = true;
                for (i, (_, point)) in points.iter().enumerate() {
                    nearest[i] = nearest[i].min(
                        self.config
                            .metric
                            .score(&point.vector, &centers[centers.len() - 1]),
                    );
                }
                selected = (0..points.len())
                    .filter(|&i| !used[i])
                    .max_by(|&a, &b| nearest[a].total_cmp(&nearest[b]).then(b.cmp(&a)))
                    .unwrap_or(0);
            }
        }
        let mut groups = vec![Vec::new(); count];
        for _ in 0..options.iterations {
            for group in &mut groups {
                group.clear();
            }
            for (i, (_, point)) in points.iter().enumerate() {
                groups[closest(self.config.metric, &point.vector, &centers)].push(i);
            }
            for (center, members) in centers.iter_mut().zip(&groups) {
                // Empty partitions keep their center. No division by zero or lost rows.
                if members.is_empty() {
                    continue;
                }
                for (d, component) in center.iter_mut().enumerate() {
                    *component = match self.config.metric {
                        Metric::SquaredEuclidean => {
                            (members
                                .iter()
                                .map(|&i| f64::from(points[i].1.vector[d]))
                                .sum::<f64>()
                                / members.len() as f64) as f32
                        }
                        Metric::Manhattan => {
                            let mut values: Vec<_> =
                                members.iter().map(|&i| points[i].1.vector[d]).collect();
                            let middle = values.len() / 2;
                            *values.select_nth_unstable_by(middle, f32::total_cmp).1
                        }
                    };
                }
            }
        }
        let mut postings = vec![Vec::new(); count];
        // Assignment must use the final updated centers, not the previous iteration.
        for (&id, point) in points {
            postings[closest(self.config.metric, &point.vector, &centers)].push(id);
        }
        self.ivf = Some(Index { centers, postings });
        Ok(())
    }

    /// Search the nearest `probes` partitions. May return fewer than k neighbors.
    /// Probing all partitions matches exact search, including distance/ID ties.
    /// Zero probes and a missing/invalidated index are explicit input errors.
    pub fn search_ivf(&self, query: &[f32], k: usize, probes: usize) -> Result<IvfSearch> {
        self.search_ivf_filtered(query, k, probes, &[])
    }

    /// Search probed partitions among documents matching every metadata pair.
    /// Full probing equals filtered exact search. Partial probing can return fewer
    /// than k matches; vector_distances counts only scored matching documents.
    pub fn search_ivf_filtered(
        &self,
        query: &[f32],
        k: usize,
        probes: usize,
        filter: &[(&str, &str)],
    ) -> Result<IvfSearch> {
        self.search_ivf_filtered_impl(query, k, probes, filter, false)
    }

    /// Probe at least `min_probes` nearest partitions, then continue until k
    /// filtered matches are found or every partition has been scanned. This
    /// avoids short result sets when at least k matches exist, but remains ANN:
    /// stopping early does not guarantee the exact nearest k. Full probing does.
    pub fn search_ivf_filtered_adaptive(
        &self,
        query: &[f32],
        k: usize,
        min_probes: usize,
        filter: &[(&str, &str)],
    ) -> Result<IvfSearch> {
        self.search_ivf_filtered_impl(query, k, min_probes, filter, true)
    }

    fn search_ivf_filtered_impl(
        &self,
        query: &[f32],
        k: usize,
        probes: usize,
        filter: &[(&str, &str)],
        fill_k: bool,
    ) -> Result<IvfSearch> {
        self.config.vector(query)?;
        if probes == 0 {
            return Err(Error::Invalid("IVF probes must be positive".into()));
        }
        let index = self.ivf.as_ref().ok_or_else(|| {
            Error::Invalid("IVF index missing or invalidated; call build_ivf".into())
        })?;
        let mut output = IvfSearch {
            neighbors: Vec::new(),
            centroid_distances: 0,
            vector_distances: 0,
            partitions_probed: 0,
        };
        if k == 0 || index.centers.is_empty() {
            return Ok(output);
        }
        let mut ranked: Vec<_> = index
            .centers
            .iter()
            .enumerate()
            .map(|(i, center)| (i, self.config.metric.score(query, center)))
            .collect();
        output.centroid_distances = ranked.len();
        ranked.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        for (partition, _) in ranked {
            if output.partitions_probed >= probes && (!fill_k || output.neighbors.len() >= k) {
                break;
            }
            output.partitions_probed += 1;
            for id in &index.postings[partition] {
                let document = &self.documents[id];
                if !matches_filter(&document.metadata, filter) {
                    continue;
                }
                output.neighbors.push(Neighbor {
                    id: *id,
                    distance: self.config.metric.score(query, &document.vector),
                });
                output.vector_distances += 1;
            }
        }
        let order =
            |a: &Neighbor, b: &Neighbor| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id));
        if k < output.neighbors.len() {
            output.neighbors.select_nth_unstable_by(k, order);
            output.neighbors.truncate(k);
        }
        output.neighbors.sort_by(order);
        Ok(output)
    }
}
