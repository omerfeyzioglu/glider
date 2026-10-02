//! Rebuildable IVF-Flat: full-precision vectors, deterministic partition training.
use crate::{
    matches_filter, store::ObjectStore, Config, Database, Error, Metric, Neighbor, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, BinaryHeap};

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
        .map(|(i, center)| (i, metric.routing_score(vector, center)))
        .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
        .unwrap()
        .0
}

/// The `n` centers nearest to `vector` as `(index, routing score)`, ordered by
/// `(score, index)`: the same deterministic tie rule as IVF assignment.
pub fn nearest_centers(
    metric: Metric,
    vector: &[f32],
    centers: &[Vec<f32>],
    n: usize,
) -> Vec<(usize, f64)> {
    let mut ranked: Vec<_> = centers
        .iter()
        .enumerate()
        .map(|(i, center)| (i, metric.routing_score(vector, center)))
        .collect();
    let order = |a: &(usize, f64), b: &(usize, f64)| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0));
    if n < ranked.len() {
        ranked.select_nth_unstable_by(n, order);
        ranked.truncate(n);
    }
    ranked.sort_by(order);
    ranked
}

/// Lloyd iterations: assign every point to its closest center, then move each
/// nonempty center to its members' mean (squared Euclidean, cosine) or
/// coordinate median (Manhattan). Empty centers keep their position.
fn refine(metric: Metric, points: &[&[f32]], centers: &mut [Vec<f32>], iterations: usize) {
    let mut groups = vec![Vec::new(); centers.len()];
    for _ in 0..iterations {
        for group in &mut groups {
            group.clear();
        }
        for (i, point) in points.iter().enumerate() {
            groups[closest(metric, point, centers)].push(i);
        }
        for (center, members) in centers.iter_mut().zip(&groups) {
            // Empty partitions keep their center. No division by zero or lost rows.
            if members.is_empty() {
                continue;
            }
            for (d, component) in center.iter_mut().enumerate() {
                *component = match metric {
                    Metric::SquaredEuclidean | Metric::Cosine => {
                        (members
                            .iter()
                            .map(|&i| f64::from(points[i][d]))
                            .sum::<f64>()
                            / members.len() as f64) as f32
                    }
                    Metric::Manhattan => {
                        let mut values: Vec<_> = members.iter().map(|&i| points[i][d]).collect();
                        let middle = values.len() / 2;
                        *values.select_nth_unstable_by(middle, f32::total_cmp).1
                    }
                };
            }
        }
    }
}

/// Seeded priority of an ID for bounded training samples (SplitMix64 of the
/// seed-mixed ID). Lower priorities are sampled first; ties break by ID.
pub fn sample_priority(seed: u64, id: u64) -> u64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    mix(seed ^ mix(id))
}

/// A bounded deterministic training sample: offered `(ID, vector)` rows are
/// kept iff their `(sample_priority, ID)` is among the `capacity` smallest.
/// Memory is `capacity` vectors regardless of how many rows are streamed, and
/// the result does not depend on offer order. Callers offer each ID once,
/// with vectors of one dimension.
pub struct TrainingSample {
    capacity: usize,
    seed: u64,
    /// Max-heap of `(priority, ID, slot in vectors)`.
    keys: BinaryHeap<(u64, u64, usize)>,
    vectors: Vec<Vec<f32>>,
}

impl TrainingSample {
    pub fn new(capacity: usize, seed: u64) -> Self {
        Self {
            capacity,
            seed,
            keys: BinaryHeap::with_capacity(capacity),
            vectors: Vec::with_capacity(capacity),
        }
    }

    pub fn offer(&mut self, id: u64, vector: &[f32]) {
        let priority = sample_priority(self.seed, id);
        if self.vectors.len() < self.capacity {
            self.keys.push((priority, id, self.vectors.len()));
            self.vectors.push(vector.to_vec());
        } else if let Some(&(largest, largest_id, slot)) = self.keys.peek() {
            if (priority, id) < (largest, largest_id) {
                self.keys.pop();
                self.keys.push((priority, id, slot));
                self.vectors[slot].copy_from_slice(vector);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Sampled `(ID, vector)` rows in `(priority, ID)` order.
    pub fn into_rows(self) -> Vec<(u64, Vec<f32>)> {
        let mut keys = self.keys.into_vec();
        keys.sort_unstable();
        let mut vectors: Vec<_> = self.vectors.into_iter().map(Some).collect();
        keys.into_iter()
            .map(|(_, id, slot)| (id, vectors[slot].take().unwrap()))
            .collect()
    }
}

/// Bounded deterministic centroid training on a sample in `(priority, ID)`
/// order, as `TrainingSample::into_rows` returns it. The first `count` sample
/// rows (a seeded uniform choice) are the initial centers, refined by
/// `iterations` Lloyd iterations over the sample only. Returns
/// `min(count, sample rows)` centers; empty clusters keep their center.
/// Callers assign rows to the returned final centers with `nearest_centers`.
pub fn train_bounded(
    metric: Metric,
    sample: &[(u64, Vec<f32>)],
    count: usize,
    iterations: usize,
) -> Vec<Vec<f32>> {
    let points: Vec<&[f32]> = sample.iter().map(|(_, vector)| vector.as_slice()).collect();
    let mut centers: Vec<Vec<f32>> = points
        .iter()
        .take(count)
        .map(|point| point.to_vec())
        .collect();
    refine(metric, &points, &mut centers, iterations);
    centers
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
                            .routing_score(&point.vector, &centers[centers.len() - 1]),
                    );
                }
                selected = (0..points.len())
                    .filter(|&i| !used[i])
                    .max_by(|&a, &b| nearest[a].total_cmp(&nearest[b]).then(b.cmp(&a)))
                    .unwrap_or(0);
            }
        }
        let vectors: Vec<&[f32]> = points
            .iter()
            .map(|(_, point)| point.vector.as_slice())
            .collect();
        refine(
            self.config.metric,
            &vectors,
            &mut centers,
            options.iterations,
        );
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
        let query = self.config.query(query)?;
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
            .map(|(i, center)| (i, self.config.metric.routing_score(&query, center)))
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
                    distance: self.config.metric.score(&query, &document.vector),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Seeded synthetic rows: `count` vectors of `dimensions` in [0, 100).
    fn rows(seed: u64, count: usize, dimensions: usize) -> Vec<(u64, Vec<f32>)> {
        (0..count as u64)
            .map(|id| {
                let vector = (0..dimensions as u64)
                    .map(|d| (sample_priority(seed, id * 1000 + d) % 10_000) as f32 / 100.)
                    .collect();
                (id, vector)
            })
            .collect()
    }

    fn sample(seed: u64, capacity: usize, offered: &[(u64, Vec<f32>)]) -> Vec<(u64, Vec<f32>)> {
        let mut sample = TrainingSample::new(capacity, seed);
        for (id, vector) in offered {
            sample.offer(*id, vector);
        }
        sample.into_rows()
    }

    #[test]
    fn training_sample_keeps_the_smallest_priorities_in_any_offer_order() {
        for seed in [0, 7, 42] {
            let data = rows(seed, 500, 4);
            let kept = sample(seed, 64, &data);
            let mut expected: Vec<_> = data
                .iter()
                .map(|(id, _)| (sample_priority(seed, *id), *id))
                .collect();
            expected.sort_unstable();
            let expected: Vec<u64> = expected[..64].iter().map(|(_, id)| *id).collect();
            let ids: Vec<u64> = kept.iter().map(|(id, _)| *id).collect();
            assert_eq!(ids, expected, "seed={seed}");
            assert!(
                kept.iter().all(|(id, v)| *v == data[*id as usize].1),
                "seed={seed}"
            );
            let reversed: Vec<_> = data.iter().rev().cloned().collect();
            assert_eq!(sample(seed, 64, &reversed), kept, "seed={seed}");
            assert_eq!(sample(seed, 1000, &data).len(), 500, "seed={seed}");
        }
        assert_ne!(
            sample(1, 64, &rows(1, 500, 4)),
            sample(2, 64, &rows(1, 500, 4))
        );
    }

    #[test]
    fn bounded_training_is_deterministic_and_assigns_to_final_centers() {
        for seed in [3_u64, 42] {
            let data = rows(seed, 2000, 8);
            for metric in [Metric::SquaredEuclidean, Metric::Manhattan, Metric::Cosine] {
                let first = train_bounded(metric, &sample(seed, 512, &data), 16, 2);
                let second = train_bounded(metric, &sample(seed, 512, &data), 16, 2);
                let bits = |centers: &[Vec<f32>]| -> Vec<Vec<u32>> {
                    centers
                        .iter()
                        .map(|c| c.iter().map(|x| x.to_bits()).collect())
                        .collect()
                };
                assert_eq!(bits(&first), bits(&second), "seed={seed} {metric:?}");
                assert_eq!(first.len(), 16, "seed={seed} {metric:?}");
                assert!(
                    first.iter().flatten().all(|x| x.is_finite()),
                    "seed={seed} {metric:?}"
                );
                for (_, vector) in &data[..50] {
                    let nearest = nearest_centers(metric, vector, &first, 2);
                    assert_eq!(nearest[0].0, closest(metric, vector, &first));
                    assert!(nearest[0].1 <= nearest[1].1, "seed={seed} {metric:?}");
                }
            }
            // The sample rule and trainer are pinned: a stored seed must
            // reproduce the same centers in later versions.
            let centers = train_bounded(Metric::SquaredEuclidean, &sample(seed, 512, &data), 16, 2);
            let mut digest = Sha256::new();
            for value in centers.iter().flatten() {
                digest.update(value.to_le_bytes());
            }
            let expected = match seed {
                3 => "3730d6e9d2b8a22a4bf7f8162a91956a5d31683a19a681b7fca23b5f12eba97b",
                _ => "72242320f6f36541f3ccda52d8b6b917d73dc5df4f749734c75a292b08867ae8",
            };
            assert_eq!(format!("{:x}", digest.finalize()), expected, "seed={seed}");
            // Centers are capped at the sample size; zero iterations keep the
            // seeded initial rows.
            let small = sample(seed, 5, &data);
            let centers = train_bounded(Metric::SquaredEuclidean, &small, 16, 0);
            let initial: Vec<_> = small.into_iter().map(|(_, v)| v).collect();
            assert_eq!(centers, initial, "seed={seed}");
        }
    }

    #[test]
    fn nearest_centers_break_score_ties_by_index() {
        let centers = vec![vec![2.0], vec![0.0], vec![2.0], vec![-2.0]];
        let ranked = nearest_centers(Metric::SquaredEuclidean, &[1.0], &centers, 4);
        let order: Vec<usize> = ranked.iter().map(|(i, _)| *i).collect();
        assert_eq!(order, [0, 1, 2, 3]);
        let top: Vec<usize> = nearest_centers(Metric::SquaredEuclidean, &[1.0], &centers, 2)
            .iter()
            .map(|(i, _)| *i)
            .collect();
        assert_eq!(top, [0, 1]);
        assert_eq!(
            nearest_centers(Metric::Manhattan, &[1.0], &centers, 9).len(),
            4
        );
    }
}
