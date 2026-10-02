//! M37 stage 2: offline clustering probe on an fvecs corpus prefix.
//!
//! Trains centroids on a bounded seeded sample (`glider::ivf::train_bounded`),
//! assigns every row to its nearest centroid (and, as a separate
//! configuration, up to 5% of rows also to their second centroid), lays out
//! cluster-contiguous postings in memory as `docs/CLUSTERED_INDEX.md`
//! specifies, and simulates cold queries within request and byte caps:
//!
//! - `whole_postings`: read whole postings in centroid order and stop at the
//!   first one that does not fit;
//! - `ranked_blocks`: rank the blocks of the nearest `probes` postings and
//!   choose ranges in rank order as the segmented planner does. Blocks are
//!   ranked by their exact nearest-row distance, an optimistic stand-in for
//!   the five-bit sketch scores the engine would use.
//!
//! Both rerank the chosen blocks' rows exactly. No objects are written and no
//! engine format is involved.
//!
//! Usage: `m37_cluster_probe BASE.fvecs QUERY.fvecs ROWS COUNTS SEED
//! [SAMPLE_ROWS] [ITERATIONS]`, where COUNTS is a comma-separated centroid
//! count list. Prints one JSON object.
use glider::{
    ivf::{nearest_centers, train_bounded, TrainingSample},
    Metric,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BinaryHeap, HashMap},
    env, fs,
    io::{BufReader, Read},
    ops::Range,
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DIMENSIONS: usize = 128;
const METRIC: Metric = Metric::SquaredEuclidean;
/// At most the first 200 queries of the query file are measured.
const QUERY_COUNT: usize = 200;
const K: usize = 10;
/// Design defaults: at most 16,384 sampled rows, two Lloyd iterations.
const SAMPLE_ROWS: usize = 16_384;
const ITERATIONS: usize = 2;
/// Boundary duplication: at most this fraction of rows gets one secondary copy.
const DUPLICATE_LIMIT: f64 = 0.05;
/// Segmented block and pack limits (`src/segmented.rs`, `codec.rs`).
const BLOCK_ROWS: usize = 170;
const MAX_RAW_BLOCK_BYTES: usize = 120 * 1024;
const MAX_PACK_BLOCKS: usize = 12;
/// Raw block bytes per pack, as the seal path bounds them (1 MiB less a
/// 64 KiB margin for compression framing).
const MAX_PACK_RAW_BYTES: usize = 1024 * 1024 - 64 * 1024;
const RAW_BLOCK_HEADER: usize = 13;
const ZSTD_LEVEL: i32 = 3;
const REQUEST_BUDGETS: [usize; 6] = [1, 2, 4, 8, 10, 12];
const BYTE_CAPS: [usize; 3] = [256 * 1024, 512 * 1024, 1024 * 1024];
/// Probe counts for `ranked_blocks`: the design's 1/2/4/8 sweep, plus 16.
const BLOCK_PROBES: [usize; 5] = [1, 2, 4, 8, 16];
const CEILING_POSTINGS: usize = 8;
/// M21/M31 metadata: IDs divisible by 100 carry the resident 1% predicate.
const FILTER: (&str, &str) = ("cohort", "one-percent");

fn read_row(reader: &mut impl Read, hash: Option<&mut Sha256>) -> Result<Vec<f32>> {
    let mut bytes = [0_u8; 4 + DIMENSIONS * 4];
    reader.read_exact(&mut bytes)?;
    if u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != DIMENSIONS {
        return Err("invalid fvecs dimension".into());
    }
    if let Some(hash) = hash {
        hash.update(bytes);
    }
    Ok(bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|part| f32::from_le_bytes(*part))
        .collect())
}

/// Exact f64 squared Euclidean distance, the oracle and rerank score.
fn distance(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| {
            let d = f64::from(x) - f64::from(y);
            d * d
        })
        .sum()
}

/// Exact top-k IDs among `ids` by `(distance, ID)`, nearest first.
fn top_k<'a>(
    query: &[f32],
    ids: impl Iterator<Item = u32>,
    row: impl Fn(u32) -> &'a [f32],
) -> Vec<u32> {
    let mut heap = BinaryHeap::with_capacity(K + 1);
    for id in ids {
        let entry = (distance(query, row(id)).to_bits(), id);
        if heap.len() < K {
            heap.push(entry);
        } else if entry < *heap.peek().unwrap() {
            heap.pop();
            heap.push(entry);
        }
    }
    heap.into_sorted_vec()
        .into_iter()
        .map(|(_, id)| id)
        .collect()
}

/// Maps `f` over `0..items` on all cores, preserving order.
fn parallel<T: Send>(items: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let chunk = items.div_ceil(threads).max(1);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..items)
            .step_by(chunk)
            .map(|start| {
                let f = &f;
                scope.spawn(move || {
                    (start..(start + chunk).min(items))
                        .map(f)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    })
}

fn peak_rss() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let value = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        value
    } else {
        value * 1024
    }
}

/// Nearest-rank percentile of sorted values.
fn percentile<T: Copy>(sorted: &[T], p: usize) -> T {
    sorted[(sorted.len() * p).div_ceil(100).saturating_sub(1)]
}

fn distribution(mut values: Vec<usize>) -> Value {
    values.sort_unstable();
    json!({
        "min": values[0],
        "p50": percentile(&values, 50),
        "p95": percentile(&values, 95),
        "max": values[values.len() - 1],
        "mean": values.iter().sum::<usize>() as f64 / values.len() as f64,
    })
}

// ---------------------------------------------------------------------------
// Layout: cluster-contiguous blocks and packs, computed without writing.

/// Raw `GLB2` record length of a put of row `id` with M31 metadata.
fn record_raw_len(id: u32) -> usize {
    let metadata = if id.is_multiple_of(100) {
        8 + FILTER.0.len() + FILTER.1.len()
    } else {
        0
    };
    17 + 4 * DIMENSIONS + 4 + metadata
}

/// The `GLB2` raw layout (`src/segmented/codec.rs`) of one cluster's block:
/// dimensions, metric, partition = cluster ID, record count, then per record
/// ID, sequence (the M31 100-row load request), put kind, vector, metadata.
fn raw_block<'a>(cluster: u32, ids: &[u32], row: impl Fn(u32) -> &'a [f32]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(
        RAW_BLOCK_HEADER + ids.iter().map(|&id| record_raw_len(id)).sum::<usize>(),
    );
    raw.extend_from_slice(&(DIMENSIONS as u32).to_le_bytes());
    raw.push(0);
    raw.extend_from_slice(&cluster.to_le_bytes());
    raw.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for &id in ids {
        raw.extend_from_slice(&u64::from(id).to_le_bytes());
        raw.extend_from_slice(&(u64::from(id) / 100 + 1).to_le_bytes());
        raw.push(0);
        for value in row(id) {
            raw.extend_from_slice(&value.to_le_bytes());
        }
        if id.is_multiple_of(100) {
            raw.extend_from_slice(&1_u32.to_le_bytes());
            for text in [FILTER.0, FILTER.1] {
                raw.extend_from_slice(&(text.len() as u32).to_le_bytes());
                raw.extend_from_slice(text.as_bytes());
            }
        } else {
            raw.extend_from_slice(&0_u32.to_le_bytes());
        }
    }
    raw
}

/// One planned block: a contiguous ID-sorted slice of one cluster's posting.
#[derive(Clone, Debug, PartialEq)]
struct PlannedBlock {
    cluster: usize,
    /// Range within the cluster's posting.
    rows: Range<usize>,
    raw: usize,
}

/// Split each posting (ID-sorted) greedily at `BLOCK_ROWS` rows or the raw
/// block limit. Blocks are emitted in cluster ID order.
fn plan_blocks(postings: &[Vec<u32>], raw_len: impl Fn(u32) -> usize) -> Vec<PlannedBlock> {
    let mut blocks = Vec::new();
    for (cluster, posting) in postings.iter().enumerate() {
        let mut start = 0;
        let mut raw = RAW_BLOCK_HEADER;
        for (index, &id) in posting.iter().enumerate() {
            let length = raw_len(id);
            if index > start && (index - start == BLOCK_ROWS || raw + length > MAX_RAW_BLOCK_BYTES)
            {
                blocks.push(PlannedBlock {
                    cluster,
                    rows: start..index,
                    raw,
                });
                start = index;
                raw = RAW_BLOCK_HEADER;
            }
            raw += length;
        }
        if start < posting.len() {
            blocks.push(PlannedBlock {
                cluster,
                rows: start..posting.len(),
                raw,
            });
        }
    }
    blocks
}

/// Each cluster's range of `blocks`, which are in cluster ID order.
fn cluster_ranges(blocks: &[PlannedBlock], clusters: usize) -> Vec<Range<usize>> {
    let mut ranges = vec![0..0; clusters];
    let mut start = 0;
    while start < blocks.len() {
        let cluster = blocks[start].cluster;
        let end = start
            + blocks[start..]
                .iter()
                .take_while(|block| block.cluster == cluster)
                .count();
        ranges[cluster] = start..end;
        start = end;
    }
    ranges
}

/// A byte range of one pack's block data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Extent {
    pack: usize,
    start: usize,
    end: usize,
}

struct Packed {
    /// Per block (in `blocks` order), its position.
    positions: Vec<Extent>,
    /// Per cluster, its cluster-contiguous extents in pack order.
    extents: Vec<Vec<Extent>>,
    /// Blocks per pack.
    pack_blocks: Vec<usize>,
}

/// Place each cluster's blocks, clusters in `order`, into packs of <=12 blocks
/// within the raw pack bound; `encoded[i]` is block i's encoded length. A
/// cluster that does not fit the open pack's remaining room starts a new
/// pack, so it is split across packs only when it exceeds one pack. Offsets
/// are relative to a pack's block data; the sketch frame before them is not
/// read by queries.
fn pack_blocks(blocks: &[PlannedBlock], encoded: &[usize], order: &[usize]) -> Packed {
    let ranges = cluster_ranges(blocks, order.len());
    let mut packed = Packed {
        positions: vec![
            Extent {
                pack: 0,
                start: 0,
                end: 0
            };
            blocks.len()
        ],
        extents: vec![Vec::new(); order.len()],
        pack_blocks: Vec::new(),
    };
    // The open pack: its index, blocks, raw bytes and encoded bytes so far.
    let (mut pack, mut count, mut raw, mut offset) = (0, 0, 0, 0);
    for &cluster in order {
        let range = ranges[cluster].clone();
        let cluster_raw: usize = blocks[range.clone()].iter().map(|block| block.raw).sum();
        let mut fits =
            count + range.len() <= MAX_PACK_BLOCKS && raw + cluster_raw <= MAX_PACK_RAW_BYTES;
        for block in range {
            if !fits || count == MAX_PACK_BLOCKS || raw + blocks[block].raw > MAX_PACK_RAW_BYTES {
                if count > 0 {
                    packed.pack_blocks.push(count);
                    (pack, count, raw, offset) = (pack + 1, 0, 0, 0);
                }
                fits = true;
            }
            let position = Extent {
                pack,
                start: offset,
                end: offset + encoded[block],
            };
            packed.positions[block] = position;
            let extents = &mut packed.extents[cluster];
            match extents.last_mut() {
                Some(extent) if extent.pack == pack => extent.end = position.end,
                _ => extents.push(position),
            }
            count += 1;
            raw += blocks[block].raw;
            offset = position.end;
        }
    }
    if count > 0 {
        packed.pack_blocks.push(count);
    }
    packed
}

/// Greedy nearest-neighbor chain over the centroids from cluster 0, ties by
/// cluster ID: a placement order that puts nearby clusters in the same pack.
fn chain_order(centers: &[Vec<f32>]) -> Vec<usize> {
    let mut order = Vec::with_capacity(centers.len());
    let mut visited = vec![false; centers.len()];
    let mut current = 0;
    while order.len() < centers.len() {
        order.push(current);
        visited[current] = true;
        let next = (0..centers.len())
            .filter(|&c| !visited[c])
            .map(|c| (distance(&centers[current], &centers[c]), c))
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        match next {
            Some((_, c)) => current = c,
            None => break,
        }
    }
    order
}

// ---------------------------------------------------------------------------
// Query path: map postings or ranked blocks to range requests within caps.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// The next posting needed more range requests than remained.
    Requests,
    /// The next posting needed more bytes than remained.
    Bytes,
    /// Every nonempty posting was read.
    Exhausted,
}

/// Range requests `(pack, start, end)`, at most one per pack.
type Spans = Vec<(usize, usize, usize)>;

/// `spans` with `extents` added, as the segmented planner coalesces: an
/// extent in a pack already being read widens that pack's range (charging any
/// gap); another pack needs a new request. Returns the new spans and their
/// bytes, or which cap they would exceed.
fn add_extents(
    spans: &Spans,
    extents: &[Extent],
    requests: usize,
    cap: usize,
) -> std::result::Result<(Spans, usize), Stop> {
    let mut spans = spans.clone();
    for extent in extents {
        match spans.iter_mut().find(|span| span.0 == extent.pack) {
            Some(span) => {
                span.1 = span.1.min(extent.start);
                span.2 = span.2.max(extent.end);
            }
            None => spans.push((extent.pack, extent.start, extent.end)),
        }
    }
    let bytes = spans.iter().map(|span| span.2 - span.1).sum();
    if spans.len() > requests {
        Err(Stop::Requests)
    } else if bytes > cap {
        Err(Stop::Bytes)
    } else {
        Ok((spans, bytes))
    }
}

#[derive(Debug, PartialEq)]
struct Plan {
    /// Postings or blocks read, in choice order.
    chosen: Vec<usize>,
    spans: Spans,
    bytes: usize,
    /// Why whole-posting probing stopped; `None` for ranked blocks.
    stop: Option<Stop>,
}

/// Read whole postings in centroid order while both caps hold, stopping at
/// the first posting that does not fit. Empty postings cost nothing.
fn plan_postings(ranked: &[usize], extents: &[Vec<Extent>], requests: usize, cap: usize) -> Plan {
    let mut plan = Plan {
        chosen: Vec::new(),
        spans: Vec::new(),
        bytes: 0,
        stop: Some(Stop::Exhausted),
    };
    for &cluster in ranked {
        if extents[cluster].is_empty() {
            continue;
        }
        match add_extents(&plan.spans, &extents[cluster], requests, cap) {
            Ok((spans, bytes)) => {
                plan.chosen.push(cluster);
                (plan.spans, plan.bytes) = (spans, bytes);
            }
            Err(stop) => {
                plan.stop = Some(stop);
                break;
            }
        }
    }
    plan
}

/// Choose blocks in rank order, skipping any that would exceed a cap, as
/// the segmented planner's `choose` does.
fn plan_ranked_blocks(ranked: &[usize], positions: &[Extent], requests: usize, cap: usize) -> Plan {
    let mut plan = Plan {
        chosen: Vec::new(),
        spans: Vec::new(),
        bytes: 0,
        stop: None,
    };
    for &block in ranked {
        if let Ok((spans, bytes)) = add_extents(&plan.spans, &[positions[block]], requests, cap) {
            plan.chosen.push(block);
            (plan.spans, plan.bytes) = (spans, bytes);
        }
    }
    plan
}

/// Largest number of true neighbors any `postings` clusters hold together.
/// `masks` gives, per cluster holding at least one neighbor, the bit set of
/// neighbor ranks it holds (a neighbor can be in two clusters when duplicated).
fn best_postings_ceiling(masks: &[u32], postings: usize) -> u32 {
    // fewest[m]: fewest clusters whose union is exactly m.
    let mut fewest = vec![usize::MAX; 1 << K];
    fewest[0] = 0;
    for &mask in masks {
        // Descending order uses each cluster at most once, as next >= union.
        for union in (0..1_usize << K).rev() {
            if fewest[union] < postings {
                let next = union | mask as usize;
                fewest[next] = fewest[next].min(fewest[union] + 1);
            }
        }
    }
    (0..1_usize << K)
        .filter(|&union| fewest[union] <= postings)
        .map(|union| union.count_ones())
        .max()
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Measurement.

struct Corpus {
    rows: Vec<f32>,
}

impl Corpus {
    fn row(&self, id: u32) -> &[f32] {
        let start = id as usize * DIMENSIONS;
        &self.rows[start..start + DIMENSIONS]
    }
}

/// One assignment's postings and their planned, encoded blocks.
struct Layout<'a> {
    postings: &'a [Vec<u32>],
    blocks: Vec<PlannedBlock>,
    encoded: Vec<usize>,
}

impl Layout<'_> {
    fn block_rows(&self, block: usize) -> &[u32] {
        let block = &self.blocks[block];
        &self.postings[block.cluster][block.rows.clone()]
    }
}

/// One query's outcome under one plan.
struct Outcome {
    plan: Plan,
    recall: f64,
    found: usize,
}

fn outcome<'a>(
    corpus: &Corpus,
    query: &[f32],
    truth: &[u32],
    plan: Plan,
    rows: impl Iterator<Item = &'a u32>,
) -> Outcome {
    let mut ids: Vec<u32> = rows.copied().collect();
    ids.sort_unstable();
    ids.dedup();
    let found = top_k(query, ids.into_iter(), |id| corpus.row(id));
    let hits = found.iter().filter(|id| truth.contains(id)).count();
    Outcome {
        plan,
        recall: hits as f64 / K as f64,
        found: found.len(),
    }
}

/// Aggregate one plan kind and budget over all queries.
fn report(outcomes: &[&Outcome], requests: usize, cap: usize, unit: &str) -> Value {
    let queries = outcomes.len() as f64;
    let mut recall: Vec<f64> = outcomes.iter().map(|o| o.recall).collect();
    recall.sort_by(f64::total_cmp);
    let mut bytes: Vec<usize> = outcomes.iter().map(|o| o.plan.bytes).collect();
    bytes.sort_unstable();
    let mean = |f: &dyn Fn(&Outcome) -> usize| {
        outcomes.iter().map(|&o| f(o)).sum::<usize>() as f64 / queries
    };
    let mut value = json!({
        "requests": requests,
        "byte_cap": cap,
        "mean_recall_at_10": recall.iter().sum::<f64>() / queries,
        "p5_recall_at_10": percentile(&recall, 5),
        "mean_result_count": mean(&|o| o.found),
        "short_result_fraction": mean(&|o| usize::from(o.found < K)),
        "mean_requests": mean(&|o| o.plan.spans.len()),
        "mean_bytes": mean(&|o| o.plan.bytes),
        "p95_bytes": percentile(&bytes, 95),
    });
    value[format!("mean_{unit}_read")] = json!(mean(&|o| o.plan.chosen.len()));
    if outcomes.iter().any(|o| o.plan.stop.is_some()) {
        for (name, stop) in [
            ("stop_requests", Stop::Requests),
            ("stop_bytes", Stop::Bytes),
            ("stop_exhausted", Stop::Exhausted),
        ] {
            value[name] = json!(outcomes
                .iter()
                .filter(|o| o.plan.stop == Some(stop))
                .count());
        }
    }
    value
}

/// Measure every budget for one placement of a layout's blocks.
fn measure_placement(
    corpus: &Corpus,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    rankings: &[Vec<usize>],
    layout: &Layout,
    packed: &Packed,
) -> Value {
    let budgets: Vec<(usize, usize)> = REQUEST_BUDGETS
        .iter()
        .flat_map(|&r| BYTE_CAPS.iter().map(move |&c| (r, c)))
        .collect();
    let ranges = cluster_ranges(&layout.blocks, layout.postings.len());
    let max_probes = BLOCK_PROBES[BLOCK_PROBES.len() - 1];
    // Per query: whole-posting outcomes per budget, then ranked-block
    // outcomes per probe count and budget.
    let measured = parallel(queries.len(), |q| {
        let (query, truth) = (&queries[q], &truth[q]);
        let mut outcomes = Vec::new();
        for &(requests, cap) in &budgets {
            let plan = plan_postings(&rankings[q], &packed.extents, requests, cap);
            let rows = plan.chosen.clone();
            let rows = rows.iter().flat_map(|&c| &layout.postings[c]);
            outcomes.push(outcome(corpus, query, truth, plan, rows));
        }
        // (nearest-row distance, block, rank of its cluster).
        let scored: Vec<(f64, usize, usize)> = rankings[q]
            .iter()
            .take(max_probes)
            .enumerate()
            .flat_map(|(rank, &cluster)| ranges[cluster].clone().map(move |b| (rank, b)))
            .map(|(rank, block)| {
                let nearest = layout
                    .block_rows(block)
                    .iter()
                    .map(|&id| distance(query, corpus.row(id)))
                    .fold(f64::INFINITY, f64::min);
                (nearest, block, rank)
            })
            .collect();
        for probes in BLOCK_PROBES {
            let mut candidates: Vec<_> = scored.iter().filter(|s| s.2 < probes).collect();
            candidates.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let ranked: Vec<usize> = candidates.iter().map(|s| s.1).collect();
            for &(requests, cap) in &budgets {
                let plan = plan_ranked_blocks(&ranked, &packed.positions, requests, cap);
                let chosen = plan.chosen.clone();
                let rows = chosen.iter().flat_map(|&b| layout.block_rows(b));
                outcomes.push(outcome(corpus, query, truth, plan, rows));
            }
        }
        outcomes
    });
    let column = |index: usize| -> Vec<&Outcome> { measured.iter().map(|q| &q[index]).collect() };
    let whole: Vec<Value> = budgets
        .iter()
        .enumerate()
        .map(|(i, &(r, c))| report(&column(i), r, c, "postings"))
        .collect();
    let ranked: Vec<Value> = BLOCK_PROBES
        .iter()
        .enumerate()
        .map(|(p, &probes)| {
            let budgets: Vec<Value> = budgets
                .iter()
                .enumerate()
                .map(|(i, &(r, c))| report(&column((p + 1) * budgets.len() + i), r, c, "blocks"))
                .collect();
            json!({"probes": probes, "budgets": budgets})
        })
        .collect();
    json!({
        "packs": packed.pack_blocks.len(),
        "blocks_per_pack": distribution(packed.pack_blocks.clone()),
        "extents_per_cluster": distribution(packed.extents.iter().map(Vec::len).collect()),
        "whole_postings": whole,
        "ranked_blocks": ranked,
    })
}

/// Mean fraction of the true top-10 that the best `CEILING_POSTINGS`
/// postings hold together, ignoring request and byte caps.
fn ceiling(truth: &[Vec<u32>], postings: &[Vec<u32>]) -> f64 {
    let mut owners: HashMap<u32, Vec<u32>> =
        truth.iter().flatten().map(|&id| (id, Vec::new())).collect();
    for (cluster, posting) in postings.iter().enumerate() {
        for id in posting {
            if let Some(clusters) = owners.get_mut(id) {
                clusters.push(cluster as u32);
            }
        }
    }
    truth
        .iter()
        .map(|ids| {
            let mut masks = BTreeMap::<u32, u32>::new();
            for (rank, id) in ids.iter().enumerate() {
                for &cluster in &owners[id] {
                    *masks.entry(cluster).or_default() |= 1 << rank;
                }
            }
            let masks: Vec<_> = masks.into_values().collect();
            f64::from(best_postings_ceiling(&masks, CEILING_POSTINGS)) / K as f64
        })
        .sum::<f64>()
        / truth.len() as f64
}

/// Lay out one assignment's postings in each placement order and measure.
fn evaluate(
    corpus: &Corpus,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    rankings: &[Vec<usize>],
    postings: &[Vec<u32>],
    orders: &[(&str, Vec<usize>)],
) -> Value {
    let started = Instant::now();
    let blocks = plan_blocks(postings, record_raw_len);
    let encoded = parallel(blocks.len(), |index| {
        let block = &blocks[index];
        let raw = raw_block(
            block.cluster as u32,
            &postings[block.cluster][block.rows.clone()],
            |id| corpus.row(id),
        );
        8 + zstd::bulk::compress(&raw, ZSTD_LEVEL).unwrap().len()
    });
    let encode_ms = started.elapsed().as_secs_f64() * 1000.;
    let layout = Layout {
        postings,
        blocks,
        encoded,
    };
    let mut posting_bytes = vec![0; postings.len()];
    for (block, length) in layout.blocks.iter().zip(&layout.encoded) {
        posting_bytes[block.cluster] += length;
    }
    let physical_rows: usize = postings.iter().map(Vec::len).sum();
    let encoded_total: usize = layout.encoded.iter().sum();
    let placements: BTreeMap<_, _> = orders
        .iter()
        .map(|(name, order)| {
            let packed = pack_blocks(&layout.blocks, &layout.encoded, order);
            let value = measure_placement(corpus, queries, truth, rankings, &layout, &packed);
            (*name, value)
        })
        .collect();
    json!({
        "physical_rows": physical_rows,
        "rows_per_cluster": distribution(postings.iter().map(Vec::len).collect()),
        "empty_clusters": postings.iter().filter(|p| p.is_empty()).count(),
        "blocks": layout.blocks.len(),
        "raw_bytes": layout.blocks.iter().map(|b| b.raw).sum::<usize>(),
        "encoded_bytes": encoded_total,
        "encoded_bytes_per_row": encoded_total as f64 / physical_rows as f64,
        "encode_ms": encode_ms,
        "posting_encoded_bytes": distribution(posting_bytes.clone()),
        "postings_over_byte_caps": BYTE_CAPS.iter().map(|&cap| posting_bytes.iter().filter(|&&b| b > cap).count()).collect::<Vec<_>>(),
        "best_8_postings_ceiling_mean_recall_at_10": ceiling(truth, postings),
        "placements": placements,
    })
}

fn run(args: &[String]) -> Result<Value> {
    let (base_path, query_path, rows, counts, seed, sample_rows, iterations) = match args {
        [base, query, rows, counts, seed, rest @ ..] if rest.len() <= 2 => (
            base,
            query,
            rows.parse::<usize>()?,
            counts
                .split(',')
                .map(str::parse::<usize>)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            seed.parse::<u64>()?,
            rest.first().map_or(Ok(SAMPLE_ROWS), |s| s.parse())?,
            rest.get(1).map_or(Ok(ITERATIONS), |s| s.parse())?,
        ),
        _ => {
            return Err(
                "usage: m37_cluster_probe BASE.fvecs QUERY.fvecs ROWS COUNTS SEED \
                        [SAMPLE_ROWS] [ITERATIONS]"
                    .into(),
            )
        }
    };
    if rows < K
        || rows > u32::MAX as usize
        || counts.contains(&0)
        || sample_rows == 0
        || iterations == 0
    {
        return Err(
            "ROWS, COUNTS, SAMPLE_ROWS and ITERATIONS must be positive (ROWS >= 10)".into(),
        );
    }

    // Pass 1: stream the prefix into the bounded sample, then train. Only the
    // sample and centers are resident here.
    let started = Instant::now();
    let mut reader = BufReader::new(fs::File::open(base_path)?);
    let mut prefix_hash = Sha256::new();
    let mut sample = TrainingSample::new(sample_rows, seed);
    for id in 0..rows {
        sample.offer(id as u64, &read_row(&mut reader, Some(&mut prefix_hash))?);
    }
    let sample = sample.into_rows();
    let mut sample_hash = Sha256::new();
    for (id, vector) in &sample {
        sample_hash.update(id.to_le_bytes());
        for value in vector {
            sample_hash.update(value.to_le_bytes());
        }
    }
    let sample_ms = started.elapsed().as_secs_f64() * 1000.;
    let mut trained = Vec::new();
    for &count in &counts {
        let started = Instant::now();
        let centers = train_bounded(METRIC, &sample, count, iterations);
        trained.push((count, centers, started.elapsed().as_secs_f64() * 1000.));
    }
    let training_peak_rss = peak_rss();
    drop(sample);

    // Pass 2: the full prefix and queries for assignment, layout and oracle.
    let mut reader = BufReader::new(fs::File::open(base_path)?);
    let mut flat = Vec::with_capacity(rows * DIMENSIONS);
    for _ in 0..rows {
        flat.extend(read_row(&mut reader, None)?);
    }
    let corpus = Corpus { rows: flat };
    // The first 200 queries, or every query of a smaller file (siftsmall has 100).
    let query_file = fs::File::open(query_path)?;
    let query_count = QUERY_COUNT.min(query_file.metadata()?.len() as usize / (4 + DIMENSIONS * 4));
    let mut reader = BufReader::new(query_file);
    let mut query_hash = Sha256::new();
    let queries: Vec<_> = (0..query_count)
        .map(|_| read_row(&mut reader, Some(&mut query_hash)))
        .collect::<Result<_>>()?;

    let started = Instant::now();
    let truth = parallel(queries.len(), |q| {
        top_k(&queries[q], 0..rows as u32, |id| corpus.row(id))
    });
    let oracle_ms = started.elapsed().as_secs_f64() * 1000.;

    let mut results = Vec::new();
    for (count, centers, training_ms) in trained {
        let started = Instant::now();
        // Per row: nearest center, and the second nearest with its
        // squared-distance ratio to the nearest (infinite at the center).
        let assigned = parallel(rows, |id| {
            let nearest = nearest_centers(METRIC, corpus.row(id as u32), &centers, 2);
            let ratio = match nearest.get(1) {
                Some(&(_, second)) if nearest[0].1 > 0. => second / nearest[0].1,
                _ => f64::INFINITY,
            };
            (nearest[0].0, nearest.get(1).map(|n| n.0), ratio)
        });
        let assignment_ms = started.elapsed().as_secs_f64() * 1000.;
        let rankings = parallel(queries.len(), |q| {
            nearest_centers(METRIC, &queries[q], &centers, centers.len())
                .into_iter()
                .map(|(cluster, _)| cluster)
                .collect::<Vec<_>>()
        });
        let mut primary = vec![Vec::new(); centers.len()];
        for (id, &(cluster, _, _)) in assigned.iter().enumerate() {
            primary[cluster].push(id as u32);
        }
        let orders = [
            ("cluster_id", (0..centers.len()).collect()),
            ("centroid_chain", chain_order(&centers)),
        ];
        let primary_report = evaluate(&corpus, &queries, &truth, &rankings, &primary, &orders);

        // Boundary duplication: the rows with the smallest second/first
        // distance ratio, ties by ID, up to the 5% budget.
        let mut candidates: Vec<(f64, u32)> = assigned
            .iter()
            .enumerate()
            .filter(|(_, a)| a.1.is_some() && a.2.is_finite())
            .map(|(id, a)| (a.2, id as u32))
            .collect();
        let budget = ((rows as f64 * DUPLICATE_LIMIT) as usize).min(candidates.len());
        if budget < candidates.len() {
            candidates
                .select_nth_unstable_by(budget, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        }
        candidates.truncate(budget);
        let threshold = candidates.iter().map(|c| c.0).reduce(f64::max);
        let mut duplicated = primary;
        for &(_, id) in &candidates {
            duplicated[assigned[id as usize].1.unwrap()].push(id);
        }
        for posting in &mut duplicated {
            posting.sort_unstable();
        }
        let duplicated_report =
            evaluate(&corpus, &queries, &truth, &rankings, &duplicated, &orders);
        results.push(json!({
            "centroids": centers.len(),
            "requested_centroids": count,
            "centroid_bytes": centers.len() * (DIMENSIONS * 4 + 4),
            "training_ms": training_ms,
            "assignment_ms": assignment_ms,
            "primary": primary_report,
            "boundary_duplication": {
                "duplicate_rows": candidates.len(),
                "duplication_fraction": candidates.len() as f64 / rows as f64,
                "max_squared_distance_ratio": threshold,
                "result": duplicated_report,
            },
        }));
        eprintln!("measured {} centroids", centers.len());
    }
    let mut truth_hash = Sha256::new();
    truth_hash.update(serde_json::to_vec(&truth)?);
    Ok(json!({
        "probe": "m37_cluster_probe",
        "base_prefix_sha256": format!("{:x}", prefix_hash.finalize()),
        "base_prefix_bytes": rows * (4 + DIMENSIONS * 4),
        "query_prefix_sha256": format!("{:x}", query_hash.finalize()),
        "rows": rows,
        "queries": query_count,
        "k": K,
        "metric": "squared_euclidean",
        "seed": seed,
        "training": {
            "sample_rule": "keep the sample_rows smallest (ivf::sample_priority(seed, ID), ID); initial centers are the first centroids sample rows in that order",
            "sample_rows": sample_rows,
            "sample_sha256": format!("{:x}", sample_hash.finalize()),
            "sample_pass_ms": sample_ms,
            "iterations": iterations,
            "peak_rss_bytes_after_training": training_peak_rss,
        },
        "layout_rules": {
            "block_rows": BLOCK_ROWS,
            "max_raw_block_bytes": MAX_RAW_BLOCK_BYTES,
            "max_pack_blocks": MAX_PACK_BLOCKS,
            "max_pack_raw_bytes": MAX_PACK_RAW_BYTES,
            "encoding": "GLB2 raw layout, zstd level 3, 8-byte header; M31 metadata and 100-row request sequences",
            "placement": "clusters in cluster_id order or a greedy nearest-centroid chain from cluster 0; a cluster that does not fit the open pack starts a new pack",
        },
        "query_rules": {
            "ranges": "one range per pack; a later extent in the same pack widens it and is charged the gap; rerank only the chosen blocks' rows, exact f64 with ID ties after deduplication",
            "whole_postings": "postings in (centroid score, cluster ID) order; stop at the first that exceeds the request or byte cap",
            "ranked_blocks": "blocks of the nearest `probes` postings in (nearest-row exact distance, block) order; skip a block that exceeds a cap and continue",
        },
        "duplicate_limit_fraction": DUPLICATE_LIMIT,
        "ceiling_postings": CEILING_POSTINGS,
        "exact_top10_sha256": format!("{:x}", truth_hash.finalize()),
        "oracle_ms": oracle_ms,
        "threads": std::thread::available_parallelism()?.get(),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
        "results": results,
        "peak_rss_bytes": peak_rss(),
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    println!("{}", serde_json::to_string(&run(&args)?)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn next(state: &mut u64) -> u64 {
        *state = glider::ivf::sample_priority(*state, 1);
        *state
    }

    fn extent(pack: usize, start: usize, end: usize) -> Extent {
        Extent { pack, start, end }
    }

    #[test]
    fn blocks_split_at_row_and_raw_limits() {
        let postings = vec![(0..400).collect::<Vec<u32>>(), Vec::new(), vec![7]];
        let blocks = plan_blocks(&postings, |_| 100);
        let shape: Vec<_> = blocks.iter().map(|b| (b.cluster, b.rows.clone())).collect();
        assert_eq!(
            shape,
            [(0, 0..170), (0, 170..340), (0, 340..400), (2, 0..1)]
        );
        assert_eq!(blocks[0].raw, RAW_BLOCK_HEADER + 170 * 100);
        // 1,000-byte rows hit the 120 KiB raw limit before 170 rows.
        let blocks = plan_blocks(&postings[..1], |_| 1000);
        assert_eq!(blocks[0].rows, 0..122);
        assert!(blocks.iter().all(|b| b.raw <= MAX_RAW_BLOCK_BYTES));
        assert_eq!(blocks.iter().map(|b| b.rows.len()).sum::<usize>(), 400);
    }

    #[test]
    fn packing_keeps_clusters_contiguous_within_pack_limits() {
        for seed in 0..20_u64 {
            let mut state = seed;
            let clusters = 1 + (next(&mut state) % 60) as usize;
            let mut id = 0;
            let postings: Vec<Vec<u32>> = (0..clusters)
                .map(|_| {
                    let len = (next(&mut state) % 2500) as u32;
                    id += len;
                    (id - len..id).collect()
                })
                .collect();
            let blocks = plan_blocks(&postings, record_raw_len);
            let encoded: Vec<usize> = blocks
                .iter()
                .map(|b| b.raw / 2 + (next(&mut state) % 1000) as usize)
                .collect();
            // A seeded permutation as the placement order.
            let keys: Vec<u64> = (0..clusters).map(|_| next(&mut state)).collect();
            let mut order: Vec<usize> = (0..clusters).collect();
            order.sort_by_key(|&c| (keys[c], c));
            let packed = pack_blocks(&blocks, &encoded, &order);
            let ranges = cluster_ranges(&blocks, clusters);
            let packs = packed.pack_blocks.len();
            let mut per_pack = vec![(0, 0, 0); packs];
            let mut placed: Vec<usize> = (0..blocks.len()).collect();
            placed.sort_by_key(|&b| (packed.positions[b].pack, packed.positions[b].start));
            for block in placed {
                let position = packed.positions[block];
                assert_eq!(position.end - position.start, encoded[block], "seed={seed}");
                let pack = &mut per_pack[position.pack];
                assert_eq!(position.start, pack.2, "seed={seed}: blocks are contiguous");
                *pack = (pack.0 + 1, pack.1 + blocks[block].raw, position.end);
            }
            for (pack, &(count, raw, _)) in per_pack.iter().enumerate() {
                assert_eq!(count, packed.pack_blocks[pack], "seed={seed}");
                assert!(
                    count <= MAX_PACK_BLOCKS && raw <= MAX_PACK_RAW_BYTES,
                    "seed={seed}"
                );
            }
            let mut last_pack = 0;
            for &cluster in &order {
                let range = ranges[cluster].clone();
                let extents = &packed.extents[cluster];
                // Extents cover exactly the cluster's blocks, one per pack.
                let covered: usize = extents.iter().map(|e| e.end - e.start).sum();
                assert_eq!(
                    covered,
                    encoded[range.clone()].iter().sum::<usize>(),
                    "seed={seed}"
                );
                assert!(
                    extents.windows(2).all(|w| w[0].pack < w[1].pack),
                    "seed={seed}"
                );
                let raw: usize = blocks[range.clone()].iter().map(|b| b.raw).sum();
                if range.len() <= MAX_PACK_BLOCKS && raw <= MAX_PACK_RAW_BYTES {
                    assert!(extents.len() <= 1, "seed={seed}: cluster {cluster} split");
                }
                if let Some(first) = extents.first() {
                    assert!(first.pack >= last_pack, "seed={seed}: placement order");
                    last_pack = extents.last().unwrap().pack;
                }
            }
        }
    }

    #[test]
    fn whole_postings_stop_at_the_first_posting_over_a_cap() {
        let extents = vec![
            vec![extent(0, 0, 100)],
            vec![],
            vec![extent(0, 300, 400)],
            vec![extent(1, 0, 50), extent(2, 0, 50)],
            vec![extent(3, 0, 10)],
        ];
        let ranked = [0, 1, 2, 3, 4];
        // Cluster 2 widens pack 0's range to 0..400, charging the gap.
        let plan = plan_postings(&ranked, &extents, 8, 400);
        assert_eq!(plan.chosen, [0, 2]);
        assert_eq!(plan.spans, [(0, 0, 400)]);
        assert_eq!(plan.stop, Some(Stop::Bytes));
        // Cluster 3 spans two packs and needs two requests.
        let plan = plan_postings(&ranked, &extents, 2, 1000);
        assert_eq!((plan.chosen, plan.bytes), (vec![0, 2], 400));
        assert_eq!(plan.stop, Some(Stop::Requests));
        let plan = plan_postings(&ranked, &extents, 4, 1000);
        assert_eq!((plan.chosen.len(), plan.spans.len()), (4, 4));
        assert_eq!((plan.bytes, plan.stop), (510, Some(Stop::Exhausted)));
        let plan = plan_postings(&ranked, &extents, 1, 50);
        assert!(plan.chosen.is_empty() && plan.spans.is_empty());
    }

    #[test]
    fn ranked_blocks_skip_blocks_over_a_cap_and_continue() {
        let positions = [
            extent(0, 0, 100),
            extent(1, 0, 600),
            extent(0, 100, 150),
            extent(2, 0, 10),
            extent(3, 0, 10),
        ];
        let plan = plan_ranked_blocks(&[0, 1, 2, 3, 4], &positions, 2, 500);
        // Block 1 exceeds the byte cap; block 2 extends pack 0; block 4
        // would need a third request.
        assert_eq!(plan.chosen, [0, 2, 3]);
        assert_eq!(plan.spans, [(0, 0, 150), (2, 0, 10)]);
        assert_eq!((plan.bytes, plan.stop), (160, None));
    }

    #[test]
    fn ceiling_takes_the_best_cluster_union() {
        assert_eq!(best_postings_ceiling(&[], 8), 0);
        assert_eq!(best_postings_ceiling(&[0b11, 0b100, 0b1000], 2), 3);
        // With duplicated neighbors, two overlapping clusters cover all five.
        let masks = [0b00111, 0b11001, 0b00110, 0b11000];
        assert_eq!(best_postings_ceiling(&masks, 1), 3);
        assert_eq!(best_postings_ceiling(&masks, 2), 5);
        assert_eq!(best_postings_ceiling(&[0b1111111111; 3], 8), 10);
    }

    #[test]
    fn chain_visits_nearest_unvisited_centroids() {
        let centers: Vec<Vec<f32>> = [0., 10., 1., 11., 5.].iter().map(|&x| vec![x]).collect();
        assert_eq!(chain_order(&centers), [0, 2, 4, 1, 3]);
        assert_eq!(chain_order(&[vec![1.], vec![1.]]), [0, 1]);
    }
}
