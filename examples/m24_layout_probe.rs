//! Exact-oracle upper bound for the current ID-sorted segmented block layout.
use glider::{
    retry::{Request, RequestId},
    segmented::SegmentedDatabase,
    store::LocalStore,
    Config, Metric, Mutation, Neighbor,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn row(reader: &mut impl Read) -> Result<Vec<f32>> {
    let mut bytes = [0_u8; 516];
    reader.read_exact(&mut bytes)?;
    if u32::from_le_bytes(bytes[..4].try_into().unwrap()) != 128 {
        return Err("invalid SIFT1M fvecs dimensions".into());
    }
    Ok(bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect())
}

fn exact(base: &[Vec<f32>], query: &[f32], filtered: bool) -> Vec<Neighbor> {
    let mut distances: Vec<_> = base
        .iter()
        .enumerate()
        .filter(|(id, _)| !filtered || id.is_multiple_of(100))
        .map(|(id, vector)| Neighbor {
            id: id as u64,
            distance: vector
                .iter()
                .zip(query)
                .map(|(&v, &q)| {
                    let diff = f64::from(v) - f64::from(q);
                    diff * diff
                })
                .sum(),
        })
        .collect();
    distances.select_nth_unstable_by(9, |a, b| {
        a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id))
    });
    distances.truncate(10);
    distances.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
    distances
}

fn upper_bound<S: glider::store::ObjectStore>(
    db: &SegmentedDatabase<S>,
    neighbors: &[Neighbor],
) -> Result<(usize, usize, usize)> {
    let mut counts = BTreeMap::new();
    for neighbor in neighbors {
        let block = db
            .current_block_of(neighbor.id)
            .ok_or("exact neighbor missing from selected root")?;
        *counts.entry(block).or_insert(0_usize) += 1;
    }
    let distinct = counts.len();
    let mut per_block: Vec<_> = counts
        .into_iter()
        .map(|((run, block), count)| -> Result<_> {
            Ok((
                count,
                db.block_payload_len(run, block)
                    .ok_or("selected block reference missing")?,
            ))
        })
        .collect::<Result<_>>()?;
    per_block.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    Ok((
        per_block.iter().take(8).map(|item| item.0).sum(),
        distinct,
        per_block.iter().take(8).map(|item| item.1).sum(),
    ))
}

fn summarize(values: &[usize]) -> serde_json::Value {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let sum: usize = values.iter().sum();
    json!({
        "mean_recall_at_10":sum as f64/(values.len()*10) as f64,
        "fifth_percentile_recall_at_10":sorted[(values.len()*5/100)-1] as f64/10.,
        "queries_below_0_9":values.iter().filter(|&&n|n<9).count(),
        "queries_below_0_8":values.iter().filter(|&&n|n<8).count(),
    })
}

fn quantized_five_bit_route<S: glider::store::ObjectStore>(
    db: &SegmentedDatabase<S>,
    base: &[Vec<f32>],
    queries: &[Vec<f32>],
    exact_ids: &[Vec<u64>],
) -> Result<serde_json::Value> {
    let mut minima = [f32::INFINITY; 128];
    let mut maxima = [f32::NEG_INFINITY; 128];
    for vector in base {
        for (axis, &value) in vector.iter().enumerate() {
            minima[axis] = minima[axis].min(value);
            maxima[axis] = maxima[axis].max(value);
        }
    }
    let scales = std::array::from_fn::<_, 128, _>(|axis| {
        let span = (maxima[axis] - minima[axis]) / 31.;
        if span == 0. {
            1.
        } else {
            span
        }
    });
    let mut codes = Vec::with_capacity(base.len());
    let mut locations = Vec::with_capacity(base.len());
    let mut blocks = BTreeMap::new();
    for (id, vector) in base.iter().enumerate() {
        let code: [u8; 128] = std::array::from_fn(|axis| {
            ((vector[axis] - minima[axis]) / scales[axis])
                .round_ties_even()
                .clamp(0., 31.) as u8
        });
        codes.push(code);
        let location = db
            .current_block_of(id as u64)
            .ok_or("quantized route ID missing from selected root")?;
        let next = blocks.len();
        locations.push(*blocks.entry(location).or_insert(next));
    }
    let start = Instant::now();
    let mut hits = Vec::with_capacity(queries.len());
    for (query, neighbors) in queries.iter().zip(exact_ids) {
        let lookup = std::array::from_fn::<_, 128, _>(|axis| {
            std::array::from_fn::<_, 32, _>(|code| {
                let value = minima[axis] + scales[axis] * code as f32;
                let diff = f64::from(value) - f64::from(query[axis]);
                diff * diff
            })
        });
        let mut best = vec![f64::INFINITY; blocks.len()];
        for (code, &block) in codes.iter().zip(&locations) {
            let distance = code
                .iter()
                .enumerate()
                .map(|(axis, &value)| lookup[axis][value as usize])
                .sum::<f64>();
            best[block] = best[block].min(distance);
        }
        let mut ranked: Vec<_> = best.into_iter().enumerate().collect();
        ranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let mut selected = vec![false; blocks.len()];
        for &(block, _) in ranked.iter().take(8) {
            selected[block] = true;
        }
        hits.push(
            neighbors
                .iter()
                .filter(|&&id| selected[locations[id as usize]])
                .count(),
        );
    }
    Ok(json!({
        "quality":summarize(&hits),
        "packed_code_bytes":base.len()*128*5/8,
        "route_200_queries_ms":start.elapsed().as_secs_f64()*1000.,
        "note":"offline unpacked codes resident; excludes object reads, reranking, RSS and writes"
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        return Err("usage: m24_layout_probe BASE.fvecs QUERY.fvecs WORK_DIRECTORY".into());
    }
    let mut base_file = BufReader::new(File::open(&args[1])?);
    let mut query_file = BufReader::new(File::open(&args[2])?);
    let queries: Vec<_> = (0..200)
        .map(|_| row(&mut query_file))
        .collect::<Result<_>>()?;
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let mut db = SegmentedDatabase::open(
        LocalStore::open(Path::new(&args[3]).join("namespace"))?,
        config,
    )?;
    let mut base = Vec::with_capacity(250_000);
    let load_start = Instant::now();
    let mut seal_steps = 0;
    let mut consolidation_steps = 0;
    for batch in 0..2_500_u64 {
        let mut mutations = Vec::with_capacity(100);
        for offset in 0..100_u64 {
            let id = batch * 100 + offset;
            let vector = row(&mut base_file)?;
            let metadata = if id.is_multiple_of(100) {
                BTreeMap::from([("cohort".into(), "one-percent".into())])
            } else {
                BTreeMap::new()
            };
            mutations.push(Mutation::Put {
                id,
                vector: vector.clone(),
                metadata,
            });
            base.push(vector);
        }
        db.apply_request(Request {
            id: RequestId {
                boundary: batch,
                nonce: u128::from(batch + 1).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        })?;
        if (batch + 1).is_multiple_of(64) || batch == 2_499 {
            db.start_seal()?;
            while db.seal_step()? {
                seal_steps += 1;
            }
            while db.cleanup_step(32)? != 0 {}
            while db.consolidate_runs_step()? {
                consolidation_steps += 1;
                while db.cleanup_step(32)? != 0 {}
            }
        }
    }
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.;
    if db.block_count() == 0 || db.current_block_of(0).is_none() {
        return Err("segmented layout was not fully published".into());
    }
    let oracle_start = Instant::now();
    let mut unfiltered = Vec::with_capacity(200);
    let mut filtered = Vec::with_capacity(200);
    let mut unfiltered_cover = Vec::with_capacity(200);
    let mut filtered_cover = Vec::with_capacity(200);
    let mut unfiltered_distinct = Vec::with_capacity(200);
    let mut filtered_distinct = Vec::with_capacity(200);
    let mut unfiltered_bytes = Vec::with_capacity(200);
    let mut filtered_bytes = Vec::with_capacity(200);
    for query in &queries {
        let exact_unfiltered = exact(&base, query, false);
        let exact_filtered = exact(&base, query, true);
        let (coverage, distinct, bytes) = upper_bound(&db, &exact_unfiltered)?;
        unfiltered_cover.push(coverage);
        unfiltered_distinct.push(distinct);
        unfiltered_bytes.push(bytes);
        let (coverage, distinct, bytes) = upper_bound(&db, &exact_filtered)?;
        filtered_cover.push(coverage);
        filtered_distinct.push(distinct);
        filtered_bytes.push(bytes);
        unfiltered.push(exact_unfiltered.iter().map(|n| n.id).collect::<Vec<_>>());
        filtered.push(exact_filtered.iter().map(|n| n.id).collect::<Vec<_>>());
    }
    let oracle_ms = oracle_start.elapsed().as_secs_f64() * 1000.;
    if db.search_exact(&queries[0], 10, &[])? != exact(&base, &queries[0], false)
        || db.search_exact(&queries[0], 10, &[("cohort", "one-percent")])?
            != exact(&base, &queries[0], true)
    {
        return Err("segmented exact search disagrees with independent oracle".into());
    }
    let quantized_route = quantized_five_bit_route(&db, &base, &queries, &unfiltered)?;
    println!(
        "{}",
        json!({
            "version":1,"rows":250000,"dimensions":128,"queries":200,"k":10,
            "metric":"squared_euclidean","filter":"id%100==0",
            "max_data_block_get":8,"load_ms":load_ms,"oracle_ms":oracle_ms,
            "seal_steps":seal_steps,"consolidation_steps":consolidation_steps,
            "runs":db.run_count(),"blocks":db.block_count(),
            "unfiltered_upper_bound":summarize(&unfiltered_cover),
            "filtered_upper_bound":summarize(&filtered_cover),
            "unfiltered_distinct_blocks":unfiltered_distinct,
            "filtered_distinct_blocks":filtered_distinct,
            "unfiltered_oracle_block_bytes":unfiltered_bytes,
            "filtered_oracle_block_bytes":filtered_bytes,
            "unfiltered_quantized_5_bit_route":quantized_route,
            "unfiltered_exact_ids":unfiltered,"filtered_exact_ids":filtered,
        })
    );
    Ok(())
}
