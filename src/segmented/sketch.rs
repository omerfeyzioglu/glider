//! Disposable five-bit scalar routing sketch for the experimental segmented reader.
use super::{consider, Ranked, SegmentedDatabase};
use crate::{store::ObjectStore, Error, Metric, Mutation, Neighbor, Result};
use std::collections::BinaryHeap;

const BITS: usize = 5;
const LEVELS: f64 = 31.;

pub(super) struct PackedSketch {
    generation: u64,
    ids: Vec<u64>,
    codes: Vec<u8>,
    block_of_row: Vec<u32>,
    live_per_block: Vec<usize>,
    references: Vec<(usize, usize)>,
    minima: Vec<f64>,
    scales: Vec<f64>,
    code_bytes: usize,
}

fn set_code(bytes: &mut [u8], axis: usize, value: u8) {
    let bit = axis * BITS;
    let byte = bit / 8;
    let shift = bit % 8;
    bytes[byte] |= value << shift;
    if shift > 3 {
        bytes[byte + 1] |= value >> (8 - shift);
    }
}

fn get_code(bytes: &[u8], axis: usize) -> usize {
    let bit = axis * BITS;
    let byte = bit / 8;
    let shift = bit % 8;
    let mut word = u16::from(bytes[byte]);
    if shift > 3 {
        word |= u16::from(bytes[byte + 1]) << 8;
    }
    usize::from((word >> shift) & 31)
}

impl PackedSketch {
    fn charged_bytes(&self) -> usize {
        size_of::<Self>()
            + self.ids.capacity() * size_of::<u64>()
            + self.codes.capacity()
            + self.block_of_row.capacity() * size_of::<u32>()
            + self.live_per_block.capacity() * size_of::<usize>()
            + self.references.capacity() * size_of::<(usize, usize)>()
            + (self.minima.capacity() + self.scales.capacity()) * size_of::<f64>()
    }
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    fn visit_current_root_puts(
        &self,
        mut visit: impl FnMut(u64, &[f32], usize) -> Result<()>,
    ) -> Result<()> {
        let mut flat = 0;
        for (run, reference) in self.root.runs.iter().enumerate() {
            for (block, block_ref) in reference.blocks.iter().enumerate() {
                let decoded = self.read_data_block(block_ref)?;
                for record in decoded.records {
                    let id = record.id();
                    if self.tail.contains_key(&id) {
                        continue;
                    }
                    let Some(location) = self.latest.get(&id) else {
                        continue;
                    };
                    if location.run != run
                        || location.entry.block as usize != block
                        || location.entry.sequence != record.sequence
                    {
                        continue;
                    }
                    match record.mutation {
                        Mutation::Put { vector, .. } if !location.entry.deleted => {
                            visit(id, &vector, flat)?;
                        }
                        Mutation::Delete { .. } if location.entry.deleted => {}
                        _ => {
                            return Err(Error::Corrupt(
                                "segmented sketch directory mutation mismatch".into(),
                            ));
                        }
                    }
                }
                flat += 1;
            }
        }
        Ok(())
    }

    /// Build a disposable routing sketch from authenticated selected-root blocks.
    /// This prototype makes two full block passes and performs no durable write.
    pub fn build_selective_index(&mut self, max_bytes: usize) -> Result<()> {
        let ids: Vec<_> = self
            .latest
            .iter()
            .filter(|(id, location)| !location.entry.deleted && !self.tail.contains_key(id))
            .map(|(&id, _)| id)
            .collect();
        let code_bytes = self
            .config
            .dimensions
            .checked_mul(BITS)
            .and_then(|bits| bits.checked_add(7))
            .map(|bits| bits / 8)
            .ok_or_else(|| Error::Invalid("selective code width overflow".into()))?;
        let mut references = Vec::new();
        for (run, entry) in self.root.runs.iter().enumerate() {
            for block in 0..entry.blocks.len() {
                references.push((run, block));
            }
        }
        let required = code_bytes
            .checked_add(size_of::<u64>() + size_of::<u32>())
            .and_then(|row_bytes| ids.len().checked_mul(row_bytes))
            .and_then(|bytes| {
                references
                    .len()
                    .checked_mul(size_of::<(usize, usize)>() + size_of::<usize>())
                    .and_then(|refs| bytes.checked_add(refs))
            })
            .and_then(|bytes| {
                self.config
                    .dimensions
                    .checked_mul(2 * size_of::<f64>())
                    .and_then(|book| bytes.checked_add(book))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<PackedSketch>()))
            .ok_or_else(|| Error::Invalid("selective index size overflow".into()))?;
        if required > max_bytes || references.len() > u32::MAX as usize {
            return Err(Error::Invalid("selective index exceeds byte budget".into()));
        }
        let mut minima = vec![f64::INFINITY; self.config.dimensions];
        let mut maxima = vec![f64::NEG_INFINITY; self.config.dimensions];
        if !ids.is_empty() {
            self.visit_current_root_puts(|_, vector, _| {
                for (axis, &value) in vector.iter().enumerate() {
                    minima[axis] = minima[axis].min(f64::from(value));
                    maxima[axis] = maxima[axis].max(f64::from(value));
                }
                Ok(())
            })?;
        }
        let scales: Vec<_> = minima
            .iter_mut()
            .zip(maxima)
            .map(|(minimum, maximum)| {
                if !minimum.is_finite() {
                    *minimum = 0.;
                    return 1.;
                }
                let span = (maximum - *minimum) / LEVELS;
                if span == 0. {
                    1.
                } else {
                    span
                }
            })
            .collect();
        let mut codes = vec![0; ids.len() * code_bytes];
        let mut block_of_row = vec![u32::MAX; ids.len()];
        if !ids.is_empty() {
            self.visit_current_root_puts(|id, vector, flat| {
                let row = ids
                    .binary_search(&id)
                    .map_err(|_| Error::Corrupt("selective sketch row not in directory".into()))?;
                if block_of_row[row] != u32::MAX {
                    return Err(Error::Corrupt("duplicate selective sketch row".into()));
                }
                block_of_row[row] = flat as u32;
                let start = row * code_bytes;
                let bytes = &mut codes[start..start + code_bytes];
                for (axis, &value) in vector.iter().enumerate() {
                    let code = ((f64::from(value) - minima[axis]) / scales[axis])
                        .round_ties_even()
                        .clamp(0., LEVELS) as u8;
                    set_code(bytes, axis, code);
                }
                Ok(())
            })?;
            if block_of_row.contains(&u32::MAX) {
                return Err(Error::Corrupt(
                    "selected-root vector missing from sketch".into(),
                ));
            }
        }
        let mut live_per_block = vec![0; references.len()];
        for &block in &block_of_row {
            live_per_block[block as usize] += 1;
        }
        let built = PackedSketch {
            generation: self.root.generation,
            ids,
            codes,
            block_of_row,
            live_per_block,
            references,
            minima,
            scales,
            code_bytes,
        };
        if built.charged_bytes() > max_bytes {
            return Err(Error::Invalid("selective index exceeds byte budget".into()));
        }
        self.selective = Some(built);
        Ok(())
    }

    pub fn selective_index_bytes(&self) -> Option<usize> {
        self.selective.as_ref().map(PackedSketch::charged_bytes)
    }

    /// Approximate unfiltered search: score all packed codes, fetch only the
    /// nearest selected blocks, then rerank their current records exactly.
    /// A root change invalidates this experimental in-memory index explicitly.
    pub fn search_selective_unfiltered(
        &self,
        query: &[f32],
        k: usize,
        max_blocks: usize,
    ) -> Result<Vec<Neighbor>> {
        self.config.vector(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        if max_blocks == 0 {
            return Err(Error::Invalid(
                "selective search needs a block budget".into(),
            ));
        }
        let sketch = self
            .selective
            .as_ref()
            .filter(|index| index.generation == self.root.generation)
            .ok_or_else(|| Error::Invalid("selective index requires rebuild".into()))?;
        let mut lookup = vec![[0_f64; 32]; self.config.dimensions];
        for (axis, row) in lookup.iter_mut().enumerate() {
            for (code, distance) in row.iter_mut().enumerate() {
                let value = sketch.minima[axis] + sketch.scales[axis] * code as f64;
                let difference = value - f64::from(query[axis]);
                *distance = match self.config.metric {
                    Metric::SquaredEuclidean => difference * difference,
                    Metric::Manhattan => difference.abs(),
                };
            }
        }
        let mut best = vec![f64::INFINITY; sketch.references.len()];
        for (row, &id) in sketch.ids.iter().enumerate() {
            if self.tail.contains_key(&id) {
                continue;
            }
            let start = row * sketch.code_bytes;
            let codes = &sketch.codes[start..start + sketch.code_bytes];
            let distance = lookup
                .iter()
                .enumerate()
                .map(|(axis, values)| values[get_code(codes, axis)])
                .sum::<f64>();
            let block = sketch.block_of_row[row] as usize;
            best[block] = best[block].min(distance);
        }
        let mut ranked: Vec<_> = best
            .into_iter()
            .enumerate()
            .filter(|(_, distance)| distance.is_finite())
            .collect();
        ranked.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let mut shadowed = vec![0; sketch.references.len()];
        for id in self.tail.keys() {
            if let Ok(row) = sketch.ids.binary_search(id) {
                shadowed[sketch.block_of_row[row] as usize] += 1;
            }
        }
        let mut heap = BinaryHeap::new();
        for (flat, _) in ranked.into_iter().take(max_blocks) {
            let (run, block) = sketch.references[flat];
            let decoded = self.read_data_block(&self.root.runs[run].blocks[block])?;
            let mut seen_live = 0;
            for record in decoded.records {
                let id = record.id();
                if self.tail.contains_key(&id) {
                    continue;
                }
                let Some(location) = self.latest.get(&id) else {
                    continue;
                };
                if location.run != run
                    || location.entry.block as usize != block
                    || location.entry.sequence != record.sequence
                {
                    continue;
                }
                match record.mutation {
                    Mutation::Put { vector, .. } if !location.entry.deleted => {
                        seen_live += 1;
                        consider(&mut heap, k, self.config, query, id, &vector);
                    }
                    Mutation::Delete { .. } if location.entry.deleted => {}
                    _ => {
                        return Err(Error::Corrupt(
                            "selective block disagrees with latest-ID directory".into(),
                        ));
                    }
                }
            }
            if sketch.live_per_block[flat].checked_sub(shadowed[flat]) != Some(seen_live) {
                return Err(Error::Corrupt(
                    "selected block omits a current vector".into(),
                ));
            }
        }
        for (&id, (_, document)) in &self.tail {
            if let Some(document) = document {
                consider(&mut heap, k, self.config, query, id, &document.vector);
            }
        }
        let mut results: Vec<_> = heap.into_iter().map(|ranked: Ranked| ranked.0).collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::{get_code, set_code};
    use crate::{
        retry::{Request, RequestId},
        segmented::SegmentedDatabase,
        store::LocalStore,
        Config, Metric, Mutation,
    };
    use std::collections::BTreeMap;

    #[test]
    fn five_bit_codec_crosses_byte_boundaries() {
        let mut bytes = [0_u8; 10];
        for axis in 0..16 {
            set_code(&mut bytes, axis, ((axis * 7) % 32) as u8);
        }
        for axis in 0..16 {
            assert_eq!(get_code(&bytes, axis), (axis * 7) % 32);
        }
    }

    #[test]
    fn finite_f32_extremes_keep_routing_scores_finite() {
        let temp = tempfile::tempdir().unwrap();
        let mut db = SegmentedDatabase::open(
            LocalStore::open(temp.path()).unwrap(),
            Config {
                dimensions: 1,
                metric: Metric::SquaredEuclidean,
            },
        )
        .unwrap();
        db.apply_request(Request {
            id: RequestId {
                boundary: 0,
                nonce: [1; 16],
            },
            conditions: Vec::new(),
            mutations: vec![
                Mutation::Put {
                    id: 1,
                    vector: vec![f32::MIN],
                    metadata: BTreeMap::new(),
                },
                Mutation::Put {
                    id: 2,
                    vector: vec![f32::MAX],
                    metadata: BTreeMap::new(),
                },
            ],
        })
        .unwrap();
        db.seal_delta().unwrap();
        db.build_selective_index(1024 * 1024).unwrap();
        assert_eq!(
            db.search_selective_unfiltered(&[f32::MAX], 1, 1).unwrap()[0].id,
            2
        );
    }
}
