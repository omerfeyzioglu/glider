//! Block format version 2: a binary record layout compressed with zstd.
//!
//! Encoded bytes: magic `GLB2`, the raw length as u32 little-endian, then the
//! zstd frame of the raw layout: dimensions u32, metric u8, partition u32,
//! record count u32, then per record id u64, sequence u64 and kind u8 (0 put,
//! 1 delete); a put continues with its f32 components and a metadata count
//! u32 followed by length-prefixed (u32) UTF-8 keys and values in key order.
//! All integers and floats are little-endian. Version 1 blocks are JSON.
use super::{Block, BlockRecord};
use crate::{Config, Error, Metric, Mutation, Result};
use std::{cell::RefCell, collections::BTreeMap};

const MAGIC: &[u8; 4] = b"GLB2";
const HEADER: usize = 13;
/// Raw layout limit; the compressed block then stays within 128 KiB.
pub(super) const MAX_RAW_BLOCK_BYTES: usize = 120 * 1024;
const LEVEL: i32 = 3;

pub(super) fn is_v2(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

fn metric_byte(metric: Metric) -> u8 {
    match metric {
        Metric::SquaredEuclidean => 0,
        Metric::Manhattan => 1,
    }
}

/// Raw bytes one record adds to a block.
pub(super) fn record_len(record: &BlockRecord) -> usize {
    17 + match &record.mutation {
        Mutation::Put {
            vector, metadata, ..
        } => put_len(vector, metadata),
        Mutation::Delete { .. } => 0,
    }
}

/// Raw bytes a put's vector and metadata add after the record header.
pub(super) fn put_len(vector: &[f32], metadata: &BTreeMap<String, String>) -> usize {
    4 * vector.len()
        + 4
        + metadata
            .iter()
            .map(|(key, value)| 8 + key.len() + value.len())
            .sum::<usize>()
}

/// Raw length of a block holding records of the given raw lengths.
pub(super) fn block_len(records: impl IntoIterator<Item = usize>) -> usize {
    HEADER + records.into_iter().sum::<usize>()
}

pub(super) fn encode(block: &Block) -> Result<Vec<u8>> {
    let mut raw = Vec::with_capacity(block_len(block.records.iter().map(record_len)));
    raw.extend_from_slice(&(block.config.dimensions as u32).to_le_bytes());
    raw.push(metric_byte(block.config.metric));
    raw.extend_from_slice(&block.partition.to_le_bytes());
    raw.extend_from_slice(&(block.records.len() as u32).to_le_bytes());
    for record in &block.records {
        raw.extend_from_slice(&record.id().to_le_bytes());
        raw.extend_from_slice(&record.sequence.to_le_bytes());
        match &record.mutation {
            Mutation::Put {
                vector, metadata, ..
            } => {
                raw.push(0);
                for value in vector {
                    raw.extend_from_slice(&value.to_le_bytes());
                }
                raw.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
                for (key, value) in metadata {
                    for text in [key, value] {
                        raw.extend_from_slice(&(text.len() as u32).to_le_bytes());
                        raw.extend_from_slice(text.as_bytes());
                    }
                }
            }
            Mutation::Delete { .. } => raw.push(1),
        }
    }
    if raw.len() > MAX_RAW_BLOCK_BYTES {
        return Err(Error::Invalid(
            "segmented block exceeds raw byte limit".into(),
        ));
    }
    let compressed = zstd::bulk::compress(&raw, LEVEL).map_err(Error::Io)?;
    let mut bytes = Vec::with_capacity(8 + compressed.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&compressed);
    Ok(bytes)
}

/// One record borrowed from a decompressed block.
pub(super) struct View<'a> {
    pub(super) id: u64,
    pub(super) sequence: u64,
    /// Little-endian f32 components and the metadata entry count/bytes.
    pub(super) put: Option<(&'a [u8], u32, &'a [u8])>,
}

thread_local! {
    static RAW: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn corrupt() -> Error {
    Error::Corrupt("invalid segmented block v2".into())
}

fn take<'a>(raw: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    if raw.len() < length {
        return Err(corrupt());
    }
    let (head, rest) = raw.split_at(length);
    *raw = rest;
    Ok(head)
}

fn take_u32(raw: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_le_bytes(take(raw, 4)?.try_into().unwrap()))
}

fn take_u64(raw: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(raw, 8)?.try_into().unwrap()))
}

/// Decompress into a reused per-thread buffer and visit every record in
/// order, validating the layout, configuration, ID order, sequences and
/// finite components. Returns the partition and record count.
pub(super) fn visit(
    config: Config,
    bytes: &[u8],
    mut visit: impl FnMut(View<'_>) -> Result<()>,
) -> Result<(u32, usize)> {
    if !is_v2(bytes) || bytes.len() < 8 {
        return Err(corrupt());
    }
    let raw_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if raw_len > MAX_RAW_BLOCK_BYTES {
        return Err(corrupt());
    }
    RAW.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        buffer.clear();
        buffer.reserve(raw_len);
        let written = zstd::bulk::Decompressor::new()
            .and_then(|mut decompressor| {
                decompressor.decompress_to_buffer(&bytes[8..], &mut *buffer)
            })
            .map_err(|_| corrupt())?;
        if written != raw_len {
            return Err(corrupt());
        }
        let mut raw = buffer.as_slice();
        if take_u32(&mut raw)? as usize != config.dimensions
            || take(&mut raw, 1)?[0] != metric_byte(config.metric)
        {
            return Err(corrupt());
        }
        let partition = take_u32(&mut raw)?;
        let count = take_u32(&mut raw)? as usize;
        if count == 0 {
            return Err(corrupt());
        }
        let mut previous = None;
        for _ in 0..count {
            let id = take_u64(&mut raw)?;
            let sequence = take_u64(&mut raw)?;
            if sequence == 0 || previous.is_some_and(|last| last >= id) {
                return Err(corrupt());
            }
            previous = Some(id);
            let put = match take(&mut raw, 1)?[0] {
                0 => {
                    let vector = take(&mut raw, 4 * config.dimensions)?;
                    if vector
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .any(|part| !f32::from_le_bytes(*part).is_finite())
                    {
                        return Err(corrupt());
                    }
                    let entries = take_u32(&mut raw)?;
                    let start = raw;
                    let mut previous_key: Option<&[u8]> = None;
                    for _ in 0..entries {
                        let key_len = take_u32(&mut raw)? as usize;
                        let key = take(&mut raw, key_len)?;
                        let value_len = take_u32(&mut raw)? as usize;
                        let value = take(&mut raw, value_len)?;
                        if std::str::from_utf8(key).is_err()
                            || std::str::from_utf8(value).is_err()
                            || previous_key.is_some_and(|last| last >= key)
                        {
                            return Err(corrupt());
                        }
                        previous_key = Some(key);
                    }
                    let metadata = &start[..start.len() - raw.len()];
                    Some((vector, entries, metadata))
                }
                1 => None,
                _ => return Err(corrupt()),
            };
            visit(View { id, sequence, put })?;
        }
        if !raw.is_empty() {
            return Err(corrupt());
        }
        Ok((partition, count))
    })
}

/// Copy a view's components into `vector`.
pub(super) fn components(bytes: &[u8], vector: &mut Vec<f32>) {
    vector.clear();
    vector.extend(
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|part| f32::from_le_bytes(*part)),
    );
}

fn metadata(entries: u32, mut bytes: &[u8]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for _ in 0..entries {
        let mut text = || {
            let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            let value = String::from_utf8(bytes[4..4 + length].to_vec()).expect("validated UTF-8");
            bytes = &bytes[4 + length..];
            value
        };
        let key = text();
        let value = text();
        map.insert(key, value);
    }
    map
}

/// Fully decode a version 2 block.
pub(super) fn decode(config: Config, bytes: &[u8]) -> Result<Block> {
    let mut records = Vec::new();
    let (partition, _) = visit(config, bytes, |view| {
        let mutation = match view.put {
            Some((vector, entries, raw)) => {
                let mut components_out = Vec::with_capacity(config.dimensions);
                components(vector, &mut components_out);
                Mutation::Put {
                    id: view.id,
                    vector: components_out,
                    metadata: metadata(entries, raw),
                }
            }
            None => Mutation::Delete { id: view.id },
        };
        records.push(BlockRecord {
            sequence: view.sequence,
            mutation,
        });
        Ok(())
    })?;
    Block::new(config, partition, records)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn value(&mut self) -> f32 {
            match self.next() % 8 {
                0 => -0.,
                1 => f32::MAX,
                2 => f32::MIN,
                3 => f32::MIN_POSITIVE,
                _ => (self.next() as i32 as f32) / 8192.,
            }
        }
    }

    #[test]
    fn v2_decoder_round_trips_and_rejects_mutated_bytes() {
        let seed = 0x49d2_7a3b_9e11_02c5_u64;
        let mut rng = Rng(seed);
        for case in 0..40 {
            let config = Config {
                dimensions: 1 + case as usize,
                metric: if case % 2 == 0 {
                    Metric::SquaredEuclidean
                } else {
                    Metric::Manhattan
                },
            };
            let records = (0..1 + rng.next() % 7)
                .map(|index| {
                    let id = index * 3 + rng.next() % 3;
                    let mutation = if rng.next().is_multiple_of(4) {
                        Mutation::Delete { id }
                    } else {
                        let metadata = (0..rng.next() % 4)
                            .map(|key| (format!("key-{key}"), format!("value-{}", rng.next())))
                            .collect();
                        Mutation::Put {
                            id,
                            vector: (0..config.dimensions).map(|_| rng.value()).collect(),
                            metadata,
                        }
                    };
                    BlockRecord {
                        sequence: 1 + rng.next() % 1000,
                        mutation,
                    }
                })
                .collect();
            let block = Block::new(config, case, records).unwrap();
            let bytes = encode(&block).unwrap();
            let decoded = decode(config, &bytes).unwrap();
            assert_eq!(
                encode(&decoded).unwrap(),
                bytes,
                "seed {seed:#x}, case {case}"
            );
            let mut visited = Vec::new();
            let (partition, count) = visit(config, &bytes, |view| {
                visited.push((view.id, view.sequence, view.put.is_some()));
                Ok(())
            })
            .unwrap();
            assert_eq!(
                (partition, count),
                (case, block.records.len()),
                "seed {seed:#x}, case {case}"
            );
            assert_eq!(
                visited.len(),
                block.records.len(),
                "seed {seed:#x}, case {case}"
            );

            let check = |candidate: &[u8], mutation: &str| {
                let outcome = std::panic::catch_unwind(|| {
                    let visited = visit(config, candidate, |_| Ok(()));
                    let decoded = decode(config, candidate);
                    (visited, decoded)
                });
                let (visited, decoded) = outcome.unwrap_or_else(|_| {
                    panic!("v2 decoder panicked: seed {seed:#x}, case {case}, {mutation}")
                });
                assert_eq!(
                    visited.is_ok(),
                    decoded.is_ok(),
                    "seed {seed:#x}, case {case}, {mutation}"
                );
                // Authentication against the root digest rejects altered
                // bytes before decoding; a decoder that still accepts them
                // must at least yield a valid, stably re-encodable block.
                if let Ok(block) = decoded {
                    let reencoded = encode(&block).unwrap();
                    let again = decode(config, &reencoded).unwrap();
                    assert_eq!(
                        encode(&again).unwrap(),
                        reencoded,
                        "seed {seed:#x}, case {case}, {mutation}"
                    );
                }
            };
            for length in 0..bytes.len().min(160) {
                check(&bytes[..length], &format!("truncate {length}"));
            }
            check(&bytes[..bytes.len() - 1], "truncate final byte");
            for flip in 0..24 {
                let mut changed = bytes.clone();
                let offset = rng.next() as usize % changed.len();
                changed[offset] ^= 1 << (rng.next() % 8);
                check(&changed, &format!("flip {flip} at {offset}"));
            }
            for suffix in [vec![0], vec![0xff, 0x12], vec![0; 8]] {
                let mut changed = bytes.clone();
                changed.extend_from_slice(&suffix);
                check(&changed, "append");
            }
            let mut changed = bytes.clone();
            changed[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
            check(&changed, "huge raw length");
            let mut raw = zstd::bulk::decompress(&bytes[8..], MAX_RAW_BLOCK_BYTES).unwrap();
            raw[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
            let compressed = zstd::bulk::compress(&raw, LEVEL).unwrap();
            let mut changed = bytes[..8].to_vec();
            changed.extend_from_slice(&compressed);
            check(&changed, "huge record count");
            let mut position = HEADER;
            for record in &block.records {
                position += 17;
                if let Mutation::Put { vector, .. } = &record.mutation {
                    position += 4 * vector.len();
                    let mut raw = zstd::bulk::decompress(&bytes[8..], MAX_RAW_BLOCK_BYTES).unwrap();
                    raw[position..position + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                    let mut changed = bytes[..8].to_vec();
                    changed.extend_from_slice(&zstd::bulk::compress(&raw, LEVEL).unwrap());
                    check(&changed, "huge metadata count");
                    break;
                }
            }
        }
    }
}
