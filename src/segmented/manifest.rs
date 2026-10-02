//! Root v5 and run manifests.
//!
//! Root versions 1, 2 and 4 embed every run's complete block list, so each
//! publication rewrote every block reference of the namespace. A root v5
//! keeps v4's header (generation, sequence, configuration, retry state,
//! fences and optional clustered view) and, per run, its sequences and index
//! reference, but names the run's block list through an immutable run
//! manifest object (`sgmanifest-{attempt}`) bound by length and SHA-256. A
//! publication creates manifests only for runs whose block lists changed and
//! reuses the selected root's references for the others, so its uploaded
//! bytes follow what it changed. A manifest is a staged orphan until the root
//! that references it is created.
//!
//! Manifest v1 layout, little-endian: magic `GLRMAN01`, pack count u32,
//! block count u32; per pack in order of first use, key length u16, UTF-8
//! `sgpack-` key and payload length u32; per block in run order, pack ordinal
//! u32, offset u32, length u32, partition u32, first ID u64, last ID u64, row
//! count u32 and SHA-256 (32 raw bytes). The encoding is canonical: a decoded
//! manifest encodes to the same bytes.
use super::{
    attempt_id, clustered, clustered::ObjectRef, validate_block_ref, BlockRef, Fences, Root,
    RunRef, SegmentedDatabase,
};
use crate::{decode, encode, retry, store::ObjectStore, Config, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MAGIC: &[u8; 8] = b"GLRMAN01";
pub(super) const PREFIX: &str = "sgmanifest-";
/// About 246,000 block references, far beyond any run the 2 MiB
/// consolidation bound produces.
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
const BLOCK_BYTES: usize = 68;
/// Manifests fetched together when a root is opened.
const OPEN_BATCH: usize = 16;

fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("invalid segmented run manifest {what}"))
}

fn valid_pack_key(key: &str) -> bool {
    key.strip_prefix("sgpack-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::Invalid("segmented manifest field overflow".into()))
}

/// Encode a run's block list. Fails for an empty or invalid list.
pub(super) fn encode_manifest(blocks: &[BlockRef]) -> Result<Vec<u8>> {
    if blocks.is_empty() {
        return Err(Error::Invalid("empty segmented run manifest".into()));
    }
    let mut packs: Vec<(&str, usize)> = Vec::new();
    let mut ordinals = BTreeMap::new();
    let mut rows = Vec::with_capacity(blocks.len());
    for block in blocks {
        validate_block_ref(block)?;
        if !valid_pack_key(&block.object) {
            return Err(Error::Invalid(format!(
                "segmented manifest pack key: {}",
                block.object
            )));
        }
        let ordinal = *ordinals.entry(block.object.as_str()).or_insert_with(|| {
            packs.push((block.object.as_str(), block.payload_len));
            packs.len() - 1
        });
        if packs[ordinal].1 != block.payload_len {
            return Err(Error::Corrupt(
                "segmented pack references disagree on length".into(),
            ));
        }
        rows.push(ordinal);
    }
    let mut bytes = Vec::with_capacity(16 + blocks.len() * BLOCK_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&to_u32(packs.len())?.to_le_bytes());
    bytes.extend_from_slice(&to_u32(blocks.len())?.to_le_bytes());
    for (key, payload_len) in &packs {
        let length = u16::try_from(key.len())
            .map_err(|_| Error::Invalid("segmented manifest pack key too long".into()))?;
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(key.as_bytes());
        bytes.extend_from_slice(&to_u32(*payload_len)?.to_le_bytes());
    }
    for (block, ordinal) in blocks.iter().zip(rows) {
        bytes.extend_from_slice(&to_u32(ordinal)?.to_le_bytes());
        bytes.extend_from_slice(&to_u32(block.offset)?.to_le_bytes());
        bytes.extend_from_slice(&to_u32(block.length)?.to_le_bytes());
        bytes.extend_from_slice(&block.partition.to_le_bytes());
        bytes.extend_from_slice(&block.first_id.to_le_bytes());
        bytes.extend_from_slice(&block.last_id.to_le_bytes());
        bytes.extend_from_slice(&to_u32(block.rows)?.to_le_bytes());
        let mut digest = [0_u8; 32];
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&block.sha256[index * 2..index * 2 + 2], 16)
                .expect("validated digest");
        }
        bytes.extend_from_slice(&digest);
    }
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(Error::Invalid(
            "segmented run manifest exceeds byte limit".into(),
        ));
    }
    Ok(bytes)
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.0.len() < count {
            return Err(corrupt("length"));
        }
        let (head, tail) = self.0.split_at(count);
        self.0 = tail;
        Ok(head)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// Decode manifest bytes the caller has authenticated against their root
/// reference. Every structural rule of the encoder is enforced.
pub(super) fn decode_manifest(bytes: &[u8]) -> Result<Vec<BlockRef>> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(corrupt("length"));
    }
    let mut reader = Reader(bytes);
    if reader.take(8)? != MAGIC {
        return Err(corrupt("magic"));
    }
    let pack_count = reader.u32()? as usize;
    let block_count = reader.u32()? as usize;
    // Each pack needs at least 7 bytes and each block 68, so the counts
    // cannot claim more than the bytes hold.
    if pack_count == 0
        || block_count == 0
        || pack_count > block_count
        || pack_count.saturating_mul(7) > reader.0.len()
        || block_count.saturating_mul(BLOCK_BYTES) > reader.0.len()
    {
        return Err(corrupt("counts"));
    }
    let mut packs: Vec<(String, usize)> = Vec::with_capacity(pack_count);
    for _ in 0..pack_count {
        let length = reader.u16()? as usize;
        let key = std::str::from_utf8(reader.take(length)?).map_err(|_| corrupt("pack key"))?;
        let payload_len = reader.u32()? as usize;
        if !valid_pack_key(key) || packs.iter().any(|(known, _)| known == key) {
            return Err(corrupt("pack key"));
        }
        packs.push((key.to_owned(), payload_len));
    }
    if reader.0.len() != block_count * BLOCK_BYTES {
        return Err(corrupt("length"));
    }
    let mut blocks = Vec::with_capacity(block_count);
    let mut used = 0;
    for _ in 0..block_count {
        let ordinal = reader.u32()? as usize;
        // Packs are listed in order of first use, which keeps the encoding
        // canonical.
        if ordinal > used || ordinal >= pack_count {
            return Err(corrupt("pack ordinal"));
        }
        used = used.max(ordinal + 1);
        let (object, payload_len) = &packs[ordinal];
        let offset = reader.u32()? as usize;
        let length = reader.u32()? as usize;
        let partition = reader.u32()?;
        let first_id = reader.u64()?;
        let last_id = reader.u64()?;
        let rows = reader.u32()? as usize;
        let sha256 = reader
            .take(32)?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let block = BlockRef {
            object: object.clone(),
            payload_len: *payload_len,
            offset,
            length,
            sha256,
            partition,
            first_id,
            last_id,
            rows,
        };
        validate_block_ref(&block)?;
        blocks.push(block);
    }
    if used != pack_count {
        return Err(corrupt("unused pack"));
    }
    Ok(blocks)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunV5 {
    first_sequence: u64,
    last_sequence: u64,
    index_object: String,
    index_len: usize,
    index_sha256: String,
    manifest: ObjectRef,
}

/// The persisted root v5. Fences and the clustered view are optional;
/// a present field must be valid.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootV5 {
    version: u32,
    generation: u64,
    sequence: u64,
    config: Config,
    retry: retry::State,
    runs: Vec<RunV5>,
    #[serde(default, skip_serializing_if = "Fences::is_empty")]
    fences: Fences,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "clustered::deserialize_view_ref"
    )]
    clustered: Option<clustered::ViewRef>,
}

/// Encode a root for publication: always version 5, with every run bound
/// to its manifest by [`SegmentedDatabase::stage_manifest`].
pub(super) fn encode_root(root: &Root) -> Result<Vec<u8>> {
    if root.version != 5 {
        return Err(Error::Invalid("only root v5 is published".into()));
    }
    let runs = root
        .runs
        .iter()
        .map(|run| {
            Ok(RunV5 {
                first_sequence: run.first_sequence,
                last_sequence: run.last_sequence,
                index_object: run.index_object.clone(),
                index_len: run.index_len,
                index_sha256: run.index_sha256.clone(),
                manifest: run
                    .manifest
                    .clone()
                    .ok_or_else(|| Error::Invalid("run manifest not staged".into()))?,
            })
        })
        .collect::<Result<_>>()?;
    encode(&RootV5 {
        version: 5,
        generation: root.generation,
        sequence: root.sequence,
        config: root.config,
        retry: root.retry.clone(),
        runs,
        fences: root.fences.clone(),
        clustered: root.clustered.clone(),
    })
}

/// Decode a root v5 without its manifests: runs carry manifest references
/// and empty block lists until [`load_manifests`] fills them.
pub(super) fn decode_root(bytes: &[u8], config: Config) -> Result<Root> {
    let stored: RootV5 = decode(bytes)?;
    if stored.version != 5 {
        return Err(Error::Corrupt("invalid segmented root identity".into()));
    }
    let mut runs = Vec::with_capacity(stored.runs.len());
    for run in stored.runs {
        run.manifest
            .validate(PREFIX, MAX_MANIFEST_BYTES)
            .map_err(|_| corrupt("reference"))?;
        runs.push(RunRef {
            first_sequence: run.first_sequence,
            last_sequence: run.last_sequence,
            index_object: run.index_object,
            index_len: run.index_len,
            index_sha256: run.index_sha256,
            blocks: Vec::new(),
            manifest: Some(run.manifest),
        });
    }
    let root = Root {
        version: stored.version,
        generation: stored.generation,
        sequence: stored.sequence,
        config: stored.config,
        retry: stored.retry,
        runs,
        fences: stored.fences,
        clustered: stored.clustered,
    };
    root.validate_header(config)?;
    Ok(root)
}

/// Read, authenticate and decode the manifests of a decoded root v5, then
/// validate the complete root. A missing or corrupt manifest fails: the
/// selected root's block lists are authoritative and never guessed.
pub(super) fn load_manifests<S: ObjectStore>(
    store: &S,
    config: Config,
    root: &mut Root,
) -> Result<()> {
    let pending: Vec<usize> = (0..root.runs.len())
        .filter(|&run| root.runs[run].blocks.is_empty())
        .collect();
    for chunk in pending.chunks(OPEN_BATCH) {
        let mut references = Vec::with_capacity(chunk.len());
        for &run in chunk {
            references.push(
                root.runs[run]
                    .manifest
                    .clone()
                    .ok_or_else(|| Error::Corrupt("segmented run without blocks".into()))?,
            );
        }
        let keys: Vec<String> = references
            .iter()
            .map(|reference| reference.key.clone())
            .collect();
        for ((&run, reference), bytes) in chunk.iter().zip(&references).zip(store.get_many(&keys)?)
        {
            let bytes = bytes.ok_or_else(|| {
                Error::Corrupt(format!("segmented run manifest missing: {}", reference.key))
            })?;
            reference.authenticate(&bytes).map_err(|_| {
                Error::Corrupt(format!(
                    "segmented run manifest digest mismatch: {}",
                    reference.key
                ))
            })?;
            root.runs[run].blocks = decode_manifest(&bytes)?;
        }
    }
    root.validate(config)
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    /// Bind every run of `root`, a root about to be published, to a durable
    /// manifest holding exactly its block list. A run whose index and blocks
    /// equal a run of the selected root reuses that run's manifest; another
    /// reuses a manifest this handle staged for the same bytes, or gets a
    /// new one. Creates at most one object per call and returns true when it
    /// did (the caller's step ends and it calls again); false means `root`
    /// is ready for [`encode_root`]. A create error poisons the handle; the
    /// staged manifest is an orphan until the root referencing it exists.
    pub(super) fn stage_manifest(&mut self, root: &mut Root) -> Result<bool> {
        root.version = 5;
        let mut unbound = Vec::new();
        for (ordinal, run) in root.runs.iter_mut().enumerate() {
            run.manifest = self
                .root
                .runs
                .iter()
                .find(|selected| {
                    selected.manifest.is_some()
                        && selected.index_object == run.index_object
                        && selected.blocks == run.blocks
                })
                .and_then(|selected| selected.manifest.clone());
            if run.manifest.is_none() {
                unbound.push(ordinal);
            }
        }
        for ordinal in unbound {
            let bytes = encode_manifest(&root.runs[ordinal].blocks)?;
            let digest: [u8; 32] = Sha256::digest(&bytes).into();
            if let Some(reference) = self.staged_manifests.get(&digest) {
                root.runs[ordinal].manifest = Some(reference.clone());
                continue;
            }
            let reference = clustered::object_ref(format!("{PREFIX}{}", attempt_id()?), &bytes);
            self.poisoned = true;
            self.create_staged(&reference.key, &bytes)?;
            self.poisoned = false;
            self.staged_manifests.insert(digest, reference);
            return Ok(true);
        }
        Ok(false)
    }

    /// Stage every manifest `root` needs (a synchronous unit).
    pub(super) fn stage_manifests(&mut self, root: &mut Root) -> Result<()> {
        while self.stage_manifest(root)? {}
        Ok(())
    }
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
    }

    fn blocks(rng: &mut Rng) -> Vec<BlockRef> {
        let packs = 1 + rng.next() as usize % 4;
        let count = packs + rng.next() as usize % 20;
        (0..count)
            .map(|index| {
                let pack = if index < packs {
                    index
                } else {
                    rng.next() as usize % packs
                };
                let payload_len = 4096 + pack * 1000;
                let length = 1 + rng.next() as usize % 512;
                let first_id = rng.next() % 1_000_000;
                BlockRef {
                    object: format!("sgpack-attempt{pack}-{:08}", pack),
                    payload_len,
                    offset: rng.next() as usize % (payload_len - length),
                    length,
                    sha256: format!("{:x}", Sha256::digest(rng.next().to_le_bytes())),
                    partition: rng.next() as u32,
                    first_id,
                    last_id: first_id + rng.next() % 1000,
                    rows: 1 + rng.next() as usize % 170,
                }
            })
            .collect()
    }

    #[test]
    fn manifest_round_trips_and_rejects_every_mutation() {
        let seed = 0x5eed_6d61_6e69_6631_u64;
        let mut rng = Rng(seed);
        for case in 0..40 {
            let blocks = blocks(&mut rng);
            let bytes = encode_manifest(&blocks).unwrap();
            assert_eq!(
                decode_manifest(&bytes).unwrap(),
                blocks,
                "seed {seed:#x}, case {case}"
            );
            let check = |candidate: &[u8], mutation: &str| {
                let outcome = std::panic::catch_unwind(|| decode_manifest(candidate));
                let decoded = outcome.unwrap_or_else(|_| {
                    panic!("manifest decoder panicked: seed {seed:#x}, case {case}, {mutation}")
                });
                match decoded {
                    // Canonical: anything accepted re-encodes to its bytes.
                    Ok(blocks) => assert_eq!(
                        encode_manifest(&blocks).unwrap(),
                        candidate,
                        "seed {seed:#x}, case {case}, {mutation}"
                    ),
                    Err(error) => assert!(
                        matches!(error, Error::Corrupt(_)),
                        "seed {seed:#x}, case {case}, {mutation}: {error}"
                    ),
                }
            };
            for length in 0..bytes.len() {
                let outcome = std::panic::catch_unwind(|| decode_manifest(&bytes[..length]));
                assert!(
                    matches!(outcome, Ok(Err(Error::Corrupt(_)))),
                    "truncation accepted: seed {seed:#x}, case {case}, {length}"
                );
            }
            for flip in 0..64 {
                let mut changed = bytes.clone();
                let offset = rng.next() as usize % changed.len();
                changed[offset] ^= 1 << (rng.next() % 8);
                check(&changed, &format!("flip {flip} at {offset}"));
            }
            let mut changed = bytes.clone();
            changed.push(0);
            check(&changed, "append");
            for field in [8, 12] {
                let mut changed = bytes.clone();
                changed[field..field + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                check(&changed, &format!("huge count at {field}"));
            }
        }
    }

    #[test]
    fn manifest_encoder_rejects_invalid_block_lists() {
        let mut rng = Rng(0x0dd5_eed5);
        let valid = blocks(&mut rng);
        assert!(matches!(encode_manifest(&[]), Err(Error::Invalid(_))));
        let mut other = valid.clone();
        other[0].object = "pack-1".into();
        assert!(encode_manifest(&other).is_err());
        let mut other = valid.clone();
        other[0].sha256 = "A".repeat(64);
        assert!(encode_manifest(&other).is_err());
        let mut other = valid.clone();
        let mut duplicate = other[0].clone();
        duplicate.payload_len += 1;
        duplicate.offset = 0;
        other.push(duplicate);
        assert!(matches!(encode_manifest(&other), Err(Error::Corrupt(_))));
    }

    #[test]
    fn root_v5_is_strict_round_trips_and_old_readers_reject_it() {
        let config = Config {
            dimensions: 2,
            metric: crate::Metric::SquaredEuclidean,
        };
        let mut rng = Rng(0x7005_0005_dead_beef);
        let mut root = Root::empty(config);
        root.version = 5;
        root.generation = 9;
        root.sequence = 40;
        root.retry = retry::State::legacy(40);
        root.fences.logs.insert(3);
        root.fences.roots.insert(2);
        for run in 0..3_u64 {
            root.runs.push(RunRef {
                first_sequence: run * 10 + 1,
                last_sequence: run * 10 + 10,
                index_object: format!("sgindex-run{run}"),
                index_len: 48,
                index_sha256: "c".repeat(64),
                blocks: Vec::new(),
                manifest: Some(clustered::object_ref(
                    format!("{PREFIX}run{run}"),
                    &encode_manifest(&blocks(&mut rng)).unwrap(),
                )),
            });
        }
        let bytes = encode_root(&root).unwrap();
        let decoded = decode_root(&bytes, config).unwrap();
        assert_eq!(encode_root(&decoded).unwrap(), bytes);
        assert!(decoded.runs.iter().all(|run| run.blocks.is_empty()));
        // Readers of v1, v2 and v4 reject v5: its runs have no block lists.
        assert!(decode::<Root>(&bytes).is_err());
        // Unbound runs and other versions are never encoded.
        let mut unbound = decoded.clone();
        unbound.runs[1].manifest = None;
        assert!(matches!(encode_root(&unbound), Err(Error::Invalid(_))));
        let mut old = decoded.clone();
        old.version = 4;
        assert!(matches!(encode_root(&old), Err(Error::Invalid(_))));
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let edits: [(&str, serde_json::Value); 8] = [
            ("/version", 4.into()),
            ("/generation", 0.into()),
            ("/runs/0/manifest/key", "sgindex-x".into()),
            ("/runs/0/manifest/sha256", "bad".into()),
            ("/runs/0/manifest/length", 0.into()),
            ("/runs/1/first_sequence", 1.into()),
            ("/fences/roots", serde_json::json!([9])),
            ("/clustered", serde_json::Value::Null),
        ];
        for (pointer, replacement) in edits {
            let mut changed = value.clone();
            match changed.pointer_mut(pointer) {
                Some(slot) => *slot = replacement,
                None => changed["clustered"] = replacement,
            }
            assert!(
                matches!(
                    decode_root(&encode(&changed).unwrap(), config),
                    Err(Error::Corrupt(_))
                ),
                "{pointer}"
            );
        }
        value["runs"][0]["blocks"] = serde_json::json!([]);
        assert!(decode_root(&encode(&value).unwrap(), config).is_err());
        let seed = 0x5eed_0005_u64;
        let mut rng = Rng(seed);
        for length in 0..bytes.len() {
            let outcome = std::panic::catch_unwind(|| decode_root(&bytes[..length], config));
            assert!(
                matches!(outcome, Ok(Err(Error::Corrupt(_)))),
                "root truncation accepted: seed {seed:#x}, {length}"
            );
        }
        for flip in 0..256 {
            let mut changed = bytes.clone();
            let offset = rng.next() as usize % changed.len();
            changed[offset] ^= 1 << (rng.next() % 8);
            let outcome = std::panic::catch_unwind(|| decode_root(&changed, config));
            let decoded = outcome.unwrap_or_else(|_| {
                panic!("root decoder panicked: seed {seed:#x}, flip {flip} at {offset}")
            });
            if let Ok(decoded) = decoded {
                // An accepted mutation is a different valid root; it still
                // round-trips exactly.
                assert_eq!(
                    decode_root(&encode_root(&decoded).unwrap(), config)
                        .map(|root| root.runs.len())
                        .unwrap(),
                    decoded.runs.len(),
                    "seed {seed:#x}, flip {flip}"
                );
            }
        }
    }

    mod upgrade {
        use super::super::super::{
            decode_root as decode_any_root, numbered_key, ConvertOptions, MetadataVersion,
            ReadBudget, RootObject, SegmentedDatabase,
        };
        use super::super::*;
        use crate::{
            retry::{Request, RequestId},
            Metric, Mutation,
        };
        use std::{
            collections::BTreeSet,
            sync::{Arc, Mutex},
        };

        const DIMENSIONS: usize = 4;

        fn config() -> Config {
            Config {
                dimensions: DIMENSIONS,
                metric: Metric::SquaredEuclidean,
            }
        }

        /// Complete-object store in memory; clones share objects.
        #[derive(Clone, Default)]
        struct Memory(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

        impl Memory {
            fn copy(&self) -> Self {
                Self(Arc::new(Mutex::new(self.0.lock().unwrap().clone())))
            }
            fn keys(&self, prefix: &str) -> Vec<String> {
                let objects = self.0.lock().unwrap();
                objects
                    .keys()
                    .filter(|key| key.starts_with(prefix))
                    .cloned()
                    .collect()
            }
            /// The newest root object that is not a fence marker.
            fn selected_root(&self) -> (String, Vec<u8>) {
                let objects = self.0.lock().unwrap();
                objects
                    .iter()
                    .rev()
                    .filter(|(key, _)| key.starts_with("sgroot-"))
                    .find(|(_, bytes)| decode::<MetadataVersion>(bytes).unwrap().version != 3)
                    .map(|(key, bytes)| (key.clone(), bytes.clone()))
                    .unwrap()
            }
            fn root_version(&self) -> u32 {
                decode::<MetadataVersion>(&self.selected_root().1)
                    .unwrap()
                    .version
            }
        }

        impl ObjectStore for Memory {
            fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
                Ok(self.0.lock().unwrap().get(key).cloned())
            }
            fn list(&self) -> Result<Vec<String>> {
                Ok(self.0.lock().unwrap().keys().cloned().collect())
            }
            fn create(&self, key: &str, value: &[u8]) -> Result<()> {
                let mut objects = self.0.lock().unwrap();
                if objects.contains_key(key) {
                    return Err(Error::Exists(key.into()));
                }
                objects.insert(key.into(), value.to_vec());
                Ok(())
            }
            fn remove(&self, key: &str) -> Result<()> {
                self.0.lock().unwrap().remove(key);
                Ok(())
            }
        }

        #[derive(Clone, Copy, Debug, PartialEq)]
        enum Fault {
            Before,
            After,
            /// A remove that reports success and lands after the run.
            Delayed,
        }

        #[derive(Default)]
        struct Plan {
            armed: bool,
            operations: usize,
            fail_at: Option<(usize, Fault)>,
            fired: bool,
            removes: Vec<bool>,
            delayed: Vec<String>,
        }

        /// Fails the chosen counted create or remove once armed.
        struct FaultStore {
            inner: Memory,
            plan: Arc<Mutex<Plan>>,
        }

        impl FaultStore {
            fn next(&self, remove: bool) -> Option<Fault> {
                let mut plan = self.plan.lock().unwrap();
                if !plan.armed {
                    return None;
                }
                let index = plan.operations;
                plan.operations += 1;
                plan.removes.push(remove);
                match plan.fail_at {
                    Some((at, fault)) if at == index => {
                        plan.fired = true;
                        Some(fault)
                    }
                    _ => None,
                }
            }
        }

        fn injected() -> Error {
            Error::Io(std::io::Error::other("injected storage failure"))
        }

        impl ObjectStore for FaultStore {
            fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
                self.inner.get(key)
            }
            fn list(&self) -> Result<Vec<String>> {
                self.inner.list()
            }
            fn create(&self, key: &str, value: &[u8]) -> Result<()> {
                match self.next(false) {
                    Some(Fault::Before | Fault::Delayed) => Err(injected()),
                    Some(Fault::After) => {
                        self.inner.create(key, value)?;
                        Err(injected())
                    }
                    None => self.inner.create(key, value),
                }
            }
            fn remove(&self, key: &str) -> Result<()> {
                match self.next(true) {
                    Some(Fault::Before) => Err(injected()),
                    Some(Fault::After) => {
                        self.inner.remove(key)?;
                        Err(injected())
                    }
                    Some(Fault::Delayed) => {
                        self.plan.lock().unwrap().delayed.push(key.into());
                        Ok(())
                    }
                    None => self.inner.remove(key),
                }
            }
        }

        fn vector(seed: u64) -> Vec<f32> {
            (0..DIMENSIONS as u64)
                .map(|axis| ((seed * 2_654_435_761 + axis * 40_503) % 1_000) as f32 / 10.)
                .collect()
        }

        type Model = BTreeMap<u64, Option<Vec<f32>>>;

        fn write<S: ObjectStore>(db: &mut SegmentedDatabase<S>, model: &mut Model, batch: u64) {
            let mutations: Vec<Mutation> = (0..60_u64)
                .map(|n| {
                    let id = (batch * 37 + n * 11) % 400;
                    if (batch + n).is_multiple_of(9) {
                        Mutation::Delete { id }
                    } else {
                        Mutation::Put {
                            id,
                            vector: vector(batch * 1_000 + n),
                            metadata: BTreeMap::new(),
                        }
                    }
                })
                .collect();
            db.apply_request(Request {
                id: RequestId {
                    boundary: db.sequence(),
                    nonce: u128::from(batch).to_le_bytes(),
                },
                conditions: Vec::new(),
                mutations: mutations.clone(),
            })
            .unwrap();
            for mutation in mutations {
                match mutation {
                    Mutation::Put { id, vector, .. } => model.insert(id, Some(vector)),
                    Mutation::Delete { id } => model.insert(id, None),
                };
            }
        }

        fn conversion() -> ConvertOptions {
            ConvertOptions {
                centroids: Some(3),
                seed: 11,
                gather_bytes: 16 * 1024,
            }
        }

        /// Writes and seals, leaving runs to consolidate, dead blocks to
        /// prune, mixed packs to reclaim and, with a view, extents to merge.
        fn namespace(version: u32) -> (Memory, Model) {
            let store = Memory::default();
            let mut db = if version == 1 {
                SegmentedDatabase::open(store.clone(), config()).unwrap()
            } else {
                SegmentedDatabase::take_over(store.clone(), config()).unwrap()
            };
            let mut model = Model::new();
            for batch in 0..24 {
                write(&mut db, &mut model, batch);
                if batch % 3 == 2 {
                    db.seal_delta().unwrap();
                }
                if version == 4 && batch == 5 {
                    db.convert_clustered(conversion()).unwrap();
                }
            }
            write(&mut db, &mut model, 24);
            (store, model)
        }

        /// A copy whose selected root is rewritten in the legacy `version`
        /// with its blocks embedded, and without run manifests: a namespace
        /// as an older binary left it.
        fn legacy(store: &Memory, version: u32) -> Memory {
            let copy = store.copy();
            let (key, bytes) = copy.selected_root();
            let generation = numbered_key(&key, "sgroot-").unwrap();
            let RootObject::Root(root) = decode_any_root(&bytes, config(), generation).unwrap()
            else {
                unreachable!()
            };
            let mut root = *root;
            load_manifests(&copy, config(), &mut root).unwrap();
            root.version = version;
            assert_eq!(root.fences.is_empty(), version == 1);
            assert_eq!(root.clustered.is_some(), version == 4);
            let legacy = encode(&root).unwrap();
            let mut objects = copy.0.lock().unwrap();
            objects.retain(|key, _| !key.starts_with(PREFIX));
            objects.insert(key, legacy);
            drop(objects);
            assert_eq!(copy.root_version(), version);
            copy
        }

        fn open<S: ObjectStore>(store: S) -> SegmentedDatabase<S> {
            SegmentedDatabase::open(store, config())
                .unwrap()
                .with_reclaim_min_garbage(1)
                .with_cluster_probes(usize::MAX)
        }

        /// Documents, exact search and full-budget selective search equal
        /// the model.
        fn check<S: ObjectStore>(db: &SegmentedDatabase<S>, model: &Model, context: &str) {
            for (&id, expected) in model {
                let found = db.get(id).unwrap().map(|document| document.vector);
                assert_eq!(&found, expected, "{context}: id {id}");
            }
            assert!(db.clustered_view_error().is_none(), "{context}");
            for seed in 0..4 {
                let query = vector(seed * 7_919);
                let mut expected: Vec<_> = model
                    .iter()
                    .filter_map(|(&id, vector)| {
                        let distance: f64 = query
                            .iter()
                            .zip(vector.as_ref()?)
                            .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                            .sum();
                        Some((distance.to_bits(), id))
                    })
                    .collect();
                expected.sort_unstable();
                expected.truncate(10);
                let exact: Vec<_> = db
                    .search_exact(&query, 10, &[])
                    .unwrap()
                    .into_iter()
                    .map(|hit| (hit.distance.to_bits(), hit.id))
                    .collect();
                assert_eq!(exact, expected, "{context}: exact {seed}");
                let budget = ReadBudget {
                    blocks: db.block_count().max(1),
                    requests: usize::MAX,
                    bytes: usize::MAX,
                    local_blocks: 0,
                };
                let selective: Vec<_> = db
                    .search_selective_within(&query, 10, budget, &[])
                    .unwrap()
                    .into_iter()
                    .map(|hit| (hit.distance.to_bits(), hit.id))
                    .collect();
                assert_eq!(selective, expected, "{context}: selective {seed}");
            }
        }

        const KINDS: [&str; 6] = [
            "seal",
            "consolidate",
            "prune",
            "reclaim",
            "merge",
            "convert",
        ];

        /// One root-publishing operation and the cleanup after it.
        fn operate<S: ObjectStore>(db: &mut SegmentedDatabase<S>, kind: &str) -> Result<()> {
            match kind {
                "seal" => db.seal_delta()?,
                "consolidate" => {
                    db.consolidate_runs_step()?;
                }
                "prune" => {
                    if db.start_prune()? {
                        while db.prune_step()? {}
                    }
                }
                "reclaim" => {
                    db.reclaim_pack_step()?;
                }
                "merge" => {
                    db.merge_postings()?;
                }
                "convert" => {
                    db.convert_clustered(conversion())?;
                }
                _ => unreachable!(),
            }
            while db.cleanup_step(3)? > 0 {}
            Ok(())
        }

        /// Exactly the selected root's manifests remain after cleanup.
        fn manifests_match_runs<S: ObjectStore>(
            db: &SegmentedDatabase<S>,
            store: &Memory,
            context: &str,
        ) {
            let referenced: BTreeSet<_> = db
                .root
                .runs
                .iter()
                .map(|run| run.manifest.as_ref().expect("bound").key.clone())
                .collect();
            let stored: BTreeSet<_> = store.keys(PREFIX).into_iter().collect();
            assert_eq!(stored, referenced, "{context}");
        }

        /// Every legacy root version opens with the same state and search
        /// results, and every kind of first publication upgrades it to v5
        /// with equal state before and after a reopen.
        #[test]
        fn legacy_roots_open_equal_and_upgrade_on_every_publication_kind() {
            for version in [1, 2, 4] {
                let (store, model) = namespace(version);
                assert_eq!(store.root_version(), 5);
                let current = open(store.clone());
                check(&current, &model, &format!("v{version} source"));
                let old = legacy(&store, version);
                let legacy_db = open(old.clone());
                assert_eq!(legacy_db.sequence(), current.sequence());
                assert_eq!(
                    legacy_db
                        .root
                        .runs
                        .iter()
                        .map(|run| &run.blocks)
                        .collect::<Vec<_>>(),
                    current
                        .root
                        .runs
                        .iter()
                        .map(|run| &run.blocks)
                        .collect::<Vec<_>>()
                );
                check(&legacy_db, &model, &format!("v{version} legacy"));
                drop((current, legacy_db));
                for kind in KINDS {
                    if kind == "merge" && version != 4 {
                        continue;
                    }
                    let context = format!("v{version} {kind}");
                    let copy = old.copy();
                    let mut db = open(copy.clone());
                    let generation = db.root.generation;
                    operate(&mut db, kind).unwrap();
                    assert!(
                        db.root.generation > generation,
                        "{context}: nothing published"
                    );
                    assert_eq!(copy.root_version(), 5, "{context}");
                    check(&db, &model, &context);
                    manifests_match_runs(&db, &copy, &context);
                    drop(db);
                    let db = open(copy.clone());
                    check(&db, &model, &format!("{context}, reopened"));
                }
            }
        }

        /// Every create and cleanup removal of an upgrading publication fails
        /// before or after landing, and every removal is also delayed until
        /// the run ends. Reopening selects whichever root exists with the
        /// acknowledged state; the publication then completes and cleanup
        /// leaves exactly the selected root's manifests.
        #[test]
        fn every_upgrade_create_and_removal_failure_recovers() {
            for version in [1, 2, 4] {
                let (store, model) = namespace(version);
                let old = legacy(&store, version);
                for kind in KINDS {
                    if kind == "merge" && version != 4 {
                        continue;
                    }
                    let plan = Arc::new(Mutex::new(Plan::default()));
                    let mut db = open(FaultStore {
                        inner: old.copy(),
                        plan: plan.clone(),
                    });
                    plan.lock().unwrap().armed = true;
                    operate(&mut db, kind).unwrap();
                    let (operations, removes) = {
                        let plan = plan.lock().unwrap();
                        (plan.operations, plan.removes.clone())
                    };
                    assert!(operations > 2, "v{version} {kind}: {operations}");
                    for (at, &remove) in removes.iter().enumerate() {
                        let faults: &[Fault] = if remove {
                            &[Fault::Before, Fault::After, Fault::Delayed]
                        } else {
                            &[Fault::Before, Fault::After]
                        };
                        for &fault in faults {
                            let context = format!("v{version} {kind}, operation {at}, {fault:?}");
                            let copy = old.copy();
                            let plan = Arc::new(Mutex::new(Plan {
                                fail_at: Some((at, fault)),
                                ..Plan::default()
                            }));
                            let mut db = open(FaultStore {
                                inner: copy.clone(),
                                plan: plan.clone(),
                            });
                            plan.lock().unwrap().armed = true;
                            let outcome = operate(&mut db, kind);
                            assert!(plan.lock().unwrap().fired, "{context}: not reached");
                            assert_eq!(outcome.is_err(), fault != Fault::Delayed, "{context}");
                            drop(db);
                            for key in std::mem::take(&mut plan.lock().unwrap().delayed) {
                                copy.remove(&key).unwrap();
                            }
                            let mut db = open(copy.clone());
                            check(&db, &model, &context);
                            operate(&mut db, kind).unwrap();
                            while db.cleanup_step(16).unwrap() > 0 {}
                            check(&db, &model, &format!("{context}, completed"));
                            assert_eq!(copy.root_version(), 5, "{context}");
                            manifests_match_runs(&db, &copy, &context);
                            drop(db);
                            let db = open(copy.clone());
                            check(&db, &model, &format!("{context}, reopened"));
                        }
                    }
                }
            }
        }

        /// A selected root's missing or corrupt manifest fails opening with
        /// a clear error instead of falling back to an older root; takeover
        /// reads only root headers, so it still fences.
        #[test]
        fn missing_or_corrupt_manifest_fails_open() {
            let (store, _) = namespace(2);
            let key = store.keys(PREFIX).pop().unwrap();
            let missing = store.copy();
            missing.remove(&key).unwrap();
            match SegmentedDatabase::open(missing.clone(), config()) {
                Err(Error::Corrupt(message)) => {
                    assert!(message.contains("manifest missing"), "{message}")
                }
                other => panic!("missing manifest opened: {:?}", other.err()),
            }
            let corrupt = store.copy();
            if let Some(byte) = corrupt.0.lock().unwrap().get_mut(&key).unwrap().last_mut() {
                *byte ^= 1;
            }
            match SegmentedDatabase::open(corrupt, config()) {
                Err(Error::Corrupt(message)) => {
                    assert!(message.contains("digest mismatch"), "{message}")
                }
                other => panic!("corrupt manifest opened: {:?}", other.err()),
            }
            let roots = missing.keys("sgroot-").len();
            assert!(matches!(
                SegmentedDatabase::take_over(missing.clone(), config()),
                Err(Error::Corrupt(_))
            ));
            assert_eq!(missing.keys("sgroot-").len(), roots + 1);
        }
    }
}
