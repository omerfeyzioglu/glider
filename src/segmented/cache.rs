//! Disposable, bounded cache for root-authenticated immutable vector blocks.
use super::{authenticate, decode_block_bytes, validate_block_ref, Block, BlockRef, Root};
use crate::{store::ObjectStore, Config, Error, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

const DIRECTORY: &str = "glider-block-cache-v1";

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CacheStats {
    pub nvme_available: bool,
    pub ram_hits: u64,
    pub nvme_hits: u64,
    pub remote_fetches: u64,
    pub remote_payload_bytes: u64,
    pub ram_payload_bytes: u64,
    pub nvme_payload_bytes: u64,
    pub corrupt_entries: u64,
    pub cache_io_errors: u64,
    pub ram_bytes: usize,
    pub nvme_bytes: usize,
    pub ram_entries: usize,
    pub nvme_entries: usize,
    pub nvme_limit: usize,
    /// Cache charge of every block the selected root references, as of the
    /// current warm-up pass (zero before the first warm-up unit).
    pub namespace_bytes: usize,
    /// Charge of those blocks the pass found in or added to the NVMe tier.
    pub warm_bytes: usize,
    /// The pass visited every pack, or stopped at the NVMe limit or after a
    /// failed admission; `warm_bytes < namespace_bytes` then.
    pub warm_complete: bool,
    /// Range reads and payload bytes issued by warm-up, not by queries.
    pub warm_fetches: u64,
    pub warm_payload_bytes: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    Ram,
    Nvme,
    Remote,
}

pub(super) struct BlockCache {
    directory: PathBuf,
    nvme_available: bool,
    ram_limit: usize,
    nvme_limit: usize,
    // Entries are keyed by the 32-byte cache key; recency is a tick, so a
    // touch or eviction is O(log n) and file names are derived on demand.
    ram: BTreeMap<Key, (Vec<u8>, u64)>,
    ram_order: BTreeMap<u64, Key>,
    ram_bytes: usize,
    nvme: BTreeMap<Key, (usize, u64)>,
    nvme_order: BTreeMap<u64, Key>,
    nvme_bytes: usize,
    tick: u64,
    stats: CacheStats,
    /// Cached entries found missing or corrupt; a change restarts warm-up.
    lost: u64,
    warm: Option<WarmPass>,
}

/// One background pass copying the selected root's blocks into the NVMe tier.
struct WarmPass {
    generation: u64,
    lost: u64,
    /// Root `(run, block)` locations ordered by pack and offset.
    blocks: Vec<(usize, usize)>,
    /// Start of each pack in `blocks`, then `blocks.len()`.
    packs: Vec<usize>,
    next: usize,
    /// The whole root fits the NVMe limit, so admission may evict entries
    /// the pass has not touched; otherwise it only fills free space.
    evict: bool,
}

type Key = [u8; 32];

impl BlockCache {
    pub(super) fn open(base: &Path, ram_limit: usize, nvme_limit: usize) -> Self {
        let directory = base.join(DIRECTORY);
        let mut cache = Self {
            directory,
            nvme_available: true,
            ram_limit,
            nvme_limit,
            ram: BTreeMap::new(),
            ram_order: BTreeMap::new(),
            ram_bytes: 0,
            nvme: BTreeMap::new(),
            nvme_order: BTreeMap::new(),
            nvme_bytes: 0,
            tick: 0,
            stats: CacheStats {
                nvme_limit,
                ..CacheStats::default()
            },
            lost: 0,
            warm: None,
        };
        if cache.initialize_nvme().is_err() {
            cache.stats.cache_io_errors += 1;
            cache.nvme_available = false;
            cache.nvme.clear();
            cache.nvme_order.clear();
            cache.nvme_bytes = 0;
        }
        cache
    }

    fn initialize_nvme(&mut self) -> Result<()> {
        match fs::symlink_metadata(&self.directory) {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(Error::Invalid("cache path is not a directory".into()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&self.directory)?;
            }
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        if !self.safe_directory() {
            return Err(Error::Invalid("cache directory unavailable".into()));
        }
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let metadata = fs::symlink_metadata(entry.path())?;
            let key = parse_filename(&name);
            if !metadata.file_type().is_file()
                || key.is_none()
                || metadata.len() > super::MAX_BLOCK_BYTES as u64
            {
                if metadata.file_type().is_dir() {
                    fs::remove_dir_all(entry.path())?;
                } else {
                    fs::remove_file(entry.path())?;
                }
                continue;
            }
            let length = usize::try_from(metadata.len())
                .map_err(|_| Error::Invalid("cache file length exceeds usize".into()))?;
            let key = key.expect("checked above");
            self.nvme_bytes += disk_charge(length);
            self.tick += 1;
            self.nvme.insert(key, (length, self.tick));
            self.nvme_order.insert(self.tick, key);
            self.trim_nvme(0)?;
        }
        Ok(())
    }

    pub(super) fn stats(&self) -> CacheStats {
        CacheStats {
            nvme_available: self.nvme_available,
            ram_bytes: self.ram_bytes,
            nvme_bytes: self.nvme_bytes,
            ram_entries: self.ram.len(),
            nvme_entries: self.nvme.len(),
            ..self.stats
        }
    }

    pub(super) fn read_block<S: ObjectStore>(
        &mut self,
        store: &S,
        config: Config,
        reference: &BlockRef,
    ) -> Result<Block> {
        loop {
            let (bytes, source) = self.fetch(store, reference)?;
            match decode_block_bytes(config, reference, &bytes) {
                Ok(block) => {
                    self.accept(reference, source, &bytes);
                    return Ok(block);
                }
                Err(error) if source == Source::Remote => return Err(error),
                Err(_) => self.reject(reference, source),
            }
        }
    }

    /// Return candidate bytes from RAM, NVMe or object storage without
    /// authenticating them. The caller must decode against the root reference,
    /// then call `accept`, or `reject` for corrupt cached bytes and fetch again.
    pub(super) fn fetch<S: ObjectStore>(
        &mut self,
        store: &S,
        reference: &BlockRef,
    ) -> Result<(Vec<u8>, Source)> {
        if let Some(found) = self.lookup(reference)? {
            return Ok(found);
        }
        let bytes = store
            .get_range(
                &reference.object,
                reference.offset,
                reference.length,
                reference.payload_len,
            )?
            .ok_or_else(|| {
                Error::Corrupt(format!("segmented pack missing: {}", reference.object))
            })?;
        self.count_remote(bytes.len());
        Ok((bytes, Source::Remote))
    }

    /// Candidate bytes from RAM or NVMe, or `None` for a miss.
    pub(super) fn lookup(&mut self, reference: &BlockRef) -> Result<Option<(Vec<u8>, Source)>> {
        validate_block_ref(reference)?;
        let key = cache_key(reference);
        if let Some((bytes, _)) = self.ram.get(&key) {
            return Ok(Some((bytes.clone(), Source::Ram)));
        }
        if self.nvme.contains_key(&key) {
            match fs::read(self.directory.join(filename(&key))) {
                Ok(bytes) => return Ok(Some((bytes, Source::Nvme))),
                Err(_) => {
                    self.stats.cache_io_errors += 1;
                    self.lost += 1;
                    self.remove_nvme(&key);
                }
            }
        }
        Ok(None)
    }

    pub(super) fn count_remote(&mut self, bytes: usize) {
        self.stats.remote_fetches += 1;
        self.stats.remote_payload_bytes += bytes as u64;
    }

    /// Record authenticated bytes: count the hit or admit fetched bytes.
    pub(super) fn accept(&mut self, reference: &BlockRef, source: Source, bytes: &[u8]) {
        let key = cache_key(reference);
        match source {
            Source::Ram => {
                self.stats.ram_hits += 1;
                self.stats.ram_payload_bytes += bytes.len() as u64;
                self.tick += 1;
                if let Some((_, tick)) = self.ram.get_mut(&key) {
                    self.ram_order.remove(tick);
                    *tick = self.tick;
                    self.ram_order.insert(self.tick, key);
                }
            }
            Source::Nvme => {
                self.stats.nvme_hits += 1;
                self.stats.nvme_payload_bytes += bytes.len() as u64;
                self.tick += 1;
                if let Some((_, tick)) = self.nvme.get_mut(&key) {
                    self.nvme_order.remove(tick);
                    *tick = self.tick;
                    self.nvme_order.insert(self.tick, key);
                }
                self.put_ram(&key, bytes);
            }
            Source::Remote => {
                self.put_ram(&key, bytes);
                self.put_nvme(&key, bytes);
            }
        }
    }

    /// Discard cached bytes that failed authentication.
    pub(super) fn reject(&mut self, reference: &BlockRef, source: Source) {
        let key = cache_key(reference);
        self.stats.corrupt_entries += 1;
        self.lost += 1;
        match source {
            Source::Ram => self.remove_ram(&key),
            Source::Nvme => self.remove_nvme(&key),
            Source::Remote => {}
        }
    }

    /// One bounded warm-up unit: read one range of the next pack's uncached
    /// blocks (at most `unit_bytes`, but at least one block), authenticate
    /// each block against the root and admit it to the NVMe tier. Packs
    /// already cached are skipped without I/O, at most 64 per unit. Returns
    /// false once the pass has nothing left to do. A new root generation or
    /// a lost cached entry starts a new pass.
    pub(super) fn warm_step<S: ObjectStore>(
        &mut self,
        store: &S,
        root: &Root,
        unit_bytes: usize,
    ) -> Result<bool> {
        if !self.nvme_available || self.nvme_limit == 0 {
            return Ok(false);
        }
        let mut pass = match self.warm.take() {
            Some(pass) if pass.generation == root.generation && pass.lost == self.lost => pass,
            _ => self.plan_warm(root),
        };
        let result = self.advance_warm(&mut pass, store, root, unit_bytes);
        self.warm = Some(pass);
        result
    }

    fn plan_warm(&mut self, root: &Root) -> WarmPass {
        let mut blocks: Vec<_> = root
            .runs
            .iter()
            .enumerate()
            .flat_map(|(run, run_ref)| {
                run_ref
                    .blocks
                    .iter()
                    .enumerate()
                    .map(move |(ordinal, block)| {
                        (block.object.as_str(), block.offset, run, ordinal)
                    })
            })
            .collect();
        blocks.sort_unstable();
        let mut packs: Vec<usize> = (0..blocks.len())
            .filter(|&index| index == 0 || blocks[index - 1].0 != blocks[index].0)
            .collect();
        packs.push(blocks.len());
        let namespace_bytes = blocks
            .iter()
            .map(|&(_, _, run, ordinal)| disk_charge(root.runs[run].blocks[ordinal].length))
            .sum();
        self.stats.namespace_bytes = namespace_bytes;
        self.stats.warm_bytes = 0;
        self.stats.warm_complete = false;
        WarmPass {
            generation: root.generation,
            lost: self.lost,
            blocks: blocks
                .into_iter()
                .map(|(_, _, run, ordinal)| (run, ordinal))
                .collect(),
            packs,
            next: 0,
            evict: namespace_bytes <= self.nvme_limit,
        }
    }

    fn advance_warm<S: ObjectStore>(
        &mut self,
        pass: &mut WarmPass,
        store: &S,
        root: &Root,
        unit_bytes: usize,
    ) -> Result<bool> {
        if self.stats.warm_complete {
            return Ok(false);
        }
        for _ in 0..64 {
            let Some(&[first, end]) = pass.packs.get(pass.next..pass.next + 2) else {
                self.stats.warm_complete = true;
                return Ok(false);
            };
            let blocks: Vec<&BlockRef> = pass.blocks[first..end]
                .iter()
                .map(|&(run, ordinal)| &root.runs[run].blocks[ordinal])
                .collect();
            let mut missing = Vec::new();
            for &reference in &blocks {
                validate_block_ref(reference)?;
                if !self.hold_nvme(&cache_key(reference), pass.evict) {
                    missing.push(reference);
                }
            }
            if missing.is_empty() {
                self.stats.warm_bytes += blocks
                    .iter()
                    .map(|reference| disk_charge(reference.length))
                    .sum::<usize>();
                pass.next += 1;
                continue;
            }
            let start = missing[0].offset;
            let take = 1 + missing[1..]
                .iter()
                .take_while(|reference| reference.offset + reference.length - start <= unit_bytes)
                .count();
            let missing = &missing[..take];
            let charge: usize = missing
                .iter()
                .map(|reference| disk_charge(reference.length))
                .sum();
            if !pass.evict && self.nvme_bytes + charge > self.nvme_limit {
                self.stats.warm_complete = true;
                return Ok(false);
            }
            let last = missing[take - 1];
            let length = last.offset + last.length - start;
            let bytes = store
                .get_range(&last.object, start, length, last.payload_len)?
                .ok_or_else(|| {
                    Error::Corrupt(format!("segmented pack missing: {}", last.object))
                })?;
            self.stats.warm_fetches += 1;
            self.stats.warm_payload_bytes += bytes.len() as u64;
            for reference in missing {
                let at = reference.offset - start;
                let block = bytes
                    .get(at..at + reference.length)
                    .ok_or_else(|| Error::Corrupt("segmented range read is short".into()))?;
                authenticate(reference, block)?;
                self.put_nvme(&cache_key(reference), block);
            }
            // A failed admission (for example a full disk) ends the pass
            // instead of fetching the same blocks again.
            if !missing
                .iter()
                .all(|reference| self.nvme.contains_key(&cache_key(reference)))
            {
                self.stats.warm_complete = true;
                return Ok(false);
            }
            return Ok(true);
        }
        Ok(true)
    }

    /// Whether the NVMe tier holds `key`; if `touch`, mark it recently used.
    fn hold_nvme(&mut self, key: &Key, touch: bool) -> bool {
        let Some((_, tick)) = self.nvme.get_mut(key) else {
            return false;
        };
        if touch {
            self.tick += 1;
            self.nvme_order.remove(tick);
            *tick = self.tick;
            self.nvme_order.insert(self.tick, *key);
        }
        true
    }

    fn remove_ram(&mut self, key: &Key) {
        if let Some((bytes, tick)) = self.ram.remove(key) {
            self.ram_bytes -= ram_charge(bytes.len());
            self.ram_order.remove(&tick);
        }
    }

    fn put_ram(&mut self, key: &Key, bytes: &[u8]) {
        let charge = ram_charge(bytes.len());
        if charge > self.ram_limit {
            return;
        }
        self.remove_ram(key);
        while self.ram_bytes + charge > self.ram_limit {
            let Some((_, oldest)) = self.ram_order.pop_first() else {
                break;
            };
            self.remove_ram(&oldest);
        }
        self.ram_bytes += charge;
        self.tick += 1;
        self.ram.insert(*key, (bytes.to_vec(), self.tick));
        self.ram_order.insert(self.tick, *key);
    }

    fn remove_nvme(&mut self, key: &Key) {
        if !self.safe_directory() {
            self.nvme_available = false;
            self.stats.cache_io_errors += 1;
            return;
        }
        let removed = match fs::remove_file(self.directory.join(filename(key))) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(_) => {
                self.stats.cache_io_errors += 1;
                false
            }
        };
        if removed {
            if let Some((length, tick)) = self.nvme.remove(key) {
                self.nvme_bytes -= disk_charge(length);
                self.nvme_order.remove(&tick);
            }
        }
    }

    fn trim_nvme(&mut self, additional: usize) -> Result<()> {
        if !self.safe_directory() {
            return Err(Error::Invalid("cache directory unavailable".into()));
        }
        while self.nvme_bytes.saturating_add(additional) > self.nvme_limit {
            let (tick, key) = self
                .nvme_order
                .pop_first()
                .ok_or_else(|| Error::Invalid("cache eviction order missing".into()))?;
            let (length, _) = self
                .nvme
                .remove(&key)
                .ok_or_else(|| Error::Invalid("cache eviction entry missing".into()))?;
            match fs::remove_file(self.directory.join(filename(&key))) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    self.nvme.insert(key, (length, tick));
                    self.nvme_order.insert(tick, key);
                    return Err(error.into());
                }
            }
            self.nvme_bytes -= disk_charge(length);
        }
        Ok(())
    }

    fn put_nvme(&mut self, key: &Key, bytes: &[u8]) {
        let charge = disk_charge(bytes.len());
        if !self.nvme_available || self.nvme.contains_key(key) || charge > self.nvme_limit {
            return;
        }
        if self.trim_nvme(charge).is_err() {
            self.stats.cache_io_errors += 1;
            self.nvme_available = false;
            return;
        }
        if !self.safe_directory() {
            self.stats.cache_io_errors += 1;
            self.nvme_available = false;
            return;
        }
        let mut nonce = [0_u8; 16];
        if getrandom::getrandom(&mut nonce).is_err() {
            self.stats.cache_io_errors += 1;
            return;
        }
        let name = filename(key);
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let temporary = self.directory.join(format!("{name}.{suffix}.tmp"));
        if fs::write(&temporary, bytes).is_err() {
            self.stats.cache_io_errors += 1;
            let _ = fs::remove_file(&temporary);
            return;
        }
        if fs::rename(&temporary, self.directory.join(&name)).is_err() {
            self.stats.cache_io_errors += 1;
            let _ = fs::remove_file(&temporary);
            return;
        }
        self.tick += 1;
        self.nvme.insert(*key, (bytes.len(), self.tick));
        self.nvme_order.insert(self.tick, *key);
        self.nvme_bytes += charge;
    }

    fn safe_directory(&self) -> bool {
        fs::symlink_metadata(&self.directory).is_ok_and(|metadata| metadata.file_type().is_dir())
    }
}

fn cache_key(reference: &BlockRef) -> Key {
    let mut hash = Sha256::new();
    hash.update(b"glider-block-cache-v1\0");
    hash.update(reference.object.as_bytes());
    hash.update([0]);
    hash.update(reference.offset.to_le_bytes());
    hash.update(reference.length.to_le_bytes());
    hash.update(reference.payload_len.to_le_bytes());
    hash.update(reference.sha256.as_bytes());
    hash.finalize().into()
}

fn filename(key: &Key) -> String {
    let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{hex}.blk")
}

fn parse_filename(name: &str) -> Option<Key> {
    let hex = name.strip_suffix(".blk").filter(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })?;
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

/// A RAM entry is charged its bytes plus a fixed index overhead.
fn ram_charge(length: usize) -> usize {
    length.saturating_add(160)
}

fn disk_charge(length: usize) -> usize {
    length.saturating_add(4095) / 4096 * 4096 + 4096
}
