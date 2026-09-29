//! Disposable, bounded cache for root-authenticated immutable vector blocks.
use super::{decode_block_bytes, validate_block_ref, Block, BlockRef};
use crate::{store::ObjectStore, Config, Error, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
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
}

pub(super) struct BlockCache {
    directory: PathBuf,
    nvme_available: bool,
    ram_limit: usize,
    nvme_limit: usize,
    ram: BTreeMap<String, Vec<u8>>,
    ram_order: VecDeque<String>,
    ram_bytes: usize,
    nvme: BTreeMap<String, usize>,
    nvme_order: VecDeque<String>,
    nvme_bytes: usize,
    stats: CacheStats,
}

impl BlockCache {
    pub(super) fn open(base: &Path, ram_limit: usize, nvme_limit: usize) -> Self {
        let directory = base.join(DIRECTORY);
        let mut cache = Self {
            directory,
            nvme_available: true,
            ram_limit,
            nvme_limit,
            ram: BTreeMap::new(),
            ram_order: VecDeque::new(),
            ram_bytes: 0,
            nvme: BTreeMap::new(),
            nvme_order: VecDeque::new(),
            nvme_bytes: 0,
            stats: CacheStats::default(),
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
            if !metadata.file_type().is_file()
                || !valid_filename(&name)
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
            self.nvme_bytes += disk_charge(length);
            self.nvme.insert(name.clone(), length);
            self.nvme_order.push_back(name);
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
        validate_block_ref(reference)?;
        let name = filename(reference);
        if let Some(bytes) = self.ram.get(&name).cloned() {
            match decode_block_bytes(config, reference, &bytes) {
                Ok(block) => {
                    self.stats.ram_hits += 1;
                    self.stats.ram_payload_bytes += bytes.len() as u64;
                    touch(&mut self.ram_order, &name);
                    return Ok(block);
                }
                Err(_) => {
                    self.stats.corrupt_entries += 1;
                    self.remove_ram(&name);
                }
            }
        }
        if self.nvme.contains_key(&name) {
            match fs::read(self.directory.join(&name)) {
                Ok(bytes) => match decode_block_bytes(config, reference, &bytes) {
                    Ok(block) => {
                        self.stats.nvme_hits += 1;
                        self.stats.nvme_payload_bytes += bytes.len() as u64;
                        touch(&mut self.nvme_order, &name);
                        self.put_ram(&name, &bytes);
                        return Ok(block);
                    }
                    Err(_) => {
                        self.stats.corrupt_entries += 1;
                        self.remove_nvme(&name);
                    }
                },
                Err(_) => {
                    self.stats.cache_io_errors += 1;
                    self.remove_nvme(&name);
                }
            }
        }
        self.stats.remote_fetches += 1;
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
        self.stats.remote_payload_bytes += bytes.len() as u64;
        let block = decode_block_bytes(config, reference, &bytes)?;
        self.put_ram(&name, &bytes);
        self.put_nvme(&name, &bytes);
        Ok(block)
    }

    fn remove_ram(&mut self, name: &str) {
        if let Some(bytes) = self.ram.remove(name) {
            self.ram_bytes -= ram_charge(name, bytes.len());
        }
        self.ram_order.retain(|entry| entry != name);
    }

    fn put_ram(&mut self, name: &str, bytes: &[u8]) {
        let charge = ram_charge(name, bytes.len());
        if charge > self.ram_limit {
            return;
        }
        self.remove_ram(name);
        while self.ram_bytes + charge > self.ram_limit {
            let Some(oldest) = self.ram_order.pop_front() else {
                break;
            };
            self.remove_ram(&oldest);
        }
        self.ram_bytes += charge;
        self.ram.insert(name.to_owned(), bytes.to_vec());
        self.ram_order.push_back(name.to_owned());
    }

    fn remove_nvme(&mut self, name: &str) {
        if !self.safe_directory() {
            self.nvme_available = false;
            self.stats.cache_io_errors += 1;
            return;
        }
        let removed = match fs::remove_file(self.directory.join(name)) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(_) => {
                self.stats.cache_io_errors += 1;
                false
            }
        };
        if removed {
            if let Some(length) = self.nvme.remove(name) {
                self.nvme_bytes -= disk_charge(length);
            }
            self.nvme_order.retain(|entry| entry != name);
        }
    }

    fn trim_nvme(&mut self, additional: usize) -> Result<()> {
        if !self.safe_directory() {
            return Err(Error::Invalid("cache directory unavailable".into()));
        }
        while self.nvme_bytes.saturating_add(additional) > self.nvme_limit {
            let name = self
                .nvme_order
                .pop_front()
                .ok_or_else(|| Error::Invalid("cache eviction order missing".into()))?;
            let length = self
                .nvme
                .remove(&name)
                .ok_or_else(|| Error::Invalid("cache eviction entry missing".into()))?;
            match fs::remove_file(self.directory.join(&name)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    self.nvme.insert(name.clone(), length);
                    self.nvme_order.push_front(name);
                    return Err(error.into());
                }
            }
            self.nvme_bytes -= disk_charge(length);
        }
        Ok(())
    }

    fn put_nvme(&mut self, name: &str, bytes: &[u8]) {
        let charge = disk_charge(bytes.len());
        if !self.nvme_available || self.nvme.contains_key(name) || charge > self.nvme_limit {
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
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let temporary = self.directory.join(format!("{name}.{suffix}.tmp"));
        if fs::write(&temporary, bytes).is_err() {
            self.stats.cache_io_errors += 1;
            let _ = fs::remove_file(&temporary);
            return;
        }
        if fs::rename(&temporary, self.directory.join(name)).is_err() {
            self.stats.cache_io_errors += 1;
            let _ = fs::remove_file(&temporary);
            return;
        }
        self.nvme.insert(name.to_owned(), bytes.len());
        self.nvme_order.push_back(name.to_owned());
        self.nvme_bytes += charge;
    }

    fn safe_directory(&self) -> bool {
        fs::symlink_metadata(&self.directory).is_ok_and(|metadata| metadata.file_type().is_dir())
    }
}

fn filename(reference: &BlockRef) -> String {
    let mut hash = Sha256::new();
    hash.update(b"glider-block-cache-v1\0");
    hash.update(reference.object.as_bytes());
    hash.update([0]);
    hash.update(reference.offset.to_le_bytes());
    hash.update(reference.length.to_le_bytes());
    hash.update(reference.payload_len.to_le_bytes());
    hash.update(reference.sha256.as_bytes());
    format!("{:x}.blk", hash.finalize())
}

fn valid_filename(name: &str) -> bool {
    name.len() == 68
        && name.ends_with(".blk")
        && name[..64]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn ram_charge(name: &str, length: usize) -> usize {
    length.saturating_add(name.len()).saturating_add(128)
}

fn disk_charge(length: usize) -> usize {
    length.saturating_add(4095) / 4096 * 4096 + 4096
}

fn touch(order: &mut VecDeque<String>, name: &str) {
    order.retain(|entry| entry != name);
    order.push_back(name.to_owned());
}
