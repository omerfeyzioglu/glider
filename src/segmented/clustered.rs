//! M37 clustered-view object formats. All integers and f32 values are little-endian.
//! The root remains JSON; centroids and catalogs use bounded binary layouts.
use super::{sketch, MAX_BLOCK_BYTES, MAX_PACK_BLOCKS, MAX_PACK_BYTES, MAX_SKETCH_BYTES};
use crate::{Config, Error, Metric, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// A present root field must be a real view; JSON `null` is not an omission.
pub(super) fn deserialize_view_ref<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<ViewRef>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    ViewRef::deserialize(deserializer).map(Some)
}

const CENTROID_MAGIC: &[u8; 8] = b"GLCENT01";
const CATALOG_MAGIC: &[u8; 8] = b"GLCLCAT1";
const MAX_CENTROIDS: usize = 4096;
const MAX_CENTROID_BYTES: usize = 2 * 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXTENTS: usize = 65_536;
const MAX_SAMPLE_ROWS: u32 = 16_384;

fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("invalid clustered {what}"))
}

fn valid_key(key: &str, prefix: &str) -> bool {
    key.strip_prefix(prefix).is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectRef {
    pub(super) key: String,
    pub(super) length: usize,
    pub(super) sha256: String,
}

impl ObjectRef {
    fn validate(&self, prefix: &str, max: usize) -> Result<()> {
        if !valid_key(&self.key, prefix)
            || self.length == 0
            || self.length > max
            || !valid_digest(&self.sha256)
        {
            return Err(corrupt("object reference"));
        }
        Ok(())
    }

    pub(super) fn authenticate(&self, bytes: &[u8]) -> Result<()> {
        if self.length != bytes.len() || format!("{:x}", Sha256::digest(bytes)) != self.sha256 {
            return Err(corrupt("object length or digest"));
        }
        Ok(())
    }
}

/// Root v4 selects one immutable centroid epoch and one complete catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ViewRef {
    pub(super) epoch: u64,
    pub(super) centroid: ObjectRef,
    pub(super) catalog: ObjectRef,
}

impl ViewRef {
    pub(super) fn validate(&self) -> Result<()> {
        if self.epoch == 0 {
            return Err(corrupt("epoch"));
        }
        self.centroid.validate("sgcentroid-", MAX_CENTROID_BYTES)?;
        self.catalog.validate("sgcluster-", MAX_CATALOG_BYTES)
    }

    pub(super) fn decode_centroids(&self, config: Config, bytes: &[u8]) -> Result<Centroids> {
        self.validate()?;
        self.centroid.authenticate(bytes)?;
        let centers = Centroids::decode(config, bytes)?;
        if centers.epoch != self.epoch {
            return Err(corrupt("centroid epoch"));
        }
        Ok(centers)
    }

    pub(super) fn decode_catalog(&self, centers: &Centroids, bytes: &[u8]) -> Result<Catalog> {
        self.validate()?;
        self.catalog.authenticate(bytes)?;
        if centers.epoch != self.epoch {
            return Err(corrupt("centroid epoch"));
        }
        Catalog::decode(centers, bytes)
    }
}

struct Reader<'a> {
    left: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.left.len() < count {
            return Err(corrupt("length"));
        }
        let (head, tail) = self.left.split_at(count);
        self.left = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
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

    fn digest(&mut self) -> Result<[u8; 32]> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn finish(self) -> Result<()> {
        if !self.left.is_empty() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(())
    }
}

fn metric_byte(metric: Metric) -> u8 {
    match metric {
        Metric::SquaredEuclidean => 0,
        Metric::Manhattan => 1,
        Metric::Cosine => 2,
    }
}

#[derive(Clone, Debug)]
pub(super) struct Center {
    pub(super) id: u32,
    pub(super) coordinates: Vec<f32>,
}

#[derive(Clone, Debug)]
pub(super) struct Centroids {
    pub(super) config: Config,
    pub(super) source_generation: u64,
    pub(super) source_sequence: u64,
    pub(super) seed: u64,
    /// v1 sample rule 1: smallest seeded hash priorities among live IDs.
    pub(super) sample_rule: u8,
    pub(super) sample_rows: u32,
    pub(super) iterations: u32,
    pub(super) sample_sha256: [u8; 32],
    pub(super) epoch: u64,
    pub(super) centers: Vec<Center>,
}

impl Centroids {
    fn validate(&self, config: Config) -> Result<()> {
        if self.config != config
            || self.config.dimensions == 0
            || self.config.dimensions > u32::MAX as usize
            || self.epoch == 0
            || self.sample_rule != 1
            || self.sample_rows == 0
            || self.sample_rows > MAX_SAMPLE_ROWS
            || self.iterations == 0
            || self.centers.is_empty()
            || self.centers.len() > MAX_CENTROIDS
            || self.centers.len() > self.sample_rows as usize
        {
            return Err(corrupt("centroid header"));
        }
        let mut ids = BTreeSet::new();
        for center in &self.centers {
            if !ids.insert(center.id)
                || center.coordinates.len() != config.dimensions
                || center.coordinates.iter().any(|value| !value.is_finite())
            {
                return Err(corrupt("center"));
            }
            if config.metric == Metric::Cosine {
                let norm = center
                    .coordinates
                    .iter()
                    .map(|&value| f64::from(value).powi(2))
                    .sum::<f64>();
                if (norm - 1.0).abs() > 1e-4 {
                    return Err(corrupt("cosine center norm"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        self.validate(self.config)?;
        let length = self
            .config
            .dimensions
            .checked_mul(4)
            .and_then(|width| width.checked_add(4))
            .and_then(|width| width.checked_mul(self.centers.len()))
            .and_then(|body| body.checked_add(90))
            .ok_or_else(|| corrupt("centroid size"))?;
        if length > MAX_CENTROID_BYTES {
            return Err(corrupt("centroid size"));
        }
        let mut out = Vec::with_capacity(length);
        out.extend_from_slice(CENTROID_MAGIC);
        out.extend_from_slice(&(self.config.dimensions as u32).to_le_bytes());
        out.push(metric_byte(self.config.metric));
        out.extend_from_slice(&self.source_generation.to_le_bytes());
        out.extend_from_slice(&self.source_sequence.to_le_bytes());
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.push(self.sample_rule);
        out.extend_from_slice(&self.sample_rows.to_le_bytes());
        out.extend_from_slice(&self.iterations.to_le_bytes());
        out.extend_from_slice(&self.sample_sha256);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&(self.centers.len() as u32).to_le_bytes());
        for center in &self.centers {
            out.extend_from_slice(&center.id.to_le_bytes());
            for value in &center.coordinates {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        debug_assert_eq!(out.len(), length);
        Ok(out)
    }

    pub(super) fn decode(config: Config, bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CENTROID_BYTES {
            return Err(corrupt("centroid size"));
        }
        let mut input = Reader { left: bytes };
        if input.take(8)? != CENTROID_MAGIC
            || input.u32()? as usize != config.dimensions
            || input.u8()? != metric_byte(config.metric)
        {
            return Err(corrupt("centroid identity"));
        }
        let source_generation = input.u64()?;
        let source_sequence = input.u64()?;
        let seed = input.u64()?;
        let sample_rule = input.u8()?;
        let sample_rows = input.u32()?;
        let iterations = input.u32()?;
        let sample_sha256 = input.digest()?;
        let epoch = input.u64()?;
        let count = input.u32()? as usize;
        let row_len = config
            .dimensions
            .checked_mul(4)
            .and_then(|length| length.checked_add(4))
            .ok_or_else(|| corrupt("centroid length"))?;
        if count == 0 || count > MAX_CENTROIDS || input.left.len() != count.saturating_mul(row_len)
        {
            return Err(corrupt("centroid length"));
        }
        let mut centers = Vec::with_capacity(count);
        for _ in 0..count {
            let id = input.u32()?;
            let mut coordinates = Vec::with_capacity(config.dimensions);
            for _ in 0..config.dimensions {
                coordinates.push(f32::from_bits(input.u32()?));
            }
            centers.push(Center { id, coordinates });
        }
        input.finish()?;
        let result = Self {
            config,
            source_generation,
            source_sequence,
            seed,
            sample_rule,
            sample_rows,
            iterations,
            sample_sha256,
            epoch,
            centers,
        };
        result.validate(config)?;
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub(super) struct CatalogBlock {
    pub(super) offset: u32,
    pub(super) length: u32,
    pub(super) sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExtentKind {
    Canonical,
    Derived,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PostingRole {
    Primary,
    Secondary,
}

#[derive(Clone, Debug)]
pub(super) struct Extent {
    pub(super) pack: String,
    pub(super) payload_len: u32,
    pub(super) offset: u32,
    pub(super) length: u32,
    pub(super) rows: u32,
    pub(super) epoch: u64,
    pub(super) cluster_id: u32,
    pub(super) kind: ExtentKind,
    pub(super) role: PostingRole,
    pub(super) blocks: Vec<CatalogBlock>,
}

#[derive(Clone, Debug)]
pub(super) struct Cluster {
    pub(super) id: u32,
    pub(super) extents: Vec<Extent>,
}

#[derive(Clone, Debug)]
pub(super) struct Catalog {
    pub(super) epoch: u64,
    pub(super) clusters: Vec<Cluster>,
}

struct PackRanges {
    payload_len: u32,
    ranges: Vec<(u32, u32)>,
    blocks: usize,
    block_bytes: usize,
}

impl Catalog {
    fn validate(&self, centers: &Centroids) -> Result<()> {
        let ids: BTreeSet<_> = centers.centers.iter().map(|center| center.id).collect();
        self.validate_for(centers.epoch, &ids)
    }

    /// Validate against a view's epoch and its unique center IDs.
    fn validate_for(&self, epoch: u64, ids: &BTreeSet<u32>) -> Result<()> {
        if self.epoch != epoch || self.clusters.len() != ids.len() {
            return Err(corrupt("catalog epoch or clusters"));
        }
        let mut previous_id = None;
        let mut pack_ranges: BTreeMap<&str, PackRanges> = BTreeMap::new();
        let mut total_extents = 0;
        for cluster in &self.clusters {
            if !ids.contains(&cluster.id) || previous_id.is_some_and(|id| id >= cluster.id) {
                return Err(corrupt("catalog cluster order"));
            }
            previous_id = Some(cluster.id);
            total_extents += cluster.extents.len();
            if total_extents > MAX_EXTENTS {
                return Err(corrupt("catalog extent count"));
            }
            let mut previous_extent: Option<(&str, u32)> = None;
            for extent in &cluster.extents {
                let end = extent
                    .offset
                    .checked_add(extent.length)
                    .ok_or_else(|| corrupt("extent range"))?;
                if !valid_key(&extent.pack, "sgpack-")
                    || extent.payload_len as usize
                        > MAX_PACK_BYTES + sketch::FRAME_HEADER + MAX_SKETCH_BYTES
                    || extent.length == 0
                    || extent.length as usize > MAX_PACK_BYTES
                    || end > extent.payload_len
                    || extent.rows == 0
                    || extent.epoch != self.epoch
                    || extent.cluster_id != cluster.id
                    || (extent.kind == ExtentKind::Canonical && extent.role != PostingRole::Primary)
                    || extent.blocks.is_empty()
                    || extent.blocks.len() > MAX_PACK_BLOCKS
                    || previous_extent.is_some_and(|(pack, offset)| {
                        (pack, offset) >= (extent.pack.as_str(), extent.offset)
                    })
                {
                    return Err(corrupt("extent"));
                }
                previous_extent = Some((&extent.pack, extent.offset));
                let mut cursor = extent.offset;
                for block in &extent.blocks {
                    if block.offset != cursor
                        || block.length == 0
                        || block.length as usize > MAX_BLOCK_BYTES
                    {
                        return Err(corrupt("extent block"));
                    }
                    cursor = cursor
                        .checked_add(block.length)
                        .ok_or_else(|| corrupt("extent block range"))?;
                }
                if cursor != end
                    || extent.rows < extent.blocks.len() as u32
                    || extent.rows > (extent.blocks.len() as u32) * 170
                {
                    return Err(corrupt("extent block coverage"));
                }
                let entry = pack_ranges
                    .entry(&extent.pack)
                    .or_insert_with(|| PackRanges {
                        payload_len: extent.payload_len,
                        ranges: Vec::new(),
                        blocks: 0,
                        block_bytes: 0,
                    });
                if entry.payload_len != extent.payload_len {
                    return Err(corrupt("pack payload length"));
                }
                entry.ranges.push((extent.offset, end));
                entry.blocks += extent.blocks.len();
                entry.block_bytes += extent.length as usize;
                if entry.blocks > MAX_PACK_BLOCKS || entry.block_bytes > MAX_PACK_BYTES {
                    return Err(corrupt("pack bounds"));
                }
            }
        }
        for pack in pack_ranges.values_mut() {
            pack.ranges.sort_unstable();
            if pack.ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return Err(corrupt("overlapping pack extents"));
            }
        }
        Ok(())
    }

    pub(super) fn encode(&self, centers: &Centroids) -> Result<Vec<u8>> {
        self.validate(centers)?;
        self.encode_validated()
    }

    fn encode_validated(&self) -> Result<Vec<u8>> {
        let mut length = 20_usize;
        for cluster in &self.clusters {
            length = length.saturating_add(8);
            for extent in &cluster.extents {
                let key_len = extent.pack.len();
                if key_len > u16::MAX as usize {
                    return Err(corrupt("pack key length"));
                }
                length = length
                    .saturating_add(36)
                    .saturating_add(key_len)
                    .saturating_add(extent.blocks.len().saturating_mul(40));
                if length > MAX_CATALOG_BYTES {
                    return Err(corrupt("catalog size"));
                }
            }
        }
        if length > MAX_CATALOG_BYTES {
            return Err(corrupt("catalog size"));
        }
        let mut out = Vec::with_capacity(length);
        out.extend_from_slice(CATALOG_MAGIC);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&(self.clusters.len() as u32).to_le_bytes());
        for cluster in &self.clusters {
            out.extend_from_slice(&cluster.id.to_le_bytes());
            out.extend_from_slice(&(cluster.extents.len() as u32).to_le_bytes());
            for extent in &cluster.extents {
                let key = extent.pack.as_bytes();
                let key_len = u16::try_from(key.len()).map_err(|_| corrupt("pack key length"))?;
                out.extend_from_slice(&key_len.to_le_bytes());
                out.extend_from_slice(key);
                for value in [
                    extent.payload_len,
                    extent.offset,
                    extent.length,
                    extent.rows,
                ] {
                    out.extend_from_slice(&value.to_le_bytes());
                }
                out.extend_from_slice(&extent.epoch.to_le_bytes());
                out.extend_from_slice(&extent.cluster_id.to_le_bytes());
                out.push(match extent.kind {
                    ExtentKind::Canonical => 0,
                    ExtentKind::Derived => 1,
                });
                out.push(match extent.role {
                    PostingRole::Primary => 0,
                    PostingRole::Secondary => 1,
                });
                out.extend_from_slice(&(extent.blocks.len() as u32).to_le_bytes());
                for block in &extent.blocks {
                    out.extend_from_slice(&block.offset.to_le_bytes());
                    out.extend_from_slice(&block.length.to_le_bytes());
                    out.extend_from_slice(&block.sha256);
                }
            }
        }
        debug_assert_eq!(out.len(), length);
        Ok(out)
    }

    pub(super) fn decode(centers: &Centroids, bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(corrupt("catalog size"));
        }
        let mut input = Reader { left: bytes };
        if input.take(8)? != CATALOG_MAGIC {
            return Err(corrupt("catalog magic"));
        }
        let epoch = input.u64()?;
        let count = input.u32()? as usize;
        if count != centers.centers.len() || count > MAX_CENTROIDS {
            return Err(corrupt("catalog count"));
        }
        let mut clusters = Vec::with_capacity(count);
        let mut total_extents = 0;
        for _ in 0..count {
            let id = input.u32()?;
            let extent_count = input.u32()? as usize;
            total_extents += extent_count;
            if total_extents > MAX_EXTENTS || extent_count > input.left.len() / 47 {
                return Err(corrupt("catalog extent count"));
            }
            let mut extents = Vec::with_capacity(extent_count);
            for _ in 0..extent_count {
                let key_len = input.u16()? as usize;
                let pack = std::str::from_utf8(input.take(key_len)?)
                    .map_err(|_| corrupt("pack key"))?
                    .to_owned();
                let payload_len = input.u32()?;
                let offset = input.u32()?;
                let length = input.u32()?;
                let rows = input.u32()?;
                let extent_epoch = input.u64()?;
                let cluster_id = input.u32()?;
                let kind = match input.u8()? {
                    0 => ExtentKind::Canonical,
                    1 => ExtentKind::Derived,
                    _ => return Err(corrupt("extent kind")),
                };
                let role = match input.u8()? {
                    0 => PostingRole::Primary,
                    1 => PostingRole::Secondary,
                    _ => return Err(corrupt("posting role")),
                };
                let block_count = input.u32()? as usize;
                if block_count == 0
                    || block_count > MAX_PACK_BLOCKS
                    || block_count > input.left.len() / 40
                {
                    return Err(corrupt("extent block count"));
                }
                let mut blocks = Vec::with_capacity(block_count);
                for _ in 0..block_count {
                    blocks.push(CatalogBlock {
                        offset: input.u32()?,
                        length: input.u32()?,
                        sha256: input.digest()?,
                    });
                }
                extents.push(Extent {
                    pack,
                    payload_len,
                    offset,
                    length,
                    rows,
                    epoch: extent_epoch,
                    cluster_id,
                    kind,
                    role,
                    blocks,
                });
            }
            clusters.push(Cluster { id, extents });
        }
        input.finish()?;
        let catalog = Self { epoch, clusters };
        catalog.validate(centers)?;
        Ok(catalog)
    }
}

impl Catalog {
    /// This catalog without the extents named by `(pack, offset)` in
    /// `removed` and with `added`, each cluster's extents in `(pack,
    /// offset)` order. The result is validated when encoded.
    pub(super) fn with_changes(
        &self,
        removed: &BTreeSet<(String, u32)>,
        added: Vec<Extent>,
    ) -> Result<Self> {
        let mut clusters = self.clusters.clone();
        for cluster in &mut clusters {
            cluster
                .extents
                .retain(|extent| !removed.contains(&(extent.pack.clone(), extent.offset)));
        }
        for extent in added {
            let index = clusters
                .binary_search_by_key(&extent.cluster_id, |cluster| cluster.id)
                .map_err(|_| corrupt("extent cluster"))?;
            clusters[index].extents.push(extent);
        }
        for cluster in &mut clusters {
            cluster
                .extents
                .sort_by(|a, b| (&a.pack, a.offset).cmp(&(&b.pack, b.offset)));
        }
        Ok(Self {
            epoch: self.epoch,
            clusters,
        })
    }
}

/// The extents of one posting pack's blocks, given in pack order: each run
/// of adjacent blocks of one cluster is one extent.
pub(super) fn extents_of(
    references: &[super::BlockRef],
    epoch: u64,
    kind: ExtentKind,
) -> Result<Vec<Extent>> {
    let mut extents: Vec<Extent> = Vec::new();
    for reference in references {
        let block = CatalogBlock {
            offset: u32::try_from(reference.offset).map_err(|_| corrupt("extent offset"))?,
            length: u32::try_from(reference.length).map_err(|_| corrupt("extent length"))?,
            sha256: sketch::digest_bytes(&reference.sha256)?,
        };
        let rows = u32::try_from(reference.rows).map_err(|_| corrupt("extent rows"))?;
        match extents.last_mut() {
            Some(extent)
                if extent.pack == reference.object
                    && extent.cluster_id == reference.partition
                    && extent.offset + extent.length == block.offset =>
            {
                extent.length += block.length;
                extent.rows += rows;
                extent.blocks.push(block);
            }
            _ => extents.push(Extent {
                pack: reference.object.clone(),
                payload_len: u32::try_from(reference.payload_len)
                    .map_err(|_| corrupt("pack length"))?,
                offset: block.offset,
                length: block.length,
                rows,
                epoch,
                cluster_id: reference.partition,
                kind,
                role: PostingRole::Primary,
                blocks: vec![block],
            }),
        }
    }
    Ok(extents)
}

/// A reference binding `key` to the length and SHA-256 of `bytes`.
pub(super) fn object_ref(key: String, bytes: &[u8]) -> ObjectRef {
    ObjectRef {
        key,
        length: bytes.len(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
    }
}

/// Identity of one center that posting sketches bind to: SHA-256 of the
/// metric byte, the dimension (u32), the center ID (u32) and the f32
/// coordinates, all little-endian. A posting block is valid for a view only
/// if this matches the view's center for its cluster.
pub(super) fn fingerprint(metric: Metric, center: &Center) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update([metric_byte(metric)]);
    hash.update((center.coordinates.len() as u32).to_le_bytes());
    hash.update(center.id.to_le_bytes());
    for value in &center.coordinates {
        hash.update(value.to_le_bytes());
    }
    hash.finalize().into()
}

/// The decoded clustered view a root v4 selects: centers in cluster-ID order
/// and the catalog. Every live sealed version not shadowed by the log tail
/// has exactly one current posting row in the catalog's extents, except
/// versions a stage 3 binary sealed after a conversion, which its canonical
/// sketches route (`load_routing` derives which from the postings).
pub(super) struct ClusterIndex {
    pub(super) epoch: u64,
    pub(super) ids: Vec<u32>,
    centers: Vec<Vec<f32>>,
    pub(super) fingerprints: BTreeMap<u32, [u8; 32]>,
    pub(super) catalog: Catalog,
    /// Posting pack keys and payload lengths.
    pub(super) packs: BTreeMap<String, u32>,
}

fn catalog_packs(catalog: &Catalog) -> BTreeMap<String, u32> {
    let mut packs = BTreeMap::new();
    for extent in catalog.clusters.iter().flat_map(|cluster| &cluster.extents) {
        packs.insert(extent.pack.clone(), extent.payload_len);
    }
    packs
}

impl ClusterIndex {
    pub(super) fn new(centroids: &Centroids, catalog: Catalog) -> Self {
        let mut centers: Vec<_> = centroids.centers.iter().collect();
        centers.sort_by_key(|center| center.id);
        Self {
            epoch: centroids.epoch,
            ids: centers.iter().map(|center| center.id).collect(),
            fingerprints: centers
                .iter()
                .map(|center| (center.id, fingerprint(centroids.config.metric, center)))
                .collect(),
            centers: centers
                .into_iter()
                .map(|center| center.coordinates.clone())
                .collect(),
            packs: catalog_packs(&catalog),
            catalog,
        }
    }

    /// The same centers with another catalog of this epoch.
    pub(super) fn with_catalog(&self, catalog: Catalog) -> Self {
        Self {
            epoch: self.epoch,
            ids: self.ids.clone(),
            centers: self.centers.clone(),
            fingerprints: self.fingerprints.clone(),
            packs: catalog_packs(&catalog),
            catalog,
        }
    }

    /// Validate and encode a catalog for this view's epoch and centers.
    pub(super) fn encode_catalog(&self, catalog: &Catalog) -> Result<Vec<u8>> {
        let ids: BTreeSet<u32> = self.ids.iter().copied().collect();
        catalog.validate_for(self.epoch, &ids)?;
        catalog.encode_validated()
    }

    /// The nearest center's ID for each vector, ties by cluster ID.
    pub(super) fn assign(&self, metric: Metric, vectors: &[&[f32]]) -> Vec<u32> {
        super::convert::assign(metric, &self.centers, vectors)
            .into_iter()
            .map(|index| self.ids[usize::from(index)])
            .collect()
    }

    /// IDs of the `count` clusters nearest to `query`, ordered by
    /// `(routing score, cluster ID)`.
    pub(super) fn probe(&self, metric: Metric, query: &[f32], count: usize) -> Vec<u32> {
        crate::ivf::nearest_centers(metric, query, &self.centers, count)
            .into_iter()
            .map(|(index, _)| self.ids[index])
            .collect()
    }

    /// Each posting pack's catalog blocks in offset order, as
    /// `(offset, length, SHA-256)`, with the extent rows they must hold.
    pub(super) fn pack_blocks(&self) -> BTreeMap<&str, PackLayout> {
        let mut packs: BTreeMap<&str, PackLayout> = BTreeMap::new();
        for extent in self.catalog.clusters.iter().flat_map(|c| &c.extents) {
            let layout = packs.entry(&extent.pack).or_default();
            layout.payload_len = extent.payload_len as usize;
            layout.extents.push((
                layout.blocks.len(),
                extent.blocks.len(),
                extent.rows as usize,
                extent.cluster_id,
            ));
            for block in &extent.blocks {
                layout
                    .blocks
                    .push((block.offset as usize, block.length as usize, block.sha256));
            }
        }
        for layout in packs.values_mut() {
            let mut order: Vec<usize> = (0..layout.blocks.len()).collect();
            order.sort_by_key(|&index| layout.blocks[index].0);
            let position: BTreeMap<usize, usize> = order
                .iter()
                .enumerate()
                .map(|(at, &index)| (index, at))
                .collect();
            layout.blocks = order.iter().map(|&index| layout.blocks[index]).collect();
            for extent in &mut layout.extents {
                extent.0 = position[&extent.0];
            }
        }
        packs
    }
}

/// One posting pack's catalog layout: blocks in offset order and, per
/// extent, `(first block, block count, rows, cluster ID)`.
#[derive(Default)]
pub(super) struct PackLayout {
    pub(super) payload_len: usize,
    pub(super) blocks: Vec<(usize, usize, [u8; 32])>,
    pub(super) extents: Vec<(usize, usize, usize, u32)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        }
    }

    fn centers() -> Centroids {
        Centroids {
            config: config(),
            source_generation: 3,
            source_sequence: 17,
            seed: 42,
            sample_rule: 1,
            sample_rows: 2,
            iterations: 2,
            sample_sha256: [7; 32],
            epoch: 5,
            centers: vec![
                Center {
                    id: 2,
                    coordinates: vec![1., 2.],
                },
                Center {
                    id: 9,
                    coordinates: vec![3., 4.],
                },
            ],
        }
    }

    fn catalog() -> Catalog {
        Catalog {
            epoch: 5,
            clusters: vec![
                Cluster {
                    id: 2,
                    extents: vec![Extent {
                        pack: "sgpack-attempt-00000000".into(),
                        payload_len: 256,
                        offset: 64,
                        length: 100,
                        rows: 3,
                        epoch: 5,
                        cluster_id: 2,
                        kind: ExtentKind::Derived,
                        role: PostingRole::Primary,
                        blocks: vec![
                            CatalogBlock {
                                offset: 64,
                                length: 40,
                                sha256: [1; 32],
                            },
                            CatalogBlock {
                                offset: 104,
                                length: 60,
                                sha256: [2; 32],
                            },
                        ],
                    }],
                },
                Cluster {
                    id: 9,
                    extents: Vec::new(),
                },
            ],
        }
    }

    fn reference(key: &str, bytes: &[u8]) -> ObjectRef {
        ObjectRef {
            key: key.into(),
            length: bytes.len(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    fn view(centroid_bytes: &[u8], catalog_bytes: &[u8]) -> ViewRef {
        ViewRef {
            epoch: 5,
            centroid: reference("sgcentroid-attempt", centroid_bytes),
            catalog: reference("sgcluster-attempt", catalog_bytes),
        }
    }

    #[test]
    fn authenticated_formats_reject_lengths_digests_epochs_and_extents() {
        let centers = centers();
        let centroid_bytes = centers.encode().unwrap();
        let catalog = catalog();
        let catalog_bytes = catalog.encode(&centers).unwrap();
        let view = view(&centroid_bytes, &catalog_bytes);
        assert_eq!(
            view.decode_centroids(config(), &centroid_bytes)
                .unwrap()
                .centers
                .len(),
            2
        );
        assert_eq!(
            view.decode_catalog(&centers, &catalog_bytes)
                .unwrap()
                .clusters
                .len(),
            2
        );
        let mut root_zero_source = centers.clone();
        root_zero_source.source_generation = 0;
        root_zero_source.source_sequence = 0;
        let bytes = root_zero_source.encode().unwrap();
        assert_eq!(
            Centroids::decode(config(), &bytes).unwrap().source_sequence,
            0
        );

        let mut bad = view.clone();
        bad.centroid.length += 1;
        assert!(matches!(
            bad.decode_centroids(config(), &centroid_bytes),
            Err(Error::Corrupt(_))
        ));
        let mut bad = view.clone();
        bad.catalog.sha256 = "0".repeat(64);
        assert!(matches!(
            bad.decode_catalog(&centers, &catalog_bytes),
            Err(Error::Corrupt(_))
        ));
        let mut bad = view.clone();
        bad.epoch += 1;
        assert!(matches!(
            bad.decode_centroids(config(), &centroid_bytes),
            Err(Error::Corrupt(_))
        ));

        let mut bad = centroid_bytes.clone();
        bad.truncate(bad.len() - 1);
        assert!(matches!(
            Centroids::decode(config(), &bad),
            Err(Error::Corrupt(_))
        ));
        let mut bad = centroid_bytes.clone();
        // Epoch starts after the 32-byte sample digest.
        bad[78..86].copy_from_slice(&0_u64.to_le_bytes());
        assert!(matches!(
            Centroids::decode(config(), &bad),
            Err(Error::Corrupt(_))
        ));
        let mut bad = centers.clone();
        bad.centers[1].id = 2;
        assert!(matches!(bad.encode(), Err(Error::Corrupt(_))));
        let mut bad = centers.clone();
        bad.centers[0].coordinates[0] = f32::NAN;
        assert!(matches!(bad.encode(), Err(Error::Corrupt(_))));
        let mut bad = centers.clone();
        bad.config.metric = Metric::Cosine;
        assert!(matches!(bad.encode(), Err(Error::Corrupt(_))));

        let mut bad = catalog_bytes.clone();
        bad.truncate(bad.len() - 1);
        assert!(matches!(
            Catalog::decode(&centers, &bad),
            Err(Error::Corrupt(_))
        ));
        let mut bad = catalog.clone();
        bad.epoch += 1;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].blocks[1].offset += 1;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].length = 99;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].epoch += 1;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        let duplicate = bad.clusters[0].extents[0].clone();
        bad.clusters[0].extents.push(duplicate);
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        let first = bad.clusters[0].extents[0].clone();
        bad.clusters[1].extents.push(Extent {
            cluster_id: 9,
            offset: 120,
            length: 20,
            rows: 1,
            blocks: vec![CatalogBlock {
                offset: 120,
                length: 20,
                sha256: [3; 32],
            }],
            ..first
        });
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].kind = ExtentKind::Canonical;
        bad.clusters[0].extents[0].role = PostingRole::Secondary;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].payload_len = 80;
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
        let mut bad = catalog.clone();
        bad.clusters[0].extents[0].pack = "../escape".into();
        assert!(matches!(bad.encode(&centers), Err(Error::Corrupt(_))));
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    #[test]
    fn decoders_do_not_panic_on_seeded_mutations() {
        let seed = 0x37ac_0149_bae5_9231_u64;
        let mut rng = Rng(seed);
        let centers = centers();
        let candidates = [
            ("centroid", centers.encode().unwrap()),
            ("catalog", catalog().encode(&centers).unwrap()),
        ];
        for (name, bytes) in candidates {
            let check = |candidate: &[u8], mutation: &str| {
                let result = std::panic::catch_unwind(|| {
                    if name == "centroid" {
                        Centroids::decode(config(), candidate).map(|value| value.encode())
                    } else {
                        Catalog::decode(&centers, candidate).map(|value| value.encode(&centers))
                    }
                })
                .unwrap_or_else(|_| panic!("{name} decoder panicked: seed {seed:#x}, {mutation}"));
                if let Ok(encoded) = result {
                    assert_eq!(
                        encoded.unwrap(),
                        candidate,
                        "seed {seed:#x}, {name}, {mutation}"
                    );
                }
            };
            for length in 0..bytes.len() {
                check(&bytes[..length], &format!("truncate {length}"));
            }
            for flip in 0..128 {
                let mut changed = bytes.clone();
                let offset = rng.next() as usize % changed.len();
                changed[offset] ^= 1 << (rng.next() % 8);
                check(&changed, &format!("flip {flip} at {offset}"));
            }
            let mut appended = bytes.clone();
            appended.push(0);
            check(&appended, "append");
            let mut huge = bytes.clone();
            if name == "centroid" {
                huge[86..90].copy_from_slice(&u32::MAX.to_le_bytes());
            } else {
                huge[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
            }
            check(&huge, "huge count");
        }
    }
}
