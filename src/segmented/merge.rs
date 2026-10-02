//! M37 stage 5: bounded posting merges of a clustered view.
//!
//! Every clustered seal adds a small canonical extent to most clusters, and a
//! query pays one range request per extent it reads. A merge round keeps each
//! cluster to at most [`MAX_SMALL_EXTENTS`] extents with fewer than
//! [`SMALL_EXTENT_ROWS`] current rows: it copies the current rows of a due
//! cluster's smallest extents into one cluster-contiguous derived extent and
//! drops extents with no current row. A round freezes the selected root and
//! catalog (other root-changing maintenance waits; acknowledged log writes may
//! continue and shadow copied rows), reads one source extent or creates one
//! object per step, and publishes one catalog and one root. Before that root
//! the previous catalog serves and every staged object is an orphan; after it
//! the replaced extents' derived packs become obsolete. A merge changes only
//! physical locality: every current version keeps exactly one posting copy.
use super::{
    clustered::{self, ClusterIndex, Extent, ExtentKind, ObjectRef},
    convert::layout,
    decode_block, root_key, sketch, Block, BlockRecord, PackSketch, SegmentedDatabase,
    MAX_PACK_BLOCKS,
};
use crate::{store::ObjectStore, Error, Mutation, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    mem,
    sync::Arc,
};

/// An extent is small while it has fewer current rows than three full blocks.
pub(super) const SMALL_EXTENT_ROWS: usize = 3 * 170;
/// A cluster is due for a merge when it has more small extents than this:
/// one base plus three deltas, the design's steady-state target.
pub(super) const MAX_SMALL_EXTENTS: usize = 3;
/// Current rows one output group may gather: one pack of full blocks.
const GROUP_ROWS: usize = MAX_PACK_BLOCKS * 170;
/// Output groups per round, bounding a round's creates before its root.
const MAX_GROUPS: usize = 32;
/// Extents without a current row that justify a round on their own.
const DEAD_EXTENTS: usize = 16;
// Two small extents always fit one output group, which fills one pack.
const _: () = assert!(2 * (SMALL_EXTENT_ROWS - 1) <= GROUP_ROWS);

/// What a completed merge round published.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct MergeSummary {
    pub root_generation: u64,
    /// Clusters whose small extents were merged.
    pub clusters: usize,
    /// Source extents replaced by merged copies.
    pub merged_extents: usize,
    /// Extents dropped because none of their rows is current.
    pub dropped_extents: usize,
    pub output_packs: usize,
    /// Current rows copied.
    pub rows: u64,
}

/// Shape of the loaded clustered view, for diagnostics and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ClusteredLayout {
    pub epoch: u64,
    pub clusters: usize,
    pub extents: usize,
    /// Extents in packs a clustered seal wrote (canonical) and in packs a
    /// conversion or merge wrote (derived copies).
    pub canonical_extents: usize,
    pub derived_extents: usize,
    pub posting_packs: usize,
    pub max_extents_per_cluster: usize,
    /// Most extents of one cluster with fewer than three blocks of current rows.
    pub max_small_extents: usize,
    /// Extents none of whose rows is current.
    pub dead_extents: usize,
    /// Canonical packs routed because they hold versions no posting covers.
    pub uncovered_packs: usize,
}

pub(super) struct MergeState {
    attempt: String,
    generation: u64,
    view: Arc<ClusterIndex>,
    /// Pending output groups: per cluster, the source extents to merge.
    groups: VecDeque<Vec<(u32, Vec<Extent>)>>,
    /// Next `(cluster, extent)` of the front group to read.
    next: (usize, usize),
    /// Current rows gathered for the front group, per cluster.
    gathered: Vec<(u32, Vec<BlockRecord>)>,
    /// Laid-out packs of a gathered group, created one per step.
    output: VecDeque<Vec<Block>>,
    /// Replaced or dropped extents by `(pack, offset)`.
    removed: BTreeSet<(String, u32)>,
    added: Vec<Extent>,
    sketches: Vec<PackSketch>,
    catalog: Option<(ObjectRef, ClusterIndex)>,
    summary: MergeSummary,
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    /// Plan a merge round if a cluster of the loaded view has more than
    /// three small extents or enough extents have no current row. Returns
    /// whether a round started; run it with [`Self::merge_step`].
    pub fn start_merge(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.maintenance_active() {
            return Err(Error::MaintenanceRequired);
        }
        let Some(view) = self.cluster.clone() else {
            return Ok(false);
        };
        let (latest, tail) = (&self.latest, &self.tail);
        let rows = self
            .sketches
            .posting_block_rows(|id, sequence| sketch::posting_current(tail, latest, id, sequence));
        let mut due = Vec::new();
        let mut dead = Vec::new();
        for cluster in &view.catalog.clusters {
            let mut small = Vec::new();
            for extent in &cluster.extents {
                let mut current = 0;
                for block in &extent.blocks {
                    current += rows
                        .get(&(extent.pack.as_str(), block.offset as usize))
                        .ok_or_else(|| {
                            Error::Corrupt(format!("posting block not loaded: {}", extent.pack))
                        })?;
                }
                if current == 0 {
                    dead.push(extent);
                } else if current < SMALL_EXTENT_ROWS {
                    small.push((current, extent));
                }
            }
            if small.len() > MAX_SMALL_EXTENTS {
                due.push((cluster.id, small));
            }
        }
        if due.is_empty() && dead.len() < DEAD_EXTENTS {
            return Ok(false);
        }
        let mut removed: BTreeSet<(String, u32)> = dead
            .iter()
            .map(|extent| (extent.pack.clone(), extent.offset))
            .collect();
        let mut groups = VecDeque::new();
        let (mut group, mut group_rows, mut group_blocks) = (Vec::new(), 0, 0);
        let mut summary = MergeSummary {
            dropped_extents: dead.len(),
            ..MergeSummary::default()
        };
        for (cluster, mut small) in due {
            // The smallest extents first, at least two, while they fit a pack.
            small.sort_by(|a, b| (a.0, &a.1.pack, a.1.offset).cmp(&(b.0, &b.1.pack, b.1.offset)));
            let (mut chosen, mut total) = (Vec::new(), 0);
            for (current, extent) in small {
                if chosen.len() >= 2 && total + current > GROUP_ROWS {
                    break;
                }
                chosen.push(extent.clone());
                total += current;
            }
            let blocks = total.div_ceil(170);
            if !group.is_empty()
                && (group_rows + total > GROUP_ROWS || group_blocks + blocks > MAX_PACK_BLOCKS)
            {
                groups.push_back(mem::take(&mut group));
                (group_rows, group_blocks) = (0, 0);
                if groups.len() == MAX_GROUPS {
                    break;
                }
            }
            summary.clusters += 1;
            summary.merged_extents += chosen.len();
            removed.extend(
                chosen
                    .iter()
                    .map(|extent| (extent.pack.clone(), extent.offset)),
            );
            group.push((cluster, chosen));
            group_rows += total;
            group_blocks += blocks;
        }
        if !group.is_empty() {
            groups.push_back(group);
        }
        self.merge = Some(MergeState {
            attempt: super::attempt_id()?,
            generation: self.root.generation,
            view,
            groups,
            next: (0, 0),
            gathered: Vec::new(),
            output: VecDeque::new(),
            removed,
            added: Vec::new(),
            sketches: Vec::new(),
            catalog: None,
            summary,
        });
        Ok(true)
    }

    /// Advance a merge round by one source extent read, one pack, catalog
    /// or root create. Only the root create changes the selected view; an
    /// uncertain create poisons the handle, and reopen selects whichever
    /// root exists. A failed read leaves the round to retry. Returns false
    /// when no round is active.
    pub fn merge_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut state) = self.merge.take() else {
            return Ok(false);
        };
        if state.generation != self.root.generation
            || !self
                .cluster
                .as_ref()
                .is_some_and(|view| Arc::ptr_eq(view, &state.view))
        {
            return Err(Error::Corrupt("root changed during a posting merge".into()));
        }
        if let Some(blocks) = state.output.pop_front() {
            let key = format!("sgpack-{}-{:08}", state.attempt, state.summary.output_packs);
            self.poisoned = true;
            let (references, sketch) =
                self.publish_pack(&key, &blocks, Some(&state.view.fingerprints))?;
            self.poisoned = false;
            state.added.extend(clustered::extents_of(
                &references,
                state.view.epoch,
                ExtentKind::Derived,
            )?);
            state.sketches.push(sketch);
            state.summary.output_packs += 1;
        } else if let Some(group) = state.groups.front() {
            let (cluster, extent) = state.next;
            if cluster < group.len() {
                let (id, extents) = &group[cluster];
                if state.gathered.len() == cluster {
                    state.gathered.push((*id, Vec::new()));
                }
                let read = self.read_current_rows(*id, &extents[extent]);
                let rows = match read {
                    Ok(rows) => rows,
                    Err(error) => {
                        self.merge = Some(state);
                        return Err(error);
                    }
                };
                state.summary.rows += rows.len() as u64;
                state.gathered[cluster].1.extend(rows);
                state.next = if extent + 1 < extents.len() {
                    (cluster, extent + 1)
                } else {
                    (cluster + 1, 0)
                };
            } else {
                state.output = layout(self.config, mem::take(&mut state.gathered))?;
                state.groups.pop_front();
                state.next = (0, 0);
            }
        } else if state.catalog.is_none() {
            let catalog = state
                .view
                .catalog
                .with_changes(&state.removed, mem::take(&mut state.added))?;
            let bytes = state.view.encode_catalog(&catalog)?;
            let reference = clustered::object_ref(format!("sgcluster-{}", state.attempt), &bytes);
            self.poisoned = true;
            self.create_staged(&reference.key, &bytes)?;
            self.poisoned = false;
            state.catalog = Some((reference, state.view.with_catalog(catalog)));
        } else {
            self.publish_merge(state)?;
            return Ok(true);
        }
        self.merge = Some(state);
        Ok(true)
    }

    /// The loaded clustered view's layout, or `None` without one.
    pub fn clustered_layout(&self) -> Option<ClusteredLayout> {
        let view = self.cluster.as_ref()?;
        let (latest, tail) = (&self.latest, &self.tail);
        let rows = self
            .sketches
            .posting_block_rows(|id, sequence| sketch::posting_current(tail, latest, id, sequence));
        let mut layout = ClusteredLayout {
            epoch: view.epoch,
            clusters: view.ids.len(),
            posting_packs: view.packs.len(),
            uncovered_packs: self.sketches.routed_canonical_packs(),
            ..ClusteredLayout::default()
        };
        for cluster in &view.catalog.clusters {
            let mut small = 0;
            for extent in &cluster.extents {
                let current: usize = extent
                    .blocks
                    .iter()
                    .filter_map(|block| rows.get(&(extent.pack.as_str(), block.offset as usize)))
                    .sum();
                match extent.kind {
                    ExtentKind::Canonical => layout.canonical_extents += 1,
                    ExtentKind::Derived => layout.derived_extents += 1,
                }
                if current == 0 {
                    layout.dead_extents += 1;
                } else if current < SMALL_EXTENT_ROWS {
                    small += 1;
                }
            }
            layout.extents += cluster.extents.len();
            layout.max_extents_per_cluster =
                layout.max_extents_per_cluster.max(cluster.extents.len());
            layout.max_small_extents = layout.max_small_extents.max(small);
        }
        Some(layout)
    }

    /// Run a whole merge round if one is due; returns what it published.
    pub fn merge_postings(&mut self) -> Result<Option<MergeSummary>> {
        if !self.start_merge()? {
            return Ok(None);
        }
        let mut summary = None;
        while let Some(state) = &self.merge {
            summary = Some(state.summary.clone());
            self.merge_step()?;
        }
        Ok(summary.map(|summary| MergeSummary {
            root_generation: self.root.generation,
            ..summary
        }))
    }

    /// The rows of one source extent that are current versions not
    /// shadowed by the log tail, from one range read whose blocks are each
    /// authenticated by their catalog digest.
    fn read_current_rows(&self, cluster: u32, extent: &Extent) -> Result<Vec<BlockRecord>> {
        let bytes = self
            .store
            .get_range(
                &extent.pack,
                extent.offset as usize,
                extent.length as usize,
                extent.payload_len as usize,
            )?
            .ok_or_else(|| Error::Corrupt(format!("posting pack missing: {}", extent.pack)))?;
        let mut rows = Vec::new();
        for block in &extent.blocks {
            let start = (block.offset - extent.offset) as usize;
            let bytes = bytes
                .get(start..start + block.length as usize)
                .ok_or_else(|| Error::Corrupt("posting range read is short".into()))?;
            if Sha256::digest(bytes).as_slice() != block.sha256 {
                return Err(Error::Corrupt(format!(
                    "posting block digest mismatch: {}",
                    extent.pack
                )));
            }
            let decoded = decode_block(self.config, bytes)?;
            if decoded.partition != cluster {
                return Err(Error::Corrupt(format!(
                    "posting block of another cluster: {}",
                    extent.pack
                )));
            }
            for record in decoded.records {
                let Mutation::Put { id, .. } = &record.mutation else {
                    return Err(Error::Corrupt(format!(
                        "posting tombstone: {}",
                        extent.pack
                    )));
                };
                if sketch::posting_current(&self.tail, &self.latest, *id, record.sequence) {
                    rows.push(record);
                }
            }
        }
        Ok(rows)
    }

    /// Publish the root selecting the merged catalog (after any manifests a
    /// legacy root still needs), then rebind routing:
    /// replaced blocks leave their packs' sketches and the merged packs'
    /// rows become routable.
    fn publish_merge(&mut self, mut state: MergeState) -> Result<()> {
        let mut root = self.next_root()?;
        root.clustered
            .as_mut()
            .ok_or_else(|| Error::Corrupt("posting merge without a clustered root".into()))?
            .catalog = state.catalog.as_ref().expect("catalog staged").0.clone();
        root.validate(self.config)?;
        // A legacy root's runs first get their manifests, one create per step.
        if self.stage_manifest(&mut root)? {
            self.merge = Some(state);
            return Ok(());
        }
        let (_, view) = state.catalog.take().expect("catalog staged");
        let bytes = super::manifest::encode_root(&root)?;
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &bytes)?;
        self.poisoned = false;
        self.replace_root(root);
        let mut listed: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
        for extent in view
            .catalog
            .clusters
            .iter()
            .flat_map(|cluster| &cluster.extents)
        {
            listed
                .entry(extent.pack.as_str())
                .or_default()
                .extend(extent.blocks.iter().map(|block| block.offset as usize));
        }
        let sources: BTreeSet<&str> = state
            .removed
            .iter()
            .map(|(pack, _)| pack.as_str())
            .collect();
        let sketches = Arc::make_mut(&mut self.sketches);
        for pack in sources {
            let offsets = listed.get(pack);
            sketches.retain_posting(pack, |offset| {
                offsets.is_some_and(|offsets| offsets.contains(&offset))
            });
        }
        let packs: BTreeSet<_> = state
            .sketches
            .iter()
            .map(|sketch| sketch.pack().to_owned())
            .collect();
        for sketch in state.sketches.drain(..) {
            sketches.install(sketch);
        }
        drop(listed);
        self.replace_view(view);
        self.refresh_sketches()?;
        let (latest, tail) = (&self.latest, &self.tail);
        Arc::make_mut(&mut self.sketches).activate_postings(Some(&packs), |id, sequence| {
            sketch::posting_current(tail, latest, id, sequence)
        });
        self.schedule_obsolete();
        Ok(())
    }
}
