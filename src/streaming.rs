//! Read-only exact search over version 3 snapshot chunks and a mutation tail.
use crate::{
    compacted_key, decode, inspect_namespace, key, matches_filter, parse_chunked_manifest,
    read_mutations, read_snapshot_chunk, scan_snapshot, segment_key, store::ObjectStore, Catalog,
    Config, Document, Error, Metric, Mutation, Neighbor, Result, SnapshotManifest, Version,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
};

struct Ranked(Neighbor);
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then(self.0.id.cmp(&other.0.id))
    }
}

fn consider(
    heap: &mut BinaryHeap<Ranked>,
    k: usize,
    id: u64,
    document: &Document,
    query: &[f32],
    metric: Metric,
) {
    let candidate = Ranked(Neighbor {
        id,
        distance: metric.score(query, &document.vector),
    });
    if heap.len() < k {
        heap.push(candidate);
    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
        heap.pop();
        heap.push(candidate);
    }
}

fn load_manifest<S: ObjectStore>(
    store: &S,
    config: Config,
    kind: &str,
    sequence: u64,
) -> Result<SnapshotManifest> {
    let key = if kind == "compacted" {
        compacted_key(sequence)
    } else {
        segment_key(sequence)
    };
    let bytes = store
        .get(&key)?
        .ok_or_else(|| Error::Corrupt(format!("listed snapshot missing: {key}")))?;
    if decode::<Version>(&bytes)?.version != 3 {
        return Err(Error::Invalid(
            "streaming reader requires version 3 snapshots; compact with compact_chunked".into(),
        ));
    }
    parse_chunked_manifest(&bytes, sequence, config)
}

/// Frozen read-only view of the latest acknowledged namespace state. It keeps
/// the selected manifest and uncheckpointed mutations in memory, and reads one
/// validated snapshot chunk at a time for ordinary exact queries. An optional
/// equality posting retains only matching base rows from the validated open.
/// The caller must still
/// exclusively own the namespace; this is not a concurrent-reader protocol.
pub struct StreamingDatabase<S> {
    store: S,
    config: Config,
    manifest: SnapshotManifest,
    kind: &'static str,
    overlay: BTreeMap<u64, Option<Document>>,
    sequence: u64,
    posting: Option<FilterPosting>,
}

struct FilterPosting {
    key: String,
    value: String,
    max_rows: usize,
    documents: Vec<(u64, Document)>,
}

impl FilterPosting {
    fn collect(&mut self, id: u64, document: &Document) -> Result<()> {
        if document.metadata.get(&self.key) == Some(&self.value) {
            if self.documents.len() == self.max_rows {
                return Err(Error::Invalid(
                    "filter posting exceeds configured row budget".into(),
                ));
            }
            self.documents.push((id, document.clone()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedDocument {
    pub vector: Vec<f32>,
    pub metadata: BTreeMap<String, String>,
}

impl<S: ObjectStore> StreamingDatabase<S> {
    /// Open an initialized namespace with a version 3 chunked snapshot. All
    /// selected chunks are validated on open without retaining their documents.
    /// The latest mutation tail is replayed into a small overlay when compacted
    /// or checkpointed recently; its size grows with uncheckpointed changes.
    pub fn open(store: S, config: Config) -> Result<Self> {
        Self::open_inner(store, config, None)
    }

    /// Retain one equality posting from the mandatory validated snapshot scan.
    /// Queries containing that predicate read only resident matching base rows
    /// and the mutation overlay. Other filters still scan all base chunks.
    /// Exceeding `max_rows` returns an explicit error during open; use the
    /// ordinary streaming open when that predicate is too broad to retain.
    pub fn open_with_filter(
        store: S,
        config: Config,
        key: &str,
        value: &str,
        max_rows: usize,
    ) -> Result<Self> {
        Self::open_inner(
            store,
            config,
            Some((key.to_owned(), value.to_owned(), max_rows)),
        )
    }

    fn open_inner(
        mut store: S,
        config: Config,
        filter: Option<(String, String, usize)>,
    ) -> Result<Self> {
        let Catalog {
            keys,
            latest_segment,
            latest_compacted,
            checkpoint_sequence,
            last_mutation,
        } = inspect_namespace(&mut store, config, false)?;
        let mut posting = filter.map(|(key, value, max_rows)| FilterPosting {
            key,
            value,
            max_rows,
            documents: Vec::new(),
        });
        let mut selected = None;
        if let Some(sequence) = latest_compacted {
            let manifest = load_manifest(&store, config, "compacted", sequence)?;
            scan_snapshot(&store, config, "compacted", &manifest, |id, document| {
                if latest_segment.is_none_or(|newer| newer <= sequence) {
                    if let Some(posting) = posting.as_mut() {
                        posting.collect(id, &document)?;
                    }
                }
                Ok(())
            })?;
            selected = Some(("compacted", manifest));
        }
        if let Some(sequence) = latest_segment.filter(|s| latest_compacted.is_none_or(|c| *s > c)) {
            if let Some(posting) = posting.as_mut() {
                posting.documents.clear();
            }
            let manifest = load_manifest(&store, config, "segment", sequence)?;
            scan_snapshot(&store, config, "segment", &manifest, |id, document| {
                if let Some(posting) = posting.as_mut() {
                    posting.collect(id, &document)?;
                }
                Ok(())
            })?;
            selected = Some(("segment", manifest));
        }
        let (kind, manifest) = selected.ok_or_else(|| {
            Error::Invalid("streaming reader requires a version 3 chunked snapshot".into())
        })?;
        let mut overlay = BTreeMap::new();
        let mut sequence = manifest.sequence;
        for object in keys.into_iter().filter(|key| key.starts_with("mutation-")) {
            if checkpoint_sequence.is_some_and(|covered| object <= key(covered)) {
                continue;
            }
            let next = sequence
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("sequence overflow".into()))?;
            if object != key(next) {
                return Err(Error::Corrupt(format!(
                    "unexpected object or log gap: {object}"
                )));
            }
            for mutation in read_mutations(&store, &object, next, config)? {
                match mutation {
                    Mutation::Put {
                        id,
                        vector,
                        metadata,
                    } => {
                        overlay.insert(id, Some(Document { vector, metadata }));
                    }
                    Mutation::Delete { id } => {
                        overlay.insert(id, None);
                    }
                }
            }
            sequence = next;
        }
        if sequence != last_mutation {
            return Err(Error::Corrupt("incomplete mutation tail".into()));
        }
        Ok(Self {
            store,
            config,
            manifest,
            kind,
            overlay,
            sequence,
            posting,
        })
    }

    pub fn config(&self) -> Config {
        self.config
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Return an owned vector and metadata map. A base-row lookup reads one
    /// chunk; a tail-row lookup uses the in-memory overlay.
    pub fn get_with_metadata(&self, id: u64) -> Result<Option<OwnedDocument>> {
        if let Some(changed) = self.overlay.get(&id) {
            return Ok(changed.as_ref().map(|doc| OwnedDocument {
                vector: doc.vector.clone(),
                metadata: doc.metadata.clone(),
            }));
        }
        let ordinal = self
            .manifest
            .chunks
            .partition_point(|reference| reference.last_id < id);
        let Some(reference) = self.manifest.chunks.get(ordinal) else {
            return Ok(None);
        };
        if id < reference.first_id {
            return Ok(None);
        }
        let chunk =
            read_snapshot_chunk(&self.store, self.config, self.kind, &self.manifest, ordinal)?;
        Ok(chunk
            .documents
            .binary_search_by_key(&id, |entry| entry.0)
            .ok()
            .map(|position| {
                let (_, document) = &chunk.documents[position];
                OwnedDocument {
                    vector: document.vector.clone(),
                    metadata: document.metadata.clone(),
                }
            }))
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<Neighbor>> {
        self.search_filtered(query, k, &[])
    }

    /// Exact filtered top-k. A query containing the indexed equality uses the
    /// resident validated posting and tail; all others re-read every base chunk.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        self.config.vector(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut heap = BinaryHeap::new();
        if let Some(posting) = self.posting.as_ref().filter(|posting| {
            filter
                .iter()
                .any(|&(key, value)| key == posting.key && value == posting.value)
        }) {
            for (id, document) in &posting.documents {
                if !self.overlay.contains_key(id) && matches_filter(&document.metadata, filter) {
                    consider(&mut heap, k, *id, document, query, self.config.metric);
                }
            }
        } else {
            scan_snapshot(
                &self.store,
                self.config,
                self.kind,
                &self.manifest,
                |id, document| {
                    if !self.overlay.contains_key(&id) && matches_filter(&document.metadata, filter)
                    {
                        consider(&mut heap, k, id, &document, query, self.config.metric);
                    }
                    Ok(())
                },
            )?;
        }
        for (&id, document) in &self.overlay {
            if let Some(document) = document.as_ref() {
                if matches_filter(&document.metadata, filter) {
                    consider(&mut heap, k, id, document, query, self.config.metric);
                }
            }
        }
        Ok(heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.0)
            .collect())
    }
}
