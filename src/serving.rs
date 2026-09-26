//! Serial single-machine serving with one authoritative owner and explicit bounds.
use crate::{
    ownership::OwnedDatabase, recovery::stage_isolated_namespace, store::ObjectStore, Config,
    Error, MaintenanceLimits, MaintenanceStatus, Mutation, Neighbor, Result,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy)]
pub struct ServingOptions {
    pub maintenance: MaintenanceLimits,
    pub max_documents: usize,
    /// Serialized vector plus metadata, excluding ID and log framing.
    pub max_document_bytes: usize,
    pub chunk_bytes: usize,
}
impl ServingOptions {
    pub const fn m8() -> Self {
        Self {
            maintenance: MaintenanceLimits::m8(),
            max_documents: 2000,
            max_document_bytes: 4096,
            chunk_bytes: 131_072,
        }
    }
    fn validate(self) -> Result<()> {
        self.maintenance.validate()?;
        if self.max_documents == 0 || self.max_document_bytes == 0 || self.chunk_bytes == 0 {
            return Err(Error::Invalid("serving bounds must be positive".into()));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Exact,
    Approximate,
}
#[derive(Debug, Clone, Copy)]
pub struct ServingStatus {
    pub maintenance: MaintenanceStatus,
    pub documents: usize,
    pub max_documents: usize,
    pub recovery_required: bool,
    pub maintenance_runs: u64,
    pub storage_errors: u64,
    pub backup_errors: u64,
    /// The M8 policy has no enabled derived ANN generation.
    pub search_mode: SearchMode,
}

/// Calls are serialized by exclusive borrowing. No background work or pinned
/// concurrent readers are needed for this serving contract. The resident map
/// supplies exact queries; M8 fits its measured memory budget.
pub struct SingleMachine<S: ObjectStore> {
    db: OwnedDatabase<S>,
    options: ServingOptions,
    maintenance_runs: u64,
    storage_errors: u64,
    backup_errors: u64,
}
impl<S: ObjectStore> SingleMachine<S> {
    pub fn open(store: S, config: Config, options: ServingOptions) -> Result<Self> {
        options.validate()?;
        let mut db = OwnedDatabase::open(store, config)?;
        if db.documents.len() > options.max_documents
            || db.documents.values().any(|d| {
                serde_json::to_vec(d).map_or(true, |bytes| bytes.len() > options.max_document_bytes)
            })
        {
            db.close()?;
            return Err(Error::Invalid(
                "recovered database exceeds serving capacity; use larger validated bounds".into(),
            ));
        }
        db.set_maintenance_limits(options.maintenance)?;
        Ok(Self {
            db,
            options,
            maintenance_runs: 0,
            storage_errors: 0,
            backup_errors: 0,
        })
    }
    pub fn status(&self) -> ServingStatus {
        ServingStatus {
            maintenance: self.db.maintenance_status(),
            documents: self.db.documents.len(),
            max_documents: self.options.max_documents,
            recovery_required: self.db.poisoned,
            maintenance_runs: self.maintenance_runs,
            storage_errors: self.storage_errors,
            backup_errors: self.backup_errors,
            search_mode: SearchMode::Exact,
        }
    }
    pub fn get(&self, id: u64) -> Option<(&[f32], &std::collections::BTreeMap<String, String>)> {
        Some((self.db.get(id)?, self.db.get_metadata(id)?))
    }
    pub fn query(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        mode: SearchMode,
    ) -> Result<Vec<Neighbor>> {
        if mode != SearchMode::Exact {
            return Err(Error::Invalid(
                "ANN is disabled: M8 quality/resource gate requires exact mode".into(),
            ));
        }
        if k > self.options.max_documents {
            return Err(Error::Invalid("k exceeds serving document bound".into()));
        }
        self.db.search_filtered(query, k, filter)
    }
    /// Validate bounds before any maintenance or mutation I/O. Maintenance, when
    /// due, completes BEFORE publication of this batch; failure never means the
    /// submitted batch was committed by this call. The batch's own create error
    /// still has the ordinary uncertain-outcome semantics.
    pub fn apply_batch(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if mutations.is_empty() || mutations.len() > self.options.maintenance.max_batch_mutations {
            return Err(Error::Invalid(
                "batch outside serving operation bound".into(),
            ));
        }
        let mut ids: BTreeSet<_> = self.db.documents.keys().copied().collect();
        for mutation in &mutations {
            match mutation {
                Mutation::Put {
                    id,
                    vector,
                    metadata,
                } => {
                    self.db.config.vector(vector)?;
                    let bytes = serde_json::to_vec(&crate::Document {
                        vector: vector.clone(),
                        metadata: metadata.clone(),
                    })
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                    if bytes.len() > self.options.max_document_bytes {
                        return Err(Error::Invalid("document exceeds serving byte bound".into()));
                    }
                    ids.insert(*id);
                }
                Mutation::Delete { id } => {
                    ids.remove(id);
                }
            }
        }
        if ids.len() > self.options.max_documents {
            return Err(Error::Invalid(
                "batch exceeds serving document capacity".into(),
            ));
        }
        if self.db.maintenance_status().should_compact {
            self.maintain()?;
        }
        let result = self.db.apply_batch(mutations);
        if result.is_err() && self.db.poisoned {
            self.storage_errors += 1;
        }
        result
    }
    /// Synchronous explicit maintenance; also called before a write at soft limits.
    pub fn maintain(&mut self) -> Result<()> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let result = self.db.compact_chunked(self.options.chunk_bytes);
        if result.is_ok() {
            self.maintenance_runs += 1;
        } else {
            self.storage_errors += 1;
        }
        result
    }
    /// Compact a quiescent committed view and stage it into a fresh, empty,
    /// nonoverlapping backup prefix. Destination failure does not poison source.
    /// A failed destination must never be promoted or reused.
    pub fn backup_to<D: ObjectStore>(&mut self, destination: D) -> Result<()> {
        self.maintain()?;
        let result = stage_isolated_namespace(&self.db.store, destination, self.db.config);
        if result.is_err() {
            self.backup_errors += 1;
        }
        result
    }
    /// Only a clean handle may release ownership. An uncertain handle leaves
    /// its claim and requires the fresh-prefix crash recovery procedure.
    pub fn close(self) -> Result<()> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        self.db.close()
    }
}
