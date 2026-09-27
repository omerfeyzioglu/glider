//! Bounded request outcomes and document observations, measured in commit boundaries.
use crate::{encode, Config, Error, Mutation, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const RETENTION_COMMITS: u64 = 128;
pub const MAX_REQUEST_MUTATIONS: usize = 100;
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_CHANGED_IDS: usize = 12_800;

/// Keep the entire ID unchanged on retry, including its observed boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestId {
    pub boundary: u64,
    pub nonce: [u8; 16],
}

/// An observation of one ID (present or absent) at a committed boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub id: u64,
    pub boundary: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: RequestId,
    /// All conditions refer to the state before the entire batch.
    pub conditions: Vec<Revision>,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Conflict {
    StaleRevision,
    ExpiredRevision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    /// Boundary of the durable decision, including rejected conditional batches.
    pub sequence: u64,
    #[serde(deserialize_with = "required_conflict")]
    pub conflict: Option<Conflict>,
}

fn required_conflict<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Conflict>, D::Error> {
    Option::<Conflict>::deserialize(d)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup {
    Retained(Outcome),
    /// No decision in this recovered history; safe to submit the unchanged ID.
    Unknown,
    /// The history can no longer resolve this ID; submission is refused.
    Expired,
    /// Observation is beyond this history (for example an older backup restore).
    Ahead,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Receipt {
    id: RequestId,
    digest: String,
    outcome: Outcome,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub(crate) revision_floor: u64,
    // Arrays preserve duplicate/order validation when reading persisted JSON.
    changed: Vec<(u64, u64)>,
    receipts: Vec<Receipt>,
}

impl Request {
    pub(crate) fn validate(&self, config: Config) -> Result<()> {
        if self.mutations.is_empty()
            || self.mutations.len() > MAX_REQUEST_MUTATIONS
            || self.conditions.len() > MAX_REQUEST_MUTATIONS
            || crate::encoded_len(self)? > MAX_REQUEST_BYTES
        {
            return Err(Error::Invalid(
                "request exceeds operation or encoded byte bounds".into(),
            ));
        }
        let mut previous = None;
        for condition in &self.conditions {
            if previous.is_some_and(|id| id >= condition.id)
                || condition.boundary > self.id.boundary
            {
                return Err(Error::Invalid(
                    "conditions must have increasing IDs and precede the request boundary".into(),
                ));
            }
            previous = Some(condition.id);
        }
        for mutation in &self.mutations {
            if let Mutation::Put { vector, .. } = mutation {
                config.vector(vector)?;
            }
        }
        Ok(())
    }
    pub(crate) fn digest(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(encode(self)?)))
    }
}

impl State {
    pub(crate) fn legacy(sequence: u64) -> Self {
        Self {
            revision_floor: sequence,
            ..Self::default()
        }
    }
    pub(crate) fn validate(&self, sequence: u64) -> Result<()> {
        let invalid = || Error::Corrupt("invalid retry/revision snapshot state".into());
        if self.revision_floor > sequence
            || self.revision_floor < sequence.saturating_sub(RETENTION_COMMITS)
            || self.changed.len() > MAX_CHANGED_IDS
            || self.receipts.len() > RETENTION_COMMITS as usize
        {
            return Err(invalid());
        }
        let mut previous = None;
        for &(id, changed) in &self.changed {
            if previous.is_some_and(|p| p >= id)
                || changed <= self.revision_floor
                || changed > sequence
            {
                return Err(invalid());
            }
            previous = Some(id);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut last = 0;
        for r in &self.receipts {
            if !ids.insert(r.id)
                || r.outcome.sequence <= last
                || r.outcome.sequence > sequence
                || r.id.boundary >= r.outcome.sequence
                || sequence - r.id.boundary > RETENTION_COMMITS
                || r.digest.len() != 64
                || !r
                    .digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(invalid());
            }
            last = r.outcome.sequence;
        }
        Ok(())
    }
    pub(crate) fn lookup(&self, id: RequestId, sequence: u64) -> Lookup {
        if id.boundary > sequence {
            return Lookup::Ahead;
        }
        if let Some(r) = self.receipts.iter().find(|r| r.id == id) {
            return Lookup::Retained(r.outcome);
        }
        if sequence - id.boundary >= RETENTION_COMMITS {
            Lookup::Expired
        } else {
            Lookup::Unknown
        }
    }
    pub(crate) fn duplicate(&self, request: &Request, sequence: u64) -> Result<Option<Outcome>> {
        match self.lookup(request.id, sequence) {
            Lookup::Retained(outcome) => {
                let r = self
                    .receipts
                    .iter()
                    .find(|r| r.id == request.id)
                    .expect("retained receipt");
                if r.digest != request.digest()? {
                    return Err(Error::RequestConflict);
                }
                Ok(Some(outcome))
            }
            Lookup::Unknown => Ok(None),
            Lookup::Expired => Err(Error::RequestExpired),
            Lookup::Ahead => Err(Error::Invalid(
                "request boundary is ahead of recovered history".into(),
            )),
        }
    }
    pub(crate) fn decide(&self, request: &Request, sequence: u64) -> Outcome {
        let conflict = request.conditions.iter().find_map(|r| {
            if r.boundary < self.revision_floor {
                Some(Conflict::ExpiredRevision)
            } else if self
                .changed
                .binary_search_by_key(&r.id, |&(id, _)| id)
                .ok()
                .is_some_and(|i| self.changed[i].1 > r.boundary)
            {
                Some(Conflict::StaleRevision)
            } else {
                None
            }
        });
        Outcome { sequence, conflict }
    }
    pub(crate) fn advance(&mut self, sequence: u64, mutations: &[Mutation]) {
        self.revision_floor = self
            .revision_floor
            .max(sequence.saturating_sub(RETENTION_COMMITS));
        let mut changed: BTreeMap<_, _> = self
            .changed
            .iter()
            .copied()
            .filter(|&(_, at)| at > self.revision_floor)
            .collect();
        for mutation in mutations {
            let id = match mutation {
                Mutation::Put { id, .. } | Mutation::Delete { id } => *id,
            };
            changed.insert(id, sequence);
            // Legacy unbounded batches may touch more IDs. Expire observations
            // conservatively instead of retaining an unbounded tombstone map.
            if changed.len() > MAX_CHANGED_IDS {
                self.revision_floor = sequence;
                changed.clear();
                break;
            }
        }
        self.changed = changed.into_iter().collect();
        self.receipts
            .retain(|r| sequence - r.id.boundary <= RETENTION_COMMITS);
    }
    pub(crate) fn retain(&mut self, request: &Request, outcome: Outcome) -> Result<()> {
        self.receipts.push(Receipt {
            id: request.id,
            digest: request.digest()?,
            outcome,
        });
        Ok(())
    }
}
