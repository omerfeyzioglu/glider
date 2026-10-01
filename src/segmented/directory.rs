//! Compact latest-ID directory: one 24-byte slot per ID in a sorted vector.
//! A B-tree map measured about 92 bytes per entry at 250,000 IDs.
use super::{IndexEntry, Location};

#[derive(Clone, Copy)]
struct Slot {
    id: u64,
    sequence: u64,
    block: u32,
    run: u16,
    deleted: bool,
}

impl Slot {
    fn new(id: u64, location: Location) -> Self {
        Self {
            id,
            sequence: location.entry.sequence,
            block: location.entry.block,
            run: u16::try_from(location.run).expect("root runs are bounded"),
            deleted: location.entry.deleted,
        }
    }

    fn location(self) -> Location {
        Location {
            run: usize::from(self.run),
            entry: IndexEntry {
                id: self.id,
                sequence: self.sequence,
                block: self.block,
                deleted: self.deleted,
            },
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct Directory {
    slots: Vec<Slot>,
}

impl Directory {
    /// Reserve room for `additional` more entries in one allocation.
    pub(super) fn reserve(&mut self, additional: usize) {
        self.slots.reserve_exact(additional);
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    fn position(&self, id: u64) -> Result<usize, usize> {
        self.slots.binary_search_by_key(&id, |slot| slot.id)
    }

    pub(super) fn get(&self, id: &u64) -> Option<Location> {
        self.position(*id)
            .ok()
            .map(|index| self.slots[index].location())
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, Location)> + '_ {
        self.slots.iter().map(|slot| (slot.id, slot.location()))
    }

    /// Insert or replace entries given in strictly increasing ID order.
    pub(super) fn merge_sorted(&mut self, entries: impl IntoIterator<Item = (u64, Location)>) {
        let mut fresh = Vec::new();
        for (id, location) in entries {
            debug_assert!(fresh.last().is_none_or(|last: &Slot| last.id < id));
            match self.position(id) {
                Ok(index) => self.slots[index] = Slot::new(id, location),
                Err(_) => fresh.push(Slot::new(id, location)),
            }
        }
        if fresh.is_empty() {
            return;
        }
        let old = self.slots.len();
        self.slots.reserve_exact(fresh.len());
        self.slots.extend_from_slice(&fresh);
        // Merge backwards in place so no second full-size buffer is needed.
        let (mut left, mut right, mut out) = (old, fresh.len(), self.slots.len());
        while right > 0 {
            out -= 1;
            if left > 0 && self.slots[left - 1].id > fresh[right - 1].id {
                self.slots[out] = self.slots[left - 1];
                left -= 1;
            } else {
                self.slots[out] = fresh[right - 1];
                right -= 1;
            }
        }
    }

    /// Keep entries for which `keep` returns true; it may update the location.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(u64, &mut Location) -> bool) {
        self.slots.retain_mut(|slot| {
            let mut location = slot.location();
            let kept = keep(slot.id, &mut location);
            *slot = Slot::new(slot.id, location);
            kept
        });
    }
}
