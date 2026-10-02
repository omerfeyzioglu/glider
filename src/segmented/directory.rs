//! Compact latest-ID directory: 24-byte sorted slots in shared pages.
//! Root publications copy only pages whose entries change while readers hold
//! an older view. A B-tree map measured about 92 bytes per entry at 250,000 IDs.
use super::{IndexEntry, Location};
use std::sync::Arc;

const PAGE_SLOTS: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
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
    pages: Vec<Arc<Vec<Slot>>>,
    len: usize,
}

impl Directory {
    /// Opening knows the index size; page pointers need far less reserve than slots.
    pub(super) fn reserve(&mut self, additional: usize) {
        self.pages.reserve(additional.div_ceil(PAGE_SLOTS));
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn page(&self, id: u64) -> usize {
        self.pages
            .partition_point(|page| page.last().unwrap().id < id)
    }

    pub(super) fn get(&self, id: &u64) -> Option<Location> {
        let page = self.pages.get(self.page(*id))?;
        page.binary_search_by_key(id, |slot| slot.id)
            .ok()
            .map(|index| page[index].location())
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, Location)> + '_ {
        self.pages
            .iter()
            .flat_map(|page| page.iter().map(|slot| (slot.id, slot.location())))
    }

    /// Insert or replace entries given in strictly increasing ID order.
    /// A batch that is large relative to the directory (such as a run index
    /// at open) is merged linearly into fresh pages; small batches (seals)
    /// update pages in place so unchanged pages stay shared with held views.
    pub(super) fn merge_sorted(&mut self, entries: impl IntoIterator<Item = (u64, Location)>) {
        let fresh: Vec<_> = entries.into_iter().collect();
        if fresh.len() >= PAGE_SLOTS && fresh.len() * 16 >= self.len {
            self.merge_linear(fresh);
            return;
        }
        let mut previous = None;
        for (id, location) in fresh {
            debug_assert!(previous.is_none_or(|last| last < id));
            previous = Some(id);
            let slot = Slot::new(id, location);
            let page = self.page(id);
            if page == self.pages.len() {
                if self
                    .pages
                    .last()
                    .is_some_and(|last| last.len() < PAGE_SLOTS)
                {
                    Arc::make_mut(self.pages.last_mut().unwrap()).push(slot);
                } else {
                    self.pages.push(Arc::new(vec![slot]));
                }
                self.len += 1;
                continue;
            }
            let slots = Arc::make_mut(&mut self.pages[page]);
            match slots.binary_search_by_key(&id, |slot| slot.id) {
                Ok(index) => slots[index] = slot,
                Err(index) => {
                    slots.insert(index, slot);
                    self.len += 1;
                    if slots.len() > PAGE_SLOTS {
                        let right = slots.split_off(slots.len() / 2);
                        self.pages.insert(page + 1, Arc::new(right));
                    }
                }
            }
        }
    }

    /// Rebuild every page from the old entries and `fresh`, releasing each
    /// old page as it is consumed so no second full-size buffer is held.
    fn merge_linear(&mut self, fresh: Vec<(u64, Location)>) {
        let mut pages = Vec::with_capacity((self.len + fresh.len()).div_ceil(PAGE_SLOTS));
        let mut page = Vec::with_capacity(PAGE_SLOTS);
        let mut push = |slot: Slot, pages: &mut Vec<Arc<Vec<Slot>>>| {
            page.push(slot);
            if page.len() == PAGE_SLOTS {
                pages.push(Arc::new(std::mem::replace(
                    &mut page,
                    Vec::with_capacity(PAGE_SLOTS),
                )));
            }
        };
        let mut fresh = fresh.into_iter().peekable();
        let mut previous = None;
        for old in std::mem::take(&mut self.pages) {
            for &slot in old.iter() {
                while let Some(&(id, location)) = fresh.peek() {
                    if id > slot.id {
                        break;
                    }
                    debug_assert!(previous.is_none_or(|last| last < id));
                    previous = Some(id);
                    fresh.next();
                    push(Slot::new(id, location), &mut pages);
                }
                // A fresh entry with the same ID replaces the old one.
                if previous != Some(slot.id) {
                    push(slot, &mut pages);
                }
            }
        }
        for (id, location) in fresh {
            debug_assert!(previous.is_none_or(|last| last < id));
            previous = Some(id);
            push(Slot::new(id, location), &mut pages);
        }
        if !page.is_empty() {
            pages.push(Arc::new(page));
        }
        self.len = pages.iter().map(|page| page.len()).sum();
        self.pages = pages;
    }

    /// Keep entries for which `keep` returns true; it may update the location.
    /// Entire pages with no changes remain shared with earlier views.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(u64, &mut Location) -> bool) {
        let mut next = Vec::with_capacity(self.pages.len());
        let mut len = 0;
        for page in self.pages.drain(..) {
            let mut changed = None;
            for (index, slot) in page.iter().enumerate() {
                let mut location = slot.location();
                let kept = keep(slot.id, &mut location);
                let updated = Slot::new(slot.id, location);
                if !kept || updated != *slot {
                    let output = changed.get_or_insert_with(|| page[..index].to_vec());
                    if kept {
                        output.push(updated);
                    }
                } else if let Some(output) = &mut changed {
                    output.push(*slot);
                }
            }
            match changed {
                Some(slots) if !slots.is_empty() => {
                    len += slots.len();
                    next.push(Arc::new(slots));
                }
                Some(_) => {}
                None => {
                    len += page.len();
                    next.push(page);
                }
            }
        }
        self.pages = next;
        self.len = len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(id: u64, run: usize) -> Location {
        Location {
            run,
            entry: IndexEntry {
                id,
                sequence: 1,
                block: 0,
                deleted: false,
            },
        }
    }

    #[test]
    fn changed_pages_detach_and_unchanged_pages_stay_shared() {
        let mut directory = Directory::default();
        directory.merge_sorted((0..3 * PAGE_SLOTS as u64).map(|id| (id, location(id, 0))));
        let old = directory.clone();
        directory.merge_sorted([(5, location(5, 1))]);
        assert_eq!(old.get(&5).unwrap().run, 0);
        assert_eq!(directory.get(&5).unwrap().run, 1);
        assert!(!Arc::ptr_eq(&old.pages[0], &directory.pages[0]));
        assert!(Arc::ptr_eq(&old.pages[1], &directory.pages[1]));
        directory.retain(|id, location| {
            if id == PAGE_SLOTS as u64 + 5 {
                location.run = 2;
            }
            id != 6
        });
        assert_eq!(old.len(), 3 * PAGE_SLOTS);
        assert_eq!(directory.len(), 3 * PAGE_SLOTS - 1);
        assert!(directory.get(&6).is_none());
        assert_eq!(old.get(&(PAGE_SLOTS as u64 + 5)).unwrap().run, 0);
        assert_eq!(directory.get(&(PAGE_SLOTS as u64 + 5)).unwrap().run, 2);
        assert!(Arc::ptr_eq(&old.pages[2], &directory.pages[2]));
        assert_eq!(directory.iter().count(), directory.len());
    }

    #[test]
    fn linear_and_incremental_merges_agree() {
        let seed = 0x000d_1ec7_u64;
        let mut state = seed;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut linear = Directory::default();
        let mut incremental = Directory::default();
        for run in 0..6 {
            let mut ids: Vec<u64> = (0..3000).map(|_| next() % 20_000).collect();
            ids.sort_unstable();
            ids.dedup();
            let batch: Vec<_> = ids.iter().map(|&id| (id, location(id, run))).collect();
            linear.merge_linear(batch.clone());
            for chunk in batch.chunks(PAGE_SLOTS / 2) {
                incremental.merge_sorted(chunk.iter().copied());
            }
            assert_eq!(linear.len(), incremental.len(), "seed {seed:#x} run {run}");
            assert!(
                linear
                    .iter()
                    .map(|(id, l)| (id, l.run))
                    .eq(incremental.iter().map(|(id, l)| (id, l.run))),
                "seed {seed:#x} run {run}"
            );
            assert!(linear.pages.iter().all(|page| !page.is_empty()));
        }
    }

    #[test]
    fn insertion_and_removal_keep_page_boundaries_searchable() {
        let mut directory = Directory::default();
        directory.merge_sorted((0..4 * PAGE_SLOTS as u64).map(|id| (id * 2, location(id * 2, 0))));
        let held = directory.clone();
        directory.merge_sorted(
            (0..4 * PAGE_SLOTS as u64).map(|id| (id * 2 + 1, location(id * 2 + 1, 1))),
        );
        assert_eq!(directory.len(), 8 * PAGE_SLOTS);
        for id in 0..8 * PAGE_SLOTS as u64 {
            assert_eq!(directory.get(&id).unwrap().run, (id % 2) as usize);
        }
        directory.retain(|id, _| id % 2 == 0);
        assert_eq!(directory.len(), held.len());
        assert!(directory
            .iter()
            .map(|(id, location)| (id, location.run))
            .eq(held.iter().map(|(id, location)| (id, location.run))));
    }
}
