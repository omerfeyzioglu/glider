//! Compact latest-ID directory: 24-byte sorted slots in shared pages.
//! Root publications copy only pages whose entries change while readers hold
//! an older view. A B-tree map measured about 92 bytes per entry at 250,000 IDs.
use super::{IndexEntry, Location};
use std::sync::Arc;

const PAGE_SLOTS: usize = 1024;
const GROUP_SLOTS: usize = PAGE_SLOTS * 16;

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

#[derive(Clone)]
struct Page {
    slots: Arc<Vec<Slot>>,
    start: usize,
    end: usize,
}

impl Page {
    fn new(slots: Vec<Slot>) -> Self {
        let end = slots.len();
        Self {
            slots: Arc::new(slots),
            start: 0,
            end,
        }
    }

    fn as_slice(&self) -> &[Slot] {
        &self.slots[self.start..self.end]
    }

    fn len(&self) -> usize {
        self.end - self.start
    }

    fn last(&self) -> Option<&Slot> {
        self.as_slice().last()
    }

    fn mutable(&mut self) -> &mut Vec<Slot> {
        if self.start != 0 || self.end != self.slots.len() {
            *self = Self::new(self.as_slice().to_vec());
        }
        Arc::make_mut(&mut self.slots)
    }
}

#[derive(Clone, Default)]
pub(super) struct Directory {
    pages: Vec<Page>,
    last_ids: Vec<u64>,
    page_starts: Vec<usize>,
    len: usize,
}

impl Directory {
    /// Opening knows the index size; page pointers need far less reserve than slots.
    pub(super) fn reserve(&mut self, additional: usize) {
        self.pages.reserve(additional.div_ceil(PAGE_SLOTS));
        self.last_ids.reserve(additional.div_ceil(PAGE_SLOTS));
        self.page_starts.reserve(additional.div_ceil(PAGE_SLOTS));
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn page(&self, id: u64) -> usize {
        self.last_ids.partition_point(|&last| last < id)
    }

    fn mutable_page(&self, id: u64) -> usize {
        self.pages
            .partition_point(|page| page.last().unwrap().id < id)
    }

    fn refresh_boundaries(&mut self) {
        self.last_ids.clear();
        self.page_starts.clear();
        let mut start = 0;
        for page in &self.pages {
            self.last_ids.push(page.last().unwrap().id);
            self.page_starts.push(start);
            start += page.len();
        }
        debug_assert_eq!(start, self.len);
    }

    pub(super) fn get(&self, id: &u64) -> Option<Location> {
        let page = self.pages.get(self.page(*id))?;
        page.as_slice()
            .binary_search_by_key(id, |slot| slot.id)
            .ok()
            .map(|index| page.as_slice()[index].location())
    }

    /// Position of an ID in iteration order, for side tables indexed like
    /// the directory.
    pub(super) fn index_of(&self, id: u64) -> Option<(usize, Location)> {
        let page_index = self.page(id);
        let page = self.pages.get(page_index)?;
        let slot_index = page
            .as_slice()
            .binary_search_by_key(&id, |slot| slot.id)
            .ok()?;
        Some((
            self.page_starts[page_index] + slot_index,
            page.as_slice()[slot_index].location(),
        ))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, Location)> + '_ {
        self.pages.iter().flat_map(|page| {
            page.as_slice()
                .iter()
                .map(|slot| (slot.id, slot.location()))
        })
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
            let page = self.mutable_page(id);
            if page == self.pages.len() {
                if self
                    .pages
                    .last()
                    .is_some_and(|last| last.len() < PAGE_SLOTS)
                {
                    let last = self.pages.last_mut().unwrap();
                    last.mutable().push(slot);
                    last.end += 1;
                } else {
                    self.pages.push(Page::new(vec![slot]));
                }
                self.len += 1;
                continue;
            }
            let slots = self.pages[page].mutable();
            match slots.binary_search_by_key(&id, |slot| slot.id) {
                Ok(index) => slots[index] = slot,
                Err(index) => {
                    slots.insert(index, slot);
                    self.len += 1;
                    if slots.len() > PAGE_SLOTS {
                        let right = slots.split_off(slots.len() / 2);
                        self.pages[page].end = slots.len();
                        self.pages.insert(page + 1, Page::new(right));
                    } else {
                        self.pages[page].end = slots.len();
                    }
                }
            }
        }
        self.refresh_boundaries();
    }

    /// Rebuild in bounded groups. Pages in one group share a backing vector
    /// until a later mutation detaches an affected page.
    fn merge_linear(&mut self, fresh: Vec<(u64, Location)>) {
        let mut groups = Vec::new();
        let mut group = Vec::with_capacity(GROUP_SLOTS);
        {
            let mut push = |slot: Slot| {
                group.push(slot);
                if group.len() == GROUP_SLOTS {
                    groups.push(Arc::new(std::mem::replace(
                        &mut group,
                        Vec::with_capacity(GROUP_SLOTS),
                    )));
                }
            };
            let mut fresh = fresh.into_iter().peekable();
            let mut previous = None;
            for old in std::mem::take(&mut self.pages) {
                for &slot in old.as_slice() {
                    while let Some(&(id, location)) = fresh.peek() {
                        if id > slot.id {
                            break;
                        }
                        debug_assert!(previous.is_none_or(|last| last < id));
                        previous = Some(id);
                        fresh.next();
                        push(Slot::new(id, location));
                    }
                    // A fresh entry with the same ID replaces the old one.
                    if previous != Some(slot.id) {
                        push(slot);
                    }
                }
            }
            for (id, location) in fresh {
                debug_assert!(previous.is_none_or(|last| last < id));
                previous = Some(id);
                push(Slot::new(id, location));
            }
        }
        if !group.is_empty() {
            groups.push(Arc::new(group));
        }
        self.len = groups.iter().map(|group| group.len()).sum();
        self.pages = groups
            .into_iter()
            .flat_map(|slots| {
                (0..slots.len()).step_by(PAGE_SLOTS).map(move |start| Page {
                    end: (start + PAGE_SLOTS).min(slots.len()),
                    slots: slots.clone(),
                    start,
                })
            })
            .collect();
        self.refresh_boundaries();
    }

    /// Keep entries for which `keep` returns true; it may update the location.
    /// Entire pages with no changes remain shared with earlier views.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(u64, &mut Location) -> bool) {
        enum Pending {
            Shared(Page),
            Changed(usize, usize, usize),
        }
        let mut next = Vec::with_capacity(self.pages.len());
        let mut groups = Vec::new();
        let mut changed_slots = Vec::new();
        let mut len = 0;
        for mut page in self.pages.drain(..) {
            if page.start == 0
                && page.end == page.slots.len()
                && Arc::get_mut(&mut page.slots).is_some()
            {
                let slots = Arc::get_mut(&mut page.slots).unwrap();
                slots.retain_mut(|slot| {
                    let mut location = slot.location();
                    let kept = keep(slot.id, &mut location);
                    *slot = Slot::new(slot.id, location);
                    kept
                });
                if !slots.is_empty() {
                    len += slots.len();
                    page.end = slots.len();
                    next.push(Pending::Shared(page));
                }
                continue;
            }
            if !changed_slots.is_empty() && GROUP_SLOTS - changed_slots.len() < page.len() {
                groups.push(Arc::new(std::mem::take(&mut changed_slots)));
            }
            let start = changed_slots.len();
            let mut changed = false;
            for (index, slot) in page.as_slice().iter().enumerate() {
                let mut location = slot.location();
                let kept = keep(slot.id, &mut location);
                let updated = Slot::new(slot.id, location);
                if !kept || updated != *slot {
                    if !changed {
                        changed_slots.extend_from_slice(&page.as_slice()[..index]);
                        changed = true;
                    }
                    if kept {
                        changed_slots.push(updated);
                    }
                } else if changed {
                    changed_slots.push(*slot);
                }
            }
            if changed {
                if changed_slots.len() > start {
                    len += changed_slots.len() - start;
                    next.push(Pending::Changed(groups.len(), start, changed_slots.len()));
                }
            } else {
                len += page.len();
                next.push(Pending::Shared(page));
            }
        }
        groups.push(Arc::new(changed_slots));
        self.pages = next
            .into_iter()
            .map(|page| match page {
                Pending::Shared(page) => page,
                Pending::Changed(group, start, end) => Page {
                    slots: groups[group].clone(),
                    start,
                    end,
                },
            })
            .collect();
        self.len = len;
        self.refresh_boundaries();
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
        assert!(!Arc::ptr_eq(&old.pages[0].slots, &directory.pages[0].slots));
        assert!(Arc::ptr_eq(&old.pages[1].slots, &directory.pages[1].slots));
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
        assert!(Arc::ptr_eq(&old.pages[2].slots, &directory.pages[2].slots));
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
            assert!(linear.pages.iter().all(|page| page.len() > 0));
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
            let (position, location) = directory.index_of(id).unwrap();
            assert_eq!(position, id as usize);
            assert_eq!(location.run, (id % 2) as usize);
        }
        directory.retain(|id, _| id % 2 == 0);
        assert_eq!(directory.len(), held.len());
        for position in 0..4 * PAGE_SLOTS {
            let id = (position * 2) as u64;
            assert_eq!(directory.index_of(id).unwrap().0, position);
            assert!(directory.index_of(id + 1).is_none());
        }
        assert!(directory
            .iter()
            .map(|(id, location)| (id, location.run))
            .eq(held.iter().map(|(id, location)| (id, location.run))));
    }

    #[test]
    fn unique_retain_reuses_page_allocation() {
        let mut directory = Directory::default();
        directory.merge_sorted((0..PAGE_SLOTS as u64).map(|id| (id, location(id, 0))));
        let page = Arc::as_ptr(&directory.pages[0].slots);
        directory.retain(|id, location| {
            location.run = 1;
            id != 7
        });
        assert_eq!(page, Arc::as_ptr(&directory.pages[0].slots));
        assert_eq!(directory.len(), PAGE_SLOTS - 1);
        assert_eq!(directory.get(&8).unwrap().run, 1);
        assert!(directory.get(&7).is_none());
    }

    #[test]
    fn shared_retain_groups_changed_pages_in_one_allocation() {
        let mut directory = Directory::default();
        directory.merge_sorted((0..33 * PAGE_SLOTS as u64).map(|id| (id, location(id, 0))));
        let old = directory.clone();
        directory.retain(|_, location| {
            location.run = 1;
            true
        });
        assert!(Arc::ptr_eq(
            &directory.pages[0].slots,
            &directory.pages[1].slots
        ));
        assert!(!Arc::ptr_eq(
            &directory.pages[0].slots,
            &directory.pages[16].slots
        ));
        assert!(!Arc::ptr_eq(&old.pages[0].slots, &directory.pages[0].slots));
        assert_eq!(old.get(&1).unwrap().run, 0);
        for id in [1, 16 * PAGE_SLOTS as u64, 32 * PAGE_SLOTS as u64] {
            assert_eq!(directory.get(&id).unwrap().run, 1);
        }
    }
}
