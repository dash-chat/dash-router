//! Sets of sequence numbers, per log and across logs.
//!
//! [`Ranges`] uses DESIGN.md's boundary encoding: a strictly increasing list
//! of sequence numbers where even positions are inclusive range starts and odd
//! positions are exclusive range ends. An odd-length list is open-ended.
//!
//! ```
//! # use dash_router_core::ranges::Ranges;
//! let r = Ranges::from_boundaries(vec![0, 3, 7, 9]).unwrap(); // 0..3 + 7..9
//! assert!(r.contains(2) && !r.contains(3) && r.contains(8) && !r.contains(9));
//! let open = Ranges::from_boundaries(vec![75]).unwrap(); // 75..
//! assert!(open.contains(u32::MAX) && !open.contains(74));
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Sequence number within a log.
pub type Seq = u32;

/// A set of contiguous, possibly open-ended, ranges of sequence numbers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Ranges(Vec<Seq>);

/// Boundaries were not strictly increasing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRanges(pub Vec<Seq>);

impl std::fmt::Display for InvalidRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "range boundaries not strictly increasing: {:?}", self.0)
    }
}

impl std::error::Error for InvalidRanges {}

impl Ranges {
    /// The empty set.
    pub fn empty() -> Self {
        Self(vec![])
    }

    /// Every sequence number: `0..`.
    pub fn full() -> Self {
        Self(vec![0])
    }

    /// The single half-open range `start..end` (empty if `start >= end`).
    pub fn range(start: Seq, end: Seq) -> Self {
        if start < end {
            Self(vec![start, end])
        } else {
            Self::empty()
        }
    }

    /// The open-ended range `start..`.
    pub fn from(start: Seq) -> Self {
        Self(vec![start])
    }

    /// Build from the raw boundary encoding, validating it.
    pub fn from_boundaries(boundaries: Vec<Seq>) -> Result<Self, InvalidRanges> {
        if boundaries.windows(2).all(|w| w[0] < w[1]) {
            Ok(Self(boundaries))
        } else {
            Err(InvalidRanges(boundaries))
        }
    }

    /// Build the smallest `Ranges` containing exactly the given sequence numbers.
    pub fn from_seqs(seqs: impl IntoIterator<Item = Seq>) -> Self {
        let mut seqs: Vec<Seq> = seqs.into_iter().collect();
        seqs.sort_unstable();
        seqs.dedup();
        let mut out: Vec<Seq> = vec![];
        for s in seqs {
            match out.last() {
                // The previous range ends exactly here: extend it.
                Some(&end) if out.len().is_multiple_of(2) && end == s => {
                    out.pop();
                }
                // The previous range is open (only possible for Seq::MAX).
                Some(_) if out.len() % 2 == 1 => continue,
                _ => out.push(s),
            }
            // `None` means the range is open-ended at Seq::MAX.
            if let Some(end) = s.checked_add(1) {
                out.push(end);
            }
        }
        Self(out)
    }

    /// The raw boundary encoding.
    pub fn boundaries(&self) -> &[Seq] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the last range is open-ended.
    pub fn is_open(&self) -> bool {
        self.0.len() % 2 == 1
    }

    pub fn contains(&self, seq: Seq) -> bool {
        self.0.partition_point(|b| *b <= seq) % 2 == 1
    }

    /// The largest sequence number contained, if the set is finite and non-empty.
    pub fn last(&self) -> Option<Seq> {
        if self.is_open() || self.is_empty() {
            None
        } else {
            Some(self.0[self.0.len() - 1] - 1)
        }
    }

    /// Every contained sequence number, ascending. An open tail runs to `Seq::MAX`.
    pub fn iter(&self) -> impl Iterator<Item = Seq> + '_ {
        self.0
            .chunks(2)
            .flat_map(|c| match c {
                [start, end] => (*start as u64)..(*end as u64),
                [start] => (*start as u64)..(Seq::MAX as u64 + 1),
                _ => unreachable!(),
            })
            .map(|s| s as Seq)
    }

    /// Number of contained sequence numbers, if finite.
    pub fn len(&self) -> Option<usize> {
        if self.is_open() {
            None
        } else {
            Some(self.0.chunks(2).map(|c| (c[1] - c[0]) as usize).sum())
        }
    }

    pub fn union(&self, other: &Self) -> Self {
        self.combine(other, |a, b| a || b)
    }

    pub fn intersection(&self, other: &Self) -> Self {
        self.combine(other, |a, b| a && b)
    }

    /// Everything in `self` not in `other`.
    pub fn difference(&self, other: &Self) -> Self {
        self.combine(other, |a, b| a && !b)
    }

    /// Everything not in `self`.
    pub fn complement(&self) -> Self {
        Self::full().difference(self)
    }

    /// Sweep over the merged boundaries of both sets. Membership in each set
    /// is constant between consecutive boundaries, so evaluating `f` at each
    /// boundary and emitting a new boundary whenever the result flips is exact.
    /// `f(false, false)` must be `false`.
    fn combine(&self, other: &Self, f: impl Fn(bool, bool) -> bool) -> Self {
        debug_assert!(!f(false, false));
        let (mut i, mut j) = (0, 0);
        let mut out = vec![];
        let mut inside = false;
        while i < self.0.len() || j < other.0.len() {
            let p = match (self.0.get(i), other.0.get(j)) {
                (Some(a), Some(b)) => *a.min(b),
                (Some(a), None) => *a,
                (None, Some(b)) => *b,
                (None, None) => unreachable!(),
            };
            while self.0.get(i) == Some(&p) {
                i += 1;
            }
            while other.0.get(j) == Some(&p) {
                j += 1;
            }
            let now = f(self.contains(p), other.contains(p));
            if now != inside {
                out.push(p);
                inside = now;
            }
        }
        Self(out)
    }
}

/// Ranges across several logs. A log absent from the map is *unknown*; a log
/// present with an empty [`Ranges`] value is *known but empty* — a marker
/// meaning "this log exists and nothing is held/wanted/relayed for it,"
/// distinct from not knowing about the log at all (e.g. a freshly-created
/// log with no data yet still wants everything). Key presence therefore
/// carries meaning independent of the value's emptiness: construction
/// ([`Self::from_pairs`], [`Self::insert`]) and [`Self::union`] preserve
/// marker keys, while the derived, wire-bound results of
/// [`Self::intersection`] and [`Self::difference`] drop empty entries, since
/// those represent "nothing to say about this log" rather than a knowledge
/// marker.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogRanges<L: Ord>(BTreeMap<L, Ranges>);

impl<L: Ord> Default for LogRanges<L> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<L: Ord + Clone> LogRanges<L> {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from pairs. A pair with an empty `Ranges` is kept as a
    /// known-but-empty marker, not dropped.
    pub fn from_pairs(iter: impl IntoIterator<Item = (L, Ranges)>) -> Self {
        Self(iter.into_iter().collect())
    }

    pub fn get(&self, log: &L) -> Option<&Ranges> {
        self.0.get(log)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&L, &Ranges)> {
        self.0.iter()
    }

    /// True when every known log maps to an empty range (vacuously true
    /// when no logs are known at all). A `LogRanges` holding only
    /// known-but-empty markers is still "empty" in this sense: there is
    /// nothing to fetch, send or accept, even though logs are known.
    pub fn is_empty(&self) -> bool {
        self.0.values().all(|r| r.is_empty())
    }

    pub fn contains(&self, log: &L, seq: Seq) -> bool {
        self.0.get(log).is_some_and(|r| r.contains(seq))
    }

    /// Set `log`'s ranges, including an empty one: presence of the key is
    /// itself meaningful (see the type doc), so an empty `ranges` is kept
    /// as a known-but-empty marker rather than removing the key.
    pub fn insert(&mut self, log: L, ranges: Ranges) {
        self.0.insert(log, ranges);
    }

    /// Keeps every key present in either operand, even where the merged
    /// value is empty, so a known-but-empty marker in either side survives
    /// (e.g. `held.union(&novel)` never loses a log's "known" status).
    pub fn union(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (log, r) in &other.0 {
            let merged = match out.0.get(log) {
                Some(mine) => mine.union(r),
                None => r.clone(),
            };
            out.insert(log.clone(), merged);
        }
        out
    }

    /// Only the logs present in both operands, with each empty result
    /// dropped: this produces wire-bound content (what to send/accept),
    /// where "nothing in common" must not be represented as a key.
    pub fn intersection(&self, other: &Self) -> Self {
        Self(
            self.0
                .iter()
                .filter_map(|(log, r)| other.0.get(log).map(|o| (log.clone(), r.intersection(o))))
                .filter(|(_, r)| !r.is_empty())
                .collect(),
        )
    }

    /// Everything in `self` not in `other`, with each empty result dropped
    /// (see [`Self::intersection`] on why: this is derived, wire-bound
    /// content, not a knowledge marker).
    pub fn difference(&self, other: &Self) -> Self {
        Self(
            self.0
                .iter()
                .map(|(log, r)| {
                    let d = match other.0.get(log) {
                        Some(o) => r.difference(o),
                        None => r.clone(),
                    };
                    (log.clone(), d)
                })
                .filter(|(_, r)| !r.is_empty())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn r(b: &[Seq]) -> Ranges {
        Ranges::from_boundaries(b.to_vec()).unwrap()
    }

    #[test]
    fn design_doc_examples() {
        let a = r(&[0, 3, 7, 9]);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![0, 1, 2, 7, 8]);
        assert!(!a.is_open());
        assert_eq!(a.len(), Some(5));
        assert_eq!(a.last(), Some(8));

        let b = r(&[75]);
        assert!(b.is_open());
        assert!(b.contains(75) && !b.contains(74) && b.contains(Seq::MAX));
        assert_eq!(b.len(), None);

        let c = r(&[10, 20, 30]);
        assert!(c.contains(10) && !c.contains(20) && !c.contains(29) && c.contains(31));
    }

    #[test]
    fn rejects_non_increasing() {
        assert!(Ranges::from_boundaries(vec![3, 3]).is_err());
        assert!(Ranges::from_boundaries(vec![5, 2]).is_err());
    }

    /// A `LogRanges` key with an empty value is a "known but empty" marker,
    /// not the same as the key being absent: construction and `union`
    /// preserve it, while `intersection`/`difference` (derived, wire-bound
    /// content) still drop empty results as before.
    #[test]
    fn empty_valued_keys_are_preserved_by_construction_and_union() {
        // from_pairs and insert keep an empty-valued key.
        let known_empty: LogRanges<u8> = LogRanges::from_pairs([(1u8, Ranges::empty())]);
        assert_eq!(known_empty.get(&1), Some(&Ranges::empty()));
        let mut via_insert = LogRanges::empty();
        via_insert.insert(1u8, Ranges::empty());
        assert_eq!(via_insert, known_empty);

        // union preserves a marker key from either side, even when the
        // other side doesn't mention that log at all.
        let other: LogRanges<u8> = LogRanges::from_pairs([(2u8, Ranges::from(0))]);
        let merged = known_empty.union(&other);
        assert_eq!(
            merged.get(&1),
            Some(&Ranges::empty()),
            "marker key from self survives"
        );
        assert_eq!(
            merged.get(&2),
            Some(&Ranges::from(0)),
            "other's key survives"
        );

        // A LogRanges holding only marker keys is still logically empty:
        // nothing to fetch, send or accept, even though a log is known.
        assert!(known_empty.is_empty());
        assert!(!merged.is_empty(), "log 2 has real content");

        // intersection/difference must not manufacture or keep empty
        // entries: they represent "nothing to say," not a knowledge marker.
        // These first three are vacuous by disjointness (key 1 vs key 2) and
        // prove only that no *new* key gets manufactured — not that a
        // same-key empty result gets pruned. The cases below use the same
        // key on both sides so the result is empty by actual cancellation.
        assert!(known_empty.intersection(&other).get(&1).is_none());
        assert!(known_empty.difference(&other).get(&1).is_none());
        assert!(other.difference(&known_empty).get(&1).is_none());

        // Both sides mention key 1 with disjoint ranges: the intersection is
        // empty by cancellation, not by absence, and must still be pruned.
        let low: LogRanges<u8> = LogRanges::from_pairs([(1u8, Ranges::range(0, 2))]);
        let high: LogRanges<u8> = LogRanges::from_pairs([(1u8, Ranges::range(5, 7))]);
        assert!(
            low.intersection(&high).get(&1).is_none(),
            "same-key disjoint ranges intersect to nothing: key must be dropped, not kept as an empty marker"
        );

        // A difference that exactly empties a shared key must also drop it.
        let whole: LogRanges<u8> = LogRanges::from_pairs([(1u8, Ranges::range(0, 5))]);
        let same: LogRanges<u8> = LogRanges::from_pairs([(1u8, Ranges::range(0, 5))]);
        assert!(
            whole.difference(&same).get(&1).is_none(),
            "subtracting everything held for a shared key must drop it, not keep an empty marker"
        );
    }

    #[test]
    fn set_ops_with_open_tails() {
        let held = r(&[0, 3, 5, 6]); // 0,1,2,5
        let wanted = held.complement(); // 3,4,6..
        assert_eq!(wanted, r(&[3, 5, 6]));
        assert_eq!(wanted.intersection(&held), Ranges::empty());
        assert_eq!(wanted.union(&held), Ranges::full());
        // Subtracting an open tail truncates.
        assert_eq!(Ranges::full().difference(&r(&[10])), r(&[0, 10]));
        // Adjacent ranges coalesce.
        assert_eq!(r(&[0, 3]).union(&r(&[3, 5])), r(&[0, 5]));
    }

    #[test]
    fn from_seqs_coalesces() {
        assert_eq!(Ranges::from_seqs([5, 2, 0, 1, 7, 6]), r(&[0, 3, 5, 8]));
        assert_eq!(Ranges::from_seqs([]), Ranges::empty());
        assert_eq!(
            Ranges::from_seqs([Seq::MAX - 1, Seq::MAX]),
            r(&[Seq::MAX - 1])
        );
    }

    /// Oracle: a bitset over a small universe, with an "and beyond" flag.
    const UNIVERSE: Seq = 12;

    fn oracle(ranges: &Ranges) -> Vec<bool> {
        (0..=UNIVERSE).map(|s| ranges.contains(s)).collect()
    }

    fn arb_ranges() -> impl Strategy<Value = Ranges> {
        proptest::collection::btree_set(0..UNIVERSE, 0..6)
            .prop_map(|set| Ranges(set.into_iter().collect()))
    }

    proptest! {
        #[test]
        fn union_matches_oracle(a in arb_ranges(), b in arb_ranges()) {
            let expect: Vec<bool> = oracle(&a).iter().zip(oracle(&b)).map(|(x, y)| *x || y).collect();
            prop_assert_eq!(oracle(&a.union(&b)), expect);
        }

        #[test]
        fn intersection_matches_oracle(a in arb_ranges(), b in arb_ranges()) {
            let expect: Vec<bool> = oracle(&a).iter().zip(oracle(&b)).map(|(x, y)| *x && y).collect();
            prop_assert_eq!(oracle(&a.intersection(&b)), expect);
        }

        #[test]
        fn difference_matches_oracle(a in arb_ranges(), b in arb_ranges()) {
            let expect: Vec<bool> = oracle(&a).iter().zip(oracle(&b)).map(|(x, y)| *x && !y).collect();
            prop_assert_eq!(oracle(&a.difference(&b)), expect);
        }

        #[test]
        fn results_are_canonical(a in arb_ranges(), b in arb_ranges()) {
            for out in [a.union(&b), a.intersection(&b), a.difference(&b)] {
                prop_assert!(Ranges::from_boundaries(out.0.clone()).is_ok());
            }
        }

        #[test]
        fn from_seqs_roundtrips(seqs in proptest::collection::btree_set(0..UNIVERSE, 0..8)) {
            let ranges = Ranges::from_seqs(seqs.iter().copied());
            prop_assert_eq!(ranges.iter().collect::<Vec<_>>(), seqs.into_iter().collect::<Vec<_>>());
            prop_assert!(!ranges.is_open());
        }
    }
}
