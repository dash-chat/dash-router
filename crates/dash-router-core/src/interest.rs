//! What a Want carries (spec 2026-09-29): per channel, the emitter's *have*
//! within an author-prefix scope. Everything else in that scope is asked
//! for. An [`Interest`] is the same channel-nested shape as [`LogRanges`]
//! with the opposite invariants: an entry with an empty have is meaningful
//! (a *pure want* for its whole scope), and an author with an empty range
//! is redundant (the same as absent) and normalised away.
//!
//! Merging interests is merging *asks* by union, which is the same thing
//! as intersecting haves: an author some entry in scope does not list is
//! asked for in full by that entry. That is what makes a flat list of
//! received Wants answerable without knowing who sent which (the F1 echo
//! bug was a union of *exclusions*; an entry has none).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    log::Log,
    ranges::{LogRanges, Ranges},
};

/// One channel's part of a Want: the emitter's have within `[lo, hi]`, an
/// inclusive interval of author prefixes ([`Log::author_prefix`]). The
/// entry asks for every `(author, seq)` in scope that `have` does not
/// list, and says nothing about authors outside its scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(bound(serialize = "L::Author: Serialize"))]
pub struct Entry<L: Log> {
    pub lo: u32,
    pub hi: u32,
    pub have: BTreeMap<L::Author, Ranges>,
}

#[derive(Deserialize)]
struct RawEntry<A: Ord> {
    lo: u32,
    hi: u32,
    have: BTreeMap<A, Ranges>,
}

/// Hand-written so the invariants survive the wire: `lo <= hi`, every
/// listed author inside the scope, and no empty ranges (normalised away).
impl<'de, L: Log> Deserialize<'de> for Entry<L>
where
    L::Author: Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw: RawEntry<L::Author> = Deserialize::deserialize(d)?;
        Entry::new(raw.lo, raw.hi, raw.have).map_err(serde::de::Error::custom)
    }
}

impl<L: Log> Default for Entry<L> {
    /// A pure want for the whole channel.
    fn default() -> Self {
        Self::whole(BTreeMap::new())
    }
}

impl<L: Log> Entry<L> {
    /// An entry over the whole author space.
    pub fn whole(have: BTreeMap<L::Author, Ranges>) -> Self {
        Self::new(0, u32::MAX, have).expect("the whole scope contains every author")
    }

    /// An entry scoped to `[lo, hi]`. Empty ranges are dropped; an author
    /// outside the scope or an inverted scope is an error.
    pub fn new(lo: u32, hi: u32, have: BTreeMap<L::Author, Ranges>) -> Result<Self, String> {
        if lo > hi {
            return Err(format!("inverted scope [{lo:#x}, {hi:#x}]"));
        }
        let mut out = Self {
            lo,
            hi,
            have: BTreeMap::new(),
        };
        for (a, r) in have {
            if !out.contains(&a) {
                return Err(format!("author {a} outside scope [{lo:#x}, {hi:#x}]"));
            }
            if !r.is_empty() {
                out.have.insert(a, r);
            }
        }
        Ok(out)
    }

    pub fn contains_prefix(&self, prefix: u32) -> bool {
        self.lo <= prefix && prefix <= self.hi
    }

    pub fn contains(&self, author: &L::Author) -> bool {
        self.contains_prefix(L::author_prefix(author))
    }

    pub fn is_whole(&self) -> bool {
        self.lo == 0 && self.hi == u32::MAX
    }

    /// Lists no author: asks for everything in scope.
    pub fn is_pure(&self) -> bool {
        self.have.is_empty()
    }

    /// The emitter's have for `author`; empty when unlisted.
    pub fn have_of(&self, author: &L::Author) -> Ranges {
        self.have.get(author).cloned().unwrap_or_default()
    }

    /// What this entry asks for, out of what `held` holds under `c`: every
    /// held range in scope that the have does not cover. Known-but-empty
    /// markers in `held` contribute nothing.
    pub fn ask_within(&self, c: &L::Channel, held: &LogRanges<L>) -> LogRanges<L> {
        LogRanges::from_pairs(held.channel(c).filter_map(|(log, r)| {
            if !self.contains(&log.author()) {
                return None;
            }
            let d = r.difference(&self.have_of(&log.author()));
            (!d.is_empty()).then_some((log, d))
        }))
    }

    /// Spec §2's finite procedure: is everything this entry asks for under
    /// `c` already asked for by `seen`?
    ///
    /// 1. The union of the seen scopes for `c` must contain this scope.
    /// 2. At every author in scope listed by this entry or by a seen
    ///    entry, the intersection of the seen haves (over the seen entries
    ///    whose scope contains the author; an unlisted author's have is
    ///    empty) must be a subset of this entry's have.
    /// 3. Every other author in scope is listed by nobody, so it is asked
    ///    for in full by the seen entries covering it.
    pub fn covered_by<'a>(
        &self,
        c: &L::Channel,
        seen: impl IntoIterator<Item = &'a Interest<L>>,
    ) -> bool
    where
        L: 'a,
    {
        let records: Vec<&Entry<L>> = seen.into_iter().filter_map(|i| i.get(c)).collect();
        if records.is_empty() {
            return false;
        }
        // 1. Scope coverage.
        let mut scopes: Vec<(u32, u32)> = records.iter().map(|r| (r.lo, r.hi)).collect();
        scopes.sort_unstable();
        let mut need = Some(self.lo);
        for (lo, hi) in scopes {
            let Some(n) = need else { break };
            if lo > n {
                break; // a gap at `n`
            }
            if hi >= n {
                need = if hi >= self.hi { None } else { Some(hi + 1) };
            }
        }
        if need.is_some() {
            return false;
        }
        // 2. Listed authors.
        let mut authors: BTreeSet<&L::Author> = self.have.keys().collect();
        for r in &records {
            authors.extend(r.have.keys().filter(|a| self.contains(a)));
        }
        for a in authors {
            let mut common: Option<Ranges> = None;
            for r in records.iter().filter(|r| r.contains(a)) {
                let h = r.have_of(a);
                common = Some(match common {
                    None => h,
                    Some(x) => x.intersection(&h),
                });
            }
            let Some(common) = common else {
                return false; // unreachable after step 1; be safe
            };
            if !common.difference(&self.have_of(a)).is_empty() {
                return false;
            }
        }
        true
    }
}

/// What a Want carries: at most one [`Entry`] per channel. Its ask is the
/// union of its entries' asks.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(bound(serialize = "L::Channel: Serialize, L::Author: Serialize"))]
pub struct Interest<L: Log>(BTreeMap<L::Channel, Entry<L>>);

impl<'de, L: Log> Deserialize<'de> for Interest<L>
where
    L::Channel: Deserialize<'de>,
    L::Author: Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let map: BTreeMap<L::Channel, Entry<L>> = Deserialize::deserialize(d)?;
        Ok(Self(map))
    }
}

impl<L: Log> Default for Interest<L> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<L: Log> Interest<L> {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn single(c: L::Channel, entry: Entry<L>) -> Self {
        let mut out = Self::empty();
        out.insert(c, entry);
        out
    }

    /// Whole-scope entries for `channels`, each carrying what `held` holds
    /// under it (an empty have for a channel with nothing held).
    pub fn from_held(held: &LogRanges<L>, channels: impl IntoIterator<Item = L::Channel>) -> Self {
        let mut out = Self::empty();
        for c in channels {
            let have = held
                .channel(&c)
                .filter(|(_, r)| !r.is_empty())
                .map(|(log, r)| (log.author(), r.clone()))
                .collect();
            out.insert(c, Entry::whole(have));
        }
        out
    }

    pub fn insert(&mut self, c: L::Channel, entry: Entry<L>) {
        self.0.insert(c, entry);
    }

    pub fn remove(&mut self, c: &L::Channel) -> Option<Entry<L>> {
        self.0.remove(c)
    }

    pub fn get(&self, c: &L::Channel) -> Option<&Entry<L>> {
        self.0.get(c)
    }

    pub fn channels(&self) -> impl Iterator<Item = &L::Channel> {
        self.0.keys()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&L::Channel, &Entry<L>)> {
        self.0.iter()
    }

    /// No entries at all. An interest with a pure-want entry is not empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The union over entries of what each asks for out of `held`.
    pub fn ask_within(&self, held: &LogRanges<L>) -> LogRanges<L> {
        self.0.iter().fold(LogRanges::empty(), |acc, (c, e)| {
            acc.union(&e.ask_within(c, held))
        })
    }

    /// The entries of `self` that `seen` does not already cover.
    pub fn uncovered_by<'a>(&self, seen: &[&'a Interest<L>]) -> Self
    where
        L: 'a,
    {
        Self(
            self.0
                .iter()
                .filter(|(c, e)| !e.covered_by(c, seen.iter().copied()))
                .map(|(c, e)| (*c, e.clone()))
                .collect(),
        )
    }

    /// Whether any entry of `self` is not covered by `seen`.
    pub fn any_uncovered_by<'a>(&self, seen: &[&'a Interest<L>]) -> bool
    where
        L: 'a,
    {
        self.0
            .iter()
            .any(|(c, e)| !e.covered_by(c, seen.iter().copied()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Pair;

    type I = Interest<Pair>;
    type E = Entry<Pair>;

    fn have(pairs: &[(u8, Ranges)]) -> BTreeMap<u8, Ranges> {
        pairs.iter().cloned().collect()
    }

    fn whole(pairs: &[(u8, Ranges)]) -> E {
        Entry::whole(have(pairs))
    }

    fn p(a: u8) -> u32 {
        Pair::author_prefix(&a)
    }

    #[test]
    fn construction_normalises_and_validates() {
        let e = whole(&[(1, Ranges::range(0, 3)), (2, Ranges::empty())]);
        assert_eq!(e.have.len(), 1, "empty ranges are dropped");
        assert!(e.is_whole() && !e.is_pure());
        assert!(E::default().is_pure());
        assert!(E::new(5, 4, BTreeMap::new()).is_err(), "inverted scope");
        assert!(
            E::new(p(3), p(3), have(&[(4, Ranges::full())])).is_err(),
            "author outside scope"
        );
        let scoped = E::new(p(3), p(5), have(&[(4, Ranges::full())])).unwrap();
        assert!(scoped.contains(&3) && scoped.contains(&5) && !scoped.contains(&6));
    }

    #[test]
    fn ask_is_held_in_scope_minus_have() {
        let x = Pair::new(7, 1);
        let y = Pair::new(7, 2);
        let z = Pair::new(8, 1);
        let held = LogRanges::from_pairs([
            (x, Ranges::range(0, 10)),
            (y, Ranges::range(0, 5)),
            (z, Ranges::range(0, 5)),
            (Pair::new(7, 3), Ranges::empty()), // known-but-empty marker
        ]);
        let i = I::single(7, whole(&[(1, Ranges::range(0, 4))]));
        let ask = i.ask_within(&held);
        assert_eq!(
            ask.get(&x),
            Some(&Ranges::range(4, 10)),
            "the tail past the have"
        );
        assert_eq!(ask.get(&y), Some(&Ranges::range(0, 5)), "unlisted: in full");
        assert_eq!(ask.get(&z), None, "other channel: nothing");
        assert_eq!(ask.len(), 2, "markers contribute nothing");

        // Scoped to author 2 only: x is out of scope.
        let scoped = I::single(7, E::new(p(2), p(2), BTreeMap::new()).unwrap());
        assert_eq!(
            scoped.ask_within(&held),
            LogRanges::from_pairs([(y, Ranges::range(0, 5))])
        );
    }

    #[test]
    fn covering_in_the_unscoped_case() {
        let mine = whole(&[(1, Ranges::range(0, 10)), (2, Ranges::range(0, 3))]);
        // Nothing seen: not covered.
        assert!(!mine.covered_by(&7, []));
        // A pure want covers everything.
        let pure = I::single(7, E::default());
        assert!(mine.covered_by(&7, [&pure]));
        // A record naming a subset of my authors, with haves within mine.
        let z = I::single(7, whole(&[(1, Ranges::range(0, 10))]));
        assert!(
            mine.covered_by(&7, [&z]),
            "z asks for 1's tail and all of 2"
        );
        // A record listing an author I don't hold: it asks only for that
        // author's tail, but I ask for all of it.
        let v = I::single(
            7,
            whole(&[
                (1, Ranges::range(0, 10)),
                (2, Ranges::range(0, 3)),
                (3, Ranges::range(0, 1)),
            ]),
        );
        assert!(!mine.covered_by(&7, [&v]));
        // Two records excluding different authors together cover a pure want:
        // 1 is asked for by the second, 2 by the first.
        let a = I::single(7, whole(&[(1, Ranges::range(0, 10))]));
        let b = I::single(7, whole(&[(2, Ranges::range(0, 3))]));
        assert!(E::default().covered_by(&7, [&a, &b]));
        assert!(!E::default().covered_by(&7, [&a]));
        // A record whose have exceeds mine asks for less than I do.
        let more = I::single(7, whole(&[(1, Ranges::range(0, 12))]));
        assert!(!mine.covered_by(&7, [&more]), "seen asks 12.., I ask 10..");
        // Other channel: irrelevant.
        assert!(!mine.covered_by(&7, [&I::single(8, E::default())]));
    }

    #[test]
    fn covering_with_scopes() {
        let mine = whole(&[(1, Ranges::range(0, 10))]);
        let low = I::single(7, E::new(0, p(4), BTreeMap::new()).unwrap());
        let high = I::single(7, E::new(p(4) + 1, u32::MAX, BTreeMap::new()).unwrap());
        assert!(!mine.covered_by(&7, [&low]), "a gap above author 4");
        assert!(!mine.covered_by(&7, [&high]), "a gap below author 5");
        assert!(
            mine.covered_by(&7, [&low, &high]),
            "together they tile the space"
        );
        // A scoped pure want does not cover the whole; the whole covers a scoped entry.
        let scoped_mine = E::new(p(1), p(1), have(&[(1, Ranges::range(0, 10))])).unwrap();
        assert!(scoped_mine.covered_by(&7, [&low]));
        assert!(scoped_mine.covered_by(&7, [&I::single(7, E::default())]));
        // The listed author's have is only checked against records in scope.
        let low_named = I::single(
            7,
            E::new(0, p(4), have(&[(1, Ranges::range(0, 12))])).unwrap(),
        );
        assert!(
            !scoped_mine.covered_by(&7, [&low_named]),
            "low asks 12.., I ask 10.."
        );
    }

    #[test]
    fn serde_round_trips_and_rejects_bad_entries() {
        let mut i = I::empty();
        i.insert(7, whole(&[(1, Ranges::range(0, 10))]));
        i.insert(8, E::default());
        i.insert(
            9,
            E::new(p(2), p(3), have(&[(3, Ranges::from(4))])).unwrap(),
        );
        let bytes = postcard::to_stdvec(&i).unwrap();
        let back: I = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            back, i,
            "an empty have survives, unlike an empty LogRanges channel"
        );

        let bad = I::single(
            7,
            Entry {
                lo: 5,
                hi: 4,
                have: BTreeMap::new(),
            },
        );
        let bytes = postcard::to_stdvec(&bad).unwrap();
        assert!(
            postcard::from_bytes::<I>(&bytes).is_err(),
            "inverted scope rejected"
        );
        let bad = I::single(
            7,
            Entry {
                lo: p(2),
                hi: p(2),
                have: have(&[(5, Ranges::full())]),
            },
        );
        let bytes = postcard::to_stdvec(&bad).unwrap();
        assert!(
            postcard::from_bytes::<I>(&bytes).is_err(),
            "author out of scope rejected"
        );
        let redundant = I::single(
            7,
            Entry {
                lo: 0,
                hi: u32::MAX,
                have: have(&[(5, Ranges::empty())]),
            },
        );
        let bytes = postcard::to_stdvec(&redundant).unwrap();
        let back: I = postcard::from_bytes(&bytes).unwrap();
        assert!(
            back.get(&7).unwrap().is_pure(),
            "an empty range is normalised away"
        );
    }

    #[test]
    fn from_held_restricts_to_the_given_channels() {
        let held = LogRanges::from_pairs([
            (Pair::new(7, 1), Ranges::range(0, 10)),
            (Pair::new(8, 1), Ranges::range(0, 1)),
            (Pair::new(7, 9), Ranges::empty()),
        ]);
        let i = I::from_held(&held, [7u8, 3u8]);
        assert_eq!(i.len(), 2);
        assert_eq!(i.get(&7), Some(&whole(&[(1, Ranges::range(0, 10))])));
        assert_eq!(i.get(&3), Some(&E::default()), "nothing held: a pure want");
        assert!(i.get(&8).is_none());
    }
}
