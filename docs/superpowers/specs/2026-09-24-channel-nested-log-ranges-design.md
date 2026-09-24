# Channel-nested LogRanges

Date: 2026-09-24. Status: design, awaiting review.

## 1. Goal

`LogRanges<L>` is a flat `BTreeMap<L, Ranges>`. In Dash Chat a log `L`
is `(LogId, author)`, and one LogId typically has many authors, so every
Want repeats the same 32-byte LogId once per author. This spec nests
`LogRanges` one level, keyed by the shared half first and the author
second, so the shared half is stored and sent once per group. It does so
without making the integer ids used by tests, models and the sim any
harder to read: their public API is unchanged.

Along the way it settles the names of the two halves of a log, replacing
`Prefix` (which implied an excluded middle) with vocabulary that matches
p2panda's.

## 2. Vocabulary

p2panda's own terms, from `p2panda-core`:

- A **log** is single-author. Its `LogId` trait doc opens with "Uniquely
  identify a single-author log", and every store and sync API takes the
  author and the log id as two arguments, author first.
- `LogId` is therefore the per-author half, scoped inside an author. The
  crate doc's chat example calls the thing a LogId names across authors a
  *channel*: "a separate log for each author-channel pairing".
- `Author` is p2panda-core's trait name for the signing identity.
- Neither p2panda nor Dash Chat names the pair.

Dash Router adopts:

| Dash Router | Meaning | p2panda / Dash Chat |
|---|---|---|
| `L: Log` | the full identity of one single-author log | the `(verifying_key, log_id)` pair |
| `L::Channel` | the half shared across authors; what a subscription names | `LogId` (`blake3(topic)`, or `digest(space_id ++ tag)`) |
| `L::Author` | the per-author half | `VerifyingKey` / `DeviceId` |

`Channel` is deliberately not `LogId`: at the Dash Chat boundary `LogId`
already means exactly this half, and giving the same word a different
meaning in the router would confuse every conversion at the edge. The
Dash Chat impl reads `type Channel = LogId`.

The word "prefix" leaves the codebase entirely: the associated type, the
method, `Pair`'s field, the Want wire field, the router's helpers and the
doc comments. The 2026-09-22 spec's §3.5 is history and is not edited;
this spec supersedes its naming.

## 3. The `Log` trait

```rust
pub trait Log: Id {
    type Channel: Id;
    type Author: Id;
    fn channel(&self) -> Self::Channel;
    fn author(&self) -> Self::Author;
    /// Inverse of `(channel(), author())`. Must be lossless.
    fn new(channel: Self::Channel, author: Self::Author) -> Self;
}
```

`new` is required so a nested `LogRanges` can hand back full `L` values
when iterated. It must satisfy `Log::new(l.channel(), l.author()) == l`
for every `l`; a unit test on `Pair` and a doc requirement on the trait
state this.

**Single-author ids.** The integer ids and polestar's `UpTo` keep
`Channel = Self` with `channel = identity`, and get `Author = IdUnit`,
polestar's one-valued id type. polestar gains a serde derive on `IdUnit`
under its `serde` feature (commit `72fb422` on polestar-rs main, which
dash-router already builds against through the `[patch]` in `Cargo.toml`)
so the wire can carry it.

**`Pair`** becomes `Pair { channel: u8, author: u8 }` with
`Channel = u8`, `Author = u8`, and `Display` unchanged (`"{channel}/{author}"`).
`Pair::new(channel, author)` is the trait's `new`.

**`WireLog`** requires `Channel: Serialize + DeserializeOwned` and
`Author: Serialize + DeserializeOwned` instead of `Prefix`.

**Dash Chat's `RouterLog`** (out of this repo, in `dashchat-node`'s
`lan_router.rs`) becomes `type Channel = LogId; type Author = [u8; 32]`
with `channel()` and `author()` returning the two halves and `new`
rebuilding the struct. Its existing `author()` returning
`anyhow::Result<VerifyingKey>` is renamed `verifying_key()`.

## 4. `LogRanges<L: Log>`

### Shape

```rust
pub struct LogRanges<L: Log>(BTreeMap<L::Channel, BTreeMap<L::Author, Ranges>>);
```

The bound moves from `L: Ord` to `L: Log`. Invariant: no outer entry has
an empty inner map. `remove` prunes an emptied channel; nothing else can
create one.

### API

The public API stays keyed by `L`. Unchanged signatures:

- `empty()`, `from_pairs(impl IntoIterator<Item = (L, Ranges)>)`
- `get(&L) -> Option<&Ranges>`, `contains(&L, Seq)`, `insert(L, Ranges)`,
  `remove(&L) -> Option<Ranges>`
- `is_empty()`, `union`, `intersection`, `difference`

One visible change: `iter()` yields `(L, &Ranges)` with `L` by value
instead of `(&L, &Ranges)`, since the full id is no longer stored. `L`
is `Copy` (via `Id`), so call sites change from `*log` to `log` and from
`log` to `&log` where a reference is needed. `len()` (count of logs) is
added for the two shell call sites that do `iter().count()`.

New, channel-level accessors that the router and node use in place of
linear filters:

- `channel(&L::Channel) -> impl Iterator<Item = (L, &Ranges)>`: every log
  under one channel, or nothing if the channel is unknown.
- `channels() -> impl Iterator<Item = &L::Channel>`.

### Semantics preserved

The known-but-empty marker is per log, exactly as today: `from_pairs`,
`insert` and `union` keep a log with an empty `Ranges`; `intersection`
and `difference` drop empty results. `is_empty` is true when every leaf
is empty. The type doc keeps its current text with "log" throughout.

### Set operations

`union`, `intersection`, `difference` become two-level merges: walk the
outer maps, and for a channel present on both sides merge the inner maps
with the current per-log logic. The existing per-log `Ranges` ops are
untouched.

### Serde

Derived, on the nested map. On the wire a channel therefore appears once,
followed by its authors and their boundary lists. For `IdUnit` authors the
inner map is one entry whose key encodes as zero bytes in postcard, so the
model world pays one extra length varint per log.

This changes the encoding of every Want: **`WIRE_VERSION` bumps to 2 and
`GOSSIP_TOPIC` to `dash-router/v2`**, per the existing compile-time
check. The Want field `prefixes: BTreeSet<L::Prefix>` is renamed
`channels: BTreeSet<L::Channel>`.

## 5. Ripple through the crates

All mechanical, listed so the plan can check them off.

**dash-router-core**

- `log.rs`: trait, `Pair`, `WireLog`, macro, tests.
- `ranges.rs`: as §4.
- `wire.rs`: field rename, serde bounds on `Channel`/`Author`, version
  and topic bump, tests.
- `router.rs`: `Record.prefixes` → `channels`; `RouterState.open` stays
  `open` but is `BTreeSet<L::Channel>`; `RouterAction::Open` likewise;
  `held_under` and `next_want`'s `named_under_open` iterate
  `held.channel(c)` for each `c` in the set instead of filtering every log
  by `.prefix()`; `others_prefixes` → `others_channels`,
  `relayed_want_prefixes` → `relayed_want_channels`,
  `note_relayed_wants(.., channels, ..)`. Doc comments say "channel".
- `node.rs`: `relay_logs_under(&L::Channel)` uses `held.channel(c)`;
  `subscriptions.contains(&log.channel())`; `ranges_of<L: Log>`.
- `storage.rs`: `Storage<L: Log>`, `EvictableStorage<L: Log>`,
  `OpsMap<L: Log>`, `StoreEffect<L: Log>`; bodies only adjust the
  `iter()` reference change.
- `tests/router.rs`, `tests/node.rs`: renames; `Pair::new(channel,
  author)` reads the same.

**dash-router**

- `pack.rs`: `prefixes` → `channels`; `for (log, r) in ranges.iter()`
  inserts `log` not `*log`. Packing still measures the encoded size, so
  the budget logic is unchanged and now benefits from the shorter
  encoding.
- `shell.rs`, `handle.rs`, `transport.rs`, `disk.rs`: renames and the
  `iter()` reference change. `disk::LogKey` keeps `Ord + Clone`; its
  users already require `Log`.
- Tests (`loop.rs`, `conformance.rs`, `panda_swarm.rs`): renames.

**dash-router-net-model**, **dash-router-sim**, **dash-router-policy**:
renames only; the integer ids' API is unchanged.

**DESIGN.md**: the `LogRanges` sketch gains the nesting and the prefix
paragraph is reworded to channel.

## 6. Testing

Existing tests must pass unchanged in meaning after the renames. New:

- **`Log::new` roundtrip** on `Pair` and the integer ids.
- **Nested set ops match the flat oracle** (proptest over `Pair` with
  small channel and author universes): build a flat
  `BTreeMap<Pair, Ranges>` alongside, compute each set op pairwise there,
  and assert the nested result equals it, including which keys survive
  (marker rules).
- **Channel pruning**: after `remove` empties a channel, `channels()`
  omits it and `channel(c)` is empty.
- **`channel(c)` equals the filtered `iter()`** for every `c`.
- **Wire size**: a Want naming `n` authors under one `Pair` channel
  encodes to fewer bytes than `n` Wants with one author each, and the
  encoded bytes contain the channel byte pattern once. With `u8` keys
  this is a weak check, so the test uses a `Log` impl with a `[u8; 8]`
  channel defined in the test.
- **Serde roundtrip** of `LogRanges<Pair>` and `LogRanges<u8>` through
  postcard.
- **Version gate**: a v1 message is rejected by v2 `decode` (the existing
  unknown-version test, updated).

## 7. Non-goals

- **Nesting `Have`.** `WireBody::Have(Vec<(L, Vec<(Seq, Op)>)>)` repeats
  the channel per author too, but op payloads dominate a Have's size and
  the change would touch the ingest path. Left for its own spec if the
  measurement warrants it.
- **Renaming `Pair`** or `RouterState.open`. Both still read correctly.
- **In-memory dedup in `OpsMap` or the relay store.** Their keys stay
  the full `L`.
- **p2panda's author-outer nesting.** p2panda nests author first because
  sync walks per peer. The router nests channel first because
  subscriptions and the relay store are keyed by channel. Same two halves,
  opposite access pattern; noted in the `LogRanges` type doc.
