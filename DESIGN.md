# Dash Router WP3

## Stated goals

- Epidemic/digest-based broadcast protocol implemented
- Relay store for encrypted messages peers carry for others
- Multi-hop relay verified across simulated topologies
- Garbage collection strategy


# Design

A broadcast sync protocol for use between members of a LAN.

> sam: did you already decide whether to use p2panda-net (iroh) or not? You mention "p2panda ephemeral streams" which makes me think yes. 

### Discovery 

Nodes advertise themselves over mDNS swarm-discovery.

Additionally, some peer discovery could be built into the protocol if needed, but we start with swarm-discovery.

### Transmission

Nodes "broadcast" to every other member of the LAN via p2panda ephemeral streams over a well-known constant topic (changing along with protocol breaking changes). We don't use true multicast here.

### Boundaries

This protocol is designed for use between members of a LAN. LAN comembership is defined by two peers who can communicate over IP addresses in the same private IP address range (one of the following):
- 10.0.0.0 to 10.255.255.255
- 172.16.0.0 to 172.31.255.255
- 192.168.0.0 to 192.168.255.255

The protocol will only be initiated and accepted by two such nodes. This is to prevent the protocol from accidentally running outside the bounds of the LAN, i.e. the Internet, while still allowing nodes in a LAN to automatically do relaying for each other.

## Storage

Nodes already have p2panda LogStores for all logs they care about. This protocol works in concert with that existing store. Now nodes additionally need a relay store for logs that they don't care about but are relaying for others.

Nodes intermix the two stores when advertising logs in the protocol. When receiving data, they place the received logs in the appropriate store. Additionally, if a node winds up subscribing or unsubscribing to a topic for which they've already stored logs in the relay store, they may move those logs to their selfish store (which has no effect on their participation in Dash Router).

For standalone use (e.g. without p2panda), this degrades to an empty or null "selfish store" so that the node's only storage is the relay store.

# Protocol

While the technical reality is one of unicast transmissions from one node to the next, the conceptual result is an open-ended perpetual sync session happening with the entire LAN simultaneously.

We take some inspiration and guidance from cft's MemoryLAN, but applied to data stored in append-only logs rather than unordered sets.

## Messages

Two message types. Want and Have.

> sam: just small note that we call the "announce log heights" message Have in p2panda-sync whereas you call it Want here. We're aware Want also makes sense in this context, just flagging the terminology difference.

```rust

enum Message {
    Want(LogRanges),
    Have {
        ops: HaveOps,
        /// Set true only by the author of the data immediately
        // after creating it. A fresh Have is relayed
        // immediately.
        fresh: bool,
    }
}

struct HaveOps(HashMap<LogId, Vec<(Seq, Op)>>);

impl HaveOps {
    /// Haves can be mapped into LogRanges too.
    pub fn log_ranges(&self) -> LogRanges { 
        todo!() 
    }
}

struct LogRanges(HashMap<LogId, Ranges>);

/// Two payloads, both opaque to relays in general.
struct Op {
    /// A relatively small blob, independently useful
    header: Bytes,
    /// The payload can be dropped separately 
    /// from the header during GC.
    payload: Option<Bytes>,
}

/// Represents a set of contiguous ranges.
/// Every even number denotes the inclusive start of a range,
/// every odd number denotes the (optional) exclusive end of one.
///
/// e.g.:
///     Ranges(vec![0, 3, 7, 9]) -> 0..3 + 7..9
///     Ranges(vec![75]) -> 75..
///     Ranges(vec![10, 20, 30]) -> 10..20 + 30..
struct Ranges(Vec<Seq>);

type Seq = u32;
```

> sam: We have a LogRanges type in p2panda-core (re-exported from p2panda-sync) which describes a set of log ranges but it can't express broken ranges like yours here. You'll need to convert yours into ours to use the sync APIs which might just make the apis slightly less nice to use.

## Sync Algorithm

There are three cases of data being emitted and received by nodes: Wants, Fresh Haves, and Non-Fresh Haves.

Each node stores records of recent (TTL-backed) Wants and Haves from other nodes, which influences its own emission of Wants and Haves, as well as what each emitted Want or Have contains.
(The record of a Have doesn't contain the op data, only the LogRanges.)

### 1. Emitting Wants

Each node emits Wants at random intervals (determined at the moment of the last Want emitted). The Want represents a request for other nodes to send a Have.

After each interval, the node calculates its next Want to send as follows:

- Compute the LogRanges for all logs in its own stores
    - Always ends with an open-ended range starting after the highest seq number held, including any earlier gaps
- Subtract from that the union of all other recent Wants from other nodes

This accounts for the expectation that others' Wants will be responded to soon, while still requesting the portion that has not recently been requested.
When other's wants' TTLs expire, it results in the next node to fire a Want re-including a request for that range to the network.

### 2. Emitting Fresh Haves

Whenever a node authors data, it emits a Fresh Have after a brief debounce, with the set of recently authored ops. All nodes who receive a Fresh Have immediately forward it on, and then store the contained ops.

This may create gaps that need to be backfilled by future Wants, but it pushes the most recent data out efficiently.

### 3. Emitting Non-Fresh Haves

Each node is also emitting Haves for non-fresh data at intervals, triggered by witnessing Wants from other nodes.

When a node sees a Want from another node, and is not already waiting to send its next Have, it chooses a random interval after which it will fire off a new Have.
(This means that as long as a node sees no new Wants from other nodes, and does not author new data itself, it will never send a Have.)

When the interval is up, the node calculates the next Have to emit:

- Compute the LogRanges held by this node
- Intersect that with the union of any recent Wants from other nodes.
- Subtract from that the union of Haves recently received from other nodes

This represents any data held by the node which could be of use to any other node, minus the data which was recently circulating through the network

### Rationale

The reason for using random intervals is analagous to MemoryLAN's reason for using probabilistic responses to avoid NACK-implosion, so that most nodes hold back from supplying redundant data.
The fact that we use a richer structure for diffs (log ranges instead of hash filters) gives us more information about what is and is not redundant data, and gives each node the opportunity to make a more informed assessment about what would be useful to send.

Nodes who choose a longer sending interval are expecting other nodes with a shorter interval to do some portion of the work they would otherwise do. By observing what happens next, nodes can delegate only the work that actually was done, rather than blindly delegating everything, making sure that what they have agency to do they can still do, with confidence that nobody else is also doing the same thing redundantly.

The random intervals need to be carefully tuned to the network conditions. Similar to in MemoryLAN, these intervals should be adaptive, based on estimated peer density at a given time. Simulations should be done to understand the proper tunings.

# Garbage collection

Many policies can be considered, with various tradeoffs. Here's one that tries to do the most good with the fewest decisions:

- Set a hard cap on storage across all relayed logs
- When the cap is reached, start removing Op payloads "oldest" first (retaining the headers), where "oldest" could mean:
    - lowest Seq, with LogId tiebreaker
    - "least recently mentioned", where both Haves and Wants from other nodes "top off" the recency timestamp
- If the cap is reached even with all payloads dropped, start removing headers in the same fashion

A more robust policy might be to choose random ranges to drop first so that each node in the network is dropping nonoverlapping patches, keeping more data available, but this is more complex.

# Privacy considerations

Topics must not be leaked by this protocol. 

Topics are treated as sensitive secrets. This protocol only operates on LogIds (which are one-way derived from topics) and there is no private topic discovery. A danger would be if a node could altruistically sync with me to see which LogIds I know about, then selfishly sync with me to see which ones I'm actually subscribed to myself, but this is protected by both nodes needing to know about the topic in order to do selfish sync. As long as this protocol never leaks topics, this privacy guarantee is upheld.
