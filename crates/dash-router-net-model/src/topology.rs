//! Who can hear whom.
//!
//! In deployment the overlay comes from p2panda's gossip (HyParView +
//! Plumtree over an mDNS-discovered membership), and later from literal
//! radio range; the protocol must work over whatever adjacency those
//! produce, so the model takes the adjacency as given. Constructors cover
//! hand-built fixtures and random trees standing in for "a spanning tree
//! over whatever the membership protocol built". There is deliberately no
//! fully-connected constructor: that is not a realistic shape here, and it
//! makes relaying trivially unnecessary.
//!
//! For now a topology is fixed for the life of a model instance, so it
//! lives on the machine, not in the state. When churn or node movement
//! is modelled, it moves into [`crate::NetState`] with a rewiring action.

use std::collections::{BTreeMap, BTreeSet};

/// An undirected graph over node ids, symmetric by construction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Topology<N: Ord> {
    adjacency: BTreeMap<N, BTreeSet<N>>,
}

impl<N: Ord> Default for Topology<N> {
    fn default() -> Self {
        Self {
            adjacency: BTreeMap::new(),
        }
    }
}

impl<N: Ord + Copy> Topology<N> {
    /// A hand-built topology. Both endpoints of every edge become nodes.
    pub fn from_edges(edges: impl IntoIterator<Item = (N, N)>) -> Self {
        let mut topo = Self::default();
        for (a, b) in edges {
            topo.add_edge(a, b);
        }
        topo
    }

    /// A node with no edges yet, e.g. one whose mDNS advertisements were
    /// all lost. Its Sends go nowhere and nothing is addressed to it.
    pub fn add_node(&mut self, n: N) {
        self.adjacency.entry(n).or_default();
    }

    /// Add a symmetric edge. Useful for laying extra cross-links over a
    /// constructed topology (PlumTree lazy links, denser LANs).
    pub fn add_edge(&mut self, a: N, b: N) {
        assert!(a != b, "no self-edges");
        self.adjacency.entry(a).or_default().insert(b);
        self.adjacency.entry(b).or_default().insert(a);
    }

    /// Nodes in a line, each hearing only its neighbours: the worst-case
    /// diameter for a given node count, so the sharpest test of relaying.
    pub fn path(nodes: impl IntoIterator<Item = N>) -> Self {
        let mut topo = Self::default();
        let mut prev: Option<N> = None;
        for n in nodes {
            topo.add_node(n);
            if let Some(p) = prev {
                topo.add_edge(p, n);
            }
            prev = Some(n);
        }
        topo
    }

    /// The first node is the hub, the rest are leaves: minimum diameter,
    /// maximum fan-out.
    pub fn star(nodes: impl IntoIterator<Item = N>) -> Self {
        let mut topo = Self::default();
        let mut nodes = nodes.into_iter();
        let Some(hub) = nodes.next() else {
            return topo;
        };
        topo.add_node(hub);
        for leaf in nodes {
            topo.add_edge(hub, leaf);
        }
        topo
    }

    /// A uniformly random labelled tree over `nodes`, decoded from a random
    /// Prüfer sequence. Deterministic per seed, no RNG dependency.
    pub fn random_tree(nodes: &[N], seed: u64) -> Self {
        let n = nodes.len();
        let mut topo = Self::default();
        for &node in nodes {
            topo.add_node(node);
        }
        if n < 2 {
            return topo;
        }
        let mut rng = seed;
        let prufer: Vec<usize> = (0..n - 2)
            .map(|_| (splitmix64(&mut rng) % n as u64) as usize)
            .collect();
        // A node's degree is one more than its remaining Prüfer occurrences;
        // it becomes a leaf exactly when its last occurrence is consumed.
        let mut degree = vec![1usize; n];
        for &p in &prufer {
            degree[p] += 1;
        }
        let mut leaves: BTreeSet<usize> = (0..n).filter(|&i| degree[i] == 1).collect();
        for &p in &prufer {
            let leaf = *leaves.iter().next().unwrap();
            leaves.remove(&leaf);
            topo.add_edge(nodes[leaf], nodes[p]);
            degree[p] -= 1;
            if degree[p] == 1 {
                leaves.insert(p);
            }
        }
        let mut rest = leaves.into_iter();
        topo.add_edge(nodes[rest.next().unwrap()], nodes[rest.next().unwrap()]);
        topo
    }

    pub fn nodes(&self) -> impl Iterator<Item = N> + '_ {
        self.adjacency.keys().copied()
    }

    pub fn neighbors(&self, n: &N) -> impl Iterator<Item = N> + '_ {
        self.adjacency.get(n).into_iter().flatten().copied()
    }

    pub fn edge_count(&self) -> usize {
        self.adjacency.values().map(BTreeSet::len).sum::<usize>() / 2
    }

    /// Whether every node can reach every other. A disconnected fixture
    /// makes any liveness property fail vacuously, so assert this at
    /// construction time in tests.
    pub fn is_connected(&self) -> bool {
        let Some(start) = self.adjacency.keys().next().copied() else {
            return true;
        };
        let mut seen = BTreeSet::from([start]);
        let mut frontier = vec![start];
        while let Some(n) = frontier.pop() {
            for peer in self.neighbors(&n) {
                if seen.insert(peer) {
                    frontier.push(peer);
                }
            }
        }
        seen.len() == self.adjacency.len()
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_and_star_shapes() {
        let path = Topology::path([0, 1, 2, 3]);
        assert_eq!(path.edge_count(), 3);
        assert_eq!(path.neighbors(&1).collect::<Vec<_>>(), vec![0, 2]);
        assert!(path.is_connected());

        let star = Topology::star([9, 0, 1, 2]);
        assert_eq!(star.edge_count(), 3);
        assert_eq!(star.neighbors(&9).count(), 3);
        assert_eq!(star.neighbors(&1).collect::<Vec<_>>(), vec![9]);
    }

    #[test]
    fn isolated_nodes_disconnect() {
        let mut topo = Topology::from_edges([(0, 1)]);
        assert!(topo.is_connected());
        topo.add_node(2);
        assert!(!topo.is_connected());
    }

    #[test]
    fn random_trees_are_trees_and_deterministic() {
        let nodes: Vec<usize> = (0..7).collect();
        let mut distinct = BTreeSet::new();
        for seed in 0..10 {
            let topo = Topology::random_tree(&nodes, seed);
            assert_eq!(topo.nodes().count(), nodes.len());
            assert_eq!(topo.edge_count(), nodes.len() - 1);
            assert!(topo.is_connected(), "seed {seed} built a non-tree");
            assert_eq!(topo, Topology::random_tree(&nodes, seed));
            distinct.insert(topo);
        }
        assert!(distinct.len() > 1, "every seed built the same tree");
    }

    #[test]
    fn tiny_trees() {
        assert_eq!(Topology::<u8>::random_tree(&[], 0).nodes().count(), 0);
        assert_eq!(Topology::random_tree(&[5], 0).edge_count(), 0);
        assert_eq!(Topology::random_tree(&[5, 6], 0).edge_count(), 1);
    }
}
