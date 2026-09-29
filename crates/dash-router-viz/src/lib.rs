//! Shows dash-router simulations through the generic `simviz` seam.
//!
//! [`RouterPresenter`] is the per-model UI module. Its visual language:
//!
//! - node fill goes from dark grey to green with the node's coverage, the
//!   share of ops authored so far that it should have and does;
//! - links are undirected; traffic on one draws an arrowhead the way it
//!   flows, amber for Wants and blue when any Have is among them, wider
//!   with more messages in flight;
//! - two moons show the node's armed timers: the top one the Want timer
//!   (amber), the next clockwise the Have timer (blue). Each is a pie of
//!   the time the timer has left, as of the sim clock, over the longest
//!   interval its policy can arm, so it empties as the fire approaches;
//! - a third moon (green) is the node's held share: the ops it holds
//!   (relay and ext together) over every op authored so far, absent until
//!   the first op exists;
//! - a node that authored during the frame gets a bright green border
//!   (in coarse time the frame merges a quantum's transitions, and every
//!   author within it is bordered);
//! - the transition that produced the frame leaves a one-frame trace: a
//!   dropped flight paints its direction of the link red, a receipt paints
//!   the receiver's border green if it taught something and red if it was
//!   redundant.
//!
//! The sidebar shows network counters, or the selected node's state with
//! links to the nodes it is talking to. Everything richer (op tracing)
//! means growing this presenter, not the viz.
//!
//! The viz steps a `polestar_sim::Scenario` over the same
//! [`SimBehavior`] and [`Metered`] net the sim crate's own `Simulation`
//! runs, so what the viewer shows is exactly one seeded sim run. A step
//! is one frame: one event in fine time, and in coarse time every event
//! of the next quantum, so a frame shows all that happened "at once"
//! ([`RunModel`]). As there, the driver rides inside the model state
//! ([`RunModel`]): simviz steps back by reset-and-replay, and a
//! `Scenario` reset restores only the model state, so a driver kept
//! outside it (its event queue, RNG, clocks) would replay a different run.

use std::{collections::BTreeMap, time::Duration};

use anyhow::Context;
use dash_router_core::{EvictableStorage, NodeState as CoreNodeState, Storage, WireBody};
use dash_router_net_model::Topology as NetTopology;
use dash_router_sim::{
    Config, LogId as SimLogId, Meter, Metered, MeteredFx, NodeId as SimNodeId, SimBehavior,
    SimFlight, SimNet, SimNetState,
};
use polestar::time::RealTime;
use polestar::{Behavior, BehaviorModel, Machine, StateMachine, TransitionResult};
use polestar_sim::Scenario;
use simviz::{
    EdgeStyle, Field, FieldValue, MoonStyle, NodeId, NodeStyle, Presenter, Topology, UiAction,
    VizEvent,
};

/// One seeded run as a single machine: the driver's state is model state.
///
/// One action is one frame. In fine time that is one event; in coarse
/// time it is every event of the next quantum, so the frame lands on the
/// quantum boundary with all of that window's events applied.
#[derive(Clone, Debug)]
pub struct RunModel(BehaviorModel<SimBehavior>);

impl RunModel {
    pub fn new(model: Metered<SimNet>) -> Self {
        Self(BehaviorModel::new(model))
    }
}

impl Machine for RunModel {
    type State = VizState;
    type Action = ();
    type Fx = Vec<MeteredFx>;
    type Error = anyhow::Error;

    fn transition(&self, state: VizState, (): ()) -> TransitionResult<Self> {
        let (mut state, mut fx) = self.0.transition(state, ())?;
        if state.0.quantum().is_some() {
            let frame_end = state.0.now();
            // The meter's one-transition trace resets each transition; the
            // frame keeps every author it merged, so an append mid-quantum
            // still shows on the frame.
            let mut authored = std::mem::take(&mut state.1.1.last.authored);
            while state
                .0
                .peek_at()
                .is_some_and(|at| state.0.quantize(at) <= frame_end)
            {
                let (next, more) = self.0.transition(state, ())?;
                state = next;
                fx.extend(more);
                authored.extend(state.1.1.last.authored.iter().copied());
            }
            state.1.1.last.authored = authored;
        }
        Ok((state, fx))
    }
}
/// The state simviz steps: the driver, then the network and its meter.
pub type VizState = (SimBehavior, (SimNetState, Meter));

/// The trivial driver over [`RunModel`]: one frame per step. Stateless, so
/// it is safe to leave outside the model state.
#[derive(Clone, Debug)]
pub struct Tick;

impl Behavior for Tick {
    type Model = RunModel;

    fn next_tick(&mut self, _: &VizState) -> anyhow::Result<Vec<()>> {
        Ok(vec![()])
    }
}

const WANT_COLOR: &str = "#e6a23c";
const HAVE_COLOR: &str = "#3c8fe6";
/// A lost flight, and a receipt that taught nothing.
const BAD_COLOR: &str = "#f85149";
/// A receipt that taught the receiver something.
const TAUGHT_COLOR: &str = "#7ee787";
/// Node fill at zero and at full coverage.
const COLD_FILL: (u8, u8, u8) = (0x30, 0x36, 0x3d);
const FULL_FILL: (u8, u8, u8) = (0x23, 0x86, 0x36);
/// Held-share moon, and the border of a node authoring this frame.
const HELD_COLOR: &str = "#3fb950";
const AUTHOR_COLOR: &str = "#26ff5c";
/// Moon slots: the Want timer on top, then clockwise the Have timer and
/// the held share.
const HELD_MOON: u8 = 0;
const WANT_MOON: u8 = 1;
const HAVE_MOON: u8 = 2;

/// Presenter for a dash-router network.
///
/// The topology lives on the net machine, not in its state, so the
/// presenter carries its own copy. The selected node is tracked here so
/// the sidebar can show that node's state.
pub struct RouterPresenter {
    topology: NetTopology<SimNodeId>,
    selected: Option<SimNodeId>,
}

impl RouterPresenter {
    pub fn new(topology: NetTopology<SimNodeId>) -> Self {
        Self {
            topology,
            selected: None,
        }
    }
}

fn text(label: &str, value: impl ToString) -> Field {
    Field {
        label: label.to_string(),
        value: FieldValue::Text {
            text: value.to_string(),
        },
    }
}

/// Sidebar link that selects `node`.
fn node_link(text: impl ToString, node: SimNodeId) -> FieldValue {
    FieldValue::Link {
        text: text.to_string(),
        action: UiAction::SelectNode {
            node: Some(NodeId(node)),
        },
    }
}

/// The directed edge a flight travels: sender to recipient.
fn flight_edge(f: &SimFlight) -> (SimNodeId, SimNodeId) {
    (f.message.sender, f.to)
}

fn kind(body: &WireBody<u8>) -> &'static str {
    match body {
        WireBody::Want(_) => "Want",
        WireBody::Have(_) => "Have",
    }
}

/// Linear blend of two colours as a CSS hex string, `t` in `0.0..=1.0`.
fn blend(from: (u8, u8, u8), to: (u8, u8, u8), t: f32) -> String {
    let t = t.clamp(0.0, 1.0);
    let c = |a: u8, b: u8| (a as f32 + (b as f32 - a as f32) * t).round() as u8;
    format!(
        "#{:02x}{:02x}{:02x}",
        c(from.0, to.0),
        c(from.1, to.1),
        c(from.2, to.2)
    )
}

/// Node fill for a coverage fraction, or `None` when nothing is expected
/// of the node yet.
fn coverage_fill(meter: &Meter, node: SimNodeId) -> Option<String> {
    let c = meter.metrics.node_coverage(node);
    (c.expected > 0).then(|| blend(COLD_FILL, FULL_FILL, c.delivered as f32 / c.expected as f32))
}

fn timer_text(remaining: Option<&impl std::fmt::Debug>) -> String {
    remaining
        .map(|r| format!("{r:?}"))
        .unwrap_or_else(|| "off".into())
}

/// How much of a timer's pie is left: `remaining` over `max`, in `0..=1`.
fn timer_ratio(remaining: Duration, max: Duration) -> f32 {
    if max.is_zero() {
        return 0.0;
    }
    (remaining.as_secs_f64() / max.as_secs_f64()).clamp(0.0, 1.0) as f32
}

/// The ops a node holds, relay and ext together. An open-ended held range
/// has no cardinality and counts nothing, but held ranges of real ops are
/// finite.
fn held_count(node: &CoreNodeState<SimNodeId, SimLogId, RealTime>) -> usize {
    node.relay
        .0
        .held_all()
        .union(&node.ext.0.held_all())
        .iter()
        .map(|(_, r)| r.len().unwrap_or(0))
        .sum()
}

/// An armed timer's moon: a pie of `remaining` over `max`, no label.
fn timer_moon(
    node: SimNodeId,
    slot: u8,
    color: &str,
    remaining: Duration,
    max: Duration,
) -> VizEvent {
    VizEvent::MoonStyle {
        node: NodeId(node),
        slot,
        style: MoonStyle {
            color: Some(color.into()),
            border_color: Some("#555".into()),
            background: Some("#333".into()),
            ratio: Some(timer_ratio(remaining, max)),
            ..Default::default()
        },
    }
}

impl Presenter for RouterPresenter {
    type State = VizState;

    /// Every link in both directions: the net's links are undirected, and
    /// the viz draws a pair as one line without an arrowhead, adding one
    /// only where a direction is styled.
    fn topology(&self, _: &VizState) -> Topology {
        Topology::from_parts(
            self.topology.nodes().map(NodeId),
            self.topology
                .edges()
                .flat_map(|(a, b)| [(NodeId(a), NodeId(b)), (NodeId(b), NodeId(a))]),
        )
    }

    fn on_action(&mut self, _: &VizState, action: &UiAction) {
        if let UiAction::SelectNode { node } = action {
            self.selected = node.map(|n| n.0);
        }
    }

    fn render(&self, (driver, (net, meter)): &VizState) -> Vec<VizEvent> {
        let mut events = Vec::new();

        // Directions with traffic on them: width grows with the number of
        // flights, colour says whether any of them is a Have.
        let mut flights: BTreeMap<(SimNodeId, SimNodeId), (usize, usize)> = BTreeMap::new();
        for flight in &net.inflight {
            let entry = flights.entry(flight_edge(flight)).or_default();
            match flight.message.body {
                WireBody::Want(_) => entry.0 += 1,
                WireBody::Have(_) => entry.1 += 1,
            }
        }
        for ((a, b), (wants, haves)) in &flights {
            events.push(VizEvent::EdgeStyle {
                source: NodeId(*a),
                target: NodeId(*b),
                style: EdgeStyle {
                    color: Some(if *haves > 0 { HAVE_COLOR } else { WANT_COLOR }.into()),
                    width: Some(1.0 + ((wants + haves) as f32).min(6.0)),
                    opacity: None,
                },
            });
        }

        // The transition that made this frame: a drop paints its direction
        // of the link, a receipt paints the receiver.
        if let Some(dropped) = &meter.last.dropped {
            let (a, b) = flight_edge(dropped);
            events.push(VizEvent::EdgeStyle {
                source: NodeId(a),
                target: NodeId(b),
                style: EdgeStyle {
                    color: Some(BAD_COLOR.into()),
                    width: Some(3.0),
                    opacity: None,
                },
            });
        }
        let receipt_border = meter
            .last
            .received
            .as_ref()
            .map(|(f, taught)| (f.to, if *taught { TAUGHT_COLOR } else { BAD_COLOR }));

        // Nodes: coverage as fill, authoring (else the last receipt) as
        // border, timers and the held share as moons. A timer's `remaining`
        // is as of the node's own clock, which trails the sim clock until
        // an event touches the node.
        let max_want = driver.max_want_interval();
        let max_have = driver.max_have_interval();
        let total_ops = meter.metrics.ops_authored();
        for (id, node) in &net.nodes {
            let lag = driver.lag(*id);
            let border_color = if meter.last.authored.contains(id) {
                Some(AUTHOR_COLOR.to_string())
            } else {
                receipt_border
                    .filter(|(to, _)| to == id)
                    .map(|(_, c)| c.to_string())
            };
            let style = NodeStyle {
                color: coverage_fill(meter, *id),
                border_color,
                ..Default::default()
            };
            if style != NodeStyle::default() {
                events.push(VizEvent::NodeStyle {
                    node: NodeId(*id),
                    style,
                });
            }
            if let Some(t) = &node.router.want_timer {
                let left = (*t.remaining).saturating_sub(lag);
                events.push(timer_moon(*id, WANT_MOON, WANT_COLOR, left, max_want));
            }
            if let Some(t) = &node.router.have_timer {
                let left = (*t.remaining).saturating_sub(lag);
                events.push(timer_moon(*id, HAVE_MOON, HAVE_COLOR, left, max_have));
            }
            if total_ops > 0 {
                events.push(VizEvent::MoonStyle {
                    node: NodeId(*id),
                    slot: HELD_MOON,
                    style: MoonStyle {
                        color: Some(HELD_COLOR.into()),
                        border_color: Some("#555".into()),
                        background: Some("#333".into()),
                        ratio: Some((held_count(node) as f32 / total_ops as f32).clamp(0.0, 1.0)),
                        ..Default::default()
                    },
                });
            }
        }

        let sidebar = match self
            .selected
            .and_then(|id| net.nodes.get(&id).map(|n| (id, n)))
        {
            Some((id, node)) => {
                let inbound: Vec<FieldValue> = net
                    .inflight
                    .iter()
                    .filter(|f| f.to == id)
                    .map(|f| {
                        node_link(
                            format!("{} from {}", kind(&f.message.body), f.message.sender),
                            f.message.sender,
                        )
                    })
                    .collect();
                let held = node.relay.0.held_all().union(&node.ext.0.held_all());
                let coverage = meter.metrics.node_coverage(id);
                VizEvent::Sidebar {
                    title: Some(format!("node {id}")),
                    fields: vec![
                        text("time", format!("{:?}", driver.now())),
                        text(
                            "coverage",
                            format!("{}/{}", coverage.delivered, coverage.expected),
                        ),
                        text("subscriptions", format!("{:?}", node.subscriptions)),
                        text("held", format!("{held:?}")),
                        text("relay usage", node.relay.0.usage()),
                        text(
                            "want timer",
                            timer_text(node.router.want_timer.as_ref().map(|t| &t.remaining)),
                        ),
                        text(
                            "have timer",
                            timer_text(node.router.have_timer.as_ref().map(|t| &t.remaining)),
                        ),
                        text("wants seen", node.router.seen.len()),
                        text(
                            "channels heard",
                            format!("{:?}", node.router.heard.keys().collect::<Vec<_>>()),
                        ),
                        Field {
                            label: "inbound in flight".into(),
                            value: FieldValue::List { items: inbound },
                        },
                    ],
                }
            }
            None => {
                let m = &meter.metrics;
                VizEvent::Sidebar {
                    title: Some("network".into()),
                    fields: vec![
                        text("time", format!("{:?}", driver.now())),
                        Field {
                            label: "nodes".into(),
                            value: FieldValue::List {
                                items: net.nodes.keys().map(|n| node_link(n, *n)).collect(),
                            },
                        },
                        text("in flight", net.inflight.len()),
                        text("want msgs", m.want_msgs),
                        text("have msgs", m.have_msgs),
                        text("receives", m.receives),
                        text("redundant receives", m.redundant_receives),
                        text("drops", m.drops),
                    ],
                }
            }
        };
        events.push(sidebar);
        events
    }
}

/// Build one seeded run of the named scenario as a steppable `Scenario`,
/// paired with the presenter that shows it. `name = None` takes the first
/// scenario in the config.
pub fn scenario(
    config: &Config,
    name: Option<&str>,
    seed: u64,
) -> anyhow::Result<(Scenario<Tick>, RouterPresenter)> {
    let (name, spec) = match name {
        Some(name) => (
            name,
            config
                .scenarios
                .get(name)
                .with_context(|| format!("no scenario named {name}"))?,
        ),
        None => config
            .scenarios
            .iter()
            .next()
            .map(|(n, s)| (n.as_str(), s))
            .context("config has no scenarios")?,
    };
    let parts = spec
        .build_parts(seed, &config.defaults)
        .with_context(|| format!("building {name} seed {seed}"))?;
    let presenter = RouterPresenter::new(parts.net.topology.clone());
    let machine = StateMachine::new(
        RunModel::new(Metered::new(parts.net)),
        (parts.behavior, (parts.state, Meter::new(parts.metrics))),
    );
    Ok((Scenario::initial(machine, Tick), presenter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use simviz::{Command, Simulation, Viewer};

    fn config() -> Config {
        Config::from_yaml(
            r#"
defaults:
  seeds: 1
  duration_ms: 1000
scenarios:
  tiny:
    nodes: 6
    topology: { kind: random-tree, extra_edges: 0.2 }
    loss: 0.1
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 5 }
    router: { want_ttl_ms: 500, have_ttl_ms: 500 }
    storage: { relay_cap: 65536 }
    policy:
      want: { kind: density-scaled, min_ms: 200, max_ms: 400, ref_n: 10 }
      have: { kind: fixed, min_ms: 20, max_ms: 80 }
    workload: { writers: 2, appends_per_sec: 20.0 }
  coarse:
    nodes: 6
    duration_ms: 10000
    topology: { kind: random-tree, extra_edges: 0.2 }
    loss: 0.1
    latency_ms: { distribution: uniform, min_ms: 1, max_ms: 5 }
    router: { want_ttl_ms: 3000, have_ttl_ms: 3000 }
    storage: { relay_cap: 65536 }
    policy:
      want: { kind: fixed, min_ms: 1000, max_ms: 2000 }
      have: { kind: fixed, min_ms: 500, max_ms: 1000 }
    workload: { writers: 2, appends_per_sec: 20.0 }
    time_quantum_ms: 500
"#,
        )
        .unwrap()
    }

    #[test]
    fn presenter_topology_mirrors_net_topology() {
        let cfg = config();
        let expected = cfg.scenarios["tiny"].topology(0);
        let (scenario, presenter) = scenario(&cfg, None, 0).unwrap();
        let topology = presenter.topology(scenario.state());
        assert_eq!(topology.nodes().len(), 6);
        // Each undirected link appears once per direction.
        assert_eq!(topology.edges().len(), 2 * expected.edge_count());
        for (a, b) in expected.edges() {
            for (s, t) in [(a, b), (b, a)] {
                assert!(
                    topology
                        .edges()
                        .iter()
                        .any(|e| e.source == NodeId(s) && e.target == NodeId(t)),
                    "{s}->{t} present"
                );
            }
        }
    }

    #[test]
    fn unknown_scenario_is_an_error() {
        assert!(scenario(&config(), Some("nope"), 0).is_err());
    }

    #[test]
    fn viewer_steps_the_run_and_highlights_traffic() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let f0 = viewer.frame();
        assert_eq!(f0.step, 0);
        assert!(
            matches!(f0.events.last(), Some(VizEvent::Sidebar { title: Some(t), .. }) if t == "network")
        );

        // Step until something is in flight; the first Want fires within a few events.
        let mut frame = None;
        for _ in 0..200 {
            let f = viewer.handle(Command::StepForward).unwrap();
            if f.events
                .iter()
                .any(|e| matches!(e, VizEvent::EdgeStyle { .. }))
            {
                frame = Some(f);
                break;
            }
        }
        let frame = frame.expect("traffic appeared within 200 steps");
        let topology = frame.topology.edges();
        let (_, (net, _)) = viewer.simulation().state();
        for e in &frame.events {
            if let VizEvent::EdgeStyle { source, target, .. } = e {
                assert!(
                    topology
                        .iter()
                        .any(|t| t.source == *source && t.target == *target),
                    "styled edge {source:?}->{target:?} is a topology edge"
                );
                // Styled the way the traffic flows, sender to recipient.
                assert!(
                    net.inflight
                        .iter()
                        .any(|f| f.message.sender == source.0 && f.to == target.0),
                    "{source:?}->{target:?} carries a flight that way"
                );
            }
        }

        // Stepping back replays from the start; with the driver inside the
        // model state the replay is the same run, so forward again matches.
        let back = viewer.handle(Command::StepBack).unwrap();
        assert_eq!(back.step, frame.step - 1);
        let again = viewer.handle(Command::StepForward).unwrap();
        assert_eq!(again, frame);
    }

    #[test]
    fn selecting_a_node_switches_the_sidebar() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let f = viewer
            .handle(Command::Ui {
                action: UiAction::SelectNode {
                    node: Some(NodeId(2)),
                },
            })
            .unwrap();
        let Some(VizEvent::Sidebar { title, fields }) = f.events.last() else {
            panic!("expected sidebar");
        };
        assert_eq!(title.as_deref(), Some("node 2"));
        assert!(fields.iter().any(|f| f.label == "relay usage"));
    }

    #[test]
    fn blend_runs_between_its_endpoints() {
        assert_eq!(blend(COLD_FILL, FULL_FILL, 0.0), "#30363d");
        assert_eq!(blend(COLD_FILL, FULL_FILL, 1.0), "#238636");
        assert_eq!(blend((0, 0, 0), (200, 100, 0), 0.5), "#643200");
        assert_eq!(blend((0, 0, 0), (10, 10, 10), 7.0), "#0a0a0a");
    }

    /// Step until `pred` holds for the frame just produced, or give up.
    fn step_until(
        viewer: &mut Viewer<Tick, RouterPresenter>,
        limit: usize,
        pred: impl Fn(&simviz::Frame, &VizState) -> bool,
    ) -> simviz::Frame {
        for _ in 0..limit {
            let f = viewer.handle(Command::StepForward).unwrap();
            if pred(&f, viewer.simulation().state()) {
                return f;
            }
        }
        panic!("condition not met within {limit} steps");
    }

    fn node_style(f: &simviz::Frame, node: u32) -> Option<&NodeStyle> {
        f.events.iter().find_map(|e| match e {
            VizEvent::NodeStyle { node: n, style } if *n == NodeId(node) => Some(style),
            _ => None,
        })
    }

    #[test]
    fn node_fill_follows_coverage() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        // Nothing authored yet: no node has a fill.
        assert!(
            viewer.frame().events.iter().all(|e| {
                !matches!(e, VizEvent::NodeStyle { style, .. } if style.color.is_some())
            })
        );
        let f = step_until(&mut viewer, 2000, |_, (_, (_, meter))| {
            (0..6).any(|n| meter.metrics.node_coverage(n).delivered > 0)
        });
        let (_, (_, meter)) = viewer.simulation().state();
        for n in 0..6 {
            let c = meter.metrics.node_coverage(n);
            let fill = node_style(&f, n).and_then(|s| s.color.as_deref());
            if c.expected == 0 {
                assert_eq!(fill, None, "node {n} has nothing expected");
            } else {
                let t = c.delivered as f32 / c.expected as f32;
                assert_eq!(
                    fill,
                    Some(blend(COLD_FILL, FULL_FILL, t).as_str()),
                    "node {n}"
                );
            }
        }
        assert!((0..6).any(|n| node_style(&f, n).is_some_and(|s| s.color.is_some())));
    }

    #[test]
    fn timers_show_as_moons() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let f = step_until(&mut viewer, 2000, |_, (_, (net, _))| {
            net.nodes.values().any(|n| n.router.have_timer.is_some())
                && net.nodes.values().any(|n| n.router.want_timer.is_some())
        });
        let (driver, (net, _)) = viewer.simulation().state();
        let moons: Vec<_> = f
            .events
            .iter()
            .filter_map(|e| match e {
                VizEvent::MoonStyle { node, slot, style } => Some((node.0, *slot, style)),
                _ => None,
            })
            .collect();
        for (id, node) in &net.nodes {
            for (slot, timer, color, max) in [
                (
                    WANT_MOON,
                    &node.router.want_timer,
                    WANT_COLOR,
                    driver.max_want_interval(),
                ),
                (
                    HAVE_MOON,
                    &node.router.have_timer,
                    HAVE_COLOR,
                    driver.max_have_interval(),
                ),
            ] {
                let moon = moons.iter().find(|(n, s, _)| n == id && *s == slot);
                match timer {
                    Some(t) => {
                        let (_, _, style) = moon.expect("armed timer has a moon");
                        assert_eq!(style.color.as_deref(), Some(color));
                        // The pie is the time left as of the sim clock, not
                        // the node's lagging one, over the policy's max.
                        let left = (*t.remaining).saturating_sub(driver.lag(*id));
                        let ratio = style.ratio.expect("timer moon has a ratio");
                        assert!(
                            (0.0..=1.0).contains(&ratio),
                            "node {id} slot {slot}: {ratio}"
                        );
                        assert_eq!(ratio, timer_ratio(left, max), "node {id} slot {slot}");
                    }
                    None => assert!(moon.is_none(), "node {id} slot {slot} has no timer"),
                }
            }
        }
        assert!(moons.iter().all(|(_, s, _)| *s < simviz::MOON_SLOTS));
    }

    #[test]
    fn held_moon_shows_the_held_share() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        // No ops yet: no held moons.
        assert!(
            !viewer
                .frame()
                .events
                .iter()
                .any(|e| matches!(e, VizEvent::MoonStyle { slot, .. } if *slot == HELD_MOON))
        );
        let f = step_until(&mut viewer, 2000, |_, (_, (_, meter))| {
            meter.metrics.ops_authored() > 0
        });
        let (_, (net, meter)) = viewer.simulation().state();
        let total = meter.metrics.ops_authored();
        for (id, node) in &net.nodes {
            let moon = f
                .events
                .iter()
                .find_map(|e| match e {
                    VizEvent::MoonStyle {
                        node: n,
                        slot,
                        style,
                    } if *n == NodeId(*id) && *slot == HELD_MOON => Some(style),
                    _ => None,
                })
                .expect("every node has a held moon once ops exist");
            assert_eq!(moon.color.as_deref(), Some(HELD_COLOR));
            assert_eq!(
                moon.ratio,
                Some(held_count(node) as f32 / total as f32),
                "node {id}"
            );
        }
    }

    #[test]
    fn authoring_paints_a_bright_border() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let f = step_until(&mut viewer, 2000, |_, (_, (_, meter))| {
            !meter.last.authored.is_empty()
        });
        let (_, (_, meter)) = viewer.simulation().state();
        for n in &meter.last.authored {
            assert_eq!(
                node_style(&f, *n).and_then(|s| s.border_color.as_deref()),
                Some(AUTHOR_COLOR)
            );
        }
        // The trace is gone on the next frame unless it happens again.
        let next = viewer.handle(Command::StepForward).unwrap();
        let (_, (_, meter)) = viewer.simulation().state();
        if meter.last.authored.is_empty() {
            assert!(!next.events.iter().any(|e| matches!(e,
                VizEvent::NodeStyle { style, .. }
                    if style.border_color.as_deref() == Some(AUTHOR_COLOR))));
        }
    }

    /// In coarse time the frame merges a quantum's transitions; an append
    /// that was not the quantum's last event still borders its author.
    #[test]
    fn coarse_frames_keep_every_author() {
        let (scenario, presenter) = scenario(&config(), Some("coarse"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let f = step_until(&mut viewer, 100, |_, (_, (_, meter))| {
            !meter.last.authored.is_empty()
        });
        let (_, (_, meter)) = viewer.simulation().state();
        for n in &meter.last.authored {
            assert_eq!(
                node_style(&f, *n).and_then(|s| s.border_color.as_deref()),
                Some(AUTHOR_COLOR),
                "author {n} bordered in the coarse frame"
            );
        }
    }

    #[test]
    fn receipts_and_drops_leave_a_one_frame_trace() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);

        let f = step_until(&mut viewer, 2000, |_, (_, (_, meter))| {
            meter.last.received.is_some()
        });
        let (_, (_, meter)) = viewer.simulation().state();
        let (flight, taught) = meter.last.received.clone().unwrap();
        let expected = if taught { TAUGHT_COLOR } else { BAD_COLOR };
        assert_eq!(
            node_style(&f, flight.to).and_then(|s| s.border_color.as_deref()),
            Some(expected)
        );
        // Only the receiver carries a border.
        let bordered = f
            .events
            .iter()
            .filter(
                |e| matches!(e, VizEvent::NodeStyle { style, .. } if style.border_color.is_some()),
            )
            .count();
        assert_eq!(bordered, 1);

        let f = step_until(&mut viewer, 5000, |_, (_, (_, meter))| {
            meter.last.dropped.is_some()
        });
        let (_, (_, meter)) = viewer.simulation().state();
        let dropped = meter.last.dropped.clone().unwrap();
        let (a, b) = flight_edge(&dropped);
        let red = f.events.iter().any(|e| {
            matches!(e,
            VizEvent::EdgeStyle { source, target, style }
                if *source == NodeId(a) && *target == NodeId(b)
                    && style.color.as_deref() == Some(BAD_COLOR))
        });
        assert!(red, "dropped flight's edge is painted");
        // The trace is gone on the next frame unless it happens again.
        let next = viewer.handle(Command::StepForward).unwrap();
        let (_, (_, meter)) = viewer.simulation().state();
        if meter.last.dropped.is_none() {
            assert!(!next.events.iter().any(|e| matches!(e,
                VizEvent::EdgeStyle { style, .. } if style.color.as_deref() == Some(BAD_COLOR))));
        }
    }

    #[test]
    fn sidebar_links_select_nodes() {
        let (scenario, presenter) = scenario(&config(), Some("tiny"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        // The network view lists every node as a link.
        let f = viewer.frame();
        let Some(VizEvent::Sidebar { fields, .. }) = f.events.last() else {
            panic!("expected sidebar");
        };
        let nodes = fields.iter().find(|f| f.label == "nodes").unwrap();
        let FieldValue::List { items } = &nodes.value else {
            panic!("nodes is a list");
        };
        assert_eq!(items.len(), 6);
        let FieldValue::Link { text, action } = &items[4] else {
            panic!("node entries are links");
        };
        assert_eq!(text, "4");
        // Following it selects that node.
        let f = viewer
            .handle(Command::Ui {
                action: action.clone(),
            })
            .unwrap();
        assert_eq!(f.selected, Some(NodeId(4)));
        assert!(
            matches!(f.events.last(), Some(VizEvent::Sidebar { title: Some(t), .. }) if t == "node 4")
        );

        // A node with something inbound links back to each sender.
        let f = step_until(&mut viewer, 2000, |f, (_, (net, _))| {
            net.inflight.iter().any(|fl| fl.to == 4) && f.selected == Some(NodeId(4))
        });
        let (_, (net, _)) = viewer.simulation().state();
        let Some(VizEvent::Sidebar { fields, .. }) = f.events.last() else {
            panic!("expected sidebar");
        };
        let inbound = fields
            .iter()
            .find(|f| f.label == "inbound in flight")
            .unwrap();
        let FieldValue::List { items } = &inbound.value else {
            panic!("inbound is a list");
        };
        let senders: Vec<u32> = net
            .inflight
            .iter()
            .filter(|fl| fl.to == 4)
            .map(|fl| fl.message.sender)
            .collect();
        assert_eq!(items.len(), senders.len());
        for (item, sender) in items.iter().zip(senders) {
            assert!(
                matches!(item, FieldValue::Link { action: UiAction::SelectNode { node: Some(n) }, .. } if n.0 == sender)
            );
        }
    }

    /// In coarse time one frame is one quantum: every event due within it
    /// happens in that step, and each node ends the frame caught up to the
    /// boundary unless a due timer holds its clock there.
    #[test]
    fn coarse_time_steps_one_quantum_per_frame() {
        let (scenario, presenter) = scenario(&config(), Some("coarse"), 0).unwrap();
        let mut viewer = Viewer::new(Simulation::new(scenario), presenter);
        let quantum = Duration::from_millis(500);
        // Frame 1 arms the timers at time zero.
        viewer.handle(Command::StepForward).unwrap();
        for k in 1..=8u32 {
            viewer.handle(Command::StepForward).unwrap();
            let (driver, (net, _)) = viewer.simulation().state();
            assert_eq!(driver.now(), quantum * k, "frame {k}");
            assert!(
                driver.peek_at().is_some_and(|at| at > driver.now()),
                "frame {k} left an event due within it"
            );
            for (id, node) in &net.nodes {
                let lag = driver.lag(*id);
                let held = node.router.next_due().is_some_and(|d| (*d).is_zero());
                assert!(lag.is_zero() || held, "node {id} lags {lag:?} at frame {k}");
            }
        }
    }
}
