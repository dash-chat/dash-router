//! Parameter sweeps: run one base scenario across a grid of interval
//! settings and tabulate the resulting cost per op.
//!
//! The grid varies the want and have `max_ms` of the base scenario's
//! interval policies while holding each `min_ms` fixed. Every cell runs
//! the full seed set and is summarized by the same [`ScenarioReport`]
//! the `sim` binary writes, so numbers here and there agree.

use anyhow::Context;
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    policy::IntervalPolicy,
    report::ScenarioReport,
    scenario::{Defaults, ScenarioSpec},
};

/// The parameter space: fixed minimums and the candidate maximums.
#[derive(Clone, Debug)]
pub struct Grid {
    pub want_min_ms: f64,
    pub want_max_ms: Vec<f64>,
    pub have_min_ms: f64,
    pub have_max_ms: Vec<f64>,
}

impl Grid {
    /// Every `(want_max, have_max)` pair worth running: a max below its
    /// min is not a valid interval and is skipped.
    pub fn cells(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.want_max_ms
            .iter()
            .filter(|w| **w >= self.want_min_ms)
            .flat_map(move |w| {
                self.have_max_ms
                    .iter()
                    .filter(|h| **h >= self.have_min_ms)
                    .map(move |h| (*w, *h))
            })
    }
}

/// `n` values from `lo` to `hi` inclusive, evenly spaced in log space
/// and rounded to whole milliseconds.
pub fn geomspace_ms(lo: f64, hi: f64, n: usize) -> Vec<f64> {
    match n {
        0 => vec![],
        1 => vec![lo.round()],
        _ => (0..n)
            .map(|i| {
                let t = i as f64 / (n - 1) as f64;
                (lo * (hi / lo).powf(t)).round()
            })
            .collect(),
    }
}

/// One grid cell's outcome.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Cell {
    pub want_max_ms: f64,
    pub have_max_ms: f64,
    pub messages_per_op: f64,
    pub coverage_rate: f64,
    pub t_full_ms_p95: f64,
}

/// The base scenario with its interval maximums replaced.
pub fn patch(base: &ScenarioSpec, want_max_ms: f64, have_max_ms: f64) -> ScenarioSpec {
    let mut spec = base.clone();
    set_max(&mut spec.policy.want, want_max_ms);
    set_max(&mut spec.policy.have, have_max_ms);
    spec
}

fn set_max(policy: &mut IntervalPolicy, max: f64) {
    match policy {
        IntervalPolicy::Fixed { max_ms, .. } | IntervalPolicy::DensityScaled { max_ms, .. } => {
            *max_ms = max
        }
    }
}

/// Run every cell of the grid in parallel. `on_done` is called from
/// worker threads as each cell finishes, in no particular order.
pub fn run_grid(
    base: &ScenarioSpec,
    defaults: &Defaults,
    grid: &Grid,
    on_done: impl Fn(&Cell) + Sync,
) -> anyhow::Result<Vec<Cell>> {
    let cells: Vec<(f64, f64)> = grid.cells().collect();
    let mut done = cells
        .par_iter()
        .map(|&(want_max_ms, have_max_ms)| {
            let spec = patch(base, want_max_ms, have_max_ms);
            let name = format!("want{want_max_ms}-have{have_max_ms}");
            let runs = (0..spec.seeds(defaults))
                .map(|seed| {
                    spec.build(seed, defaults)
                        .and_then(|mut sim| sim.run(seed))
                        .with_context(|| format!("{name} seed {seed}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let report = ScenarioReport::new(name, spec, runs);
            let cell = Cell {
                want_max_ms,
                have_max_ms,
                messages_per_op: report.summary.messages_per_op,
                coverage_rate: report.summary.coverage_rate,
                t_full_ms_p95: report.summary.t_full_ms_p95.mean,
            };
            on_done(&cell);
            Ok(cell)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    done.sort_by(|a, b| {
        (a.want_max_ms, a.have_max_ms).partial_cmp(&(b.want_max_ms, b.have_max_ms)).unwrap()
    });
    Ok(done)
}

pub fn to_csv(cells: &[Cell]) -> String {
    let mut out = String::from("want_max_ms,have_max_ms,messages_per_op,coverage_rate,t_full_ms_p95\n");
    for c in cells {
        out.push_str(&format!(
            "{},{},{:.3},{:.4},{:.1}\n",
            c.want_max_ms, c.have_max_ms, c.messages_per_op, c.coverage_rate, c.t_full_ms_p95
        ));
    }
    out
}

/// A self-contained Plotly heatmap: want max on x, have max on y,
/// messages per op as color. Skipped cells render as gaps.
pub fn to_html(grid: &Grid, cells: &[Cell], title: &str) -> String {
    let xs: Vec<String> = grid.want_max_ms.iter().map(|v| format!("{v}")).collect();
    let ys: Vec<String> = grid.have_max_ms.iter().map(|v| format!("{v}")).collect();
    let lookup = |w: f64, h: f64| cells.iter().find(|c| c.want_max_ms == w && c.have_max_ms == h);
    let z: Vec<Vec<Option<f64>>> = grid
        .have_max_ms
        .iter()
        .map(|&h| {
            grid.want_max_ms
                .iter()
                .map(|&w| lookup(w, h).map(|c| c.messages_per_op))
                .collect()
        })
        .collect();
    // [coverage %, t_full p95 ms] per cell, for the tooltip.
    let custom: Vec<Vec<Option<[f64; 2]>>> = grid
        .have_max_ms
        .iter()
        .map(|&h| {
            grid.want_max_ms
                .iter()
                .map(|&w| lookup(w, h).map(|c| [c.coverage_rate * 100.0, c.t_full_ms_p95]))
                .collect()
        })
        .collect();
    let data = serde_json::json!({
        "x": xs, "y": ys, "z": z, "customdata": custom,
        "want_min_ms": grid.want_min_ms, "have_min_ms": grid.have_min_ms,
        "title": title,
        "rows": cells,
    });
    HTML_TEMPLATE.replace("__DATA__", &data.to_string())
}

const HTML_TEMPLATE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Interval sweep</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
<style>
  :root { --bg: #ffffff; --ink: #1f1f1e; --muted: #6b6b68; --line: #e4e3df; }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #1b1b1a; --ink: #ecebe7; --muted: #a3a29d; --line: #3a3a37; }
  }
  body { margin: 0; padding: 16px; background: var(--bg); color: var(--ink);
         font: 14px/1.4 system-ui, sans-serif; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  p.sub { color: var(--muted); margin: 0 0 12px; }
  #plot { width: 100%; max-width: 900px; height: min(80vh, 720px); }
  details { max-width: 900px; margin-top: 16px; }
  table { border-collapse: collapse; font-variant-numeric: tabular-nums; }
  th, td { text-align: right; padding: 2px 10px; border-bottom: 1px solid var(--line); }
</style>
</head>
<body>
<h1 id="title"></h1>
<p class="sub" id="sub"></p>
<div id="plot"></div>
<details><summary>Data table</summary><table id="table"></table></details>
<script>
const D = __DATA__;
const dark = matchMedia('(prefers-color-scheme: dark)').matches;
const css = v => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
document.getElementById('title').textContent = D.title;
document.getElementById('sub').textContent =
  `messages per op; want min ${D.want_min_ms} ms, have min ${D.have_min_ms} ms`;
// One-hue sequential ramp (blue 100 -> 700).
const ramp = ['#cde2fb','#9ec5f4','#6da7ec','#3987e5','#256abf','#184f95','#0d366b'];
const colorscale = ramp.map((c, i) => [i / (ramp.length - 1), c]);
Plotly.newPlot('plot', [{
  type: 'heatmap', x: D.x, y: D.y, z: D.z, customdata: D.customdata,
  colorscale, xgap: 2, ygap: 2,
  texttemplate: '%{z:.0f}', textfont: { size: 11 },
  hovertemplate: 'want max %{x} ms<br>have max %{y} ms' +
    '<br><b>%{z:.1f} msgs/op</b>' +
    '<br>coverage %{customdata[0]:.1f}%' +
    '<br>t_full p95 %{customdata[1]:.0f} ms<extra></extra>',
  colorbar: { title: { text: 'msgs/op' }, thickness: 12, outlinewidth: 0 },
}], {
  paper_bgcolor: css('--bg'), plot_bgcolor: css('--bg'),
  font: { color: css('--ink') },
  xaxis: { title: { text: 'want max_ms' }, type: 'category', showgrid: false },
  yaxis: { title: { text: 'have max_ms' }, type: 'category', showgrid: false },
  margin: { l: 70, r: 20, t: 10, b: 60 },
}, { responsive: true, displaylogo: false });
const cols = ['want_max_ms','have_max_ms','messages_per_op','coverage_rate','t_full_ms_p95'];
const tbl = document.getElementById('table');
tbl.innerHTML = '<tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr>' +
  D.rows.map(r => '<tr>' + cols.map(c => `<td>${
    typeof r[c] === 'number' ? +r[c].toFixed(3) : r[c]}</td>`).join('') + '</tr>').join('');
</script>
</body>
</html>
"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometric_grid_hits_both_endpoints_and_rounds_to_ms() {
        let g = geomspace_ms(300.0, 3000.0, 4);
        assert_eq!(g, vec![300.0, 646.0, 1392.0, 3000.0]);
    }

    #[test]
    fn geometric_grid_with_one_step_is_the_low_end() {
        assert_eq!(geomspace_ms(30.0, 1000.0, 1), vec![30.0]);
    }

    #[test]
    fn grid_skips_cells_whose_max_undercuts_min() {
        let grid = Grid {
            want_min_ms: 300.0,
            want_max_ms: vec![200.0, 400.0],
            have_min_ms: 30.0,
            have_max_ms: vec![20.0, 50.0],
        };
        let cells: Vec<(f64, f64)> = grid.cells().collect();
        assert_eq!(cells, vec![(400.0, 50.0)]);
    }

    #[test]
    fn csv_has_a_header_and_one_row_per_cell() {
        let cells = vec![Cell {
            want_max_ms: 400.0,
            have_max_ms: 50.0,
            messages_per_op: 12.5,
            coverage_rate: 1.0,
            t_full_ms_p95: 321.0,
        }];
        let csv = to_csv(&cells);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines,
            vec![
                "want_max_ms,have_max_ms,messages_per_op,coverage_rate,t_full_ms_p95",
                "400,50,12.500,1.0000,321.0",
            ]
        );
    }

    #[test]
    fn html_embeds_every_cell_and_nulls_the_skipped_ones() {
        let grid = Grid {
            want_min_ms: 300.0,
            want_max_ms: vec![200.0, 400.0],
            have_min_ms: 30.0,
            have_max_ms: vec![50.0],
        };
        let cells = vec![Cell {
            want_max_ms: 400.0,
            have_max_ms: 50.0,
            messages_per_op: 12.5,
            coverage_rate: 1.0,
            t_full_ms_p95: 321.0,
        }];
        let html = to_html(&grid, &cells, "title");
        assert!(html.contains("[[null,12.5]]"), "{html}");
        assert!(html.contains("plotly"));
    }
}
