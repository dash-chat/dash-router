[default]
list:
    just --list

# run the full rust test suite with cargo nextest
test *ARGS:
    cargo nextest run {{ ARGS }}

sim SCENARIO OUT:
    cargo run --release --bin sim -- crates/dash-router-sim/scenarios/{{ SCENARIO }}.yaml --out {{ OUT }}

# sweep want/have interval maximums; extra args go to the sweep binary
sweep OUT *ARGS:
    cargo run --release --bin sweep -- --out {{ OUT }} {{ ARGS }}

# serve the web viz for one scenario on :3001; extra args go to dash-router-viz (--seed N, --port N, --record STEPS --out FILE)
viz SCENARIO="lan-20" *ARGS:
    RUST_LOG=info cargo run --release -p dash-router-viz -- crates/dash-router-sim/scenarios/example.yaml --scenario {{ SCENARIO }} {{ ARGS }}

# record SCENARIO and bundle it with the prebuilt simviz frontend into a standalone
# static dir at target/viz-export/SCENARIO; serve it anywhere (assets are relative)
# and open index.html?recording=recording.json
viz-export SCENARIO="lan-20-coarse" STEPS="50" *ARGS:
    mkdir -p target/viz-export/{{ SCENARIO }}
    just viz {{ SCENARIO }} --record {{ STEPS }} --out target/viz-export/{{ SCENARIO }}/recording.json {{ ARGS }}
    cp -r /home/michael/work/simdash/crates/simviz/web/build/. target/viz-export/{{ SCENARIO }}/
    @echo "exported: target/viz-export/{{ SCENARIO }}  (try: python3 -m http.server -d target/viz-export/{{ SCENARIO }})"

# build the static demo site into target/viz-site: the demo.yaml scenarios recorded
# for STEPS steps each (as scenario:seed pairs), plus the simviz frontend in replay mode.
# Servable from any path.
viz-site STEPS="300":
    rm -rf target/viz-site
    mkdir -p target/viz-site/recordings
    cargo run --release -p dash-router-viz -- --export-ui target/viz-site/viewer
    for run in line-5:0 line-3-lossy:26 lan-20:0; do cargo run --release -p dash-router-viz -- crates/dash-router-sim/scenarios/demo.yaml --scenario ${run%:*} --seed ${run#*:} --record {{ STEPS }} --out target/viz-site/recordings/${run%:*}.json; done
    cp -r site/. target/viz-site/
    touch target/viz-site/.nojekyll
    @echo "built: target/viz-site"
