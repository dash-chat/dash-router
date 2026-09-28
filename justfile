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
