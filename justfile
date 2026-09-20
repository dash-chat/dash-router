[default]
list:
    just --list

# run the full rust test suite with cargo nextest
test *ARGS:
    cargo nextest run {{ ARGS }}

sim SCENARIO OUT:
    cargo run --release --bin sim -- crates/dash-router-sim/scenarios/{{ SCENARIO }}.yaml --out {{ OUT }}
