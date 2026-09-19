[default]
list:
    just --list

# run the full rust test suite with cargo nextest
test *ARGS:
    cargo nextest run {{ ARGS }}
