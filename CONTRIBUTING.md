# Contributing to graph-node

## Development flow

Install development helpers:

```sh
cargo install cargo-watch
rustup component add rustfmt-preview
```

Set environment variables:

```sh
# Only required when testing the Diesel/Postgres store
export THEGRAPH_STORE_POSTGRES_DIESEL_URL=<Postgres database URL>
```

While developing, a useful command to run in the background is this:

```sh
cargo watch                       \
    -x "fmt -all"                 \
    -x check                      \
    -x "test -- --test-threads=1" \
    -x "doc --no-deps"
```

This will watch your source directory and continuously do the following on changes:

1.  Build all packages in the workspace `target/`.
2.  Generate docs for all packages in the workspace in `target/doc/`.
3.  Automatically format all your source files.

## Commit messages

We use the following format for commit messages:
`{crate-name}: {Brief description of changes}`, for example: `store: Support 'Or' filters`.

If multiple crates are being changed list them all like this: `core, graphql, mock, runtime, postgres-diesel: Add event source to store`
