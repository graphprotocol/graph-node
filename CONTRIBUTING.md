# Contributing to graph-node

Welcome to the Graph Protocol! Thanks a ton for your interest in contributing.

If you run into any problems feel free to create an issue. PRs are much appreciated for simple things. Here's [a list of good first issues](https://github.com/graphprotocol/graph-node/labels/good%20first%20issue). If it's something more complex we'd appreciate having a quick chat in GitHub Issues or Discord.

Join the conversation on our [Discord](https://discord.gg/9a5VCua).

Please follow the [Code of Conduct](https://github.com/graphprotocol/graph-node/blob/master/CODE_OF_CONDUCT.md) for all the communications and at events. Thank you!

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
    -x "fmt --all"                 \
    -x check                      \
    -x "test -- --test-threads=1" \
    -x "doc --no-deps"
```

This will watch your source directory and continuously do the following on changes:

1.  Build all packages in the workspace `target/`.
2.  Generate docs for all packages in the workspace in `target/doc/`.
3.  Automatically format all your source files.

### Testing with a sharded store

The tests can (and should) be run against a sharded store. The tests will
read their [configuration](docs/multiple-databases.md) from the file that the
environment variable `GRAPH_NODE_TEST_CONFIG` points to.

## Commit messages

We use the following format for commit messages:
`{crate-name}: {Brief description of changes}`, for example: `store: Support 'Or' filters`.

If multiple crates are being changed list them all like this: `core, graphql, mock, runtime, postgres-diesel: Add event source to store`
