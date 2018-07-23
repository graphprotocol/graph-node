<p align="center">
  <img width="100%" src="resources/construction.svg" title="Under Construction" />
</p>

# The Graph

This repository contains the source code for The Graph node.

Right now, this includes:

- `node` — A local Graph node.
- `thegraph` — A library providing traits for system components and types for
  common data.
- `core` — A library providing implementations for core components, used by all
  nodes.
- `datasource/ethereum` — A library with components for obtaining data from
  Ethereum.
- `graphql` — A GraphQL implementation with API schema generation,
  introspection and more.
- `mock` — A library providing mock implementations for all system components.
- `runtime/wasm` — A library for running WASM data extraction scripts.
- `server/http` — A library providing a GraphQL server over HTTP.
- `store/postgres` — A Postgres store with a GraphQL friendly interface
  and audit logs.

## Prerequisites

The network components of The Graph are written in Rust. In order to build and
run this project you need to have Rust installed on your system:

- [How to install Rust](https://www.rust-lang.org/en-US/install.html)

## Getting Started

### Environment Variables

The Graph supports, and in some cases requires, the following environment variables:

```
THEGRAPH_SENTRY_URL (optional) — Activates error reporting using Sentry
```

### Running a local node

```
USAGE:
    thegraph-node
      --data-source <IPFS_HASH>
      --ethereum-ipc <FILE>
        or --ethereum-rpc <URL>
        or --ethereum-ws <URL>
      --ipfs <HOST:PORT>
      --postgres-url <URL>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --data-source <IPFS_HASH>    IPFS hash of the data source definition file
        --ethereum-ipc <FILE>        Ethereum IPC pipe
        --ethereum-rpc <URL>         Ethereum RPC endpoint
        --ethereum-ws <URL>          Ethereum WebSocket endpoint
        --ipfs <HOST:PORT>           HTTP address of an IPFS node
        --postgres-url <URL>         Location of the Postgres database used for storing entities
```

### Developing

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
