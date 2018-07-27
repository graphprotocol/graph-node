<p align="center">
  <img width="100%" src="resources/construction.svg" title="Under Construction" />
</p>

# The Graph

This repository contains the source code for The Graph node.

Right now, this includes:

- `node` — A local Graph node.
- `graph` — A library providing traits for system components and types for
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

The Graph is written in Rust. In order to build and run this project you need
to have the following installed on your system:

- [How to install Rust](https://www.rust-lang.org/en-US/install.html)
- [PostgreSQL Downloads](https://www.postgresql.org/download/)
- [Installing IPFS](https://ipfs.io/docs/install/)

For Ethereum network data you can either run a local node or use Infura.io:

- [Installing and running Ethereum node](https://ethereum.gitbooks.io/frontier-guide/content/getting_a_client.html)
- [Infura.io](https://infura.io/)

## Getting Started

### Environment Variables

The Graph supports the following environment variables:

```
THEGRAPH_SENTRY_URL (optional) — Activates error reporting using Sentry
```

### Running a local node

*Steps to set up Postgres*

1. Set up: `initdb -D .postgres` (somewhere; creates a Postgres config in `.postgres/`)
2. Start: `pg_ctl -D .postgres start` (or `postgres -D .postgres`?)
3. Create database: `createdb decentraland`
4. Delete database (whenever you want): `dropdb decentraland`
5. Log in to the database `psql decentraland` (edited)

*Start the Graph node*

1. Install IPFS and run `ipfs init` followed by `ipfs daemon`
2. Install and start Postgres and create a decentraland db with `createdb decentraland`
2. Clone https://github.com/graphprotocol/decentraland and build it with `yarn build-ipfs --verbosity debug` -> remember/copy the IPFS hash
3. Clone https://github.com/graphprotocol/graph-node and run `cargo build`

Once you have all the dependencies setup you can run the following:
```
cargo run -p graph-node -- \
  --postgres-url postgresql://localhost:5432/decentraland \
  --ethereum-ws wss://mainnet.infura.io/_ws \
  --ipfs 127.0.0.1:5001 \
  --data-source IPFS_HASH
```

This will also sping up GraphiQL interface at `http://127.0.0.1:8000/`

```
USAGE:
    graph-node
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

## License

Copyright &copy; 2018 Graph Protocol, Inc. and contributors.

The Graph is dual-licensed under the [MIT license](LICENSE-MIT) and the
[Apache License, Version 2.0](LICENSE-APACHE).
