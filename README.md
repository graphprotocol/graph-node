<p align="center">
  <img width="100%" src="resources/construction.svg" title="Under Construction" />
</p>

# The Graph Network Components

This repository contains the source code for the network components of The Graph.

Right now, this includes:

1.  `thegraph-local-node`: A local-only The Graph node.
2.  `thegraph`: A library providing traits for system components and types for common data.
3.  `thegraph-core`: A library providing implementations for core components, used by all nodes.
4.  `thegraph-hyper`: A library providing an implementation of the GraphQL server component
    based on Hyper.
5.  `thegraph-mock`: A library providing mock implementations for all system components.

## Prerequisites

The network components of The Graph are written in Rust. In order to build and
run this project you need to have Rust installed on your system:

* [How to install Rust](https://www.rust-lang.org/en-US/install.html)

## Getting Started

### Environment Variables

The Graph supports, and in some cases requires, the following environment variables:

```
THEGRAPH_SENTRY_URL (optional) — Activates error reporting using Sentry
```

### Running a local node

```
USAGE:
    thegraph-local-node --postgres-url <URL>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --postgres-url <URL>    Location of the Postgres database used for storing entities
```

### Developing

Install development helpers:

```sh
cargo install cargo-watch
rustup component add rustfmt-preview
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
