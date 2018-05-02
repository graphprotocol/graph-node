# The Graph Network Components

This repository contains the source code for the network components of The Graph.
Right now, this includes:

1. A common library used by all nodes, called `thegraph`.
2. A local-only The Graph node, called `thegraph-local-node`.

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

### Running

To build and run the local node from the root directory in this repository, simply run
```sh
cargo run -p thegraph-local-node
````
