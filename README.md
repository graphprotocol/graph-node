
# Graph Node

[![Build Status](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml/badge.svg)](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml?query=branch%3Amaster)
[![Getting Started Docs](https://img.shields.io/badge/docs-getting--started-brightgreen.svg)](docs/getting-started.md)

## Overview

[The Graph](https://thegraph.com/) is a decentralized protocol that organizes and distributes blockchain data across the leading Web3 networks. A key component of The Graph's tech stack is Graph Node.

Before using `graph-node,` it is highly recommended that you read the [official Graph documentation](https://thegraph.com/docs/en/subgraphs/quick-start/) to understand Subgraphs, which are the central mechanism for extracting and organizing blockchain data.

This guide is for:

1. Subgraph developers who want to run `graph-node` locally to test their Subgraphs during development
2. Contributors who want to add features or fix bugs to `graph-node` itself

## Running `graph-node` from Docker images

For subgraph developers, it is highly recommended to use prebuilt Docker
images to set up a local `graph-node` environment. Please read [these
instructions](./docker/README.md) to learn how to do that.

## Running `graph-node` from source

This is usually only needed for developers who want to contribute to `graph-node`.

### Prerequisites

To build and run this project, you need to have the following installed on your system:

- Rust (latest stable): Follow [How to install
  Rust](https://www.rust-lang.org/en-US/install.html). Run `rustup install
stable` in _this directory_ to make sure all required components are
  installed. The `graph-node` code assumes that the latest available
  `stable` compiler is used.
- PostgreSQL: [PostgreSQL Downloads](https://www.postgresql.org/download/) lists
  downloads for almost all operating systems.
  - For OSX: We highly recommend [Postgres.app](https://postgresapp.com/).
  - For Linux: Use the Postgres version that comes with the distribution.
- IPFS: [Installing IPFS](https://docs.ipfs.io/install/)
- Protobuf Compiler: [Installing Protobuf](https://grpc.io/docs/protoc-installation/)

For Ethereum network data, you can either run your own Ethereum node or use an Ethereum node provider of your choice.

### Create a database

Once Postgres is running, you need to issue the following commands to create a database
and configure it for use with `graph-node`.

The name of the `SUPERUSER` depends on your installation, but is usually `postgres` or your username.

```bash
psql -U <SUPERUSER> <<EOF
create user graph with password '<password>';
create database "graph-node" with owner=graph template=template0 encoding='UTF8' locale='C';
create extension pg_trgm;
create extension btree_gist;
create extension postgres_fdw;
grant usage on foreign data wrapper postgres_fdw to graph;
EOF
```

For convenience, set the connection string to the database in an environment
variable, and save it, e.g., in `~/.bashrc`:

```bash
export POSTGRES_URL=postgresql://graph:<password>@localhost:5432/graph-node
```

Use the `POSTGRES_URL` from above to have `graph-node` connect to the
database. If you ever need to manually inspect the contents of your
database, you can do that by running `psql $POSTGRES_URL`. Running this
command is also a convenient way to check that the database is up and
running and that the connection string is correct.

### Build and Run `graph-node`

Clone this repository and run this command at the root of the repository:

```bash
export GRAPH_LOG=debug
cargo run -p graph-node --release -- \
  --postgres-url $POSTGRES_URL \
  --ethereum-rpc NETWORK_NAME:[CAPABILITIES]:URL \
  --ipfs 127.0.0.1:5001
```

The argument for `--ethereum-rpc` contains a network name (e.g. `mainnet`) and
a list of provider capabilities (e.g. `archive,traces`). The URL is the address
of the Ethereum node you want to connect to, usually a `https` URL, so that the
entire argument might be `mainnet:archive,traces:https://provider.io/some/path`.

When `graph-node` starts, it prints the various ports that it is listening on.
The most important of these is the GraphQL HTTP server, which by default
is at `http://localhost:8000`. You can use routes like `/subgraphs/name/<subgraph-name>`
and `/subgraphs/id/<IPFS hash>` to query subgraphs once you have deployed them.

### Deploying a Subgraph

Follow the [Subgraph deployment
guide](https://thegraph.com/docs/en/subgraphs/developing/introduction/).
After setting up `graph-cli` as described, you can deploy a Subgraph to your
local Graph Node instance.

### Advanced Configuration

The command line arguments generally are all that is needed to run a
`graph-node` instance. For advanced uses, various aspects of `graph-node`
can further be configured through [environment
variables](https://github.com/graphprotocol/graph-node/blob/master/docs/environment-variables.md).

Very large `graph-node` instances can also be configured using a
[configuration file](./docs/config.md) That is usually only necessary when
the `graph-node` needs to connect to multiple chains or if the work of
indexing and querying needs to be split across [multiple databases](./docs/config.md).

## Contributing

Please check [CONTRIBUTING.md](CONTRIBUTING.md) for development flow and conventions we use.
Here's [a list of good first issues](https://github.com/graphprotocol/graph-node/labels/good%20first%20issue).

## License

Copyright &copy; 2018-2019 Graph Protocol, Inc. and contributors.

The Graph is dual-licensed under the [MIT license](LICENSE-MIT) and the [Apache License, Version 2.0](LICENSE-APACHE).

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied. See the License for the specific language governing permissions and limitations under the License.
