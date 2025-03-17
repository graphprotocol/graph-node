# Graph Node

[![Build Status](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml/badge.svg)](https://github.com/graphprotocol/graph-node/actions/workflows/ci.yml?query=branch%3Amaster)
[![Getting Started Docs](https://img.shields.io/badge/docs-getting--started-brightgreen.svg)](docs/getting-started.md)

[The Graph](https://thegraph.com/) is a protocol that organizes and serves web3 data.

Graph Node extracts and transforms blockchain data and makes the transformed
data available to decentralized applications (dApps) via GraphQL queries.
You can find more details on how to write these transformations, called
_subgraphs_, in the official [Graph
documentation](https://thegraph.com/docs/en/subgraphs/quick-start/). If you
are not familiar already with subgraphs, we highly recommend at least
reading through that documentation first.

The rest of this page is geared towards two audiences:

1. Subgraph developers who want to run `graph-node` locally so they can test
   their subgraphs during development
2. Developers who want to contribute bug fixes and features to `graph-node`
   itself

## Running `graph-node` from Docker images

For subgraph developers, it is highly recommended to use prebuilt Docker
images to set up a local `graph-node` environment. Please read [these
instructions](./docker/README.md) to learn how to do that.

## Running `graph-node` from source

This is usually only needed for developers who want to contribute to `graph-node`.

### Prerequisites

To build and run this project you need to have the following installed on your system:

- Rust (latest stable) – [How to install Rust](https://www.rust-lang.org/en-US/install.html)
  has general instructions. Run `rustup install stable` in this directory to make
  sure all required components are installed. The `graph-node` code assumes that the
  latest available `stable` compiler is used.
- PostgreSQL – [PostgreSQL Downloads](https://www.postgresql.org/download/) lists
  downloads for almost all operating systems. For OSX users, we highly recommend
  using [Postgres.app](https://postgresapp.com/). Linux users can simply use the
  Postgres version that comes with their distribution.
- IPFS – [Installing IPFS](https://docs.ipfs.io/install/)
- Protobuf Compiler - [Installing Protobuf](https://grpc.io/docs/protoc-installation/)

For Ethereum network data, you can either run your own Ethereum node or use an Ethereum node provider of your choice.

### Create a database

Once Postgres is running, you need to issue the following commands to create a database
and set it up so that `graph-node` can use it. The name of the `SUPERUSER` depends
on your installation, but is usually `postgres` or your username.

```bash
psql -U <SUPERUSER> <<EOF
create user graph with password '<password>';
create database "graph-node" with owner=graph template=template0 encoding='UTF8' locale='C';
create extension pg_trgm;
create extension btree_gist;
create extension postgres_fdw;
grant usage on foreign data wrapper postgres_fdw to graph;
EOF

# Save this in ~/.bashrc or similar
export POSTGRES_URL=postgresql://graph:<password>@localhost:5432/graph-node
```

With this setup, the URL that you will use to have `graph-node` connect to the
database will be `postgresql://graph:<password>@localhost:5432/graph-node`. If
you ever need to manually inspect the contents of your database, you can do
that by running `psql $POSTGRES_URL`.

### Build `graph-node`

To build `graph-node`, clone this repository and run this command at the
root of the repository:

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
The most important of these is the GraphQL HTTP server, which is by default
is at `http://localhost:8000`. You can use routes like `/subgraphs/name/<subgraph-name>`
and `/subgraphs/id/<IPFS hash>` to query subgraphs once you have deployed them.

### Deploying a Subgraph

Instructions for how to deploy subgraphs can be found [here](https://thegraph.com/docs/en/subgraphs/developing/introduction/) After setting up `graph-cli` as described there, you can deploy a subgraph to your local Graph Node instance.

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
