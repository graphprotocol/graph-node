<p align="center">
  <img width="100%" src="resources/construction.svg" title="Under Construction" />
</p>

# Graph Node

[![Build Status](https://travis-ci.org/graphprotocol/graph-node.svg?branch=master)](https://travis-ci.org/graphprotocol/graph-node)
[![Getting Started Docs](https://img.shields.io/badge/docs-getting--started-brightgreen.svg)](docs/getting-started.md)

[The Graph](https://thegraph.com/) is a protocol for building decentralized applications quickly on Ethereum and IPFS using GraphQL.

Graph Node is an open source Rust implementation that event-sources the Ethereum blockchain to deterministically update a datastore which can be queried via the GraphQL endpoint.

For detailed instructions and more context check out the [Getting Started Guide](docs/getting-started.md).

_Note: this project is heavily WIP and until it reaches v1.0 the API is subject to change in breaking ways without notice._

## Quick Start

### Prerequisites

To build and run this project you need
to have the following installed on your system:

- Rust (latest stable) - [How to install Rust](https://www.rust-lang.org/en-US/install.html)
- PostgreSQL – [PostgreSQL Downloads](https://www.postgresql.org/download/)
- IPFS – [Installing IPFS](https://ipfs.io/docs/install/)

For Ethereum network data you can either run a local node or use Infura.io:

- Local node – [Installing and running Ethereum node](https://ethereum.gitbooks.io/frontier-guide/content/getting_a_client.html)
- Infura infra – [Infura.io](https://infura.io/)

### Running a local Graph Node

This is a quick example to get you up and running, it uses a [subgraph for ENS](https://github.com/graphprotocol/ens-subgraph) that we built as a reference.
                                                   
1. Install IPFS and run `ipfs init` followed by `ipfs daemon`
2. Install PostgreSQL and run `initdb -D .postgres` followed by `createdb graph-node`
3. If using Ubuntu, you may need to install additional packages:
   - `sudo apt-get install -y clang libpq-dev libssl-dev pkg-config`
4. Clone https://github.com/graphprotocol/ens-subgraph, install dependencies and generate types for contract ABIs:

```
yarn install
yarn codegen
```

5. Clone https://github.com/graphprotocol/graph-node and run `cargo build`

Once you have all the dependencies set up you can run the following:

```
cargo run -p graph-node --release -- \
  --postgres-url postgresql://USERNAME[:PASSWORD]@localhost:5432/graph-node \
  --ethereum-rpc mainnet:https://mainnet.infura.io/ \
  --ipfs 127.0.0.1:5001
```

Try your OS username as `USERNAME` and `PASSWORD`. The password might be optional, it depends on your setup.

This will also spin up a GraphiQL interface at `http://127.0.0.1:8000/`.

6.  Back in your subgraph directory, run

```
yarn deploy --verbosity debug
```

in order to build and deploy your subgraph to the Graph Node. It should start indexing the subgraph immediately.

### Command-line interface

```
USAGE:
    graph-node
      --subgraph <IPFS_HASH>
      --ethereum-ipc <FILE>
        or --ethereum-rpc <URL>
        or --ethereum-ws <URL>
      --ipfs <HOST:PORT>
      --postgres-url <URL>

FLAGS:
        --debug      Enable debug logging
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --subgraph [<NAME>:]<IPFS_HASH>         Name (optional) and IPFS hash of the subgraph manifest
        --ethereum-ipc <NETWORK_NAME>:<FILE>    Ethereum network name (e.g. 'mainnet') and Ethereum IPC pipe path, separated by a ':'
        --ethereum-rpc <NETWORK_NAME>:<URL>     Ethereum network name (e.g. 'mainnet') and Ethereum RPC endpoint URL, separated by a ':'
        --ethereum-ws <NETWORK_NAME>:<URL>      Ethereum network name (e.g. 'mainnet') and Ethereum WebSocket endpoint URL, separated by a ':'
        --ipfs <HOST>:<PORT>                    HTTP address of an IPFS node
        --postgres-url <URL>                    Location of the Postgres database used for storing entities
```

### Environment Variables

The Graph supports the following environment variables:

```
THEGRAPH_SENTRY_URL (optional) — Activates error reporting using Sentry
```

## Project Layout

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

## Roadmap

🔨 = In Progress

🛠 = Feature complete. Additional testing required.

✅ = Feature complete


| Feature |  Status |
| ------- |  :------: |
| **Ethereum** |    |
| Indexing Smart Contract Events | ✅ |
| Handle chain reorganizations | 🛠 |
| **Mappings** |    |
| WASM-based mappings| ✅ |
| TypeScript-to-WASM toolchain | ✅ |
| Autogenerated TypeScript types | ✅ |
| **GraphQL** |     |
| Query entities by ID | ✅ |
| Query entity collections | ✅ |
| Pagination | ✅ |
| Filtering | ✅ |
| Entity relationships | ✅ |
| Subscriptions | 🔨|


## Contributing

Please check [CONTRIBUTING.md](CONTRIBUTING.md) for development flow and conventions we use.
Here's [a list of good first issues](https://github.com/graphprotocol/graph-node/labels/good%20first%20issue).

## License

Copyright &copy; 2018 Graph Protocol, Inc. and contributors.

The Graph is dual-licensed under the [MIT license](LICENSE-MIT) and the
[Apache License, Version 2.0](LICENSE-APACHE).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
