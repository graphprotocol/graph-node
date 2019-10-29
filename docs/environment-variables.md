# Environment Variables

**Warning**: the names of some of these environment variables will be changed at
some point in the near future.

This page lists the environment variables used by `graph-node` and what effect
they have. Some environment variables can be used instead of command line flags.
Those are not listed here, please consult `graph-node --help` for details on
those.

## Getting blocks from Ethereum

- `ETHEREUM_POLLING_INTERVAL`: how often to poll Ethereum for new blocks (in ms,
  defaults to 500ms)
- `ETHEREUM_RPC_MAX_PARALLEL_REQUESTS`: how many RPC connections to start in
  parallel for block retrieval (defaults to 64)
- `ETHEREUM_FAST_SCAN_END`: `graph-node` locates blocks with events for a
  particular subgraph. Most subgraphs do not have events in the first few
  million blocks, so `graph-node` optimizes for that case by inquiring about a
  large range. The value of this variable is the block number at which we switch
  from probing large ranges to ranges of size `ETHEREUM_BLOCK_RANGE_SIZE`
  (defaults to 4000000).
- `ETHEREUM_TRACE_STREAM_STEP_SIZE`: `graph-node` queries traces for a given
  block range when a subgraph defines call handlers or block handlers with a
  call filter. The value of this variable controls the number of blocks to scan
  in a single RPC request for traces from the Ethereum node.
- `DISABLE_BLOCK_INGESTOR`: set to `true` to disable block ingestion. Leave
  unset or set to `false` to leave block ingestion enabled.
- `ETHEREUM_BLOCK_BATCH_SIZE`: number of Ethereum blocks to request in parallel
  (defaults to 50)
- `ETHEREUM_BLOCK_RANGE_SIZE`: number of blocks to scan for events in each
  request (defaults to 10000).
- `ETHEREUM_PARALLEL_BLOCK_RANGES`: Maximum number of parallel `eth_getLogs`
  calls to make when scanning logs for a subgraph. Defaults to 100.
- `GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE`: Maximum range size for `eth.getLogs`
  requests that dont filter on contract address, only event signature.
- `GRAPH_ETHEREUM_JSON_RPC_TIMEOUT`: Timeout for Ethereum JSON-RPC requests.

## Running mapping handlers

- `GRAPH_MAPPING_HANDLER_TIMEOUT`: amount of time a mapping handler is allowed to
  take (in seconds, default is unlimited)
- `GRAPH_IPFS_SUBGRAPH_LOADING_TIMEOUT`: timeout for IPFS requests made to load
  subgraph files from IPFS (in seconds, default is 60).
- `GRAPH_IPFS_TIMEOUT`: timeout for IPFS requests from mappings using `ipfs.cat`
  or `ipfs.map` (in seconds, default is 60).
- `GRAPH_MAX_IPFS_FILE_BYTES`: maximum size for a file that can be retrieved
  with `ipfs.cat` (in bytes, default is unlimited)
- `GRAPH_MAX_IPFS_MAP_FILE_SIZE`: maximum size of files that can be processed
  with `ipfs.map`. When a file is processed through `ipfs.map`, the entities
  generated from that are kept in memory until the entire file is done
  processing. This setting therefore limits how much memory a call to `ipfs.map`
  may use. (in bytes, defaults to 256MB)
- `GRAPH_MAX_IPFS_CACHE_SIZE`: maximum number of files cached in the the
  `ipfs.cat` cache (defaults to 50).
- `GRAPH_MAX_IPFS_CACHE_FILE_SIZE`: maximum size of files that are cached in the
  `ipfs.cat` cache (defaults to 1MiB)

## GraphQL

- `GRAPH_GRAPHQL_QUERY_TIMEOUT`: maximum execution time for a graphql query, in
  seconds. Default is unlimited.
- `SUBSCRIPTION_THROTTLE_INTERVAL`: while a subgraph is syncing, subscriptions
  to that subgraph get updated at most this often, in ms. Default is 1000ms.
- `GRAPH_GRAPHQL_MAX_COMPLEXITY`: maximum complexity for a graphql query. See
  [here](https://developer.github.com/v4/guides/resource-limitations) for what
  that means. Default is unlimited. Typical introspection queries have a
  complexity of just over 1 million, so setting a value below that may interfere
  with introspection done by graphql clients.
- `GRAPH_GRAPHQL_MAX_DEPTH`: maximum depth of a graphql query. Default (and
  maximum) is 255.
- `GRAPH_GRAPHQL_MAX_FIRST`: maximum value that can be used for the `first`
  argument in GraphQL queries. If not provided, `first` defaults to 100. The
  default value for `GRAPH_GRAPHQL_MAX_FIRST` is 1000.
- `GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION`: maximum number of GraphQL
  operations per WebSocket connection. Any operation created after the limit
  will return an error to the client. Default: unlimited.

## Tokio

- `GRAPH_TOKIO_THREAD_COUNT`: controls the number of threads allotted to the Tokio runtime. Default is 100.

## Miscellaneous

- `GRAPH_NODE_ID`: sets the node ID, allowing to run multiple Graph Nodes
  in parallel and deploy to specific nodes; each ID must be unique among the set
  of nodes.
- `GRAPH_LOG`: control log levels, the same way that `RUST_LOG` is described
  [here](https://docs.rs/env_logger/0.6.0/env_logger/)
- `THEGRAPH_STORE_POSTGRES_DIESEL_URL`: postgres instance used when running
  tests. Set to `postgresql://<DBUSER>:<DBPASSWORD>@<DBHOST>:<DBPORT>/<DBNAME>`
