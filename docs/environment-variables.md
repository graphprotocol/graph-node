# Environment Variables

**Warning**: the names of some of these environment variables will be changed
at some point in the near future.

This page lists the environment variables used by `graph-node` and what
effect they have. Some environment variables can be used instead of command
line flags. Those are not listed here, please consult `graph-node --help`
for details on those.

## Getting blocks from Ethereum

* `ETHEREUM_POLLING_INTERVAL`: how often to poll Ethereum for new blocks
(in ms, defaults to 500ms)
* `ETHEREUM_RPC_MAX_PARALLEL_REQUESTS`: how many RPC connections to start
in parallel for block retrieval (defaults to 64)
* `ETHEREUM_FAST_SCAN_END`: `graph-node` locates
  blocks with events for a particular subgraph. Most subgraphs do not have
  events in the first few million blocks, so `graph-node` optimizes for that
  case by inquiring about a large range. The value of this variable
  is the block number at which we switch from probing large ranges to
  smaller ranges (defaults to 4000000)
* `DISABLE_BLOCK_INGESTOR`: set to `true` to disable block ingestion. Leave unset or set to `false` to leave block ingestion enabled.
* `ETHEREUM_BLOCK_BATCH_SIZE`: number of Ethereum blocks to request in
  parallel (defaults to 50)

## Running mapping handlers
* `GRAPH_EVENT_HANDLER_TIMEOUT`: amount of time an event handler is allowed
  to take (in seconds, default is unlimited)

## Miscellaneous
* `GRAPH_LOG`: control log levels, the same way that `RUST_LOG` is
described [here](https://docs.rs/env_logger/0.6.0/env_logger/)
* `THEGRAPH_SENTRY_URL`:
* `THEGRAPH_STORE_POSTGRES_DIESEL_URL`: postgres instance used when running
   tests. Set to
   `postgresql://<DBUSER>:<DBPASSWORD>@<DBHOST>:<DBPORT>/<DBNAME>`
