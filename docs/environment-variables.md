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
- `GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE`: The ideal amount of triggers
  to be processed in a batch. If this is too small it may cause too many requests
  to the ethereum node, if it is too large it may cause unreasonably expensive
  calls to the ethereum node and excessive memory usage (defaults to 100).
- `ETHEREUM_TRACE_STREAM_STEP_SIZE`: `graph-node` queries traces for a given
  block range when a subgraph defines call handlers or block handlers with a
  call filter. The value of this variable controls the number of blocks to scan
  in a single RPC request for traces from the Ethereum node. Defaults to 50.
- `DISABLE_BLOCK_INGESTOR`: set to `true` to disable block ingestion. Leave
  unset or set to `false` to leave block ingestion enabled.
- `ETHEREUM_BLOCK_BATCH_SIZE`: number of Ethereum blocks to request in parallel.
  Also limits other parallel requests such such as trace_filter. Defaults to 10.
- `GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE`: Maximum number of blocks to scan for
  triggers in each request (defaults to 1000).
- `GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE`: Maximum range size for `eth.getLogs`
  requests that dont filter on contract address, only event signature.
- `GRAPH_ETHEREUM_JSON_RPC_TIMEOUT`: Timeout for Ethereum JSON-RPC requests.
- `GRAPH_ETHEREUM_REQUEST_RETRIES`: Number of times to retry JSON-RPC requests
  made against Ethereum. This is used for requests that will not fail the
  subgraph if the limit is reached, but will simply restart the syncing step,
  so it can be low. This limit guards against scenarios such as requesting a
  block hash that has been reorged. Defaults to 10.
- `GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS`:
   The maximum number of concurrent requests made against Ethereum for
   requesting transaction receipts during block ingestion.
   Defaults to 1,000.
- `GRAPH_ETHEREUM_CLEANUP_BLOCKS` : Set to `true` to clean up unneeded
  blocks from the cache in the database. When this is `false` or unset (the
  default), blocks will never be removed from the block cache. This setting
  should only be used during development to reduce the size of the
  database. In production environments, it will cause multiple downloads of
  the same blocks and therefore slow the system down. This setting can not
  be used if the store uses more than one shard.

## Running mapping handlers

- `GRAPH_MAPPING_HANDLER_TIMEOUT`: amount of time a mapping handler is allowed to
  take (in seconds, default is unlimited)
- `GRAPH_IPFS_TIMEOUT`: timeout for IPFS, which includes requests for manifest files
  and from mappings using `ipfs.cat` or `ipfs.map` (in seconds, default is 30).
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
- `GRAPH_ENTITY_CACHE_SIZE`: Size of the entity cache, in kilobytes. Defaults to 10000 which is 10MB.
- `GRAPH_QUERY_CACHE_BLOCKS`: How many recent blocks per network should be kept
   in the query cache. This should be kept small since the lookup time and the
   cache memory usage are proportional to this value. Set to 0 to disable the cache.
   Defaults to 1.
- `GRAPH_QUERY_CACHE_MAX_MEM`: Maximum total memory to be used by the query
   cache, in MB. The total amount of memory used for caching will be twice
   this value - once for recent blocks, divided evenly among the
   `GRAPH_QUERY_CACHE_BLOCKS`, and once for frequent queries against older
   blocks.  The default is plenty for most loads, particularly if
   `GRAPH_QUERY_CACHE_BLOCKS` is kept small.  Defaults to 1000, which
   corresponds to 1GB.
- `GRAPH_QUERY_CACHE_STALE_PERIOD`: Number of queries after which a cache
  entry can be considered stale. Defaults to 100.
- `GRAPH_MAX_API_VERSION`: Maximum `apiVersion` supported, if a developer tries to create a subgraph
  with a higher `apiVersion` than this in their mappings, they'll receive an error. Defaults to `0.0.6`.
- `GRAPH_RUNTIME_MAX_STACK_SIZE`: Maximum stack size for the WASM runtime, if exceeded the execution
  stops and an error is thrown. Defaults to 512KiB.

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
- `GRAPH_GRAPHQL_MAX_SKIP`: maximum value that can be used for the `skip`
  argument in GraphQL queries. The default value for
  `GRAPH_GRAPHQL_MAX_SKIP` is unlimited.
- `GRAPH_GRAPHQL_WARN_RESULT_SIZE` and `GRAPH_GRAPHQL_ERROR_RESULT_SIZE`:
  if a GraphQL result is larger than these sizes in bytes, log a warning
  respectively abort query execution and return an error. The size of the
  result is checked while the response is being constructed, so that
  execution does not take more memory than what is configured. The default
  value for both is unlimited.
- `GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION`: maximum number of GraphQL
  operations per WebSocket connection. Any operation created after the limit
  will return an error to the client. Default: unlimited.
- `GRAPH_SQL_STATEMENT_TIMEOUT`: the maximum number of seconds an
  individual SQL query is allowed to take during GraphQL
  execution. Default: unlimited
- `GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATIONS`: disables the internal
  mechanism that is used to trigger updates on GraphQL subscriptions. When
  this variable is set to any value, `graph-node` will still accept GraphQL
  subscriptions, but they won't receive any updates.

## Miscellaneous

- `GRAPH_NODE_ID`: sets the node ID, allowing to run multiple Graph Nodes
  in parallel and deploy to specific nodes; each ID must be unique among the set
  of nodes.
- `GRAPH_LOG`: control log levels, the same way that `RUST_LOG` is described
  [here](https://docs.rs/env_logger/0.6.0/env_logger/)
- `THEGRAPH_STORE_POSTGRES_DIESEL_URL`: postgres instance used when running
  tests. Set to `postgresql://<DBUSER>:<DBPASSWORD>@<DBHOST>:<DBPORT>/<DBNAME>`
- `GRAPH_KILL_IF_UNRESPONSIVE`: If set, the process will be killed if unresponsive.
- `GRAPH_LOG_QUERY_TIMING`: Control whether the process logs details of
  processing GraphQL and SQL queries. The value is a comma separated list
  of `sql`,`gql`, and `cache`. If `gql` is present in the list, each
  GraphQL query made against the node is logged at level `info`. The log
  message contains the subgraph that was queried, the query, its variables,
  the amount of time the query took, and a unique `query_id`. If `sql` is
  present, the SQL queries that a GraphQL query causes are logged. The log
  message contains the subgraph, the query, its bind variables, the amount
  of time it took to execute the query, the number of entities found by the
  query, and the `query_id` of the GraphQL query that caused the SQL
  query. These SQL queries are marked with `component: GraphQlRunner` There
  are additional SQL queries that get logged when `sql` is given. These are
  queries caused by mappings when processing blocks for a subgraph, and
  queries caused by subscriptions. If `cache` is present in addition to
  `gql`, also logs information for each toplevel GraphQL query field
  whether that could be retrieved from cache or not. Defaults to no
  logging.
- `STORE_CONNECTION_POOL_SIZE`: How many simultaneous connections to allow to the store.
  Due to implementation details, this value may not be strictly adhered to. Defaults to 10.
- `GRAPH_LOG_POI_EVENTS`: Logs Proof of Indexing events deterministically.
  This may be useful for debugging.
- `GRAPH_LOAD_WINDOW_SIZE`, `GRAPH_LOAD_BIN_SIZE`: Load can be
  automatically throttled if load measurements over a time period of
  `GRAPH_LOAD_WINDOW_SIZE` seconds exceed a threshold. Measurements within
  each window are binned into bins of `GRAPH_LOAD_BIN_SIZE` seconds. The
  variables default to 300s and 1s
- `GRAPH_LOAD_THRESHOLD`: If wait times for getting database connections go
  above this threshold, throttle queries until the wait times fall below
  the threshold. Value is in milliseconds, and defaults to 0 which
  turns throttling and any associated statistics collection off.
- `GRAPH_LOAD_JAIL_THRESHOLD`: When the system is overloaded, any query
  that causes more than this fraction of the effort will be rejected for as
  long as the process is running (i.e., even after the overload situation
  is resolved) If this variable is not set, no queries will ever be jailed,
  but they will still be subject to normal load management when the system
  is overloaded.
- `GRAPH_LOAD_SIMULATE`: Perform all the steps that the load manager would
  given the other load management configuration settings, but never
  actually decline to run a query, instead log about load management
  decisions. Set to `true` to turn simulation on, defaults to `false`
- `GRAPH_STORE_CONNECTION_TIMEOUT`: How long to wait to connect to a
  database before assuming the database is down in ms. Defaults to 5000ms.
- `EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE`: default is `instant`, set 
  to `synced` to only switch a named subgraph to a new deployment once it 
  has synced, making the new deployment the "Pending" version.
- `GRAPH_REMOVE_UNUSED_INTERVAL`: How long to wait before removing an
  unused deployment. The system periodically checks and marks deployments
  that are not used by any subgraphs any longer. Once a deployment has been
  identified as unused, `graph-node` will wait at least this long before
  actually deleting the data (value is in minutes, defaults to 360, i.e. 6
  hours)
