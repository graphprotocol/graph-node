# NEWS

## Unreleased
- Gracefully handle syntax errors on fulltext search. Specifically provides information about common use case 
where whitespace characters were part of the terms.

## 0.25.0

### Api Version 0.0.6
This release ships support for API version 0.0.6 in mappings:
- Added `nonce` field for `Transaction` objects.
- Added `baseFeePerGas` field for `Block` objects ([EIP-1559](https://eips.ethereum.org/EIPS/eip-1559)).

#### Block Cache Invalidation and Reset

All cached block data must be refetched to account for the new `Block` and `Trasaction`
struct versions, so this release includes a `graph-node` startup check that will:
1. Truncate all block cache tables.
2. Bump the `db_version` value from `2` to `3`.

_(Table truncation is a fast operation and no downtime will occur because of that.)_


### Ethereum

- 'Out of gas' errors on contract calls are now considered deterministic errors,
  so they can be handled by `try_` calls. The gas limit is 50 million.

### Environment Variables

- The `GRAPH_ETH_CALL_GAS` environment is removed to prevent misuse, its value
  is now hardcoded to 50 million.

### Multiblockchain
- Initial support for NEAR subgraphs.
- Added `FirehoseBlockStream` implementation of `BlockStream` (#2716)

### Misc
- Rust docker image is now based on Debian Buster.
- Optimizations to the PostgreSQL notification queue.
- Improve PostgreSQL robustness in multi-sharded setups. (#2815)
- Added 'networks' to the 'subgraphFeatures' endpoint. (#2826)
- Check and limit the size of GraphQL query results. (#2845)
- Allow `_in` and `_not_in` GraphQL filters. (#2841)
- Add PoI for failed subgraphs. (#2748)
- Make `graphman rewind` safer to use. (#2879)
- Add `subgraphErrors` for all GraphQL schemas. (#2894)
- Add `Graph-Attestable` response header. (#2946)
- Add support for minimum block constraint in GraphQL queries (`number_gte`) (#2868).
- Handle revert cases from Hardhat and Ganache (#2984)
- Fix bug on experimental prefetching optimization feature (#2899)


## 0.24.2

This release only adds a fix for an issue where certain GraphQL queries
could lead to `graph-node` running out of memory even on very large
systems. This release adds code that checks the size of GraphQL responses
as they are assembled, and can warn about large responses in the logs
resp. abort query execution based on the values of the two new environment
variables `GRAPH_GRAPHQL_WARN_RESULT_SIZE` and
`GRAPH_GRAPHQL_ERROR_RESULT_SIZE`. It also adds Prometheus metrics
`query_result_size` and `query_result_max` to track the memory consumption
of successful GraphQL queries. The unit for the two environment variables
is bytes, based on an estimate of the memory used by the result; it is best
to set them after observing the Prometheus metrics for a while to establish
what constitutes a reasonable limit for them.

We strongly recommend updating to this version as quickly as possible.

## 0.24.1

### Feature Management

This release supports the upcoming Spec Version 0.0.4 that enables subgraph features to be declared in the manifest and
validated during subgraph deployment
[#2682](https://github.com/graphprotocol/graph-node/pull/2682)
[#2746](https://github.com/graphprotocol/graph-node/pull/2746).

> Subgraphs using previous versions are still supported and won't be affected by this change.

#### New Indexer GraphQL query: `subgraphFetaures`

It is now possible to query for the features a subgraph uses given its Qm-hash ID.

For instance, the following query...

```graphql
{
  subgraphFeatures(subgraphId: "QmW9ajg2oTyPfdWKyUkxc7cTJejwdyCbRrSivfryTfFe5D") {
    features
    errors
  }
}
```

... would produce this result:

```json
{
  "data": {
    "subgraphFeatures": {
      "errors": [],
      "features": [
        "nonFatalErrors",
        "ipfsOnEthereumContracts"
      ]
    }
  }
}
```

Subraphs with any Spec Version can be queried that way.

### Api Version 0.0.5

- Added better error message for null pointers in the runtime [#2780](https://github.com/graphprotocol/graph-node/pull/2780).

### Environment Variables

- When `GETH_ETH_CALL_ERRORS_ENV` is unset, it doesn't make `eth_call` errors to be considered determinsistic anymore [#2784](https://github.com/graphprotocol/graph-node/pull/2784)

### Robustness

- Tolerate a non-primary shard being down during startup [#2727](https://github.com/graphprotocol/graph-node/pull/2727).
- Check that at least one replica for each shard has a non-zero weight [#2749](https://github.com/graphprotocol/graph-node/pull/2749).
- Reduce locking for the chain head listener [#2763](https://github.com/graphprotocol/graph-node/pull/2763).

### Logs

- Improve block ingestor error reporting for missing receipts [#2743](https://github.com/graphprotocol/graph-node/pull/2743).

## 0.24.0

### Api Version 0.0.5

This release ships support for API version 0.0.5 in mappings. hIt contains a fix for call handlers
and the long awaited AssemblyScript version upgrade!

- AssemblyScript upgrade: The mapping runtime is updated to support up-to-date versions of the
  AssemblyScript compiler. The graph-cli/-ts releases to support this are in alpha, soon they will
  be released along with a migration guide for subgraphs.
- Call handlers fix: Call handlers will never be triggered on transactions with a failed status,
  resolving issue [#2409](https://github.com/graphprotocol/graph-node/issues/2409). Done in [#2511](https://github.com/graphprotocol/graph-node/pull/2511).

### Logs
- The log `"Skipping handler because the event parameters do not match the event signature."` was downgraded from info to trace level.
- Some block ingestor error logs were upgrded from debug to info level [#2666](https://github.com/graphprotocol/graph-node/pull/2666).

### Metrics
- `query_semaphore_wait_ms` is now by shard, and has the `pool` and `shard` labels.
- `deployment_failed` metric added, it is `1` if the subgraph has failed and `0` otherwise.

### Other
- Upgrade to tokio 1.0 and futures 0.3 [#2679](https://github.com/graphprotocol/graph-node/pull/2679), the first major contribution by StreamingFast!
- Support Celo block reward events [#2670](https://github.com/graphprotocol/graph-node/pull/2670).
- Reduce the maximum WASM stack size and make it configurable [#2719](https://github.com/graphprotocol/graph-node/pull/2719).
- For robustness, ensure periodic updates to the chain head listener [#2725](https://github.com/graphprotocol/graph-node/pull/2725).

## 0.23.1

- Fix ipfs timeout detection [#2584](https://github.com/graphprotocol/graph-node/pull/2584).
- Fix discrepancy between a database table and its Diesel model [#2586](https://github.com/graphprotocol/graph-node/pull/2586).

## 0.23.0

The Graph Node internals are being heavily refactored to prepare it for the multichain future.
In the meantime, here are the changes for this release:

- The `GRAPH_ETH_CALL_BY_NUMBER` environment variable has been removed. Graph Node requires an
  Ethereum client that supports EIP-1898, which all major clients support.
- Added support for IPFS versions larger than 0.4. Several changes to make
  `graph-node` more tolerant of slow/flaky IPFS nodes.
- Added Ethereum ABI encoding and decoding functionality [#2348](https://github.com/graphprotocol/graph-node/pull/2348).
- Experimental support for configuration files, see the documentation [here](https://github.com/graphprotocol/graph-node/blob/master/docs/config.md).
- Better PoI performance [#2329](https://github.com/graphprotocol/graph-node/pull/2329).
- Improve grafting performance and robustness by copying in batches [#2293](https://github.com/graphprotocol/graph-node/pull/2293).
- Subgraph metadata storage has been simplified and reorganized. External
  tools (e.g., Grafana dashboards) that access the database directly will need to be updated.
- Ordering in GraphQL queries is now truly reversible
  [#2214](https://github.com/graphprotocol/graph-node/pull/2214/commits/bc559b8df09a7c24f0d718b76fa670313911a6b1)
- The `GRAPH_SQL_STATEMENT_TIMEOUT` environment variable can be used to
  enforce a timeout for individual SQL queries that are run in the course of
  processing a GraphQL query
  [#2285](https://github.com/graphprotocol/graph-node/pull/2285)
- Using `ethereum.call` in mappings in globals is deprecated

### Graphman
Graphman is a CLI tool to manage your subgraphs. It is now included in the Docker container
[#2289](https://github.com/graphprotocol/graph-node/pull/2289). And new commands have been added:
- `graphman copy` can copy subgraphs across DB shards [#2313](https://github.com/graphprotocol/graph-node/pull/2313).
- `graphman rewind` to rewind a deployment to a given block [#2373](https://github.com/graphprotocol/graph-node/pull/2373).
- `graphman query` to log info about a GraphQL query [#2206](https://github.com/graphprotocol/graph-node/pull/2206).
- `graphman create` to create a subgraph name [#2419](https://github.com/graphprotocol/graph-node/pull/2419).

### Metrics
- The `deployment_blocks_behind` metric has been removed, and a
  `deployment_head` metric has been added. To see how far a deployment is
  behind, use the difference between `ethereum_chain_head_number` and
  `deployment_head`.
- The `trigger_type` label was removed from the metric `deployment_trigger_processing_duration`.

## 0.22.0

### Feature: Block store sharding
This release makes it possible to [shard the block and call cache](./docs/config.md) for chain
data across multiple independent Postgres databases. **This feature is considered experimental. We
encourage users to try this out in a test environment, but do not recommend it yet for production
use.** In particular, the details of how sharding is configured may change in backwards-incompatible
ways in the future.

### Feature: Non-fatal errors update
Non-fatal errors (see release 0.20 for details) is documented and can now be enabled on graph-cli.
Various related bug fixes have been made #2121 #2136 #2149 #2160.

### Improvements
- Add bitwise operations and string constructor to BigInt #2151.
- docker: Allow custom ethereum poll interval #2139.
- Deterministic error work in preparation for gas #2112

### Bug fixes
- Fix not contains filter #2146.
- Resolve __typename in _meta field #2118
- Add CORS for all HTTP responses #2196

## 0.21.1

- Fix subgraphs failing with a `fatalError` when deployed while already running
  (#2104).
- Fix missing `scalar Int` declaration in index node GraphQL API, causing
  indexer-service queries to fail (#2104).

## 0.21.0

### Feature: Database sharding

This release makes it possible to [shard subgraph
storage](./docs/config.md) and spread subgraph deployments, and the load
coming from indexing and querying them across multiple independent Postgres
databases.

**This feature is considered experimenatal. We encourage users to try this
out in a test environment, but do not recommend it yet for production use**
In particular, the details of how sharding is configured may change in
backwards-incompatible ways in the future.

### Breaking change: Require a block number in `proofOfIndexing` queries

This changes the `proofOfIndexing` GraphQL API from

```graphql
type Query {
  proofOfIndexing(subgraph: String!, blockHash: Bytes!, indexer: Bytes): Bytes
}
```

to

```graphql
type Query {
  proofOfIndexing(
    subgraph: String!
    blockNumber: Int!
    blockHash: Bytes!
    indexer: Bytes
  ): Bytes
}
```

This allows the indexer agent to provide a block number and hash to be able
to obtain a POI even if this block is not cached in the Ethereum blocks
cache. Prior to this, the POI would be `null` if this wasn't the case, even
if the subgraph deployment in question was up to date, leading to the indexer
missing out on indexing rewards.

### Misc

- Fix non-determinism caused by not (always) correctly reverting dynamic
  sources when handling reorgs.
- Integrate the query cache into subscriptions to improve their performance.
- Add `graphman` crate for managing Graph Node infrastructure.
- Improve query cache logging.
- Expose indexing status port (`8030`) from Docker image.
- Remove support for unnecessary data sources `templates` inside subgraph
  data sources. They are only supported at the top level.
- Avoid sending empty store events through the database.
- Fix database connection deadlocks.
- Rework the codebase to use `anyhow` instead of `failure`.
- Log stack trace in case of database connection timeouts, to help with root-causing.
- Fix stack overflows in GraphQL parsing.
- Disable fulltext search by default (it is nondeterministic and therefore
  not currently supported in the network).

## 0.20.0

**NOTE: JSONB storage is no longer supported. Do not upgrade to this
release if you still have subgraphs that were deployed with a version
before 0.16. They need to be redeployed before updating to this version.**

You can check if you have JSONB subgraphs by running the query `select count(*) from deployment_schemas where version='split'` in `psql`. If that
query returns `0`, you do not have JSONB subgraphs and it is safe to upgrde
to this version.

### Feature: `_meta` field

Subgraphs sometimes fall behind, be it due to failing or the Graph Node may be having issues. The
`_meta` field can now be added to any query so that it is possible to determine against which block
the query was effectively executed. Applications can use this to warn users if the data becomes
stale. It is as simple as adding this to your query:

```graphql
_meta {
  block {
    number
    hash
  }
}
```

### Feature: Non-fatal errors

Indexing errors on already synced subgraphs no longer need to cause the entire subgraph to grind to
a halt. Subgraphs can now be configured to continue syncing in the presence of errors, by simply
skipping the problematic handler. This gives subgraph authors time to correct their subgraphs while the nodes can continue to serve up-to-date the data. This requires setting a flag on the subgraph manifest:

```yaml
features:
  - nonFatalErrors
```

And the query must also opt-in to querying data with potential inconsistencies:

```graphql
foos(first: 100, subgraphError: allow) {
  id
}
```

If the subgraph encounters and error the query will return both the data and a graphql error with
the message `indexing_error`.

Note that some errors are still fatal, to be non-fatal the error must be known to be deterministic. The `_meta` field can be used to check if the subgraph has skipped over errors:

```graphql
_meta {
  hasIndexingErrors
}
```

The `features` section of the manifest requires depending on the graph-cli master branch until the next version (after `0.19.0`) is released.

### Ethereum

- Support for `tuple[]` (#1973).
- Support multiple Ethereum endpoints per network with different capabilities (#1810).

### Performance

- Avoid cloning results assembled from partial results (#1907).

### Security

- Add `cargo-audit` to the build process, update dependencies (#1998).

## 0.19.2

- Add `GRAPH_ETH_CALL_BY_NUMBER` environment variable for disabling
  EIP-1898 (#1957).
- Disable `ipfs.cat` by default, as it is non-deterministic (#1958).

## 0.19.1

- Detect reorgs during query execution (#1801).
- Annotate SQL queries with the GraphQL query ID that caused them (#1946).
- Fix potential deadlock caused by reentering the load manager semaphore (#1948).
- Fix fulltext query issue with optional and unset fields (#1937 via #1938).
- Fix build warnings with --release (#1949 via #1953).
- Dependency updates: async-trait, chrono, wasmparser.

## 0.19.0

- Skip `trace_filter` on empty blocks (#1923).
- Ensure runtime hosts are unique to avoid double-counting, improve logging
  (#1904).
- Add administrative Postgres views (#1889).
- Limit the GraphQL `skip` argument in the same way as we limit `first` (#1912).
- Fix GraphQL fragment bugs (#1825).
- Don't crash node and show better error when multiple graph nodes are indexing
  the same subgraph (#1903).
- Add a query semaphore to allow to control the number of concurrent queries and
  subscription queries being executed (#1802).
- Call Ethereum contracts by block hash (#1905).
- Fix fetching the correct function ABI from the contract ABI (#1886).
- Add LFU cache for historical queries (#1878, #1879, #1891).
- Log GraphQL queries only once (#1873).
- Gracefully fail on a null block hash and encoding failures in the Ethereum
  adapter (#1872).
- Improve metrics by using labels more (#1868, ...)
- Log when decoding a contract call result fails to decode (#1842).
- Fix Ethereum node requirements parsing based on the manifest (#1834).
- Speed up queries that involve checking for inclusion in an array (#1820).
- Add better error message when blocking a query due to load management (#1822).
- Support multiple Ethereum nodes/endpoints per network, with different
  capabilities (#1810).
- Change how we index foreign keys (#1811).
- Add an experimental Ethereum node config file (#1819).
- Allow using GraphQL variables in block constraints (#1803).
- Add Solidity struct array / Ethereum tuple array support (#1815).
- Resolve subgraph names in a blocking task (#1797).
- Add environmen variable options for sensitive arguments (#1784).
- USe blocking task for store events (#1789).
- Refactor servers, log GraphQL panics (#1783).
- Remove excessive logging in the store (#1772).
- Add dynamic load management for GraphQL queries (#1762, #1773, #1774).
- Add ability to block certain queries (#1749, #1771).
- Log the complexity of each query executed (#1752).
- Add support for running against read-only Postgres replicas (#1746, #1748,
  #1753, #1750, #1754, #1860).
- Catch invalid opcode reverts on Geth (#1744).
- Optimize queries for single-object lookups (#1734).
- Increase the maximum number of blocking threads (#1742).
- Increase default JSON-RPC timeout (#1732).
- Ignore flaky network indexers tests (#1724).
- Change default max block range size to 1000 (#1727).
- Fixed aliased scalar fields (#1726).
- Fix issue inserting fulltext fields when all included field values are null (#1710).
- Remove frequent "GraphQL query served" log message (#1719).
- Fix `bigDecimal.devidedBy` (#1715).
- Optimize GraphQL execution, remove non-prefetch code (#1712, #1730, #1733,
  #1743, #1775).
- Add a query cache (#1708, #1709, #1747, #1751, #1777).
- Support the new Geth revert format (#1713).
- Switch WASM runtime from wasmi to wasmtime and cranelift (#1700).
- Avoid adding `order by` clauses for single-object lookups (#1703).
- Refactor chain head and store event listeners (#1693).
- Properly escape single quotes in strings for SQL queries (#1695).
- Revamp how Graph Node Docker image is built (#1644).
- Add BRIN indexes to speed up revert handling (#1683).
- Don't store chain head block in `SubgraphDeployment` entity (#1673).
- Allow varying block constraints across different GraphQL query fields (#1685).
- Handle database tables that have `text` columns where they should have enums (#1681).
- Make contract call cache collision-free (#1680).
- Fix a SQL query in `cleanup_cached_blocks` (#1672).
- Exit process when panicking in the notification listener (#1671).
- Rebase ethabi and web3 forks on top of upstream (#1662).
- Remove parity-wasm dependency (#1663).
- Normalize `BigDecimal` values, limit `BigDecimal` exponent (#1640).
- Strip nulls from strings (#1656).
- Fetch genesis block by number `0` instead of `"earliest"` (#1658).
- Speed up GraphQL query execution (#1648).
- Fetch event logs in parallel (#1646).
- Cheaper block polling (#1646).
- Improve indexing status API (#1609, #1655, #1659, #1718).
- Log Postgres contention again (#1643).
- Allow `User-Agent` in CORS headers (#1635).
- Docker: Increase startup wait timeouts (Postgres, IPFS) to 120s (#1634).
- Allow using `Bytes` for `id` fields (#1607).
- Increase Postgres connection pool size (#1620).
- Fix entities updated after being removed in the same block (#1632).
- Pass `log_index` to mappings in place of `transaction_log_index` (required for
  Geth).
- Don't return `__typename` to mappings (#1629).
- Log warnings after 10 successive failed `eth_call` requests. This makes
  it more visible when graph-node is not operating against an Ethereum
  archive node (#1606).
- Improve use of async/await across the codebase.
- Add Proof Of Indexing (POI).
- Add first implementation of subgraph grafting.
- Add integration test for handling Ganache reverts (#1590).
- Log all GraphQL and SQL queries performed by a node, controlled through
  the `GRAPH_LOG_QUERY_TIMING` [environment
  variable](docs/environment-variables.md) (#1595).
- Fix loading more than 200 dynamic data sources (#1596).
- Fix fulltext schema validation (`includes` fields).
- Dependency updates: anyhow, async-trait, bs58, blake3, bytes, chrono, clap,
  crossbeam-channel derive_more, diesel-derive-enum, duct, ethabi,
  git-testament, hex-literal, hyper, indexmap, jsonrpc-core, mockall, once_cell,
  petgraph, reqwest, semver, serde, serde_json, slog-term, tokio, wasmparser.

## 0.18.0

**NOTE: JSONB storage is deprecated and will be removed in the next release.
This only affects subgraphs that were deployed with a graph-node version
before 0.16. Starting with this version, graph-node will print a warning for
any subgraph that uses JSONB storage when that subgraph starts syncing. Please
check your logs for this warning. You can remove the warning by redeploying
the subgraph.**

### Feature: Fulltext Search (#1521)

A frequently requested feature has been support for more advanced text-based
search, e.g. to power search fields in dApps. This release introduces a
`@fulltext` directive on a new, reserved `_Schema_` type to define fulltext
search APIs that can then be used in queries. The example below shows how
such an API can be defined in the subgraph schema:

```graphql
type _Schema_
  @fulltext(
    name: "artistSearch"
    language: en
    algorithm: rank
    include: [
      {
        entity: "Artist"
        fields: [
          { name: "name" }
          { name: "bio" }
          { name: "genre" }
          { name: "promoCopy" }
        ]
      }
    ]
  )
```

This will add a special database column for `Artist` entities that can be
used for fulltext search queries across all included entity fields, based on
the `tsvector` and `tsquery` features provided by Postgres.

The `@fulltext` directive will also add an `artistSearch` field on the root
query object to the generated subgraph GraphQL API, which can be used as
follows:

```graphql
{
  artistSearch(text: "breaks & electro & detroit") {
    id
    name
    bio
  }
}
```

For more information about the supported operators (like the `&` in the above
query), please refer to the [Postgres
documentation](https://www.postgresql.org/docs/10/textsearch.html).

### Feature: 3Box Profiles (#1574)

[3Box](https://3box.io) has become a popular solution for integrating user
profiles into dApps. Starting with this release, it is possible to fetch profile
data for Ethereum addresses and DIDs. Example usage:

```ts
import { box } from '@graphprotocol/graph-ts'

let profile = box.profile("0xc8d807011058fcc0FB717dcd549b9ced09b53404")
if (profile !== null) {
  let name = profile.get("name")
  ...
}

let profileFromDid = box.profile(
  "id:3:bafyreia7db37k7epoc4qaifound6hk7swpwfkhudvdug4bgccjw6dh77ue"
)
...
```

### Feature: Arweave Transaction Data (#1574)

This release enables accessing [Arweave](https://arweave.org) transaction data
using Arweave transaction IDs:

```ts
import { arweave, json } from '@graphprotocol/graph-ts'

let data = arweave.transactionData(
  "W2czhcswOAe4TgL4Q8kHHqoZ1jbFBntUCrtamYX_rOU"
)

if (data !== null) {
  let data = json.fromBytes(data)
  ...
}

```

### Feature: Data Source Context (#1404 via #1537)

Data source contexts allow passing extra configuration when creating a data
source from a template. As an example, let's say a subgraph tracks exchanges
that are associated with a particular trading pair, which is included in the
`NewExchange` event. That information can be passed into the dynamically
created data source, like so:

```ts
import { DataSourceContext } from "@graphprotocol/graph-ts";
import { Exchange } from "../generated/templates";

export function handleNewExchange(event: NewExchange): void {
  let context = new DataSourceContext();
  context.setString("tradingPair", event.params.tradingPair);
  Exchange.createWithContext(event.params.exchange, context);
}
```

Inside a mapping of the Exchange template, the context can then be accessed
as follows:

```ts
import { dataSource } from '@graphprotocol/graph-ts'

...

let context = dataSource.context()
let tradingPair = context.getString('tradingPair')
```

There are setters and getters like `setString` and `getString` for all value
types to make working with data source contexts convenient.

### Feature: Error Handling for JSON Parsing (#1588 via #1578)

With contracts anchoring JSON data on IPFS on chain, there is no guarantee
that this data is actually valid JSON. Until now, failure to parse JSON in
subgraph mappings would fail the subgraph. This release adds a new
`json.try_fromBytes` host export that allows subgraph to gracefully handle
JSON parsing errors.

```ts
import { json } from '@graphprotocol/graph-ts'

export function handleSomeEvent(event: SomeEvent): void {
  // JSON data as bytes, e.g. retrieved from IPFS
  let data = ...

  // This returns a `Result<JSONValue, boolean>`, meaning that the error type is
  // just a boolean (true if there was an error, false if parsing succeeded).
  // The actual error message is logged automatically.
  let result = json.try_fromBytes(data)

  if (result.isOk) { // or !result.isError
    // Do something with the JSON value
    let value = result.value
    ...
  } else {
    // Handle the error
    let error = result.error
    ...
  }
}
```

### Ethereum

- Add support for calling overloaded contract functions (#48 via #1440).
- Add integration test for calling overloaded contract functions (#1441).
- Avoid `eth_getLogs` requests with block ranges too large for Ethereum nodes
  to handle (#1536).
- Simplify `eth_getLogs` fetching logic to reduce the risk of being rate
  limited by Ethereum nodes and the risk of overloading them (#1540).
- Retry JSON-RPC responses with a `-32000` error (Alchemy uses this for
  timeouts) (#1539).
- Reduce block range size for `trace_filter` requests to prevent request
  timeouts out (#1547).
- Fix loading dynamically created data sources with `topic0` event handlers
  from the database (#1580).
- Fix handling contract call reverts in newer versions of Ganache (#1591).

### IPFS

- Add support for checking multiple IPFS nodes when fetching files (#1498).

### GraphQL

- Use correct network when resolving block numbers in time travel queries
  (#1508).
- Fix enum field validation in subgraph schemas (#1495).
- Prevent WebSocket connections from hogging the blocking thread pool and
  freezing the node (#1522).

### Database

- Switch subgraph metadata from JSONB to relational storage (#1394 via #1454,
  #1457, #1459).
- Clean up large notifications less frequently (#1505).
- Add metric for Postgres connection errors (#1484).
- Log SQL queries executed as part of the GraphQL API (#1465, #1466, #1468).
- Log entities returned by SQL queries (#1503).
- Fix several GraphQL prefetch / SQL query execution issues (#1523, #1524,
  #1526).
- Print deprecation warnings for JSONB subgraphs (#1527).
- Make sure reorg handling does not affect metadata of other subgraphs (#1538).

### Performance

- Maintain an in-memory entity cache across blocks to speed up `store.get`
  (#1381 via #1416).
- Speed up revert handling by making use of cached blocks (#1449).
- Speed up simple queries by delaying building JSON objects for results (#1476).
- Resolve block numbers to hashes using cached blocks when possible (#1477).
- Improve GraphQL prefetching performance by using lateral joins (#1450 via
  #1483).
- Vastly reduce memory consumption when indexing data sources created from
  templates (#1494).

### Misc

- Default to IPFS 0.4.23 in the Docker Compose setup (#1592).
- Support Elasticsearch endpoints without HTTP basic auth (#1576).
- Fix `--version` not reporting the current version (#967 via #1567).
- Convert more code to async/await and simplify async logic (#1558, #1560,
  #1571).
- Use lossy, more tolerant UTF-8 conversion when converting strings to bytes
  (#1541).
- Detect when a node is unresponsive and kill it (#1507).
- Dump core when exiting because of a fatal error (#1512).
- Update to futures 0.3 and tokio 0.2, enabling `async`/`await` (#1448).
- Log block and full transaction hash when handlers fail (#1496).
- Speed up network indexer tests (#1453).
- Fix Travis to always install Node.js 11.x. (#1588).
- Dependency updates: bytes, chrono, crossbeam-channel, ethabi, failure,
  futures, hex, hyper, indexmap, jsonrpc-http-server, num-bigint,
  priority-queue, reqwest, rust-web3, serde, serde_json, slog-async, slog-term,
  tokio, tokio-tungstenite, walkdir, url.
