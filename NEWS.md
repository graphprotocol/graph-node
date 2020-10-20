# NEWS

## Unreleased

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
