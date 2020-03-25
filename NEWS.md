# NEWS

## 0.18.0

**NOTE: JSONB storage is deprecated and will be removed in the next release.
This only affects subgraphs that were deployed with a graph-node version
before 0.16. Starting with this version, graph-node will print a warning for
any subgraph that uses JSONB storage when that subgraph starts syncing. Please
check your logs for this warning. You can remove the warning by redeploying
the subgraph.**

### Feature: Data Source Context (#1404 via #1537)

TODO: Document

### Ethereum

- Add support for calling overloaded contract functions (#48 via #1440).
- Add integration test for calling overloaded contract functions (#1441).
- Avoid `eth_getLogs` requests with block ranges too large for Ethereum nodes to
  handle (#1536).
- Simplify `eth_getLogs` fetching logic to reduce the risk of being rate limited
  by Ethereum nodes and the risk of overloading them (#1540).
- Retry JSON-RPC responses with a `-32000` error (Alchemy uses this for
  timeouts) (#1539).
- Reduce block range size for `trace_filter` requests to prevent request
  timeouts out (#1547).

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

- Use lossy, more tolerant UTF-8 conversion when converting strings to bytes
  (#1541).
- Detect when a node is unresponsive and kill it (#1507).
- Dump core when exiting because of a fatal error (#1512).
- Update to futures 0.3 and tokio 0.2, enabling `async`/`await` (#1448).
- Log block and full transaction hash when handlers fail (#1496).
- Speed up network indexer tests (#1453).
- Dependency updates: bytes, chrono, crossbeam-channel, ethabi, failure,
  futures, hex, hyper, indexmap, jsonrpc-http-server, num-bigint,
  priority-queue, reqwest, rust-web3, serde, serde_json, slog-async, slog-term,
  tokio, tokio-tungstenite, walkdir, url.
