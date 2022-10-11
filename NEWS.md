# NEWS

## Unreleased

## v0.28.2

**Indexers are advised to migrate to `v0.28.2`** and entirely bypass `v0.28.0` and `v0.28.1`.

Fixed a bug which would cause subgraphs to stop syncing under some `graph-node` deployment configurations. [#4046](https://github.com/graphprotocol/graph-node/pull/4046), [#4051](https://github.com/graphprotocol/graph-node/pull/4051)

## v0.28.1

Yanked. Please migrate to `v0.28.2`.

## v0.28.0

#### Upgrade notes

- **New DB table for dynamic data sources.**
  For new subgraph deployments, dynamic data sources will be recorded under the `sgd*.data_sources$` table, rather than `subgraphs.dynamic_ethereum_contract_data_source`. As a consequence new deployments will not work correctly on earlier graph node versions, so _downgrading to an earlier graph node version is not supported_.  
  See issue [#3405](https://github.com/graphprotocol/graph-node/issues/3405) for other details.

### What's new

- The filepath which "too expensive qeueries" are sourced from is now configurable. You can use either the `GRAPH_NODE_EXPENSIVE_QUERIES_FILE` environment variable or the `expensive_queries_filename` option in the TOML configuration. [#3710](https://github.com/graphprotocol/graph-node/pull/3710)
- The output you'll get from `graphman query` is less cluttered and overall nicer. The new options `--output` and `--trace` are available for detailed query information. [#3860](https://github.com/graphprotocol/graph-node/pull/3860)
- `docker build` will now `--target` the production build stage by default. When you want to get the debug build, you now need `--target graph-node-debug`. [#3814](https://github.com/graphprotocol/graph-node/pull/3814)
- Node IDs can now contain any character. The Docker start script still replaces hyphens with underscores for backwards compatibility reasons, but this behavior can be changed with the `GRAPH_NODE_ID_USE_LITERAL_VALUE` environment variable. With this new option, you can now seamlessly use the K8s-provided host names as node IDs, provided you reassign your deployments accordingly. [#3688](https://github.com/graphprotocol/graph-node/pull/3688)
- You can now use the `conn_pool_size` option in TOML configuration files to configure the connection pool size for Firehose providers. [#3833](https://github.com/graphprotocol/graph-node/pull/3833)
- Index nodes now have an endpoint to perform block number to canonical hash conversion, which will unblock further work towards multichain support. [#3942](https://github.com/graphprotocol/graph-node/pull/3942)
- `_meta.block.timestamp` is now available for subgraphs indexing EVM chains. [#3738](https://github.com/graphprotocol/graph-node/pull/3738), [#3902](https://github.com/graphprotocol/graph-node/pull/3902)
- The `deployment_eth_rpc_request_duration` metric now also observes `eth_getTransactionReceipt` requests' duration. [#3903](https://github.com/graphprotocol/graph-node/pull/3903)
- New Prometheus metrics `query_parsing_time` and `query_validation_time` for monitoring query processing performance. [#3760](https://github.com/graphprotocol/graph-node/pull/3760)
- New command `graphman config provider`, which shows what providers are available for new deployments on a given network and node. [#3816](https://github.com/graphprotocol/graph-node/pull/3816)
  E.g. `$ graphman --node-id index_node_0 --config graph-node.toml config provider mainnet`
- Experimental support for GraphQL API versioning has landed. [#3185](https://github.com/graphprotocol/graph-node/pull/3185)
- Progress towards experimental support for off-chain data sources. [#3791](https://github.com/graphprotocol/graph-node/pull/3791)
- Experimental integration for substreams. [#3777](https://github.com/graphprotocol/graph-node/pull/3777), [#3784](https://github.com/graphprotocol/graph-node/pull/3784), [#3897](https://github.com/graphprotocol/graph-node/pull/3897), [#3765](https://github.com/graphprotocol/graph-node/pull/3765), and others 

### Bug fixes

- `graphman stats` now complains instead of failing silently when incorrectly setting `account-like` optimizations. [#3918](https://github.com/graphprotocol/graph-node/pull/3918)
- Fixed inconsistent logic in the provider selection when the `limit` TOML configuration option was set. [#3816](https://github.com/graphprotocol/graph-node/pull/3816)
- Fixed issues that would arise from dynamic data sources' names clashing against template names. [#3851](https://github.com/graphprotocol/graph-node/pull/3851)
- Dynamic data sources triggers are now processed by insertion order. [#3851](https://github.com/graphprotocol/graph-node/pull/3851), [#3854](https://github.com/graphprotocol/graph-node/pull/3854)
- When starting, the Docker image now replaces the `bash` process with the `graph-node` process (with a PID of 1). [#3803](https://github.com/graphprotocol/graph-node/pull/3803)
- Refactor subgraph store tests by @evaporei in https://github.com/graphprotocol/graph-node/pull/3662
- The `ethereum_chain_head_number` metric doesn't get out of sync anymore on chains that use Firehose. [#3771](https://github.com/graphprotocol/graph-node/pull/3771), [#3732](https://github.com/graphprotocol/graph-node/issues/3732)
- Fixed a crash caused by bad block data from the provider. [#3944](https://github.com/graphprotocol/graph-node/pull/3944)
- Fixed some minor Firehose connectivity issues via TCP keepalive, connection and request timeouts, and connection window size tweaks. [#3822](https://github.com/graphprotocol/graph-node/pull/3822), [#3855](https://github.com/graphprotocol/graph-node/pull/3855), [#3877](https://github.com/graphprotocol/graph-node/pull/3877), [#3810](https://github.com/graphprotocol/graph-node/pull/3810), [#3818](https://github.com/graphprotocol/graph-node/pull/3818)
- Copying private data sources' tables across shards now works as expected. [#3836](https://github.com/graphprotocol/graph-node/pull/3836)

### Performance improvements

- Firehose GRPC stream requests are now compressed with `gzip`, if the server supports it. [#3893](https://github.com/graphprotocol/graph-node/pull/3893)
- Memory efficiency improvements within the entity cache. [#3594](https://github.com/graphprotocol/graph-node/pull/3594)
- Identical queries now benefit from GraphQL validation caching, and responses are served faster. [#3759](https://github.com/graphprotocol/graph-node/pull/3759)

### Other

- Avoid leaking some sensitive information in logs. [#3812](https://github.com/graphprotocol/graph-node/pull/3812)

### Dependency updates

| Dependency        | PR(s)                                                                                                                                                                                                                                                          | Old version | Current version |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | --------------- |
| `serde_yaml`      | [#3746](https://github.com/graphprotocol/graph-node/pull/3746)                                                                                                                                                                                                 | `v0.8.24`   | `v0.8.26`       |
| `web3`            | [#3806](https://github.com/graphprotocol/graph-node/pull/3806)                                                                                                                                                                                                 | `2760dbd`   | `7f8eb6d`       |
| `clap`            | [#3794](https://github.com/graphprotocol/graph-node/pull/3794), [#3848](https://github.com/graphprotocol/graph-node/pull/3848), [#3931](https://github.com/graphprotocol/graph-node/pull/3931)                                                                 | `v3.2.8`    | `3.2.21`        |
| `cid`             | [#3824](https://github.com/graphprotocol/graph-node/pull/3824)                                                                                                                                                                                                 | `v0.8.5`    | `v0.8.6`        |
| `anyhow`          | [#3826](https://github.com/graphprotocol/graph-node/pull/3826), [#3841](https://github.com/graphprotocol/graph-node/pull/3841), [#3865](https://github.com/graphprotocol/graph-node/pull/3865), [#3932](https://github.com/graphprotocol/graph-node/pull/3932) | `v1.0.57`   | `1.0.65`        |
| `chrono`          | [#3827](https://github.com/graphprotocol/graph-node/pull/3827), [#3849](https://github.com/graphprotocol/graph-node/pull/3839), [#3868](https://github.com/graphprotocol/graph-node/pull/3868)                                                                 | `v0.4.19`   | `v0.4.22`       |
| `proc-macro2`     | [#3845](https://github.com/graphprotocol/graph-node/pull/3845)                                                                                                                                                                                                 | `v1.0.40`   | `1.0.43`        |
| `ethabi`          | [#3847](https://github.com/graphprotocol/graph-node/pull/3847)                                                                                                                                                                                                 | `v17.1.0`   | `v17.2.0`       |
| `once_cell`       | [#3870](https://github.com/graphprotocol/graph-node/pull/3870)                                                                                                                                                                                                 | `v1.13.0`   | `v1.13.1`       |
| `either`          | [#3869](https://github.com/graphprotocol/graph-node/pull/3869)                                                                                                                                                                                                 | `v1.7.0`    | `v1.8.0`        |
| `sha2`            | [#3904](https://github.com/graphprotocol/graph-node/pull/3904)                                                                                                                                                                                                 | `v0.10.2`   | `v0.10.5`       |
| `mockall`         | [#3776](https://github.com/graphprotocol/graph-node/pull/3776)                                                                                                                                                                                                 | `v0.9.1`    | removed         |
| `croosbeam`       | [#3772](https://github.com/graphprotocol/graph-node/pull/3772)                                                                                                                                                                                                 | `v0.8.1`    | `v0.8.2`        |
| `async-recursion` | [#3873](https://github.com/graphprotocol/graph-node/pull/3873)                                                                                                                                                                                                 | none        | `v1.0.0`        |

<!--
### Leftover PRs from GitHub's auto-generated release notes. We don't care about these.


- [x] wire substreams by @mangas in https://github.com/graphprotocol/graph-node/pull/3813
- [x] Feature/substream data source by @Eduard-Voiculescu in https://github.com/graphprotocol/graph-node/pull/3780
- [x] Feature/substreams v2 blocks by @Eduard-Voiculescu in https://github.com/graphprotocol/graph-node/pull/3876
- [x] Substreams' Protobuf. [#3765](https://github.com/graphprotocol/graph-node/pull/3765)
- [x] substreams trigger processor by @mangas in https://github.com/graphprotocol/graph-node/pull/3787
- [x] substreams: data source validation by @mangas in https://github.com/graphprotocol/graph-node/pull/3783
- [x] Filipe/test run substream fixes by @mangas in https://github.com/graphprotocol/graph-node/pull/3857
- [x] fix(ipfs): Allowlist of safe hashes by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3792
- [x] firehose: fix request for v2 by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3837
- [x] Polling file monitor by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3411
- [x] test(store): Fix race condition in graft test by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3790
- [x] Automated generation of the `Asc` types by @pienkowb in https://github.com/graphprotocol/graph-node/pull/3722
- [x] node: Remove assert-cli tests by leoyvens in https://github.com/graphprotocol/graph-node/pull/3789
- [x] Adding tests for decoding of gRPC server response from substreams by @Eduard-Voiculescu in https://github.com/graphprotocol/graph-node/pull/3896
- [x] Small improvements to `graphman stats` by @lutter in https://github.com/graphprotocol/graph-node/pull/3918
- [x] graphql: Add deterministic error test by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3788
- [x] core: add missing word in MetricsRegistry initialization error msg by @tilacog in https://github.com/graphprotocol/graph-node/pull/3859
- [x] async && refactor block_number by @mangas in https://github.com/graphprotocol/graph-node/pull/3873
- [x] Release 0.27.0 by @evaporei in https://github.com/graphprotocol/graph-node/pull/3778
- [x] firehose: Print error messages without whitespace by @leoyvens in https://github.com/graphprotocol/graph-node/pull/3856
- [x] Move `DEAD_WEIGHT` env. flag initialization logic under the global `struct EnvVars` by @neysofu in https://github.com/graphprotocol/graph-node/pull/3744
- [x] graphql,server: make `Resolver::resolve_object` async by @tilacog in https://github.com/graphprotocol/graph-node/pull/3938
- [x] Fix: typos by @omahs in https://github.com/graphprotocol/graph-node/pull/3910
- [x] Derive `IndexNodeResolver`'s `Clone` implementation by @neysofu in https://github.com/graphprotocol/graph-node/pull/3943

-->

## 0.27.0

- Store writes are now carried out in parallel to the rest of the subgraph process, improving indexing performance for subgraphs with significant store interaction. Metrics & monitoring was updated for this new pipelined process;
- This adds support for apiVersion 0.0.7, which makes receipts accessible in Ethereum event handlers. [Documentation link](https://thegraph.com/docs/en/developing/creating-a-subgraph/#transaction-receipts-in-event-handlers);
- This introduces some improvements to the subgraph GraphQL API, which now supports filtering on the basis of, and filtering for entities which changed from a certain block;
- Support was added for Arweave indexing. Tendermint was renamed to Cosmos in Graph Node. These integrations are still in "beta";
- Callhandler block filtering for contract calls now works as intended (this was a longstanding bug);
- Gas costing for mappings is still set at a very high default, as we continue to benchmark and refine this metric;
- A new `graphman fix block` command was added to easily refresh a block in the block cache, or clear the cache for a given network;
- IPFS file fetching now uses `files/stat`, as `object` was deprecated;
- Subgraphs indexing via a Firehose can now take advantage of Firehose-side filtering;
- NEAR subgraphs can now match accounts for receipt filtering via prefixes or suffixes.

## Upgrade notes

- In the case of you having custom SQL, there's a [new SQL migration](https://github.com/graphprotocol/graph-node/blob/master/store/postgres/migrations/2022-04-26-125552_alter_deployment_schemas_version/up.sql);
- On the pipelining of the store writes, there's now a new environment variable `GRAPH_STORE_WRITE_QUEUE` (default value is `5`), that if set to `0`, the old synchronous behaviour will come in instead. The value stands for the amount of write/revert parallel operations [#3177](https://github.com/graphprotocol/graph-node/pull/3177);
- There's now support for TLS connections in the PostgreSQL `notification_listener` [#3503](https://github.com/graphprotocol/graph-node/pull/3503);
- GraphQL HTTP and WebSocket ports can now be set via environment variables [#2832](https://github.com/graphprotocol/graph-node/pull/2832);
- The genesis block can be set via the `GRAPH_ETHEREUM_GENESIS_BLOCK_NUMBER` env var [#3650](https://github.com/graphprotocol/graph-node/pull/3650);
- There's a new experimental feature to limit the number of subgraphs for a specific web3 provider. [Link for documentation](https://github.com/graphprotocol/graph-node/blob/master/docs/config.md#controlling-the-number-of-subgraphs-using-a-provider);
- Two new GraphQL validation environment variables were included: `ENABLE_GRAPHQL_VALIDATIONS` and `SILENT_GRAPHQL_VALIDATIONS`, which are documented [here](https://github.com/graphprotocol/graph-node/blob/master/docs/environment-variables.md#graphql);
- A bug fix for `graphman index` was landed, which fixed the behavior where if one deployment was used by multiple names would result in the command not working [#3416](https://github.com/graphprotocol/graph-node/pull/3416);
- Another fix landed for `graphman`, the bug would allow the `unassign`/`reassign` commands to make two or more nodes index the same subgraph by mistake [#3478](https://github.com/graphprotocol/graph-node/pull/3478);
- Error messages of eth RPC providers should be clearer during `graph-node` start up [#3422](https://github.com/graphprotocol/graph-node/pull/3422);
- Env var `GRAPH_STORE_CONNECTION_MIN_IDLE` will no longer panic, instead it will log a warning if it exceeds the `pool_size` [#3489](https://github.com/graphprotocol/graph-node/pull/3489);
- Failed GraphQL queries now have proper timing information in the service metrics [#3508](https://github.com/graphprotocol/graph-node/pull/3508);
- Non-primary shards now can be disabled through setting the `pool_size` to `0` [#3513](https://github.com/graphprotocol/graph-node/pull/3513);
- Queries with large results now have a `query_id` [#3514](https://github.com/graphprotocol/graph-node/pull/3514);
- It's now possible to disable the LFU Cache by setting `GRAPH_QUERY_LFU_CACHE_SHARDS` to `0` [#3522](https://github.com/graphprotocol/graph-node/pull/3522);
- `GRAPH_ACCOUNT_TABLES` env var is not supported anymore [#3525](https://github.com/graphprotocol/graph-node/pull/3525);
- [New documentation](https://github.com/graphprotocol/graph-node/blob/master/docs/implementation/metadata.md) landed on the metadata tables;
- `GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION` for GraphQL subscriptions now has a default of `1000` [#3735](https://github.com/graphprotocol/graph-node/pull/3735)

## 0.26.0

### Features

- Gas metering #2414
- Adds support for Solidity Custom Errors #2577
- Debug fork tool #2995 #3292
- Automatically remove unused deployments #3023
- Fix fulltextsearch space handling #3048
- Allow placing new deployments onto one of several shards #3049
- Make NEAR subgraphs update their sync status #3108
- GraphQL validations #3164
- Add special treatment for immutable entities #3201
- Tendermint integration #3212
- Skip block updates when triggers are empty #3223 #3268
- Use new GraphiQL version #3252
- GraphQL prefetching #3256
- Allow using Bytes as well as String/ID for the id of entities #3271
- GraphQL route for dumping entity changes in subgraph and block #3275
- Firehose filters #3323
- NEAR filters #3372

### Robustness

- Improve our `CacheWeight` estimates #2935
- Refactor GraphQL execution #3005
- Setup databases in parallel #3019
- Block ingestor now fetches receipts in parallel #3030
- Prevent subscriptions from back-pressuring the notification queue #3053
- Avoid parsing X triggers if the filter is empty #3083
- Pipeline `BlockStream` #3085
- More robust `proofOfIndexing` GraphQL route #3348

### `graphman`

- Add `run` command, for running a subgraph up to a block #3079
- Add `analyze` command, for analyzing a PostgreSQL table, which can improve performance #3170
- Add `index create` command, for adding an index to certain attributes #3175
- Add `index list` command, for listing indexes #3198
- Add `index drop` command, for dropping indexes #3198

### Dependency Updates

These are the main ones:

- Updated protobuf to latest version for NEAR #2947
- Update `web3` crate #2916 #3120 #3338
- Update `graphql-parser` to `v0.4.0` #3020
- Bump `itertools` from `0.10.1` to `0.10.3` #3037
- Bump `clap` from `2.33.3` to `2.34.0` #3039
- Bump `serde_yaml` from `0.8.21` to `0.8.23` #3065
- Bump `tokio` from `1.14.0` to `1.15.0` #3092
- Bump `indexmap` from `1.7.0` to `1.8.0` #3143
- Update `ethabi` to its latest version #3144
- Bump `structopt` from `0.3.25` to `0.3.26` #3180
- Bump `anyhow` from `1.0.45` to `1.0.53` #3182
- Bump `quote` from `1.0.9` to `1.0.16` #3112 #3183 #3384
- Bump `tokio` from `1.15.0` to `1.16.1` #3208
- Bump `semver` from `1.0.4` to `1.0.5` #3229
- Bump `async-stream` from `0.3.2` to `0.3.3` #3361
- Update `jsonrpc-server` #3313

### Misc

- More context when logging RPC calls #3128
- Increase default reorg threshold to 250 for Ethereum #3308
- Improve traces error logs #3353
- Add warning and continue on parse input failures for Ethereum #3326

### Upgrade Notes

When upgrading to this version, we recommend taking a brief look into these changes:

- Gas metering #2414
  - Now there's a gas limit for subgraph mappings, if the limit is reached the subgraph will fail with a non-deterministic error, you can make them recover via the environment variable `GRAPH_MAX_GAS_PER_HANDLER`
- Improve our `CacheWeight` estimates #2935
  - This is relevant because a couple of releases back we've added a limit for the memory size of a query result. That limit is based of the `CacheWeight`.

These are some of the features that will probably be helpful for indexers 😊

- Allow placing new deployments onto one of several shards #3049
- GraphQL route for dumping entity changes in subgraph and block #3275
- Unused deployments are automatically removed now #3023
  - The interval can be set via `GRAPH_REMOVE_UNUSED_INTERVAL`
- Setup databases in parallel #3019
- Block ingestor now fetches receipts in parallel #3030
  - `GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES` can be set to `true` for the old fetching behavior
- More robust `proofOfIndexing` GraphQL route #3348
  - A token can be set via `GRAPH_POI_ACCESS_TOKEN` to limit access to the POI route
- The new `graphman` commands 🙂


### Api Version 0.0.7 and Spec Version 0.0.5
This release brings API Version 0.0.7 in mappings, which allows Ethereum event handlers to require transaction receipts to be present in the `Event` object.
Refer to [PR #3373](https://github.com/graphprotocol/graph-node/pull/3373) for instructions on how to enable that.


## 0.25.2

This release includes two changes:

- Bug fix of blocks being skipped from processing when: a deterministic error happens **and** the `index-node` gets restarted. Issue [#3236](https://github.com/graphprotocol/graph-node/issues/3236), Pull Request: [#3316](https://github.com/graphprotocol/graph-node/pull/3316).
- Automatic retries for non-deterministic errors. Issue [#2945](https://github.com/graphprotocol/graph-node/issues/2945), Pull Request: [#2988](https://github.com/graphprotocol/graph-node/pull/2988).

This is the last patch on the `0.25` minor version, soon `0.26.0` will be released. While that we recommend updating to this version to avoid determinism issues that could be caused on `graph-node` restarts.

## 0.25.1

This release only adds two fixes:

- The first is to address an issue with decoding the input of some calls [#3194](https://github.com/graphprotocol/graph-node/issues/3194) where subgraphs that would try to index contracts related to those would fail. Now they can advance normally.
- The second one is to fix a non-determinism issue with the retry mechanism for errors. Whenever a non-deterministic error happened, we would keep retrying to process the block, however we should've clear the `EntityCache` on each run so that the error entity changes don't get transacted/saved in the database in the next run. This could make the POI generation non-deterministic for subgraphs that failed and retried for non-deterministic reasons, adding a new entry to the database for the POI.

We strongly recommend updating to this version as quickly as possible.

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
