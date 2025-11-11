# Adding a new `apiVersion`

This document explains how to coordinate an `apiVersion` upgrade
across all impacted projects:

1.  [`graph-node`](https:github.com/graphprotocol/graph-node)
2.  [`graph-ts`](https:github.com/graphprotocol/graph-ts)
3.  [`graph-cli`](https:github.com/graphprotocol/graph-cli)
4.  `graph-docs`

## Steps

Those steps should be taken after all relevant `graph-node` changes
have been rolled out to production (hosted-service):

1. Update the default value of the `GRAPH_MAX_API_VERSION` environment
   variable, currently located at this file: `graph/src/data/subgraph/mod.rs`.
   If you're setting it up somewhere manually, you should change there
   as well, or just remove it.

2.  Update `graph-node` minor version and create a new release.

3.  Update `graph-ts` version and create a new release.

4.  For `graph-cli`:

    1.  Write migrations for the new `apiVersion`.
    2.  Update the version restriction on the `build` and `deploy`
        commands to match the new `graph-ts` and `apiVersion` versions.
    3.  Update the `graph-cli` version in `package.json`.
    4.  Update `graph-ts` and `graph-cli` version numbers on scaffolded code and examples.
    5.  Recompile all the examples by running `=$ npm install` inside
        each example directory.
    6.  Update `graph-cli`\'s version and create a new release.
    7.  Release in NPM

5.  Update `graph-docs` with the new `apiVersion` content.

## Query Block Cache

The query block cache is used to speed up subgraph queries by storing recently
processed blocks in memory. This helps avoid re-fetching and recomputing
block data repeatedly.

### Modules Reading from the Cache
- `GraphQLExecutor`: Reads blocks before executing queries.
- `Store`: Retrieves cached blocks when processing subgraph queries.

### Modules Writing to the Cache
- `BlockStreamProcessor`: Writes newly processed blocks to the cache.
- `Indexer`: Updates the cache after syncing new events.

### Configuration
- `CACHE_SIZE`: Maximum number of blocks stored in memory.
- `CACHE_EXPIRY`: Time (in seconds) before cached blocks are considered stale.

### Debugging Tips
- To check cache hits/misses, enable debug logging in the `graph-node` service.
- If queries are slow, verify that `CACHE_SIZE` is sufficient and `CACHE_EXPIRY` is reasonable.

