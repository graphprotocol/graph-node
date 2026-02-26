# Amp-powered subgraphs

> [!NOTE]
> This features is available starting from spec version `1.5.0`

Amp-powered subgraphs are a new kind of subgraphs with SQL data sources that query and index data from the Amp servers.
They are significantly more efficient than the standard subgraphs, and the indexing time can be reduced from days and weeks,
to minutes and hours in most cases.

## Prerequisites

To enable Amp-powered subgraphs, the `GRAPH_AMP_FLIGHT_SERVICE_ADDRESS` ENV variable must be set to a valid Amp Flight gRPC service address.

Additionally, if authentication is required for the Amp Flight gRPC service, the `GRAPH_AMP_FLIGHT_SERVICE_TOKEN` ENV variable must contain a valid authentication token.

## Subgraph manifest

Amp-powered subgraphs introduce a new structure for defining Amp subgraph data sources within the manifest.

### Spec version

The minimum spec version for Amp-powered subgraphs is `1.5.0`.

<details>
<summary>Example YAML:</summary>

```diff
+ specVersion: 1.5.0
  dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
      source:
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### Data source structure

### `kind`

Every Amp data source must have the `kind` set to `amp`, and Amp-powered subgraphs must contain only Amp data sources.
This is used to assign the subgraph to the appropriate indexing process.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
+   - kind: amp
      name: Transfers
      network: ethereum-mainnet
      source:
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `name`

Every Amp data source must have the `name` set to a non-empty string, containing only numbers, letters, hypens, or underscores.
This name is used for observability purposes and to identify progress and potential errors produced by the data source.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
+     name: Transfers
      network: ethereum-mainnet
      source:
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `network`

Every Amp data source must have the `network` field set to a valid network name.
This is used to validate that the SQL queries for this data source produce results for the expected network.

> [!NOTE]
> Currently, the SQL queries are required to produce results for a single network in order to maintain compatibility with non-Amp subgraphs.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
+     network: ethereum-mainnet
      source:
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `source`

Every Amp data source must have a valid `source` that describes the behavior of SQL queries from this data source.

### `source.dataset`

Contains the name of the dataset that can be queried by SQL queries in this data source.
This is used to validate that the SQL queries for this data source only query the expected dataset.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
+     source:
+       dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `source.tables`

Contains the names of the tables that can be queried by SQL queries in this data source.
This is used to validate that the SQL queries for this data source only query the expected tables.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
+     source:
        dataset: edgeandnode/ethereum_mainnet
+       tables:
+         - blocks
+         - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### Optional `source.address`

Contains the contract address with which SQL queries in the data source interact.

Enables SQL query reuse through `sg_source_address()` calls instead of hard-coding the contract address.
SQL queries resolve `sg_source_address()` calls to this contract address.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
+     source:
+       address: "0xc944E90C64B2c07662A292be6244BDf05Cda44a7"
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### Optional `source.startBlock`

Contains the minimum block number that SQL queries in the data source can query.
This is used as a starting point for the indexing process.

_When not provided, defaults to block number `0`._

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
+     source:
+       startBlock: 11446769
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### Optional `source.endBlock`

Contains the maximum block number that SQL queries in the data source can query.
Reaching this block number will complete the indexing process.

_When not provided, defaults to the maximum possible block number._

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
+     source:
+       endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
      transformer:
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `transformer`

Every Amp data source must have a valid `transformer` that describes the transformations of source tables indexed by the Amp-powered subgraph.

### `transformer.apiVersion`

Represents the version of this transformer. Each version may contain a different set of features.

> [!NOTE]
> Currently, only the version `0.0.1` is available.

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
      source:
        endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
+     transformer:
+       apiVersion: 0.0.1
        tables:
          - name: Transfers
            file: <IPFS CID of the SQL query file>
```

</details>

### Optional `transformer.abis`

Contains a list of ABIs that SQL queries can reference to extract event signatures.

Enables the use of `sg_event_signature('CONTRACT_NAME', 'EVENT_NAME')` calls in the
SQL queries which are resolved to full event signatures based on this list.

_When not provided, defaults to an empty list._

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Transfers
      network: ethereum-mainnet
      source:
        endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
+     transformer:
+       abis:
+         - name: ERC721 # The name of the contract
+           file: <IPFS CID of the JSON ABI file>
        apiVersion: 0.0.1
        tables:
          - name: Transfer
            file: <IPFS CID of the SQL query file>
```

</details>

### `transformer.tables`

Contains a list of transformed tables that extract data from source tables into subgraph entities.

### Transformer table structure

### `transformer.tables[i].name`

Represents the name of the transformed table. Must reference a valid entity name from the subgraph schema.

<details>
<summary>Example:</summary>

**GraphQL schema:**

```graphql
type Block @entity(immutable: true) {
    # .. entity fields ...
}
```

**YAML manifest:**

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Blocks
      network: ethereum-mainnet
      source:
        endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
+     transformer:
        apiVersion: 0.0.1
+       tables:
+         - name: Block
            file: <IPFS CID of the SQL query file>
```

</details>

### `transformer.tables[i].query`

Contains an inline SQL query that executes on the Amp server.
This is useful for simple SQL queries like `SELECT * FROM "edgeandnode/ethereum_mainnet".blocks;`.
For more complex cases, a separate file containing the SQL query can be used in the `file` field.

The data resulting from this SQL query execution transforms into subgraph entities.

_When not provided, the `file` field is used instead._

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Blocks
      network: ethereum-mainnet
      source:
        endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
+     transformer:
        apiVersion: 0.0.1
+       tables:
          - name: Block
+           query: SELECT * FROM "edgeandnode/ethereum_mainnet".blocks;
```

</details>

### `transformer.tables[i].file`

Contains the IPFS link to the SQL query that executes on the Amp server.

The data resulting from this SQL query execution transforms into subgraph entities.

_Ignored when the `query` field is provided._
_When not provided, the `query` field is used instead._

<details>
<summary>Example YAML:</summary>

```diff
  specVersion: 1.5.0
+ dataSources:
    - kind: amp
      name: Blocks
      network: ethereum-mainnet
      source:
        endBlock: 23847939
        dataset: edgeandnode/ethereum_mainnet
        tables:
          - blocks
          - transactions
+     transformer:
        apiVersion: 0.0.1
+       tables:
          - name: Block
+           file: <IPFS CID of the SQL query file>
```

</details>

### Amp-powered subgraph examples

Complete examples on how to create, deploy and query Amp-powered subgraphs are available in a separate repository:
https://github.com/edgeandnode/amp-subgraph-examples

## SQL query requirements

### Names

The names of tables, columns, and aliases must not start with `amp_` as this
prefix is reserved for internal use.

### Block numbers

Every SQL query in Amp-powered subgraphs must return the block number for every row.
This is required because subgraphs rely on this information for storing subgraph entities.

Graph-node will look for block numbers in the following columns:
`_block_num`, `block_num`, `blockNum`, `block`, `block_number`, `blockNumber`.

Example SQL query: `SELECT _block_num, /* .. other projections .. */ FROM "edgeandnode/ethereum_mainnet".blocks;`

### Block hashes

Every SQL query in Amp-powered subgraphs is expected to return the block hash for every row.
This is required because subgraphs rely on this information for storing subgraph entities.

When a SQL query does not have the block hash projection, graph-node will attempt to get it from the
source tables specified in the subgraph manifest.

Graph-node will look for block hashes in the following columns:
`hash`, `block_hash`, `blockHash`.

Example SQL query: `SELECT hash, /* .. other projections .. */ FROM "edgeandnode/ethereum_mainnet".blocks;`

> [!NOTE]
> If a table does not contain the block hash column, it can be retrieved by joining that table with another that contains the column on the `_block_num` column.

### Block timestamps

> [!NOTE]
> Only required for Amp-powered subgraphs that use subgraph aggregations.

Every SQL query in Amp-powered subgraphs is expected to return the block timestamps for every row.
This is required because subgraphs rely on this information for storing subgraph entities.

When a SQL query does not have the block timestamps projection, graph-node will attempt to get it from the
source tables specified in the subgraph manifest.

Graph-node will look for block timestamps in the following columns:
`timestamp`, `block_timestamp`, `blockTimestamp`.

Example SQL query: `SELECT timestamp, /* .. other projections .. */ FROM "edgeandnode/ethereum_mainnet".blocks;`

> [!NOTE]
> If a table does not contain the block timestamp column, it can be retrieved by joining that table with another that contains the column on the `_block_num` column.

## Type conversions

Amp core SQL data types are converted intuitively to compatible subgraph entity types.

## Schema generation

Amp-powered subgraphs support the generation of GraphQL schemas based on the schemas of SQL queries referenced in the subgraph manifest.
This is useful when indexing entities that do not rely on complex relationships, such as contract events.

The generated subgraph entities are immutable.

To enable schema generation, simply remove the `schema` field from the subgraph manifest.

> [!NOTE]
> For more flexibility and control over the schema, a manually created GraphQL schema is preferred.

## Aggregations

Amp-powered subgraphs fully support the subgraph aggregations feature.
This allows having complex aggregations on top of data indexed from the Amp servers.

For more information on using the powerful subgraph aggregations feature,
refer to the [documentation](https://github.com/graphprotocol/graph-node/blob/master/docs/aggregations.md).

## Composition

Amp-powered subgraphs fully support the subgraph composition feature.
This allows applying complex subgraph mappings on top of data indexed from the Amp servers.

For more information on using the powerful subgraph composition feature,
refer to the [documentation](https://github.com/graphprotocol/example-composable-subgraph).

## ENV variables

Amp-powered subgraphs feature introduces the following new ENV variables:

- `GRAPH_AMP_FLIGHT_SERVICE_ADDRESS` – The address of the Amp Flight gRPC service. _Defaults to `None`, which disables support for Amp-powered subgraphs._
- `GRAPH_AMP_FLIGHT_SERVICE_TOKEN` – Token used to authenticate Amp Flight gRPC service requests. _Defaults to `None`, which disables authentication._
- `GRAPH_AMP_BUFFER_SIZE` – Maximum number of response batches to buffer in memory per stream for each SQL query. _Defaults to `1,000`._
- `GRAPH_AMP_MAX_BLOCK_RANGE` – Maximum number of blocks to request per stream for each SQL query. _Defaults to `2,000,000`._
- `GRAPH_AMP_QUERY_RETRY_MIN_DELAY_SECONDS` – Minimum time to wait before retrying a failed SQL query to the Amp server. _Defaults to `1` second._
- `GRAPH_AMP_QUERY_RETRY_MAX_DELAY_SECONDS` – Maximum time to wait before retrying a failed SQL query to the Amp server. _Defaults to `600` seconds._

## Metrics

In addition to reporting updates to the existing `deployment_status`, `deployment_head`, `deployment_synced` and `deployment_blocks_processed_count`
metrics, Amp-powered subgraphs feature introduces the following new metrics:

- `deployment_target` – Tracks the maximum block number currently available for indexing within a deployment.
- `deployment_indexing_duration_seconds` – Tracks the total duration in seconds of deployment indexing.

Additionally, the `deployment_sync_secs` is extended with new sections specific to the Amp indexing process.
