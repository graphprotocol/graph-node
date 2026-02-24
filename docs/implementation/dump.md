## Dump Format

The `graphman dump` command exports all entity data and metadata for a
single subgraph deployment into a self-contained directory of Parquet files
and JSON metadata. The resulting dump can be used to restore the deployment
into a different `graph-node` instance via `graphman restore`. Dumps are
consistent snapshots of the deployment's state at a specific point in time.

### Directory layout

A dump directory has the following structure:

```
<dump-dir>/
  metadata.json                  -- deployment metadata + per-table state
  schema.graphql                 -- raw GraphQL schema text
  subgraph.yaml                  -- raw subgraph manifest YAML (optional)
  <EntityType>/
    chunk_000000.parquet         -- rows ordered by vid
    chunk_000001.parquet         -- incremental append (future chunks)
    ...
  data_sources$/
    chunk_000000.parquet         -- dynamic data sources
```

Each entity type defined in the GraphQL schema gets its own subdirectory,
named after the entity type exactly as it appears in the schema (e.g.
`Token/`, `Pool/`). The Proof of Indexing appears as a regular entity
directory name `Poi$`. The special `data_sources$` directory holds dynamic
data sources created at runtime.

Within each directory, data is stored in numbered chunk files
(`chunk_000000.parquet`, `chunk_000001.parquet`, ...). A fresh dump
produces a single `chunk_000000.parquet` per table. Incremental dumps
append new chunks rather than rewriting existing ones.

The GraphQL schema and subgraph manifest are stored as separate plain-text
files `schema.graphql` and `subgraph.yaml`.

### metadata.json

The top-level `metadata.json` contains everything needed to reconstruct the
deployment's table structure, plus diagnostic information captured at dump
time. Its structure is:

```json
{
  "version": 1,
  "deployment": "Qm...",
  "network": "mainnet",

  "manifest": {
    "spec_version": "1.0.0",
    "description": "Optional subgraph description",
    "repository": "https://github.com/...",
    "features": ["..."],
    "entities_with_causality_region": ["EntityType1"],
    "history_blocks": 2147483647
  },

  "earliest_block_number": 12345,
  "start_block": { "number": 12345, "hash": "0xabc..." },
  "head_block": { "number": 99999, "hash": "0xdef..." },
  "entity_count": 150000,

  "graft_base": null,
  "graft_block": null,
  "debug_fork": null,

  "health": {
    "failed": false,
    "health": "healthy",
    "fatal_error": null,
    "non_fatal_errors": []
  },

  "indexes": {
    "token": [
      "CREATE INDEX CONCURRENTLY IF NOT EXISTS attr_0_0_id ON sgd.token USING btree (id)"
    ]
  },

  "tables": {
    "Token": {
      "immutable": true,
      "has_causality_region": false,
      "chunks": [
        {
          "file": "Token/chunk_000000.parquet",
          "min_vid": 0,
          "max_vid": 50000,
          "row_count": 50000
        }
      ],
      "max_vid": 50000
    },
    "data_sources$": {
      "immutable": false,
      "has_causality_region": true,
      "chunks": [
        {
          "file": "data_sources$/chunk_000000.parquet",
          "min_vid": 0,
          "max_vid": 100,
          "row_count": 100
        }
      ],
      "max_vid": 100
    }
  }
}
```

**Field descriptions:**

| Field                                     | Description                                                                                                                         |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `version`                                 | Format version. Must be `1`.                                                                                                        |
| `deployment`                              | Deployment hash (`Qm...`).                                                                                                          |
| `network`                                 | The blockchain network (e.g. `mainnet`, `goerli`).                                                                                  |
| `manifest`                                | Manifest metadata extracted from `subgraphs.subgraph_manifest`.                                                                     |
| `manifest.spec_version`                   | Subgraph API version. Required to parse `schema.graphql`.                                                                           |
| `manifest.entities_with_causality_region` | Entity types that have a `causality_region` column.                                                                                 |
| `manifest.history_blocks`                 | How many blocks of entity version history are retained.                                                                             |
| `earliest_block_number`                   | Earliest block for which data exists (accounts for pruning).                                                                        |
| `start_block`                             | The block where indexing started. Null if not set.                                                                                  |
| `head_block`                              | The latest indexed block at dump time.                                                                                              |
| `entity_count`                            | Total entity count across all tables.                                                                                               |
| `graft_base`                              | Deployment hash of the graft base, if any.                                                                                          |
| `graft_block`                             | Block pointer of the graft point, if any.                                                                                           |
| `debug_fork`                              | Debug fork deployment hash, if any.                                                                                                 |
| `health`                                  | Point-in-time health snapshot. Not used during restore.                                                                             |
| `indexes`                                 | Point-in-time index definitions as SQL. Not used during restore (indexes are auto-created by `Layout::create_relational_schema()`). |
| `tables`                                  | Per-table metadata keyed by entity type name (or `data_sources$`).                                                                  |

Each entry in `tables` contains:

| Field                  | Description                                                                    |
| ---------------------- | ------------------------------------------------------------------------------ |
| `immutable`            | Whether the entity type is immutable (uses `block$` instead of `block_range`). |
| `has_causality_region` | Whether rows have a `causality_region` column.                                 |
| `chunks`               | Ordered list of Parquet chunk files for this table.                            |
| `chunks[].file`        | Relative path from the dump directory.                                         |
| `chunks[].min_vid`     | Minimum `vid` value in this chunk.                                             |
| `chunks[].max_vid`     | Maximum `vid` value in this chunk.                                             |
| `chunks[].row_count`   | Number of rows in this chunk.                                                  |
| `max_vid`              | Maximum `vid` across all chunks. `-1` if the table is empty.                   |

### Parquet schema: entity tables

Each entity table's Parquet file uses an Arrow schema derived from the
entity's GraphQL definition. Columns are ordered as follows:

1. **System columns** (always present, in this order):
   - `vid` (Int64, non-nullable) -- row version ID
   - Block tracking (one of):
     - Immutable entities: `block$` (Int32, non-nullable)
     - Mutable entities: `block_range_start` (Int32, non-nullable),
       `block_range_end` (Int32, nullable -- null means unbounded/current)
   - `causality_region` (Int32, non-nullable) -- only if the entity has one

2. **Data columns** in GraphQL declaration order, skipping fulltext
   (`TSVector`) columns which are generated and rebuilt on restore.

The PostgreSQL `int4range` type used for `block_range` is decomposed into
two scalar columns (`block_range_start`, `block_range_end`) in the Parquet
representation. This avoids the need for a custom range type in Arrow.

#### Type mapping

GraphQL/PostgreSQL column types map to Arrow data types as follows:

| ColumnType      | Arrow DataType                 | Notes                                                          |
| --------------- | ------------------------------ | -------------------------------------------------------------- |
| `Boolean`       | `Boolean`                      |                                                                |
| `Int`           | `Int32`                        |                                                                |
| `Int8`          | `Int64`                        |                                                                |
| `Bytes`         | `Binary`                       | Raw bytes, no hex encoding                                     |
| `BigInt`        | `Utf8`                         | Stored as decimal string for arbitrary precision               |
| `BigDecimal`    | `Utf8`                         | Stored as decimal string for arbitrary precision               |
| `Timestamp`     | `Timestamp(Microsecond, None)` | Microseconds since epoch, no timezone                          |
| `String`        | `Utf8`                         |                                                                |
| `Enum(...)`     | `Utf8`                         | Enum variant as string (cast from PG enum to text during dump) |
| `TSVector(...)` | _skipped_                      | Fulltext index columns are generated; rebuilt on restore       |

**Array columns:** A GraphQL list field (e.g. `tags: [String!]!`) is
stored as `List<T>` where `T` is the base Arrow type from the table
above. Whether a column is a list is determined by the GraphQL field type,
not by `ColumnType`. For example, `[String!]!` becomes `List<Utf8>` and
`[Int!]` becomes `List<Int32>`.

**Nullability** follows the GraphQL schema: non-null fields produce
non-nullable Arrow columns; optional fields produce nullable columns. List
elements within list columns are always marked nullable in the Arrow schema.

### Parquet schema: data_sources$

The `data_sources$` table has a fixed schema independent of the GraphQL
definition:

| Column              | Arrow DataType | Nullable | Description                                        |
| ------------------- | -------------- | -------- | -------------------------------------------------- |
| `vid`               | `Int64`        | no       | Row version ID                                     |
| `block_range_start` | `Int32`        | no       | Lower bound of `block_range`                       |
| `block_range_end`   | `Int32`        | yes      | Upper bound (null = unbounded)                     |
| `causality_region`  | `Int32`        | no       | Causality region                                   |
| `manifest_idx`      | `Int32`        | no       | Index into the manifest's data source list         |
| `parent`            | `Int32`        | yes      | Self-referencing parent data source                |
| `id`                | `Binary`       | yes      | Data source identifier                             |
| `param`             | `Binary`       | yes      | Data source parameter                              |
| `context`           | `Utf8`         | yes      | JSON context                                       |
| `done_at`           | `Int32`        | yes      | Block number where the data source was marked done |

### Compression

All Parquet files use ZSTD compression (default level).

### Row ordering

Within each Parquet chunk file, rows are ordered by `vid` (ascending).
This matches the primary key ordering in PostgreSQL and enables efficient
sequential reads during restore.

### Incremental dumps

An incremental dump reads the existing `metadata.json`, determines the
`max_vid` for each table, and queries only rows with `vid > max_vid`. New
rows are written to new chunk files (e.g. `chunk_000001.parquet`) and the
metadata is updated atomically (write to a temp file, then rename).

### Atomicity

The `metadata.json` file is always written atomically: the dump writes to
`metadata.json.tmp` first, then renames it to `metadata.json`. This
ensures that a reader never sees a partially-written metadata file. If the
dump process crashes mid-write, the previous `metadata.json` remains
intact. The Parquet chunk files are written before `metadata.json` is
updated, so chunk files referenced by `metadata.json` are always complete.
