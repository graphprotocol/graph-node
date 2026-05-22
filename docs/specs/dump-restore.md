# Spec: Parquet Dump/Restore for Subgraph Data

## Problem

Subgraph entity data lives exclusively in PostgreSQL. There's no way to export a subgraph's data for backup, migration between environments, or sharing. We need a file-based dump/restore mechanism.

## Goals

1. **Dump** a subgraph's entity data to parquet files
2. **Restore** a subgraph from parquet files
3. **Incremental append** -- add newly arrived data to an existing dump without rewriting it
4. Long-term: make dumping an **ongoing process** (not just a one-off CLI operation)

## Non-goals (for now)

- S3/GCS output
- Schema evolution / migration between different schema versions

## Data Model Recap

Each subgraph deployment has:
- A PostgreSQL schema (e.g., `sgd123`)
- One table per entity type, with columns:
  - `vid` (bigserial) -- row version ID, primary key
  - `block_range` (int4range) for mutable entities OR `block$` (int) for immutable
  - `causality_region` (int, optional) -- for offchain data sources
  - Data columns matching GraphQL fields (text, int, bigint, numeric, bytea, bool, timestamptz, arrays, enums)
- `data_sources$` table -- dynamic data sources created at runtime (defined in `dynds/private.rs`, separate from Layout):
  - `vid` (int, identity PK), `block_range` (int4range), `causality_region` (int)
  - `manifest_idx` (int), `parent` (int, self-ref FK), `id` (bytea)
  - `param` (bytea, nullable), `context` (jsonb, nullable), `done_at` (int, nullable)
  - Note: `parent` and `id` exist in the DDL but are currently unused by `insert()`, `load()`, and `copy_to()`. The dump should include them for completeness (they may contain data in older deployments).
- `poi2$` table -- Proof of Indexing data (a **mutable** entity table in the Layout, i.e. it uses `block_range` not `block$`; has `digest` (bytea), `id` (text), optionally `block_time$` (int8); `has_causality_region: false`). Conditionally created when `catalog.use_poi` is true. Excluded from some Layout operations like `find_changes()`.
- Metadata in `subgraphs.subgraph_manifest`, `subgraphs.deployment`, and `subgraphs.head` tables

## Dump Format

### Directory layout

```
<dump-dir>/
  metadata.json                  -- deployment metadata + per-table state
  schema.graphql                 -- raw GraphQL schema text
  subgraph.yaml                  -- raw subgraph manifest YAML (optional)
  <EntityType>/
    chunk_000000.parquet         -- rows ordered by vid
    chunk_000001.parquet         -- incremental append
    ...
  data_sources$/
    chunk_000000.parquet         -- dynamic data sources
```

One parquet file per entity type (each has a different columnar schema). The `data_sources$` table is dumped alongside entity tables. The `poi2$` table, when present, is a regular entity table in the `Layout` (conditionally created when `catalog.use_poi` is true) and appears as any other entity type directory. Incremental dumps produce new chunk files rather than rewriting existing ones.

The GraphQL schema and subgraph manifest YAML are stored as separate files rather than embedded in `metadata.json`. This matches the existing dump code in `dump.rs` and keeps the files human-readable and diffable.

### Parquet schema per entity type

System columns (always present):
- `vid` -> Int64
- Immutable entities: `block$` -> Int32
- Mutable entities: `block_range_start` -> Int32, `block_range_end` -> Int32 (nullable; null = unbounded/current)
- `causality_region` -> Int32 (only if table has it)

Data columns mapped from `ColumnType` (all 10 variants defined in `relational.rs:1342`):

| ColumnType         | Arrow DataType             | Notes                        |
|--------------------|---------------------------|-------------------------------|
| Boolean            | Boolean                    |                               |
| Int                | Int32                      |                               |
| Int8               | Int64                      |                               |
| Bytes              | Binary                     | Raw bytes                     |
| BigInt             | Utf8                       | Arbitrary precision as string |
| BigDecimal         | Utf8                       | Arbitrary precision as string |
| Timestamp          | TimestampMicrosecond(None) | Matches Value::Timestamp      |
| String             | Utf8                       |                               |
| Enum(EnumType)     | Utf8                       | String value of enum variant  |
| TSVector(FulltextConfig) | **Skip**             | Generated; rebuild on restore |

**List/array columns:** `List(T)` is not a `ColumnType` variant. Whether a column is an array is determined by `Column.is_list()` (delegates to the GraphQL `field_type`). A `[String]` field has `column_type: ColumnType::String` with a list-typed `field_type`. For Arrow mapping, check `column.is_list()` and wrap the base Arrow type in `List<mapped T>`. In `OidValue`, arrays have separate variants (`StringArray`, `BytesArray`, `BoolArray`, `Ints`, `Int8Array`, `BigDecimalArray`, `TimestampArray`).

Nullability follows the GraphQL schema (non-null fields -> non-nullable Arrow columns).

### metadata.json

Contains everything needed to reconstruct the deployment's table structure, plus diagnostic information (health, indexes) captured at dump time. The GraphQL schema and manifest YAML are stored in separate files (`schema.graphql`, `subgraph.yaml`), not embedded here.

The struct backing this file is `Metadata` (evolved from the existing `Control` struct in `dump.rs`).

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
        { "file": "Token/chunk_000000.parquet", "min_vid": 0, "max_vid": 50000, "row_count": 50000 }
      ],
      "max_vid": 50000
    },
    "data_sources$": {
      "immutable": false,
      "has_causality_region": true,
      "chunks": [
        { "file": "data_sources$/chunk_000000.parquet", "min_vid": 0, "max_vid": 100, "row_count": 100 }
      ],
      "max_vid": 100
    }
  }
}
```

**Field sources:**

| Field | Source | Code path |
|-------|--------|-----------|
| `manifest.*` | `subgraphs.subgraph_manifest` | `SubgraphManifestEntity` via `deployment_entity()` in `detail.rs` |
| `start_block` | `subgraphs.subgraph_manifest` | `start_block_number`, `start_block_hash` columns; available via `StoredSubgraphManifest` in `detail.rs:542-543`, assembled into `SubgraphDeploymentEntity.start_block` |
| `earliest_block_number` | `subgraphs.deployment` | `SubgraphDeploymentEntity.earliest_block_number` |
| `graft_base`, `graft_block` | `subgraphs.deployment` | `SubgraphDeploymentEntity.graft_base`, `.graft_block` |
| `debug_fork` | `subgraphs.deployment` | `SubgraphDeploymentEntity.debug_fork` |
| `head_block` | `subgraphs.head` | `SubgraphDeploymentEntity.latest_block` |
| `entity_count` | `subgraphs.head` | `DeploymentDetail.entity_count` (i64 in DB, usize in Rust) |
| `health.*` | `subgraphs.deployment` + `subgraph_error` | `SubgraphDeploymentEntity.{failed, health, fatal_error, non_fatal_errors}` |
| `indexes` | `pg_indexes` catalog | `IndexList::load()` → `CreateIndex::to_sql()` (existing code in `dump.rs:163-179`) |
| `network` | `deployment_schemas` | `Site.network` |
| `tables.*` | `Layout.tables` | `Table.{immutable, has_causality_region}` |

**Notes:**
- `use_bytea_prefix` is not stored in the dump. It is hardcoded to `true` in `create_deployment` (deployment.rs:1302) and will always be set to `true` on restore.
- `health` and `indexes` are point-in-time diagnostic snapshots. They are not used during restore (a restored deployment starts healthy; indexes are auto-created by `Layout::create_relational_schema()`). They are included for inspection and debugging.
- `indexes` are serialized as SQL strings using `CreateIndex::with_nsp("sgd")` + `to_sql(true, true)`, producing `CREATE INDEX CONCURRENTLY IF NOT EXISTS` statements with a normalized `sgd` namespace.
- The `manifest` fields mirror the existing `Manifest` struct in `dump.rs` (derived from `SubgraphManifestEntity`). The `schema` and `raw_yaml` fields of `SubgraphManifestEntity` are written to separate files instead.
- The `poi2$` table, when present, is a regular mutable entity table in `Layout.tables` and appears in the `tables` map like any other entity. It does not need special handling.

The raw GraphQL schema (in `schema.graphql`) is sufficient to reconstruct the full relational layout via `InputSchema::parse(spec_version, schema, deployment_hash)` → `Layout::new()`. The `InputSchema::parse()` call requires `manifest.spec_version` for version-specific parsing logic.

## Dump Process

**Existing code:** There is already a metadata-only dump in `store/postgres/src/relational/dump.rs` (`Layout::dump()`) that writes `control.json`, `schema.graphql`, and `subgraph.yaml`. It is called via `DeploymentStore::dump()` (deployment_store.rs:901) which loads `Layout` + `IndexList` and passes both to `Layout::dump()`. The connection is `AsyncPgConnection` via `pool.get_permitted()`. The new parquet dump extends this to include entity data.

The existing `Control` struct is renamed to `Metadata` and extended with the fields described above. The existing `Manifest`, `BlockPtr`, `Health`, and `Error` structs in `dump.rs` are reused and extended.

1. Resolve the deployment (by name, hash, or sgdN)
2. Read deployment metadata from `subgraph_manifest` + `deployment` + `head` tables (via `deployment_entity()` in `detail.rs`)
3. Write `schema.graphql` and `subgraph.yaml` (existing behavior)
4. For each entity type table in `Layout.tables` (sorted by name for determinism; includes `poi2$` when present):
   a. Query rows in vid order, batched (adaptive sizing like `VidBatcher`)
   b. Convert PG rows directly to Arrow `RecordBatch` (no JSON intermediate)
   c. Write batches to parquet file
   d. Record chunk info (file path, min_vid, max_vid, row_count)
5. Dump `data_sources$` table (fixed schema, same batch approach; include all DDL columns: `vid`, `block_range`, `causality_region`, `manifest_idx`, `parent`, `id`, `param`, `context`, `done_at`). Note: `parent` and `id` are in the DDL (`private.rs:68-69`) but not in the `DataSourcesTable` struct — dumping them requires raw SQL or extending the struct.
6. Write `metadata.json` atomically (write to tmp file, rename)

### Incremental append

- Read existing `metadata.json` to get `max_vid` per entity type
- Query rows with `vid > max_vid`
- Write as new chunk files (`chunk_000001.parquet`, etc.)
- Update metadata atomically

## Restore Process

1. Read `metadata.json` and `schema.graphql`
2. Parse schema via `InputSchema::parse(manifest.spec_version, schema_text, deployment_hash)`
3. Create a `Site` entry in `deployment_schemas` (needed for the deployment to be discoverable)
4. Create deployment via `create_deployment(conn, site, DeploymentCreate { .. })` -- this populates three tables:
   - `subgraphs.head` (block pointers, entity count -- initially null/0)
   - `subgraphs.deployment` (deployment hash, earliest_block, graft info, health)
   - `subgraphs.subgraph_manifest` (schema from file, features/spec_version/etc. from metadata)
5. Create tables via `Layout::create_relational_schema()` -- this generates DDL from the parsed schema and creates all entity tables with default indexes
6. Restore `data_sources$` table via DDL from `DataSourcesTable::new().as_ddl()` + batch-insert
7. For each entity type (including `poi2$` if present), read all parquet chunks in order, batch-insert into PG
8. Reset vid sequences to `max_vid + 1` for all entity tables and data_sources$
9. Update `subgraphs.head` with `head_block.number`, `head_block.hash`, and `entity_count` from dump metadata

## PG Read Strategy: OidValue-based Dynamic Columns

Use the existing `dsl::Table::select_cols()` + `DynamicRow<OidValue>` pattern (see `store/postgres/src/relational/dsl.rs` and `store/postgres/src/relational/value.rs`). This already solves dynamic-schema typed extraction through the connection pool:

1. `select_cols()` builds a typed SELECT using `DynamicSelectClause` for any set of columns
2. Results are `DynamicRow<OidValue>` where `OidValue` dispatches on PG OID at runtime
3. `OidValue` captures all needed types: String, Bytes, Bool, Int, Int8, BigDecimal, Timestamp, plus array variants
4. Convert `OidValue` -> Arrow `ArrayBuilder` (analogous to existing `OidValue` -> `Entity` in `FromOidRow`)

No JSON, no separate connection. The existing `DeploymentStore::dump()` already uses `AsyncPgConnection` via `pool.get_permitted()`.

**Block range handling:** Add `OidValue::Int4Range(Bound<i32>, Bound<i32>)` variant (OID 3904). Diesel already has `FromSql<Range<Integer>, Pg>` for `(Bound<i32>, Bound<i32>)` which parses the binary format. ~15 lines of code in `value.rs` + fix the `BLOCK_RANGE_COL` placeholder in `dsl.rs:46-49` (currently `ColumnType::Bytes`, with comment "we can't deserialize in4range"). This resolves the existing TODO at dsl.rs line 294.

**Key existing code:**
- `dsl::Table::select_cols()` (`store/postgres/src/relational/dsl.rs:305`)
- `OidValue` enum and `FromSql<Any, Pg>` impl (`store/postgres/src/relational/value.rs:33`)
- `FromOidRow` trait for result deserialization (`value.rs:206`)
- `selected_columns()` for building column list with system columns (`dsl.rs:246`)

### Vid continuity on restore

Preserve original vid values. Needed for incremental consistency and simpler to implement. Reset vid sequence to `max_vid + 1` after restore.

## Where Code Lives

- `store/postgres/src/relational/dump.rs` -- **Existing** metadata-only dump. Contains `Manifest`, `BlockPtr`, `Error`, `Health`, `Control` structs. `Control` will be renamed to `Metadata` and extended. The existing helper structs (`Manifest`, `BlockPtr`, `Health`, `Error`) are reused. Called via `DeploymentStore::dump()` → `Layout::dump()`.
- `store/postgres/src/parquet/` -- New module for parquet read/write/schema mapping
- `node/src/manager/commands/dump.rs` -- **Existing** CLI command skeleton (resolves deployment, calls `SubgraphStore::dump()`)
- `node/src/manager/commands/restore.rs` -- New CLI command for restore
- Expose via `command_support` in `store/postgres/src/lib.rs`

## Dependencies to Add

- `parquet = "=57.3.0"` (same version as existing `arrow`) to workspace and `store/postgres`
- `arrow` workspace dep to `store/postgres`

## Existing Code to Reuse

- `Layout`, `Table`, `Column`, `ColumnType` (`store/postgres/src/relational.rs`) -- schema introspection; `poi2$` is in Layout as a mutable entity table (conditionally, when `catalog.use_poi` is true)
- `Column.is_list()` (`relational.rs:1574`) -- determines if a column is an array type (delegates to GraphQL `field_type.is_list()`)
- `DataSourcesTable` (`store/postgres/src/dynds/private.rs`) -- `data_sources$` DDL via `as_ddl()` method; note that `parent` and `id` columns are in the DDL but not in the struct's typed fields
- `VidBatcher` (`store/postgres/src/vid_batcher.rs`) -- adaptive batch iteration using PG histogram statistics
- `copy.rs` pattern -- progress reporting, batch operation lifecycle; already handles both entity tables and `data_sources$` copying
- `InputSchema::parse(spec_version, raw, id)` (`graph/src/schema/input/mod.rs:965`) -- schema reconstruction from text
- `DeploymentSearch` (`node/src/manager/deployment.rs`) -- CLI deployment resolution (supports name, Qm hash, sgdN namespace)
- `create_deployment` (`store/postgres/src/deployment.rs:1224`) -- populates `head`, `deployment`, and `subgraph_manifest` tables
- `DeploymentCreate` + `SubgraphManifestEntity` (`graph/src/data/subgraph/schema.rs:103`) -- structs needed by `create_deployment`
- `deployment_entity()` (`store/postgres/src/detail.rs`) -- reads deployment metadata into `SubgraphDeploymentEntity`; note that `start_block_*` is in the DB table (`StoredSubgraphManifest`) but only partially exposed through `SubgraphDeploymentEntity`
- `IndexList::load()` + `CreateIndex::to_sql()` (`store/postgres/src/relational/index.rs`) -- loads and serializes indexes
- Existing `dump.rs` (`store/postgres/src/relational/dump.rs`) -- metadata serialization types: `Manifest`, `BlockPtr`, `Health`, `Error`, `Control` (to be renamed `Metadata`)

## Implementation Order

1. Schema mapping + metadata types (foundation, unit-testable)
2. Parquet writer (dump from PG) + graphman `dump` command
3. Incremental append support
4. Parquet reader (restore to PG) + graphman `restore` command
5. Ongoing dump integration (run as part of graph-node, not just CLI)
