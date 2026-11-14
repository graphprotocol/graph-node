# Log Store Configuration and Usage

This guide explains how to configure subgraph indexing logs storage in graph-node.

## Table of Contents

- [Overview](#overview)
- [How Log Stores Work](#how-log-stores-work)
- [Log Store Types](#log-store-types)
  - [File-based Logs](#file-based-logs)
  - [Elasticsearch](#elasticsearch)
  - [Loki](#loki)
  - [Disabled](#disabled)
- [Configuration](#configuration)
  - [`[log_store]` TOML](#log_store-toml)
  - [File backend](#file-backend-toml)
  - [Elasticsearch backend](#elasticsearch-backend-toml)
  - [Loki backend](#loki-backend-toml)
  - [Disabled](#disabled-toml)
- [Querying Logs](#querying-logs)
- [Migrating from Deprecated Configuration](#migrating-from-deprecated-configuration)
- [Choosing the Right Backend](#choosing-the-right-backend)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Graph Node supports multiple logs storage backends for subgraph indexing logs. Subgraph indexing logs include:
- **User-generated logs**: Explicit logging from subgraph mapping code (`log.info()`, `log.error()`, etc.)
- **Runtime logs**: Handler execution, event processing, data source activity
- **System logs**: Warnings, errors, and diagnostics from the indexing system

**Available backends:**
- **File**: JSON Lines files on local filesystem (for local development)
- **Elasticsearch**: Enterprise-grade search and analytics (for production)
- **Loki**: Grafana's lightweight log aggregation system (for production)
- **Disabled**: No log storage (default)

All backends share the same query interface through GraphQL, making it easy to switch between them.

**Important Note:** When log storage is disabled (the default), subgraph logs still appear in stdout/stderr as they always have. The "disabled" setting simply means logs are not stored separately in a queryable format. You can still see logs in your terminal or container logs - they just won't be available via the `_logs` GraphQL query.

## How Log Stores Work

### Architecture

```
┌─────────────────┐
│  Subgraph Code  │
│   (mappings)    │
└────────┬────────┘
         │ log.info(), log.error(), etc.
         ▼
┌─────────────────┐
│  Graph Runtime  │
│   (WebAssembly) │
└────────┬────────┘
         │ Log events
         ▼
┌─────────────────┐
│   Log Drain     │  ◄─── slog-based logging system
└────────┬────────┘
         │ Write
         ▼
┌─────────────────┐
│   Log Store     │  ◄─── Configurable backend
│  (ES/Loki/File) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  GraphQL API    │  ◄─── Unified query interface
│  (port 8000)    │
└─────────────────┘
```

### Log Flow

1. **Log sources** generate logs from:
   - User mapping code (explicit `log.info()`, `log.error()`, etc. calls)
   - Subgraph runtime (handler execution, event processing, data source triggers)
   - System warnings and errors (indexing issues, constraint violations, etc.)
2. **Graph runtime** captures these logs with metadata (timestamp, level, source location)
3. **Log drain** formats logs and writes to configured backend
4. **Log store** persists logs and handles queries
5. **GraphQL API** exposes logs through the `_logs` query

### Log Entry Structure

Each log entry contains:
- **`id`**: Unique identifier
- **`subgraphId`**: Deployment hash (QmXxx...)
- **`timestamp`**: ISO 8601 timestamp (e.g., `2024-01-15T10:30:00.123456789Z`)
- **`level`**: CRITICAL, ERROR, WARNING, INFO, or DEBUG
- **`text`**: Log message
- **`arguments`**: Key-value pairs from structured logging
- **`meta`**: Source location (module, line, column)

## Log Store Types

### File-based Logs

**Best for:** Local development, testing

#### How It Works

File-based logs store each subgraph's logs in a separate JSON Lines (`.jsonl`) file:

```
graph-logs/
├── QmSubgraph1Hash.jsonl
├── QmSubgraph2Hash.jsonl
└── QmSubgraph3Hash.jsonl
```

Each line in the file is a complete JSON object representing one log entry.

#### Storage Format

```json
{"id":"QmTest-2024-01-15T10:30:00.123456789Z","subgraphId":"QmTest","timestamp":"2024-01-15T10:30:00.123456789Z","level":"error","text":"Handler execution failed, retries: 3","arguments":[{"key":"retries","value":"3"}],"meta":{"module":"mapping.ts","line":42,"column":10}}
```

#### Query Performance

File-based logs stream through files line-by-line with bounded memory usage.

**Performance characteristics:**
- Query time: O(n) where n = number of log entries
- Memory usage: O(skip + first) - only matching entries kept in memory
- Suitable for: Development and testing

#### Configuration

Add a `[log_store]` section to the TOML config passed with `--config`
(there are no `--log-store-*` CLI flags or `GRAPH_LOG_STORE_*` env vars):

```toml
[log_store]
backend = "file"
directory = "./graph-logs"
# retention_hours = 72   # optional; 0 (default) keeps forever
```

```bash
graph-node --config ./graph-node.toml
```

#### Features

**Advantages:**
- No external dependencies
- Simple setup (just specify a directory)
- Human-readable format (JSON Lines)
- Easy to inspect with standard tools (`jq`, `grep`, etc.)
- Good for debugging during development

**Limitations:**
- Not suitable for production with high log volume
- No indexing (O(n) query time scales with file size)
- Retention is time-based only (`retention_hours`); there is no max-size rotation
- Single file per subgraph (no sharding)

#### When to Use

Use file-based logs when:
- Developing subgraphs locally
- Testing on a development machine
- Running low-traffic subgraphs (< 1000 total logs/day including system logs)
- You want simple log access without external services

### Elasticsearch

**Best for:** Production deployments, high log volume, advanced search

#### How It Works

Elasticsearch stores logs in indices with full-text search capabilities, making it ideal for production deployments with high log volume.

**Architecture:**
```
graph-node → Elasticsearch HTTP API → Elasticsearch cluster
                                    → Index: subgraph-logs-*
                                    → Query DSL for filtering
```

#### Features

**Advantages:**
- **Indexed searching**: Fast queries even with millions of logs
- **Full-text search**: Powerful text search across log messages
- **Scalability**: Handles billions of log entries
- **High availability**: Supports clustering and replication
- **Kibana integration**: Rich visualization and dashboards for operators
- **Time-based indices**: Efficient retention management

**Considerations:**
- Requires Elasticsearch cluster (infrastructure overhead)
- Resource-intensive (CPU, memory, disk)

#### Configuration

```toml
[log_store]
backend = "elasticsearch"
url = "http://localhost:9200"
# username = "elastic"          # optional
# password = "secret"           # optional
# index = "subgraph"            # default: "subgraph"
# timeout_secs = 10             # default: 10
```

```bash
graph-node --config ./graph-node.toml
```

#### Index Configuration

Logs are stored in the configured index (default: `subgraph`). The index mapping is automatically created.

**Recommended index settings for production:**
```json
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "refresh_interval": "5s"
  }
}
```

#### Query Performance

**Performance characteristics:**
- Query time: O(log n) with indexing
- Memory usage: Minimal (server-side filtering)
- Suitable for: Millions to billions of log entries

#### When to Use

Use Elasticsearch when:
- Running production deployments
- High log volume
- Need advanced search and filtering
- Want to build dashboards with Kibana
- Need high availability and scalability
- Have DevOps resources to manage Elasticsearch or can set up a managed ElasticSearch deployment

### Loki

**Best for:** Production deployments, Grafana users, cost-effective at scale

#### How It Works

Loki is Grafana's log aggregation system, designed to be cost-effective and easy to operate. Unlike Elasticsearch, Loki only indexes metadata (not full-text), making it more efficient for time-series log data.

**Architecture:**
```
graph-node → Loki HTTP API → Loki
                           → Stores compressed chunks
                           → Indexes labels only
```

#### Features

**Advantages:**
- **Cost-effective**: Lower storage costs than Elasticsearch
- **Grafana integration**: Native integration with Grafana
- **Horizontal scalability**: Designed for cloud-native deployments
- **Multi-tenancy**: Built-in tenant isolation
- **Efficient compression**: Optimized for log data
- **LogQL**: Powerful query language similar to PromQL
- **Lower resource usage**: Less CPU/memory than Elasticsearch

**Considerations:**
- No full-text indexing (slower text searches)
- Best used with Grafana (less tooling than Elasticsearch)
- Younger ecosystem than Elasticsearch
- Query performance depends on label cardinality

#### Configuration

```toml
[log_store]
backend = "loki"
url = "http://localhost:3100"
# tenant_id = "my-graph-node"   # optional
# username = "..."              # optional
# password = "..."              # optional
```

```bash
graph-node --config ./graph-node.toml
```

#### Labels

Loki uses labels for indexing. Graph Node automatically creates labels:
- `subgraph_id`: Deployment hash
- `level`: Log level
- `job`: "graph-node"

#### Query Performance

**Performance characteristics:**
- Query time: O(n) for text searches, O(log n) for label queries
- Memory usage: Minimal (server-side processing)
- Suitable for: Millions to billions of log entries
- Best performance with label-based filtering

#### When to Use

Use Loki when:
- Already using Grafana for monitoring
- Need cost-effective log storage at scale
- Want simpler operations than Elasticsearch
- Multi-tenancy is required
- Log volume is very high (> 1M logs/day)
- Full-text search is not critical

### Disabled

**Best for:** Minimalist deployments, reduced overhead

#### How It Works

When log storage is disabled (the default), subgraph logs are **still written to stdout/stderr** along with all other graph-node logs. They are just **not stored separately** in a queryable format.

**Important:** "Disabled" does NOT mean logs are discarded. It means:
- Logs appear in stdout/stderr (traditional behavior)
- Logs are not stored in a separate queryable backend
- The `_logs` GraphQL query returns empty results

This is the default behavior - logs continue to work exactly as they did before this feature was added.

#### Configuration

**Explicitly disable:**

```toml
[log_store]
backend = "disabled"
```

**Or simply omit `[log_store]`** (defaults to disabled). Logs still appear on
stdout/stderr; only the queryable store is off.

#### Features

**Advantages:**
- Zero additional overhead
- No external dependencies
- Minimal configuration
- Logs still appear in stdout/stderr for debugging

**Limitations:**
- Cannot query logs via GraphQL (`_logs` returns empty results)
- No separation of subgraph logs from other graph-node logs in stdout
- Logs mixed with system logs (harder to filter programmatically)
- No structured querying or filtering capabilities

#### When to Use

Use disabled log storage when:
- Running minimal test deployments with less dependencies
- Exposing logs to users is not required for your use case
- You'd like subgraph logs sent to external log collection (e.g., container logs)

## Configuration

Log storage is configured **only** via a `[log_store]` section in the TOML
configuration file passed with `--config` (or the `GRAPH_NODE_CONFIG`
environment variable). There are no `--log-store-*` CLI flags and no
`GRAPH_LOG_STORE_*` environment variables.

See also [`docs/config.md`](config.md) for the rest of the TOML schema, and
`node/src/config.rs` (`LogStoreSection`) for the authoritative field list.

### `[log_store]` TOML

| Field | Backends | Description |
|-------|----------|-------------|
| `backend` | all | `file`, `elasticsearch` (aliases: `elastic`, `es`), `loki`, or `disabled` / `none` |
| `directory` | file | Directory for per-subgraph `.jsonl` files (**required** for `file`) |
| `retention_hours` | file | Hours to retain file logs; `0` (default) keeps forever |
| `url` | elasticsearch, loki | Backend endpoint (**required**) |
| `username` / `password` | elasticsearch, loki | Optional HTTP basic auth |
| `index` | elasticsearch | Index name (default: `subgraph`) |
| `timeout_secs` | elasticsearch | HTTP timeout in seconds (default: `10`) |
| `tenant_id` | loki | Optional Loki tenant id |

### File backend TOML

```toml
[log_store]
backend = "file"
directory = "/var/log/graph-node/subgraph-logs"
retention_hours = 72
```

### Elasticsearch backend TOML

```toml
[log_store]
backend = "elasticsearch"
url = "https://es.example.com:9200"
username = "elastic"
password = "secret"
index = "subgraph-logs"
timeout_secs = 10
```

### Loki backend TOML

```toml
[log_store]
backend = "loki"
url = "http://localhost:3100"
tenant_id = "my-graph-node"
```

### Disabled TOML

Omit `[log_store]`, or set:

```toml
[log_store]
backend = "disabled"
```

## Migrating from Deprecated Configuration

In v0.44.0 ([#6278](https://github.com/graphprotocol/graph-node/pull/6278)),
the `--elasticsearch-url` / `--elasticsearch-user` / `--elasticsearch-password`
CLI flags and the matching `ELASTICSEARCH_*` / `GRAPH_ELASTICSEARCH_*`
environment variables were removed. Configure Elasticsearch (or File / Loki)
under `[log_store]` instead:

```toml
# Before (removed): --elasticsearch-url http://localhost:9200
# After:
[log_store]
backend = "elasticsearch"
url = "http://localhost:9200"
```

Do not use the fictional `GRAPH_LOG_STORE_*` env vars or `--log-store-*` CLI
flags that appeared in earlier drafts of this guide — they were never
implemented. File retention is `retention_hours` (not `retention_days`), and
there is no `max_size` setting.

## Querying Logs

All log backends share the same GraphQL query interface. Logs are queried through the subgraph-specific GraphQL endpoint:

- **Subgraph by deployment**: `http://localhost:8000/subgraphs/id/<deployment-hash>`
- **Subgraph by name**: `http://localhost:8000/subgraphs/name/<subgraph-name>`

The `_logs` query is automatically scoped to the subgraph in the URL, so you don't need to pass a `subgraphId` parameter.

**Note**: Queries return all log types - both user-generated logs from mapping code and system-generated runtime logs (handler execution, events, warnings, etc.). Use the `search` filter to search for specific messages, or `level` to filter by severity.

### Basic Query

Query the `_logs` field at your subgraph's GraphQL endpoint:

```graphql
query {
  _logs(
    first: 100
  ) {
    id
    timestamp
    level
    text
  }
}
```

**Example endpoint**: `http://localhost:8000/subgraphs/id/QmYourDeploymentHash`

### Query with Filters

```graphql
query {
  _logs(
    level: ERROR
    from: "2024-01-01T00:00:00Z"
    to: "2024-01-31T23:59:59Z"
    search: "timeout"
    first: 50
    skip: 0
  ) {
    id
    timestamp
    level
    text
    arguments {
      key
      value
    }
    meta {
      module
      line
      column
    }
  }
}
```

### Available Filters

| Filter | Type | Description |
|--------|------|-------------|
| `level` | LogLevel | Filter by level: CRITICAL, ERROR, WARNING, INFO, DEBUG |
| `from` | String | Start timestamp (ISO 8601) |
| `to` | String | End timestamp (ISO 8601) |
| `search` | String | Case-insensitive substring search in log messages |
| `first` | Int | Number of results to return (default: 100, max: 1000) |
| `skip` | Int | Number of results to skip for pagination (max: 10000) |

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Unique log entry ID |
| `timestamp` | String | ISO 8601 timestamp with nanosecond precision |
| `level` | LogLevel | Log level (CRITICAL, ERROR, WARNING, INFO, DEBUG) |
| `text` | String | Complete log message with arguments |
| `arguments` | [(String, String)] | Structured key-value pairs |
| `meta.module` | String | Source file name |
| `meta.line` | Int | Line number |
| `meta.column` | Int | Column number |

### Query Examples

#### Recent Errors

```graphql
query RecentErrors {
  _logs(
    level: ERROR
    first: 20
  ) {
    timestamp
    text
    meta {
      module
      line
    }
  }
}
```

#### Search for Specific Text

```graphql
query SearchTimeout {
  _logs(
    search: "timeout"
    first: 50
  ) {
    timestamp
    level
    text
  }
}
```

#### Handler Execution Logs

```graphql
query HandlerLogs {
  _logs(
    search: "handler"
    first: 50
  ) {
    timestamp
    level
    text
  }
}
```

#### Time Range Query

```graphql
query LogsInRange {
  _logs(
    from: "2024-01-15T00:00:00Z"
    to: "2024-01-15T23:59:59Z"
    first: 1000
  ) {
    timestamp
    level
    text
  }
}
```

#### Pagination

```graphql
# First page
query Page1 {
  _logs(
    first: 100
    skip: 0
  ) {
    id
    text
  }
}

# Second page
query Page2 {
  _logs(
    first: 100
    skip: 100
  ) {
    id
    text
  }
}
```

### Querying the logs store using cURL

```bash
curl -X POST http://localhost:8000/subgraphs/id/<deployment-hash> \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ _logs(level: ERROR, first: 10) { timestamp level text } }"
  }'
```

### Performance Considerations

**File-based:** _for development only_
- Streams through files line-by-line (bounded memory usage)
- Memory usage limited to O(skip + first) entries
- Query time is O(n) where n = total log entries in file

**Elasticsearch:**
- Indexed queries are fast regardless of size
- Text searches are optimized with full-text indexing
- Can handle billions of log entries
- Best for production with high query volume

**Loki:**
- Label-based queries are fast (indexed)
- Text searches scan compressed chunks (slower than Elasticsearch)
- Good performance with proper label filtering
- Best for production with Grafana integration

## Choosing the Right Backend

### Decision Matrix

| Scenario | Recommended Backend | Reason                                                                            |
|----------|-------------------|-----------------------------------------------------------------------------------|
| Local development | **File** | Simple, no dependencies, easy to inspect                                          |
| Testing/staging | **File** or **Elasticsearch** | File for simplicity, ES if testing production config                              |
| Production | **Elasticsearch** or **Loki** | Both handle scale well                                                            |
| Using Grafana | **Loki** | Native integration                                                                |
| Cost-sensitive at scale | **Loki** | Lower storage costs                                                               |
| Want rich ecosystem | **Elasticsearch** | More tools and plugins                                                            |
| Minimal deployment | **Disabled** | No overhead                                                                       |

### Resource Requirements

#### File-based
- **Disk**: Minimal (log files only)
- **Memory**: Depends on file size during queries
- **CPU**: Minimal
- **Network**: None
- **External services**: None

#### Elasticsearch
- **Disk**: High (indices + replicas)
- **Memory**: 4-8GB minimum for small deployments
- **CPU**: Medium to high
- **Network**: HTTP API calls
- **External services**: Elasticsearch cluster

#### Loki
- **Disk**: Medium (compressed chunks)
- **Memory**: 2-4GB minimum
- **CPU**: Low to medium
- **Network**: HTTP API calls
- **External services**: Loki server

## Best Practices

### General

1. **Start with file-based for development** - Simplest setup, easy debugging
2. **Use Elasticsearch or Loki for production** - Better performance and features
3. **Monitor log volume** - Set up alerts if log volume grows unexpectedly (includes both user logs and system-generated runtime logs)
4. **Set retention policies** - Don't keep logs forever (disk space and cost)
5. **Use structured logging** - Pass key-value pairs to log functions for better filtering

### File-based Logs

1. **Monitor file size** - While queries use bounded memory, larger files take longer to scan (O(n) query time)
2. **Archive old logs** - Manually archive/delete old files or implement external rotation
3. **Monitor disk usage** - Files can grow quickly with verbose logging
4. **Use JSON tools** - `jq` is excellent for inspecting .jsonl files locally

**Example local inspection:**
```bash
# Count logs by level
cat graph-logs/QmExample.jsonl | jq -r '.level' | sort | uniq -c

# Find errors in last 1000 lines
tail -n 1000 graph-logs/QmExample.jsonl | jq 'select(.level == "error")'

# Search for specific text
cat graph-logs/QmExample.jsonl | jq 'select(.text | contains("timeout"))'
```

### Elasticsearch

1. **Use index patterns** - Time-based indices for easier management
2. **Configure retention** - Use Index Lifecycle Management (ILM)
3. **Monitor cluster health** - Set up Elasticsearch monitoring
4. **Tune for your workload** - Adjust shards/replicas based on log volume
5. **Use Kibana** - Visualize and explore logs effectively

**Example Elasticsearch retention policy:**
```json
{
  "policy": "graph-logs-policy",
  "phases": {
    "hot": { "min_age": "0ms", "actions": {} },
    "warm": { "min_age": "7d", "actions": {} },
    "delete": { "min_age": "30d", "actions": { "delete": {} } }
  }
}
```

### Loki

1. **Use proper labels** - Don't over-index, keep label cardinality low
2. **Configure retention** - Set retention period in Loki config
3. **Use Grafana** - Native integration provides best experience
4. **Compress efficiently** - Loki's compression works best with batch writes
5. **Multi-tenancy** - Use tenant IDs if running multiple environments

**Example Grafana query:**
```logql
{subgraph_id="QmExample", level="error"} |= "timeout"
```

## Troubleshooting

### File-based Logs

**Problem: Log file doesn't exist**
- Check `[log_store].directory` in your TOML config is set correctly
- Verify directory is writable by graph-node

**Problem: Queries are slow**
- Subgraph logs file may be very large
- Consider archiving old logs or implementing retention
- For high-volume production use, switch to Elasticsearch or Loki

**Problem: Disk filling up**
- Implement log rotation
- Reduce log verbosity in subgraph code
- Set up monitoring for disk usage

### Elasticsearch

**Problem: Cannot connect to Elasticsearch**
- Verify `[log_store].url` (and username/password) in your TOML config
- Check Elasticsearch is running: `curl http://localhost:9200`
- Verify authentication credentials if using security features
- Check network connectivity and firewall rules

**Problem: No logs appearing in Elasticsearch**
- Check Elasticsearch cluster health
- Verify index exists: `curl http://localhost:9200/_cat/indices`
- Check graph-node logs for write errors
- Verify Elasticsearch has disk space

**Problem: Queries are slow**
- Check Elasticsearch cluster health and resources
- Verify indices are not over-sharded
- Consider adding replicas for query performance
- Review query patterns and add appropriate indices

### Loki

**Problem: Cannot connect to Loki**
- Verify `[log_store].url` (and `tenant_id`) in your TOML config
- Check Loki is running: `curl http://localhost:3100/ready`
- Verify tenant ID if using multi-tenancy
- Check network connectivity

**Problem: No logs appearing in Loki**
- Check Loki service health
- Verify Loki has disk space for chunks
- Check graph-node logs for write errors
- Verify Loki retention settings aren't deleting logs immediately

**Problem: Queries return no results in Grafana**
- Check label selectors match what graph-node is sending
- Verify time range includes when logs were written
- Check Loki retention period
- Verify tenant ID matches if using multi-tenancy

## Further Reading

- [Environment Variables Reference](environment-variables.md) (log store is TOML-only; see above)
- [Graph Node Configuration](config.md)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Grafana Loki Documentation](https://grafana.com/docs/loki/latest/)