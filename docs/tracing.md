# OpenTelemetry tracing

graph-node can export traces of block processing, database writes and
GraphQL requests to any OpenTelemetry collector or vendor agent that accepts
OTLP. Tracing is off by default. When it is off, no exporter is installed
and the instrumented code costs one atomic load at the start and one at the
end of each section.

Traces complement the [Prometheus metrics](./metrics.md), which say how
long things take on average, by showing where the time went in one specific
block or request.

## Enabling tracing

Set `GRAPH_OTEL_TRACING=true` and point the exporter at a collector:

```sh
export GRAPH_OTEL_TRACING=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=graph-node
```

graph-node logs one line at startup when tracing is on, `OpenTelemetry
tracing enabled`, naming the endpoint, the service name and the enabled span
groups.

The exporter speaks OTLP over `http/protobuf`, which the OpenTelemetry
Collector and most vendor agents accept, some after enabling an OTLP HTTP
receiver. gRPC is not supported. Everything about
the exporter is configured with the standard `OTEL_*` variables, listed in
[environment-variables.md](./environment-variables.md#opentelemetry-tracing).
graph-node adds only two variables of its own: `GRAPH_OTEL_TRACING` and
`GRAPH_OTEL_SPANS`.

### A local collector

The quickest way to see traces is the OpenTelemetry Collector with its
debug exporter, which prints every span it receives:

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
```

```sh
docker run --rm -p 4318:4318 \
  -v "$PWD/otel-collector.yaml:/etc/otelcol/config.yaml" \
  otel/opentelemetry-collector:latest --config /etc/otelcol/config.yaml
```

To keep the traces, add an `otlp` or `otlphttp` exporter that forwards them
to a backend such as Jaeger or Tempo, both of which accept OTLP. Vendor
exporters usually need the `otel/opentelemetry-collector-contrib` image and
their own configuration.

### Datadog

The Datadog Agent accepts OTLP directly. Enable its HTTP receiver with
`DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318` and point
`OTEL_EXPORTER_OTLP_ENDPOINT` at the agent. The `service.name` resource
attribute becomes the Datadog service, so set `OTEL_SERVICE_NAME` to
whatever the service should be called there. graph-node does not read
`DD_SERVICE`, `DD_ENV` or `DD_VERSION`; the equivalent resource attributes
go in `OTEL_RESOURCE_ATTRIBUTES`, for example
`deployment.environment.name=prod` for the `env` tag. The agent's own
configuration can still add tags on its side.

## What is traced

### Span groups

Spans belong to groups that are turned on and off together with
`GRAPH_OTEL_SPANS`. The default is `block,handler,store,graphql`.

| Group | Spans | Volume |
| --- | --- | --- |
| `block` | One trace per processed block, with the runner's sections under it. | One root span and a handful of children per block per subgraph. |
| `handler` | One span per trigger matched and per handler run. | One or two spans per trigger. |
| `host_fn` | One span per host function call made by a handler, such as `store.set` or `ethereum.call`. | Can be thousands per handler. Off by default. |
| `store` | One trace per batch written by the write queue, plus one span per SQL query that serves a GraphQL request. | One root and a few children per batch; one span per entity query. |
| `graphql` | One trace per GraphQL request. | One root span per request. |

`block`, `store` and `graphql` own the roots of traces. Turning one of them
off removes those traces entirely, including spans of other groups that
would have nested in them: `handler` and `host_fn` spans need `block`, and
`db_query` spans need `graphql`. Turning `store` off removes the
`write_batch` traces and the `db_query` spans, but not the store sections
under `transact_block`, which belong to the block's trace.

`handler` and `host_fn` only add spans inside a trace. Turning one off skips
its spans, and anything nested in them attaches to the nearest span that
is left. With `handler` off and `host_fn` on, for example, host function
spans sit directly under `process_block`.

### The span tree

```
process_block {subgraph_id, block_number, block_hash, network, shard, stage=process}
├── process_triggers
│   ├── match_and_decode                       (one per trigger, handler)
│   └── run_handler {handler, trigger, tx_hash?, log_index?}       (handler)
│       ├── host_export_ethereum_call {contract, function, address}   (host_fn)
│       ├── host_export_store_set {entity_type}                       (host_fn)
│       └── host_export_store_get, host_export_ipfs_cat, ...          (host_fn)
├── as_modifications
├── transact_block {entity_count}
└── update_proof_of_indexing

write_batch {subgraph_id, first_block, last_block, entity_count, dropped_links, shard, stage=writer}
│   links -> the process_block spans whose changes it contains
├── transact_blocks_get_conn
├── apply_entity_modifications
│   ├── apply_entity_modifications_delete
│   ├── check_interface_entity_uniqueness
│   └── apply_entity_modifications_insert
└── transact_blocks_prune

graphql_query {subgraph, api_version, query_id?, deployment?, error_count?}
│   parent <- the traceparent header of the request, if any
└── db_query {subgraph_id, entity_count, sql?}             (one per SQL query)
```

The span names are the section names of the existing stopwatch metrics, so
a `deployment_sync_secs{section="run_handler"}` time series and a
`run_handler` span measure the same thing. Host function spans are named
`host_export_` followed by the import name with dots replaced by
underscores.

Attributes worth knowing:

- `tx_hash` is set on `run_handler` for event and call handlers, and
  `log_index` for event handlers, which is where a trace of a block connects
  to a transaction. Block handlers have neither. `trigger` is `onchain`,
  `offchain` or `subgraph`.
- `shard` is the database shard the deployment lives in and `stage` is
  `process` for the runner and `writer` for the write queue.
- `sql` on `db_query` is the query with its bind values, cut at 20 KiB. It is
  only set when `GRAPH_LOG_QUERY_TIMING` includes `sql`, the same gate as
  the query timing logs, because it can be large and can contain query
  variables.
- `query_id` on `graphql_query` is the id that the query timing logs carry,
  and the `route` in the comment that prefixes every SQL query, so a span, a
  log line and a statement in `pg_stat_activity` can be matched up. It is
  missing when the request did not parse. `deployment` is set once the
  result names one, and `error_count` only when there were errors.

A span whose work failed carries an error status with the error message,
cut at 4 KiB. A failed block has the error on `process_block`; a GraphQL
request whose result has errors has them joined on `graphql_query`, with
`error_count` saying how many there were.

### Writes are their own traces

The write queue batches the changes of several blocks and writes them after
those blocks were processed, so a write does not belong to any one block.
Each batch is a trace of its own rooted at `write_batch`, and that span
carries a [span link](https://opentelemetry.io/docs/concepts/signals/traces/#span-links)
to the `process_block` span of every block in the batch that has one, so
blocks whose trace was not sampled, or `block` being off, leave gaps. Tracing backends
show links next to the span; follow one to get from a slow write back to
the blocks that caused it. Going the other way, from a block to the batch
that wrote it, depends on whether the backend indexes links.

A batch keeps at most 32 links. When more blocks than that end up in one
batch, `dropped_links` says how many were left out.

With the write queue disabled (`GRAPH_STORE_WRITE_QUEUE=0`), writes happen
inside `transact_block` and the store sections nest under the block's
trace instead.

### GraphQL requests

A request's trace starts at the HTTP server. If the request carries a W3C
[`traceparent`](https://www.w3.org/TR/trace-context/) header, the
`graphql_query` span joins that trace, so a gateway or client that traces
its own requests sees graph-node's work inside them. Without the header,
the request starts a new trace.

Every entity query run for the request becomes a `db_query` child. Other
SQL the request needs, such as looking up the deployment, is not traced.
Results served from the query cache run no queries and have no children.

This is separate from the `GRAPH_GRAPHQL_TRACE_TOKEN` feature, which
returns timing information in the response body of a single request.

## Sampling

Sampling is decided once per trace, at its root span, by the sampler
configured with `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`. The
default, `parentbased_always_on`, exports everything. On a node that
indexes many subgraphs or serves many queries that is a lot of spans, so
either sample a fraction:

```sh
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.01
```

or sample in the collector with the
[tail sampling processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor),
which can keep only slow or failed traces. Tail sampling needs all the
spans first, so budget for the export volume even if little of it is kept.

A `parentbased_*` sampler follows the decision in the `traceparent` header
of a GraphQL request, so a client that samples its own requests decides
whether graph-node's part is kept too.

An unsampled trace records and exports nothing: once the sampler has said
no at the root, no spans are created under it. What remains is the cost of
building that root and asking the sampler, once per trace.

## Shutdown

Spans are buffered and exported in batches by a background thread. When
graph-node receives SIGINT or SIGTERM, it flushes the buffer before
exiting. The flush is best effort: with the collector unreachable it gives
up after the exporter timeout, once per batch left in the buffer. On
SIGKILL, or if the process panics, the spans of the last few seconds are
lost.

## Overhead

With tracing off, each instrumented section does one atomic load at its
start and one at its end more than before, and nothing else.

With tracing on, every sampled span allocates its attributes and is handed
to the batch processor. The `block`, `store` and `graphql` groups add a few
spans per block or request and are cheap. `handler` adds one or two spans
per trigger. `host_fn` is the expensive one: a handler that reads and writes
thousands of entities makes thousands of spans, which is why it is off by
default. Turn it on for a specific investigation, together with a low
sampling ratio.
