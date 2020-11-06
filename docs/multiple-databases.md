# Support for Multiple Databases

For most use cases, a single Postgres database is sufficient to support a
`graph-node` instance. When a `graph-node` instance outgrows a single
Postgres database, it is possible to split the storage of `graph-node`'s
data across multiple Postgres databases.

Support for multiple databases is configured through a TOML configuration
file. The location of the file is passed with the `--config` command line
switch. When using a configuration file, it is not possible to use the
options `--postgres-url`, `--postgres-secondary-hosts`, and
`--postgres-host-weights`.

The TOML file consists of two sections:
* `[store]` describes the available databases
* `[deployment]` describes how to place newly deployed subgraphs

## Configuring Multiple Databases

The `[store]` section must always have a primary store configured, which
must be called `primary`. Each store can have additional read replicas that
are used for responding to queries. Only queries are processed by read
replicas. Indexing and block ingestion will always use the main database.

Any number of additional stores, with their own read replicas, can also be
configured. When read replicas are used, query traffic is split between the
main database and the replicas according to their weights. In the example
below, for the primary store, no queries will be sent to the main database,
and the replicas will receive 50% of the traffic each. In the `vip` store,
50% of the traffic go to the main database, and 50% to the replica.

```toml
[store]
[store.primary]
connection = "postgresql://graph:${PGPASSWORD}@primary/graph"
weight = 0
[store.primary.replicas.repl1]
connection = "postgresql://graph:${PGPASSWORD}@primary-repl1/graph"
weight = 1
[store.primary.replicas.repl2]
connection = "postgresql://graph:${PGPASSWORD}@primary-repl2/graph"
weight = 1

[store.vip]
connection = "postgresql://graph:${PGPASSWORD}@${VIP_MAIN}/graph"
weight = 1
[store.vip.replicas.repl1]
connection = "postgresql://graph:${PGPASSWORD}@${VIP_REPL1}/graph"
weight = 1
```

The `connection` string must be a valid [libpq connection
string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). Before
passing the connection string to Postgres, environment variables embedded
in the string are expanded.

## Controlling Deployment

When `graph-node` receives a request to deploy a new subgraph deployment,
it needs to decide in which store to store the data for the deployment, and
which of any number of nodes connected to the store should index the
deployment. That decision is based on a number of rules defined in the
`[deployment]` which can match on the subgraph name and the network that
the deployment is indexing.

Rules are evaluated in order, and the first rule that matches determines
where the deployment is placed. The `match` element of a rule can have a
`name`, a [regular expression](https://docs.rs/regex/1.4.2/regex/#syntax)
that is matched against the subgraph name for the deployment, and a
`network` name that is compared to the network that the new deployment
indexes.

The last rule must not have a `match` statement to make sure that there is
always some store and some indexer that will work on a deployment.

The rule indicates the name of the `store` where the data for the
deployment should be stored, which defaults to `primary`, and a list of
`indexers`. For the matching rule, one indexer is chosen from the
`indexers` list so that deployments are spread evenly across all the nodes
mentioned in `indexers`. The names for the indexers must be the same names
that are passed with `--node-id` when those index nodes are started.

```toml
[deployment]
[[deployment.rule]]
match = { name = "(vip|important)/.*" }
store = "vip"
indexers = [ "index-node-vip-0", "index-node-vip-1" ]
[[deployment.rule]]
match = { network = "kovan" }
# No store, so we use the default store called 'primary'
indexers = [ "index-node-kovan-0" ]
[[deployment.rule]]
# There's no 'match', so any subgraph matches
indexers = [
    "index-node-community-0",
    "index-node-community-1",
    "index-node-community-2",
    "index-node-community-3",
    "index-node-community-4",
    "index-node-community-5"
  ]

```

## Basic Setup

The following file is equivalent to using the `--postgres-url` command line
option:
```toml
[store]
[store.primary]
connection="<.. postgres-url argument ..>"
[deployment]
[[deployment.rule]]
indexers = [ <.. list of all indexing nodes ..> ]
```
