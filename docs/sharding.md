# Support for Multiple Databases (experimental)

**This feature is considered experimental. In particular, the format of the configuration file might still change in backwards-incompatible ways**

For most use cases, a single Postgres database is sufficient to support a
`graph-node` instance. When a `graph-node` instance outgrows a single
Postgres database, it is possible to split the storage of `graph-node`'s
data across multiple Postgres databases. All databases together form the
store of the `graph-node` instance. Each individual database is called a
_shard_.

Support for multiple databases is configured through a TOML configuration
file. The location of the file is passed with the `--config` command line
switch. When using a configuration file, it is not possible to use the
options `--postgres-url`, `--postgres-secondary-hosts`, and
`--postgres-host-weights`.

The TOML file consists of three sections:
* `[store]` describes the available databases
* `[ingestor]` sets the name of the node responsible for block ingestion
* `[deployment]` describes how to place newly deployed subgraphs

## Configuring Multiple Databases

The `[store]` section must always have a primary shard configured, which
must be called `primary`. Each shard can have additional read replicas that
are used for responding to queries. Only queries are processed by read
replicas. Indexing and block ingestion will always use the main database.

Any number of additional shards, with their own read replicas, can also be
configured. When read replicas are used, query traffic is split between the
main database and the replicas according to their weights. In the example
below, for the primary shard, no queries will be sent to the main database,
and the replicas will receive 50% of the traffic each. In the `vip` shard,
50% of the traffic goes to the main database, and 50% to the replica.

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

## Configuring Ethereum Providers

The `[chains]` section controls the ethereum providers that `graph-node`
connects to, and where blocks and other metadata for each chain are
stored. The section consists of the name of the node doing block ingestion
(currently not used), and a list of chains. The configuration for a chain
`name` is specified in the section `[chains.<name>]`, and consists of the
`shard` where chain data is stored and a list of providers for that
chain. For each provider, the following information must be given:

* `label`: a label that is used when logging information about that
  provider (not implemented yet)
* `transport`: one of `rpc`, `ws`, and `ipc`. Defaults to `rpc`
* `url`: the URL for the provider
* `features`: an array of features that the provider supports, either empty
  or any combination of `traces` and `archive`

The following example configures two chains, `mainnet` and `kovan`, where
blocks for `mainnet` are stored in the `vip` shard and blocks for `kovan`
are stored in the primary shard. The `mainnet` chain can use two different
providers, whereas `kovan` only has one provider.

```toml
[chains]
ingestor = "block_ingestor_node"
[chains.mainnet]
shard = "vip"
provider = [
  { label = "mainnet1", url = "http://..", features = [] },
  { label = "mainnet2", url = "http://..", features = [ "archive", "traces" ] }
]
[chains.kovan]
shard = "primary"
provider = [ { label = "kovan", url = "http://..", features = [] } ]
```

## Controlling Deployment

When `graph-node` receives a request to deploy a new subgraph deployment,
it needs to decide in which shard to store the data for the deployment, and
which of any number of nodes connected to the store should index the
deployment. That decision is based on a number of rules defined in the
`[deployment]` section. Deployment rules can match on the subgraph name and
the network that the deployment is indexing.

Rules are evaluated in order, and the first rule that matches determines
where the deployment is placed. The `match` element of a rule can have a
`name`, a [regular expression](https://docs.rs/regex/1.4.2/regex/#syntax)
that is matched against the subgraph name for the deployment, and a
`network` name that is compared to the network that the new deployment
indexes.

The last rule must not have a `match` statement to make sure that there is
always some shard and some indexer that will work on a deployment.

The rule indicates the name of the `shard` where the data for the
deployment should be stored, which defaults to `primary`, and a list of
`indexers`. For the matching rule, one indexer is chosen from the
`indexers` list so that deployments are spread evenly across all the nodes
mentioned in `indexers`. The names for the indexers must be the same names
that are passed with `--node-id` when those index nodes are started.

```toml
[deployment]
[[deployment.rule]]
match = { name = "(vip|important)/.*" }
shard = "vip"
indexers = [ "index-node-vip-0", "index-node-vip-1" ]
[[deployment.rule]]
match = { network = "kovan" }
# No shard, so we use the default shard called 'primary'
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

## Validating configuration files

A configuration file can be checked for validity by passing the `--check-config`
flag to `graph-node`. The command
```shell
graph-node --config $CONFIG_FILE --check-config --ethereum-rpc 'something'
```
will read the configuration file and print information about syntax errors or, for
valid files, a JSON representation of the configuration. (The fact that
`--ethereum-rpc` needs to be specified will be addressed in a future release)

## Simulating deployment placement

Given a configuration file, placement of newly deployed subgraphs can be
simulated with
```shell
graphman --config $CONFIG_FILE place some/subgraph mainnet
```
The command will not make any changes, but simply print where that subgraph
would be placed. The output will indicate the database shard that will hold
the subgraph's data, and a list of indexing nodes that could be used for
indexing that subgraph. During deployment, `graph-node` chooses the indexing
nodes with the fewest subgraphs currently assigned from that list.
