# Sharding subgraph (and block) storage

Sharding makes it possible to store different parts of the data set that a
`graph-node` manages in different databases; these databases might be just
different databases on the same Postgres instance, or completely different
Postgres servers.

All databases involved will have the exact same schema, but depending on
how `graph-node` is configured, only some tables in the schema will be
used.

A `graph-node` installation always has a `primary` database which contains
information that needs to be shared across all shards. The primary database
is also used for database notifications (`LISTEN`/`NOTIFY`) In the simplest
case, that's where the story ends: there is a primary database that stores
all the data.

In more complex setups, databases can be used for three different purposes:
the primary, subgraph data shards (`data`), and block cache shards
(`block`). There can be multiple `data` and `block` shards. In that setup,
tables will be split in the following way:

| Table                                    | Shard   |
|------------------------------------------|:-------:|
| __diesel_schema_migrations               | primary |
| __graph_node_global_lock                 | primary |
| db_version                               | primary |
| deployment_schemas                       | primary |
| ens_names                                | primary |
| eth_call_cache                           | primary (or block?) |
| eth_call_meta                            | primary (or block?)|
| ethereum_blocks                          | block   |
| ethereum_networks                        | primary |
| large_notifications                      | primary |
| subgraphs.subgraph                       | primary |
| subgraphs.subgraph_version               | primary |
| subgraphs.subgraph_deployment_assignemnt | primary |
| other subgraphs.*                        | data    |
| sgd.*                                    | data    |

Whether we put the `eth_call_*` tables into primary or block storage
depends on how hard it is to separate `EthereumCallCache` out from `Store`
in the code and make sure it gets everywhere where it is needed.

With this split, most operations will only affect one database, and are therefore
fully transactional. The biggest exception are metadata operations that affect
both `subgraphs.subgraph`, `subgraphs.subgraph_version` and another
`subgraphs.*` table. Here, operations need to be organized in such a way that
if they succeed in one database, and fail in the other, we do not end up with
data that is inconsistent at the application level, and that it will be possible
to redo the entire operation. That will mostly mean that all but the last
transaction for the operation are done idempotently.
