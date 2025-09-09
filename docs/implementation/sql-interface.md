# SQL Queries

**This interface is extremely experimental. There is no guarantee that this
interface will ever be brought to production use. It's solely here to help
evaluate the utility of such an interface**

**The interface is only available if the environment variable `GRAPH_ENABLE_SQL_QUERIES` is set to `true`**

SQL queries can be issued by posting a JSON document to
`/subgraphs/sql`. The server will respond with a JSON response that
contains the records matching the query in JSON form.

The body of the request must contain the following keys:

* `deployment`: the hash of the deployment against which the query should
  be run
* `query`: the SQL query
* `mode`: either `info` or `data`. When the mode is `info` only some
  information of the response is reported, with a mode of `data` the query
  result is sent in the response

The SQL query can use all the tables of the given subgraph. Table and
attribute names are snake-cased from their form in the GraphQL schema, so
that data for `SomeDailyStuff` is stored in a table `some_daily_stuff`.

The query can use fairly arbitrary SQL, including aggregations and most
functions built into PostgreSQL.

## Example

For a subgraph whose schema defines an entity `Block`, the following query
```json
{
    "query": "select number, hash, parent_hash, timestamp from block order by number desc limit 2",
    "deployment": "QmSoMeThInG",
    "mode": "data"
}
```

might result in this response
```json
{
  "data": [
    {
      "hash": "\\x5f91e535ee4d328725b869dd96f4c42059e3f2728dfc452c32e5597b28ce68d6",
      "number": 5000,
      "parent_hash": "\\x82e95c1ee3a98cd0646225b5ae6afc0b0229367b992df97aeb669c898657a4bb",
      "timestamp": "2015-07-30T20:07:44+00:00"
    },
    {
      "hash": "\\x82e95c1ee3a98cd0646225b5ae6afc0b0229367b992df97aeb669c898657a4bb",
      "number": 4999,
      "parent_hash": "\\x875c9a0f8215258c3b17fd5af5127541121cca1f594515aae4fbe5a7fbef8389",
      "timestamp": "2015-07-30T20:07:36+00:00"
    }
  ]
}
```

## Limitations/Ideas/Disclaimers

Most of these are fairly easy to address:

- bind variables/query parameters are not supported, only literal SQL
  queries
* queries must finish within `GRAPH_SQL_STATEMENT_TIMEOUT` (unlimited by
  default)
* queries are always executed at the subgraph head. It would be easy to add
  a way to specify a block at which the query should be executed
* the interface right now pretty much exposes the raw SQL schema for a
  subgraph, though system columns like `vid` or `block_range` are made
  inaccessible.
* it is not possible to join across subgraphs, though it would be possible
  to add that. Implenting that would require some additional plumbing that
  hides the effects of sharding.
* JSON as the response format is pretty terrible, and we should change that
  to something that isn't so inefficient
* the response contains data that's pretty raw; as the example shows,
  binary data uses Postgres' notation for hex strings
* because of how broad the supported SQL is, it is pretty easy to issue
  queries that take a very long time. It will therefore not be hard to take
  down a `graph-node`, especially when no query timeout is set

Most importantly: while quite a bit of effort has been put into making this
interface safe, in particular, making sure it's not possible to write
through this interface, there's no guarantee that this works without bugs.
