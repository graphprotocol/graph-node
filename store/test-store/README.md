# Setting up for running tests

This document describes what needs to be done to run tests against a
Postgres database.

## Initial setup

Before starting, you will need to set up a database user `graph` and give
it the password `graph` by running `createuser -W graph`.

You also need to arrange for `psql` and similar tools to connect to
Postgres automatically as the `postgres` user.

For that, add this line as the first non-comment line to
`$PGDATA/pg_hba.conf`:
```
local   all             postgres                                peer map=admin
```

and add these lines to `data/pg_ident.conf`:
```
admin           postgres                postgres
admin           <your username>         postgres
```

You might also want to set `log_statement = 'all'` to
`$PGDATA/postgresql.conf`. That will cause Postgres to log all SQL
statements that the tests send it which can sometimes be very helpful when
tests fail.

After making all these changes, restart Postgres to make sure it actually
uses these settings.

## Resetting the test databases

The script `db-reset` will (re)create databases in the test cluster. This
script can be used to remove old databases and/or broken databases schemas
and cause `cargo test` to create the schema from scratch on the next test
run.

Note that `db-reset` will completely delete the databases `graph-test` and
`graph-sgd` in the databases cluster that a plain invocation of `psql`
connects to.

## Running tests

Before running tests set the environment variable `GRAPH_NODE_TEST_CONFIG`
to point either at `config.simple.toml` or `config.sharded.toml`.

Run tests with `cargo test --workspace --exclude graph-tests`; this will
run all unit and integration tests. The `graph-tests` crate contains more
extensive integration tests that depend on running docker containers; that
setup is outside the scope of the document, and we therefore exclude them
from running for basic usage.

When you switch from one of the test configurations to another, you will
need to clean out the test databases by running `db-reset`.
