# Subgraph:SQL Service

The Subgraph:SQL Service, developed by Semiotic Labs in collaboration with The Guild
and Edge & Node, offers a secure SQL interface for querying a subgraph's entities.
To deploy this with minimal changes to the existing indexer stack, consumers (or the
Studio they use) can wrap an SQL query in a GraphQL query.

## Querying with Subgraph:SQL Service

### Running Queries

Say we have the following SQL query: 

```sql
SELECT * FROM users WHERE age > 18
```

The Subgraph:SQL Service allows consumers to create a corresponding GraphQL query
using the Subgraph:SQL Service `sql` field, with a `query` field containing the SQL
query:

```graphql
query {
  sql(input: {
    query: "SELECT * FROM users WHERE age > 18",
    format: JSON
  }) {
    ... on SqlJSONOutput {
      columns
      rowCount
      rows
    }
  }
}
```

We use the `sql` field in the GraphQL query, passing an input object with the SQL
query, optional parameters, and format. The SQL query selects all columns from the
`users` table where the `age` column is greater than 18, returning the requested
data formatted as JSON.

### SQL Parameters and Bind Parameters

#### SQL Query Parameters

You can pass optional SQL query parameters to the SQL query as positional parameters.
The parameters are converted to the SQL types based on the GraphQL types of the parameters.
In the GraphQL schema, parameters are passed as an array of `SqlVariable` objects
within the `parameters` field of the `SqlInput` input object. See the GraphQL schema
types in `graph/src/schema/sql.graphql`.

#### Bind Parameters

We currently do not support bind parameters, but plan to support this feature in a future
version of Graph Node.

## Configuration

The Subgraph:SQL Service can be enabled or disabled using the `GRAPH_GRAPHQL_ENABLE_SQL_SERVICE`
environment variable.

- **Environment Variable:** `GRAPH_GRAPHQL_ENABLE_SQL_SERVICE`
- **Default State:** Off (Disabled)
- **Purpose:** Enables queries on the `sql()` field of the root query.
- **Impact on Schema:** Adds a global `SqlInput` type to the GraphQL schema. The `sql`
field accepts values of this type.

To enable the Subgraph:SQL Service, set the `GRAPH_GRAPHQL_ENABLE_SQL_SERVICE` environment
variable to `true` or `1`. This allows clients to execute SQL queries using the
`sql()` field in GraphQL queries.

```bash
export GRAPH_GRAPHQL_ENABLE_SQL_SERVICE=true
```

Alternatively, configure the environment variable in your deployment scripts or
environment setup as needed.

### SQL Coverage

The Subgraph:SQL Service covers a wide range of SQL functionality, allowing you to execute
`SELECT` queries against your database. It supports basic querying, parameter binding, and
result formatting into JSON or CSV. 

#### Whitelisted and Blacklisted SQL Functions

The `POSTGRES_WHITELISTED_FUNCTIONS` constant contains a whitelist of SQL functions that are
permitted to be used within SQL queries executed by the Subgraph:SQL Service, while `POSTGRES_BLACKLISTED_FUNCTIONS`
serves as a safety mechanism to restrict the usage of certain PostgreSQL functions within SQL
queries. These blacklisted functions are deemed inappropriate or potentially harmful to the
system's integrity or performance. Both constants are defined in `store/postgres/src/sql/constants.rs`.

### SQL Query Validation

Graph Node's SQL query validation ensures that SQL queries adhere to predefined criteria:

- **Function Name Validation**: Validates function names used within SQL queries, distinguishing
between unknown, whitelisted, and blacklisted functions.
- **Statement Validation**: Validates SQL statements, ensuring that only `SELECT` queries are
supported and that multi-statement queries are not allowed.
- **Table Name Validation**: Validates table names referenced in SQL queries, identifying
unknown tables and ensuring compatibility with the schema.
- **Common Table Expression (CTE) Handling**: Handles common table expressions, adding them
to the set of known tables during validation.

See the test suite in `store/postgres/src/sql/validation.rs` for examples of various scenarios
and edge cases encountered during SQL query validation, including function whitelisting and
blacklisting, multi-statement queries, unknown table references, and more.

### Relating GraphQL Schema to Tables

The GraphQL schema provided by the Subgraph:SQL Service reflects the structure of the SQL queries
it can execute. It does not directly represent tables in a database. Users need to
construct SQL queries compatible with their database schema.

### Queryable Attributes/Columns

The columns that can be queried depend on the SQL query provided. In the example GraphQL
query above, the columns returned would be all columns from the `users` table.
