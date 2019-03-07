# GraphQL API

## 1 Queries
### 1.1 Basics
For each `Entity` type that you define in your schema, an `entity` and `entities` field will be generated on the top-level `Query` type. Note that `query` does not need to be included at the top of the `graphql` query when using The Graph.

#### Example
Query for a single `Token` entity defined in your schema:
```graphql
{
  token(id: "1") {
    id
    owner
  }
}
```
When querying for a single entity, the `id` field is required.

#### Example
Collection query for `Token` entities:
```graphql
{
  tokens(first: 100) {
    id
    owner
  }
}
```
### 1.2 Sorting
When querying a collection, the `orderBy` parameter may be used to sort by a specific attribute. Additionally, the `orderDirection` can be used to specify the sort direction, `asc` for ascending or `desc` for descending.

#### Example
```graphql
{
  tokens(first: 100, orderBy: price, orderDirection: asc) {
    id
    owner
  }
}
```

### 1.3 Pagination
When querying a collection, the `first` parameter must be used to paginate from the beginning of the collection.

To query for groups of entities in the middle of a collection, the `skip` parameter may be used to skip a specified number of entities starting at the beginning of the collection.

#### Example
Query 10 `Token` entities, offset by 10:
```graphql
{
  tokens(first: 10, skip: 10) {
    id
    owner
  }
}
```

### 1.4 Filtering

You can use the `where` parameter in your queries to filter for different properties.

#### Example
Query challenges with `failed` outcome:

```graphql
{
  challenges(first: 100, where: {outcome: "failed"}) {
    challenger
    outcome
    application(first: 100) {
      id
    }
  }
}
```

You can use suffixes like `_gt`, `_lte` for value comparison:

#### Example
```graphql
{
  applications(first: 100, where: {deposit_gt:"10000000000"}) {
    id
    whitelisted
    deposit
  }
}
```

Full list of parameter suffixes:
```
_not
_gt
_lt
_gte
_lte
_in
_not_in
_contains
_not_contains
_starts_with
_ends_with
_not_starts_with
_not_ends_with
```

Please note that some suffixes are only supported for specific types. For example, `Boolean` only supports `_not`, `_in`, and `_not_in`.

## 2 Subscriptions
Graph Protocol subscriptions are GraphQL spec-compliant subscriptions. Unlike query operations, GraphQL subscriptions may only have a single top-level field at the root level for each subscription operation.

### 2.1 Basics
The root Subscription type for subscription operations mimics the root Query type used for query operations to minimize the cognitive overhead for writing subscriptions.

#### Example
Query the first 100 `Token` entities along with their `id` and `owner` attributes:

```graphql
query {
  tokens(first: 100) {
    id
    owner
  }
}
```

Subscribe to all `Token` entity changes and fetch the values of the `id` and `owner` attributes on the updated entity:

```graphql
subscription {
  tokens(first: 100) {
    id
    owner
  }
}
```

## 3 Schema

The schema of your data source--that is, the entity types, values, and relationships that are available to query--are defined through the [GraphQL Interface Definition Langauge (IDL)](http://facebook.github.io/graphql/draft/#sec-Type-System).

### 3.1 Basics

GraphQL requests consist of three basic operations: `query`, `subscription`, and `mutation`. Each of these has a corresponding root level `Query`, `Subscription`, and `Mutation` type in the schema of a GraphQL endpoint.

> **Note:** Our API does not expose mutations because developers are expected to issue transactions directly against the underlying blockchain from their applications.

It is typical for developers to define their own root `Query` and `Subscription` types when building a GraphQL API server, but with The Graph, we generate these top-level types based on the entities that you define in your schema as well as several other types for exploring blockchain data, which we describe in depth in the [Query API](#Basics).

### 3.2 Entities

All GraphQL types with `@entity` directives in your schema will be treated as entities and must have an `ID` field.

> **Note:** Currently, all types in your schema must have an `@entity` directive. In the future, we will treat types without an `@entity` directive as value objects, but this is not yet supported.

#### Example
Define a `Token` entity:

```graphql
type Token @entity {
  # The unique ID of this entity
  id: ID!
  name: String!
  symbol: String!
  decimals: Int!
}
```

### 3.3 Built-In Types

#### 3.3.1 GraphQL Built-In Scalars
All the scalars defined in the GraphQL spec are supported: `Int`, `Float`, `String`, `Boolean`, and `ID`.

#### 3.3.2 Bytes
There is a `Bytes` scalar for variable-length byte arrays.

#### 3.3.2 Numbers
The GraphQL spec defines `Int` and `Float` to have sizes of 32 bytes.

This API additionally includes a `BigInt` number type to represent arbitrarily large integer numbers.

### 3.4 Enums

You can also create `enums` within a schema. Enums have the following syntax:

```graphql
enum TokenStatus {
  OriginalOwner,
  SecondOwner,
  ThirdOwner,
}
```

To set a store value with an enum, use the name of the enum value as a string. In the example above, you can set the `TokenStatus` to the second owner with `SecondOwner`.
More detail on writing enums can be found in the [GraphQL documentation](https://graphql.org/learn/schema/).

### 3.5 Entity Relationships
An entity may have a relationship to one or more other entities in your schema. These relationships may be traversed in your queries and subscriptions.

Relationships in The Graph are unidirectional. Despite this, relationships may be traversed in *either* direction by defining reverse lookups on an entity.

#### 3.5.1 Basics

Relationships are defined on entities just like any other scalar type except that the type specified is that of another entity.

#### Example
Define a `Transaction` entity type with an optional one-to-one relationship with a `TransactionReceipt` entity type:
```graphql
type Transaction @entity {
  id: ID!
  transactionReceipt: TransactionReceipt
}

type TransactionReceipt @entity {
  id: ID!
  transaction: Transaction
}
```

#### Example
Define a `Token` entity type with a required one-to-many relationship with a `TokenBalance` entity type:
```graphql
type Token @entity {
  id: ID!
  tokenBalances: [TokenBalance!]!
}

type TokenBalance @entity {
  id: ID!
  amount: Int!
}
```

#### 3.5.2 Reverse Lookups
Defining reverse lookups can be defined on an entity through the `@derivedFrom` field. This creates a virtual field on the entity that may be queried but cannot be set manually through the mappings API. Rather, it is derived from the relationship defined on the other entity.

The type of an `@derivedFrom` field must be a collection since multiple entities may specify relationships to a single entity.

#### Example
Define a reverse lookup from a `User` entity type to an `Organization` entity type:
```graphql
type Organization @entity {
  id: ID!
  name: String!
  members: [User]!
}

type User @entity {
  id: ID!
  name: String!
  organizations: [Organization!] @derivedFrom(field: "members")
}
```
