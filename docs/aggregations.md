# Timeseries and aggregations

**This feature is experimental. We very much encourage users to try this
out, but we might still need to make material changes to what's described
here in a manner that is not backwards compatible. That might require
deleting and redeploying any subgraph that uses the features here.**

_This feature is available from spec version 1.1.0 onwards_

## Overview

Aggregations are declared in the subgraph schema through two types: one that
stores the raw data points for the time series, and one that defines how raw
data points are to be aggregated. A very simple aggregation can be declared like this:

```graphql
type Data @entity(timeseries: true) {
  id: Int8!
  timestamp: Timestamp!
  price: BigDecimal!
}

type Stats @aggregation(intervals: ["hour", "day"], source: "Data") {
  id: Int8!
  timestamp: Timestamp!
  sum: BigDecimal! @aggregate(fn: "sum", arg: "price")
}
```

Mappings for this schema will add data points by creating `Data` entities
just as they would for normal entities. `graph-node` will then automatically
populate the `Stats` aggregations whenever a given hour or day ends.

The type for the raw data points is defined with an `@entity(timeseries:
true)` annotation. Timeseries types are immutable, and must have an `id`
field and a `timestamp` field. The `id` must be of type `Int8` and is set
automatically so that ids are increasing in insertion order. The `timestamp`
is set automatically by `graph-node` to the timestamp of the current block;
if mappings set this field, it is silently overridden when the entity is
saved.

Aggregations are declared with an `@aggregation` annotation instead of an
`@entity` annotation. They must have an `id` field and a `timestamp` field.
Both fields are set automatically by `graph-node`. The `timestamp` is set to
the beginning of the time period that that aggregation instance represents,
for example, to the beginning of the hour for an hourly aggregation. The
`id` field is set to the `id` of one of the raw data points that went into
the aggregation. Which one is chosen is not specified and should not be
relied on.

**TODO**: figure out whether we should just automatically add `id` and
`timestamp` and have validation just check that these fields don't exist

Aggregations can also contain _dimensions_, which are fields that are not
aggregated but are used to group the data points. For example, the
`TokenStats` aggregation below has a `token` field that is used to group the
data points by token:

```graphql
# Normal entity
type Token @entity { .. }

# Raw data points
type TokenData @entity(timeseries: true) {
    id: Bytes!
    timestamp: Timestamp!
    token: Token!
    amount: BigDecimal!
    priceUSD: BigDecimal!
}

# Aggregations over TokenData
type TokenStats @aggregation(intervals: ["hour", "day"], source: "TokenData") {
  id: Int8!
  timestamp: Timestamp!
  token: Token!
  totalVolume: BigDecimal! @aggregate(fn: "sum", arg: "amount")
  priceUSD: BigDecimal! @aggregate(fn: "last", arg: "priceUSD")
  count: Int8! @aggregate(fn: "count", cumulative: true)
}
```

Fields in aggregations without the `@aggregate` directive are called
_dimensions_, and fields with the `@aggregate` directive are called
_aggregates_. A timeseries type really represents many timeseries, one for
each combination of values for the dimensions.

The same timeseries can be used for multiple aggregations. For example, the
`Stats` aggregation could also be formed by aggregating over the `TokenData`
timeseries. Since `Stats` doesn't have a `token` dimension, all aggregates
will be formed across all tokens.

Each `@aggregate` by default starts at 0 for each new bucket and therefore
just aggregates over the time interval for the bucket. The `@aggregate`
directive also accepts a boolean flag `cumulative` that indicates whether
the aggregation should be cumulative. Cumulative aggregations aggregate over
the entire timeseries up to the end of the time interval for the bucket.

## Specification

### Timeseries

A timeseries is an entity type with the annotation `@entity(timeseries:
true)`. It must have an `id` attribute of type `Int8` and a `timestamp`
attribute of type `Timestamp`. It must not also be annotated with
`immutable: false` as timeseries are always immutable.

### Aggregations

An aggregation is defined with an `@aggregation` annotation. The annotation
must have two arguments:

- `intervals`: a non-empty array of intervals; currently, only `hour` and `day`
  are supported
- `source`: the name of a timeseries type. Aggregates are computed based on
  the attributes of the timeseries type.

The aggregation type must have an `id` attribute of type `Int8` and a
`timestamp` attribute of type `Timestamp`.

The aggregation type must have at least one attribute with the `@aggregate`
annotation. These attributes must be of a numeric type (`Int`, `Int8`,
`BigInt`, or `BigDecimal`) The annotation must have two arguments:

- `fn`: the name of an aggregation function
- `arg`: the name of an attribute in the timeseries type, or an expression
  using only constants and attributes of the timeseries type

#### Aggregation functions

The following aggregation functions are currently supported:

| Name    | Description       |
| ------- | ----------------- |
| `sum`   | Sum of all values |
| `count` | Number of values  |
| `min`   | Minimum value     |
| `max`   | Maximum value     |
| `first` | First value       |
| `last`  | Last value        |

The `first` and `last` aggregation function calculate the first and last
value in an interval by sorting the data by `id`; `graph-node` enforces
correctness here by automatically setting the `id` for timeseries entities.

#### Aggregation expressions

The `arg` can be the name of any attribute in the timeseries type, or an
expression using only constants and attributes of the timeseries type such
as `price * amount` or `greatest(amount0, amount1)`. Expressions use SQL
syntax and support a subset of builtin SQL functions, operators, and other
constructs.

Supported operators are `+`, `-`, `*`, `/`, `%`, `^`, `=`, `!=`, `<`, `<=`,
`>`, `>=`, `<->`, `and`, `or`, and `not`. In addition the operators `is
[not] {null|true|false}`, and `is [not] distinct from` are supported.

The supported SQL functions are the [math
functions](https://www.postgresql.org/docs/current/functions-math.html)
`abs`, `ceil`, `ceiling`, `div`, `floor`, `gcd`, `lcm`, `mod`, `power`,
`sign`, and the [conditional
functions](https://www.postgresql.org/docs/current/functions-conditional.html)
`coalesce`, `nullif`, `greatest`, and `least`.

The
[statement](https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-CASE)
`case when .. else .. end` is also supported.

Some examples of valid expressions, assuming the underlying timeseries
contains the mentioned fields:

- Aggregate the value of a token: `@aggregate(fn: "sum", arg: "priceUSD * amount")`
- Aggregate the maximum positive amount of two different amounts:
  `@aggregate(fn: "max", arg: "greatest(amount0, amount1, 0)")`
- Conditionally sum an amount: `@aggregate(fn: "sum", arg: "case when amount0 > amount1 then amount0 else 0 end")`

## Querying

We create a toplevel query field for each aggregation. That query field
accepts the following arguments:

- For each dimension, an optional filter to test for equality of that
  dimension
- A mandatory `interval`
- An optional `current` to indicate whether to include the current,
  partially filled bucket in the response. Can be either `ignore` (the
  default) or `include` (still **TODO** and not implemented)
- Optional `timestamp_{gte|gt|lt|lte|eq|in}` filters to restrict the range
  of timestamps to return. The timestamp to filter by must be a string
  containing microseconds since the epoch. The value `"1704164640000000"`
  corresponds to `2024-01-02T03:04Z`.
- Timeseries are always sorted by `timestamp` and `id` in descending order

```graphql
token_stats(interval: "hour",
      current: ignore,
      where: {
        token: "0x1234",
        timestamp_gte: 1234567890,
        timestamp_lt: 1234567890 }) {
  id
  timestamp
  token
  totalVolume
  avgVolume
}
```
