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
  timestamp: Int8!
  price: BigDecimal!
}

type Stats @aggregation(intervals: ["hour", "day"], source: "Data") {
  id: Int8!
  timestamp: Int8!
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

**TODO**: add a `Timestamp` type and use that for `timestamp`

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
    timestamp: Int8!
    token: Token!
    amount: BigDecimal!
    priceUSD: BigDecimal!
}

# Aggregations over TokenData
type TokenStats @aggregation(intervals: ["hour", "day"], source: "TokenData") {
  id: Int8!
  timestamp: Int8!
  token: Token!
  totalVolume: BigDecimal! @aggregate(fn: "sum", arg: "amount")
  priceUSD: BigDecimal! @aggregate(fn: "last", arg: "priceUSD")
  count: Int8! @aggregate(fn: "count")
}
```

Fields in aggregations without the `@aggregate` directive are called
_dimensions_, and fields with the `@aggregate` directive are called
_aggregates_. A timeseries type really represents many timeseries, one for
each combination of values for the dimensions.

**TODO** As written, this supports buckets that start at zero with every new
hour/day. We also want to support cumulative statistics, i.e., snapshotting
of time series where a new bucket starts with the values of the previous
bucket.

**TODO** Since average is a little more complicated to handle for cumulative
aggregations, and it doesn't seem like it used in practice, we won't
initially support it. (same for variance, stddev etc.)

**TODO** The timeseries type can be simplified for some situations if
aggregations can be done over expressions, for example over `priceUSD *
amount` to track `totalVolumeUSD`

**TODO** It might be necessary to allow `@aggregate` fields that are only
used for some intervals. We could allow that with syntax like
`@aggregate(fn: .., arg: .., interval: "day")`

## Specification

### Timeseries

A timeseries is an entity type with the annotation `@entity(timeseries:
true)`. It must have an `id` attribute and a `timestamp` attribute of type
`Int8`. It must not also be annotated with `immutable: false` as timeseries
are always immutable.

### Aggregations

An aggregation is defined with an `@aggregation` annotation. The annotation
must have two arguments:

- `intervals`: a non-empty array of intervals; currently, only `hour` and `day`
  are supported
- `source`: the name of a timeseries type. Aggregates are computed based on
  the attributes of the timeseries type.

The aggregation type must have an `id` attribute and a `timestamp` attribute
of type `Int8`.

The aggregation type must have at least one attribute with the `@aggregate`
annotation. These attributes must be of a numeric type (`Int`, `Int8`,
`BigInt`, or `BigDecimal`) The annotation must have two arguments:

- `fn`: the name of an aggregation function
- `arg`: the name of an attribute in the timeseries type

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

## Querying

_This section is not implemented yet, and will require a bit more thought
about details_

**TODO** As written, timeseries points like `TokenData` can be queried like
any other entity. It would be nice to restrict how these data points can be
queried, maybe even forbid it, as that would give us more latitude in how we
store that data.

We create a toplevel query field for each aggregation. That query field
accepts the following arguments:

- For each dimension, an optional filter to test for equality of that
  dimension
- A mandatory `interval`
- An optional `current` to indicate whether to include the current,
  partially filled bucket in the response. Can be either `ignore` (the
  default) or `include` (still **TODO** and not implemented)
- Optional `timestamp_{gte|gt|lt|lte|eq|in}` filters to restrict the range
  of timestamps to return
- Timeseries are always sorted by `timestamp` and `id` in descending order

**TODO** It would be nicer to sort by the dimensions and `timestamp`, but we
don't have the internal plumbing for multi-column sort in place.

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

**TODO**: what about time-travel? Is it possible to include a block
constraint?
