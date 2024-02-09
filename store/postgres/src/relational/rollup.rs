//! Queries and helpers to do rollups for aggregations, i.e., the queries
//! that aggregate source data into aggregations.
//!
//! There are two different kinds of queries that we generate: one for
//! aggregations that do not contain any cumulative aggregates, and one for
//! those that do.
//!
//! When there are no cumulative aggregates, the query is relatively
//! straightforward: we form a `group by` query that groups by timestamp
//! rounded to the beginning of the interval and all the dimensions. We
//! select the required source columns over the interval and aggregate over
//! them. For the `id` of the aggregation, we use the max of the ids we
//! aggregate over; the actual value doesn't matter much, we just need it to
//! be unique. The query has the overall shape
//!
//! ```text
//!   select id, timestamp, <dimensions>, <aggregates> from ( select max(id)
//!     as id, date_trunc(interval, timestamp), <source_columns> from
//!       <timeseries> where timestamp >= $start and timestamp < $end) data
//!      group by timestamp, <dimensions>
//! ```
//!
//! When there are cumulative aggregations, things get more complicated. We
//! form the aggregations for the current interval as in the previous case,
//! but also need to find the corresponding previous aggregated values for
//! each group, taking into account that the previous value might not be in
//! the previous bucket for groups that only receive updates sporadically.
//! We find those previous values through a lateral join subquery that looks
//! for the latest aggregate with a given group key. We only want previous
//! values for cumulative aggregates, and therefore select `null` for the
//! non-cumulative ones when forming the list of previous values. We then
//! combine the two by aggregating over the combined set of rows. We need to
//! be careful to do the combination in the correct order, expressed by the
//! `seq` variable in the combined query. We also need to be carful to use
//! the right aggregation function for combining aggregates; in particular,
//! counts need to be combined with `sum` and not `count`. That query looks
//! like
//!
//! ```text
//!   with bucket as (<query from above>),
//!        prev as (select bucket.id, bucket.timestamp, bucket.<dimensions>,
//!                        <null for non-cumulative aggregates>,
//!                        <values of cumulative aggregates>
//!                   from bucket cross join lateral (
//!                        select * from <aggregation> prev
//!                         where prev.timestamp < $start
//!                           and prev.<dimensions> = bucket.<dimensions>
//!                         order by prev.timestamp desc limit 1) prev),
//!       combined as (select id, timestamp, <dimensions>, <combined aggregates>
//!                      from (select *, 1 as seq from prev
//!                             union all
//!                            select *, 2 as seq from bucket) u
//!                    group by id, timestamp, <dimensions>)
//!   select id, timestamp, <dimensions>, <aggregates> from combined
//! ```
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use diesel::{sql_query, PgConnection, RunQueryDsl as _};

use diesel::sql_types::{BigInt, Integer};
use graph::blockchain::BlockTime;
use graph::components::store::{BlockNumber, StoreError};
use graph::data::store::IdType;
use graph::schema::{Aggregate, AggregateFn, Aggregation, AggregationInterval};

use crate::relational::Table;

use super::{Column, SqlName};

#[derive(Debug, Clone)]
pub(crate) struct Agg<'a> {
    aggregate: &'a Aggregate,
    src_column: Option<&'a Column>,
    agg_column: &'a Column,
}

impl<'a> Agg<'a> {
    fn new(
        aggregate: &'a Aggregate,
        src_table: &'a Table,
        agg_table: &'a Table,
    ) -> Result<Self, StoreError> {
        let src_column = aggregate
            .arg
            .as_ref()
            .map(|arg| src_table.column_for_field(&arg.name))
            .transpose()?;
        let agg_column = agg_table.column_for_field(&aggregate.name)?;
        Ok(Self {
            aggregate,
            src_column,
            agg_column,
        })
    }

    fn aggregate_over(
        &self,
        src: Option<&SqlName>,
        time: &str,
        w: &mut dyn fmt::Write,
    ) -> fmt::Result {
        use AggregateFn::*;

        match self.aggregate.func {
            Sum => write!(w, "sum(\"{}\")", src.unwrap())?,
            Max => write!(w, "max(\"{}\")", src.unwrap())?,
            Min => write!(w, "min(\"{}\")", src.unwrap())?,
            First => {
                let sql_type = self.agg_column.column_type.sql_type();
                write!(w, "arg_min_{}((\"{}\", {time}))", sql_type, src.unwrap())?
            }
            Last => {
                let sql_type = self.agg_column.column_type.sql_type();
                write!(w, "arg_max_{}((\"{}\", {time}))", sql_type, src.unwrap())?
            }
            Count => write!(w, "count(*)")?,
        }
        write!(w, " as \"{}\"", self.agg_column.name)
    }

    /// Generate a SQL fragment `func(src_column) as agg_column` where
    /// `func` is the aggregation function. The `time` parameter is the name
    /// of the column with respect to which `first` and `last` should decide
    /// which values are earlier or later
    fn aggregate(&self, time: &str, w: &mut dyn fmt::Write) -> fmt::Result {
        self.aggregate_over(self.src_column.map(|col| &col.name), time, w)
    }

    /// Generate a SQL fragment `func(src_column) as agg_column` where
    /// `func` is a function that combines preaggregated values into a
    /// combined aggregate. The `time` parameter has the same meaning as for
    /// `aggregate` are earlier or later
    fn combine(&self, time: &str, w: &mut dyn fmt::Write) -> fmt::Result {
        use AggregateFn::*;

        match self.aggregate.func {
            Sum | Max | Min | First | Last => {
                // For these, combining and aggregating is done by the same
                // function
                return self.aggregate_over(Some(&self.agg_column.name), time, w);
            }
            Count => write!(w, "sum(\"{}\")", self.agg_column.name)?,
        }
        write!(w, " as \"{}\"", self.agg_column.name)
    }

    /// Generate a SQL fragment that computes that selects the previous
    /// value from an aggregation when the aggregation is cumulative and
    /// `null` when it is not
    fn prev_agg(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        if self.aggregate.cumulative {
            write!(w, "prev.\"{}\"", self.agg_column.name)
        } else {
            let sql_type = self.agg_column.column_type.sql_type();
            write!(w, "null::{sql_type} as \"{}\"", self.agg_column.name)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Rollup {
    pub(crate) interval: AggregationInterval,
    #[allow(dead_code)]
    agg_table: Arc<Table>,
    insert_sql: String,
}

impl Rollup {
    pub(crate) fn new(
        interval: AggregationInterval,
        aggregation: &Aggregation,
        src_table: &Table,
        agg_table: Arc<Table>,
    ) -> Result<Self, StoreError> {
        let dimensions: Box<[_]> = aggregation
            .dimensions()
            .map(|field| src_table.column_for_field(&field.name))
            .collect::<Result<_, _>>()?;
        let aggregates: Box<[Agg<'_>]> = aggregation
            .aggregates
            .iter()
            .map(|aggregate| Agg::new(aggregate, src_table, &agg_table))
            .collect::<Result<_, _>>()?;
        let sql = RollupSql::new(
            interval,
            &src_table.qualified_name,
            &agg_table,
            &dimensions,
            &aggregates,
        );
        let mut insert_sql = String::new();
        sql.insert(&mut insert_sql)?;
        Ok(Self {
            interval,
            agg_table,
            insert_sql,
        })
    }

    pub(crate) fn insert(
        &self,
        conn: &PgConnection,
        bucket: &Range<BlockTime>,
        block: BlockNumber,
    ) -> Result<usize, diesel::result::Error> {
        let query = sql_query(&self.insert_sql)
            .bind::<BigInt, _>(bucket.start.as_secs_since_epoch())
            .bind::<BigInt, _>(bucket.end.as_secs_since_epoch())
            .bind::<Integer, _>(block);
        query.execute(conn)
    }
}

struct RollupSql<'a> {
    interval: AggregationInterval,
    src_table: &'a SqlName,
    agg_table: &'a Table,
    dimensions: &'a [&'a Column],
    aggregates: &'a [Agg<'a>],
}

impl<'a> RollupSql<'a> {
    fn new(
        interval: AggregationInterval,
        src_table: &'a SqlName,
        agg_table: &'a Table,
        dimensions: &'a [&Column],
        aggregates: &'a [Agg],
    ) -> Self {
        Self {
            interval,
            src_table,
            agg_table,
            dimensions,
            aggregates,
        }
    }

    fn has_cumulative_aggregates(&self) -> bool {
        self.aggregates.iter().any(|agg| agg.aggregate.cumulative)
    }

    /// Generate a query to roll up the source timeseries into an
    /// aggregation over one time window
    ///
    /// select id, $ts, $block, <dimensions>, <aggregates>
    ///   from (select
    ///           max(id) as id,
    ///           <dimensions>,
    ///           <agggregates> from (
    ///           select id, rounded_timestamp, <dimensions>, <source cols>
    ///             from <source>
    ///            where timestamp >= $start
    ///              and timestamp < $end
    ///             order by timestamp) data
    ///  group by <dimensions>) agg;
    ///
    ///
    /// Bind variables:
    ///   $1: beginning timestamp (inclusive)
    ///   $2: end timestamp (exclusive)
    ///   $3: block number
    fn select_bucket(&self, with_block: bool, w: &mut dyn fmt::Write) -> fmt::Result {
        let max_id = match self.agg_table.primary_key().column_type.id_type() {
            Ok(IdType::Bytes) => "max(id::text)::bytea",
            Ok(IdType::String) | Ok(IdType::Int8) => "max(id)",
            Err(_) => unreachable!("we make sure that the primary key has an id_type"),
        };
        write!(w, "select {max_id} as id, timestamp, ")?;
        if with_block {
            write!(w, "$3, ")?;
        }
        write_dims(self.dimensions, w)?;
        comma_sep(self.aggregates, self.dimensions.is_empty(), w, |w, agg| {
            agg.aggregate("id", w)
        })?;
        let secs = self.interval.as_duration().as_secs();
        write!(
            w,
            " from (select id, timestamp/{secs}*{secs} as timestamp, "
        )?;
        write_dims(self.dimensions, w)?;
        let agg_srcs = {
            let mut agg_srcs: Vec<_> = self
                .aggregates
                .iter()
                .filter_map(|agg| agg.src_column.map(|col| col.name.as_str()))
                .collect();
            agg_srcs.sort();
            agg_srcs.dedup();
            agg_srcs
        };
        comma_sep(agg_srcs, self.dimensions.is_empty(), w, |w, col: &str| {
            write!(w, "\"{}\"", col)
        })?;
        write!(
            w,
            " from {src_table} where {src_table}.timestamp >= $1 and {src_table}.timestamp < $2",
            src_table = self.src_table
        )?;
        write!(
            w,
            " order by {src_table}.timestamp) data group by timestamp",
            src_table = self.src_table
        )?;
        Ok(if !self.dimensions.is_empty() {
            write!(w, ", ")?;
            write_dims(self.dimensions, w)?;
        })
    }

    fn select(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        self.select_bucket(true, w)
    }

    /// Generate the insert into statement
    ///
    /// insert into <aggregation>(id, timestamp, <dimensions>,
    /// <aggregates>, block$)
    fn insert_into(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        write!(
            w,
            "insert into {}(id, timestamp, block$, ",
            self.agg_table.qualified_name
        )?;
        write_dims(self.dimensions, w)?;
        comma_sep(self.aggregates, self.dimensions.is_empty(), w, |w, agg| {
            write!(w, "\"{}\"", agg.agg_column.name)
        })?;
        write!(w, ") ")
    }

    /// Generate a query
    ///
    /// insert into <aggregation>(id, timestamp, block$, <dimensions>,
    /// <aggregates>) <select>;
    ///
    /// where `select` is the result of `Self::select`
    fn insert_bucket(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        self.insert_into(w)?;
        self.select(w)
    }

    /// Generate a query that selects the previous value of the aggregates
    /// for any group keys that appear in `bucket`
    fn select_prev(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        write!(w, "select bucket.id, bucket.timestamp")?;
        comma_sep(self.dimensions, false, w, |w, col| {
            write!(w, "bucket.\"{}\"", col.name)
        })?;
        comma_sep(self.aggregates, false, w, |w, agg| agg.prev_agg(w))?;
        write!(w, " from bucket cross join lateral (")?;
        write!(w, "select * from {} prev", self.agg_table.qualified_name)?;
        write!(w, " where prev.timestamp < $1")?;
        for dim in self.dimensions {
            write!(
                w,
                " and prev.\"{name}\" = bucket.\"{name}\"",
                name = dim.name
            )?;
        }
        write!(w, " order by prev.timestamp desc limit 1) prev")
    }

    fn select_combined(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        write!(w, "select id, timestamp")?;
        comma_sep(self.dimensions, false, w, |w, col| {
            write!(w, "\"{}\"", col.name)
        })?;
        comma_sep(self.aggregates, false, w, |w, agg| agg.combine("seq", w))?;
        write!(
            w,
            " from (select *, 1 as seq from prev union all select *, 2 as seq from bucket) u "
        )?;
        write!(w, " group by id, timestamp")?;
        if !self.dimensions.is_empty() {
            write!(w, ", ")?;
            write_dims(self.dimensions, w)?;
        }
        Ok(())
    }

    /// Generate the common table expression for the select query when we
    /// have cumulative aggregates
    ///
    /// with bucket as (<select_bucket>),
    ///      prev as (<select_prev>),
    ///      combined as (<select_combined>)
    fn select_cte(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        write!(w, "with bucket as (")?;
        self.select_bucket(false, w)?;
        write!(w, "), prev as (")?;
        self.select_prev(w)?;
        write!(w, "), combined as (")?;
        self.select_combined(w)?;
        write!(w, ")")
    }

    /// Generate a query for inserting aggregates if some of them are
    /// cumulative
    ///
    /// with bucket as (select <select>),
    ///      prev as (select ..),
    /// combined as (select .. )
    /// insert into (..)
    /// select ..;
    fn insert_cumulative(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        self.select_cte(w)?;
        write!(w, " ")?;
        self.insert_into(w)?;
        write!(w, "select id, timestamp, $3 as block$, ")?;
        write_dims(self.dimensions, w)?;
        comma_sep(self.aggregates, self.dimensions.is_empty(), w, |w, agg| {
            write!(w, "\"{}\"", agg.agg_column.name)
        })?;
        write!(w, " from combined")
    }

    fn insert(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        if self.has_cumulative_aggregates() {
            self.insert_cumulative(w)
        } else {
            self.insert_bucket(w)
        }
    }
}

/// Write the elements in `list` separated by commas into `w`. The list
/// elements are written by calling `out` with each of them.
fn comma_sep<T, F>(
    list: impl IntoIterator<Item = T>,
    mut first: bool,
    w: &mut dyn fmt::Write,
    out: F,
) -> fmt::Result
where
    F: Fn(&mut dyn fmt::Write, T) -> fmt::Result,
{
    for elem in list {
        if !first {
            write!(w, ", ")?;
        }
        first = false;
        out(w, elem)?;
    }
    Ok(())
}

/// Write the names of the columns in `dimensions` into `w` as a
/// comma-separated list of quoted column names.
fn write_dims(dimensions: &[&Column], w: &mut dyn fmt::Write) -> fmt::Result {
    comma_sep(dimensions, true, w, |w, col| write!(w, "\"{}\"", col.name))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use graph::{data::subgraph::DeploymentHash, schema::InputSchema};
    use itertools::Itertools as _;

    use crate::{
        layout_for_tests::{make_dummy_site, Namespace},
        relational::{rollup::Rollup, Catalog, Layout},
    };

    // Check that the two strings are the same after replacing runs of
    // whitespace with a single space
    #[track_caller]
    fn check_eqv(left: &str, right: &str) {
        use pretty_assertions::assert_eq;

        fn normalize(s: &str) -> String {
            s.replace("\\\n", "")
                .split_whitespace()
                .join(" ")
                .replace("( ", "(")
        }

        let left = normalize(left);
        let right = normalize(right);
        assert_eq!(left, right);
    }

    #[test]
    fn rollup() {
        const SCHEMA: &str = r#"
    type Data @entity(timeseries: true) {
        id: Int8!
        timestamp: Int8!
        token: Bytes!
        price: BigDecimal!
        amount: Int!
      }

      type Stats @aggregation(intervals: ["day", "hour"], source: "Data") {
        id: Int8!
        timestamp: Int8!
        token: Bytes!
        sum: BigDecimal! @aggregate(fn: "sum", arg: "price")
        max: BigDecimal! @aggregate(fn: "max", arg: "amount")
      }

      type TotalStats @aggregation(intervals: ["day"], source: "Data") {
        id: Int8!
        timestamp: Int8!
        max: BigDecimal! @aggregate(fn: "max", arg: "price")
      }

      type OpenClose @aggregation(intervals: ["day"], source: "Data") {
        id: Int8!
        timestamp: Int8!
        open: BigDecimal! @aggregate(fn: "first", arg: "price")
        close: BigDecimal! @aggregate(fn: "last", arg: "price")
        first_amt: Int! @aggregate(fn: "first", arg: "amount")
      }

      type Lifetime @aggregation(intervals: ["day"], source: "Data") {
        id: Int8!
        timestamp: Int8!
        count: Int8! @aggregate(fn: "count")
        sum: BigDecimal! @aggregate(fn: "sum", arg: "amount")
        total_count: Int8! @aggregate(fn: "count", cumulative: true)
        total_sum: BigDecimal! @aggregate(fn: "sum", arg: "amount", cumulative: true)
      }
      "#;

        const STATS_HOUR_SQL: &str = r#"\
        insert into "sgd007"."stats_hour"(id, timestamp, block$, "token", "sum", "max") \
        select max(id) as id, timestamp, $3, "token", sum("price") as "sum", max("amount") as "max" from (\
            select id, timestamp/3600*3600 as timestamp, "token", "amount", "price" \
              from "sgd007"."data" \
             where "sgd007"."data".timestamp >= $1 and "sgd007"."data".timestamp < $2 \
             order by "sgd007"."data".timestamp) data \
        group by timestamp, "token""#;

        const STATS_DAY_SQL: &str = r#"\
        insert into "sgd007"."stats_day"(id, timestamp, block$, "token", "sum", "max") \
        select max(id) as id, timestamp, $3, "token", sum("price") as "sum", max("amount") as "max" from (\
            select id, timestamp/86400*86400 as timestamp, "token", "amount", "price" \
              from "sgd007"."data" \
             where "sgd007"."data".timestamp >= $1 and "sgd007"."data".timestamp < $2 \
             order by "sgd007"."data".timestamp) data \
        group by timestamp, "token""#;

        const TOTAL_SQL: &str = r#"\
        insert into "sgd007"."total_stats_day"(id, timestamp, block$, "max") \
        select max(id) as id, timestamp, $3, max("price") as "max" from (\
            select id, timestamp/86400*86400 as timestamp, "price" from "sgd007"."data" \
             where "sgd007"."data".timestamp >= $1 and "sgd007"."data".timestamp < $2 \
             order by "sgd007"."data".timestamp) data \
        group by timestamp"#;

        const OPEN_CLOSE_SQL: &str = r#"\
        insert into "sgd007"."open_close_day"(id, timestamp, block$, "open", "close", "first_amt")
        select max(id) as id, timestamp, $3, \
               arg_min_numeric(("price", id)) as "open", \
               arg_max_numeric(("price", id)) as "close", \
               arg_min_int4(("amount", id)) as "first_amt" \
          from (select id, timestamp/86400*86400 as timestamp, "amount", "price" \
                  from "sgd007"."data"
                 where "sgd007"."data".timestamp >= $1
                   and "sgd007"."data".timestamp < $2
                 order by "sgd007"."data".timestamp) data \
         group by timestamp"#;

        const LIFETIME_SQL: &str = r#"\
        with bucket as (
            select max(id) as id, timestamp, count(*) as "count",
                   sum("amount") as "sum", count(*) as "total_count",
                   sum("amount") as "total_sum"
              from (select id, timestamp/86400*86400 as timestamp, "amount"
                      from "sgd007"."data"
                     where "sgd007"."data".timestamp >= $1
                       and "sgd007"."data".timestamp < $2
                     order by "sgd007"."data".timestamp) data
              group by timestamp),
             prev as (select bucket.id, bucket.timestamp,
                             null::int8 as "count", null::numeric as "sum",
                             prev."total_count", prev."total_sum"
                        from bucket cross join lateral (
                             select * from "sgd007"."lifetime_day" prev
                              where prev.timestamp < $1
                              order by prev.timestamp desc limit 1) prev),
             combined as (select id, timestamp,
                                 sum("count") as "count", sum("sum") as "sum",
                                 sum("total_count") as "total_count",
                                 sum("total_sum") as "total_sum" from (
                            select *, 1 as seq from prev
                            union all
                            select *, 2 as seq from bucket) u
                          group by id, timestamp)
        insert into "sgd007"."lifetime_day"(id, timestamp, block$, "count", "sum", "total_count", "total_sum")
        select id, timestamp, $3 as block$, "count", "sum", "total_count", "total_sum" from combined
        "#;

        #[track_caller]
        fn rollup_for<'a>(layout: &'a Layout, table_name: &str) -> &'a Rollup {
            layout
                .rollups
                .iter()
                .find(|rollup| rollup.agg_table.name.as_str() == table_name)
                .unwrap()
        }

        let hash = DeploymentHash::new("rollup").unwrap();
        let nsp = Namespace::new("sgd007".to_string()).unwrap();
        let schema = InputSchema::parse_latest(SCHEMA, hash.clone()).unwrap();
        let site = Arc::new(make_dummy_site(hash, nsp, "rollup".to_string()));
        let catalog = Catalog::for_tests(site.clone(), BTreeSet::new()).unwrap();
        let layout = Layout::new(site, &schema, catalog).unwrap();
        assert_eq!(5, layout.rollups.len());

        // Intervals are non-decreasing
        assert!(layout.rollups[0].interval <= layout.rollups[1].interval);
        assert!(layout.rollups[1].interval <= layout.rollups[2].interval);

        // Generated SQL is correct
        let stats_hour = rollup_for(&layout, "stats_hour");
        let stats_day = rollup_for(&layout, "stats_day");
        let stats_total = rollup_for(&layout, "total_stats_day");
        check_eqv(STATS_HOUR_SQL, &stats_hour.insert_sql);
        check_eqv(STATS_DAY_SQL, &stats_day.insert_sql);
        check_eqv(TOTAL_SQL, &stats_total.insert_sql);

        let open_close = rollup_for(&layout, "open_close_day");
        check_eqv(OPEN_CLOSE_SQL, &open_close.insert_sql);

        let lifetime = rollup_for(&layout, "lifetime_day");
        check_eqv(LIFETIME_SQL, &lifetime.insert_sql);
    }
}
