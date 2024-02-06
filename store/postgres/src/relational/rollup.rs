//! Queries and helpers to do rollups for aggregations, i.e., the queries
//! that aggregate source data into aggregations
use std::fmt::{self, Write as _};
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

    fn aggregate(&self, w: &mut dyn fmt::Write) -> fmt::Result {
        use AggregateFn::*;

        match self.aggregate.func {
            Sum => write!(w, "sum(\"{}\")", self.src_column.unwrap().name)?,
            Max => write!(w, "max(\"{}\")", self.src_column.unwrap().name)?,
            Min => write!(w, "min(\"{}\")", self.src_column.unwrap().name)?,
            First => {
                let src = self.src_column.unwrap();
                let sql_type = src.column_type.sql_type();
                write!(w, "arg_min_{}((\"{}\", id))", sql_type, src.name)?
            }
            Last => {
                let src = self.src_column.unwrap();
                let sql_type = src.column_type.sql_type();
                write!(w, "arg_max_{}((\"{}\", id))", sql_type, src.name)?
            }
            Count => write!(w, "count(*)")?,
        }
        write!(w, " as \"{}\"", self.agg_column.name)
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
        let insert_sql = Self::insert_sql(
            interval,
            &src_table.qualified_name,
            &agg_table,
            &dimensions,
            &aggregates,
        )?;
        Ok(Self {
            interval,
            agg_table,
            insert_sql,
        })
    }

    /// Generate a query
    ///
    /// insert into <aggregation>(id, timestamp, block$, <dimensions>,
    /// <aggregates>) select id, $ts, $block, <dimensions>, <aggregates> from
    /// (select max(id) as id, <dimensions>, <agggregates> from (select id, <dimensions>,
    ///         <source cols> from <source> where timestamp >= $ts and
    ///                 timestamp < $ts + $interval order by timestamp) data
    ///                group by <dimensions>) agg;
    ///
    /// The 'order by' is only necessary once we support first/last as
    /// aggregates following this:
    /// https://wiki.postgresql.org/wiki/First/last_(aggregate)
    ///
    /// Bind variables:
    ///   $1: beginning timestamp (inclusive)
    ///   $2: end timestamp (exclusive)
    ///   $3: block number
    fn insert_sql(
        interval: AggregationInterval,
        src_table: &SqlName,
        agg_table: &Table,
        dimensions: &[&Column],
        aggregates: &[Agg],
    ) -> Result<String, fmt::Error> {
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

        fn write_dims(dimensions: &[&Column], w: &mut dyn fmt::Write) -> fmt::Result {
            comma_sep(dimensions, true, w, |w, col| write!(w, "\"{}\"", col.name))
        }

        let mut s = String::new();
        write!(
            s,
            "insert into {agg}(id, timestamp, block$, ",
            agg = agg_table.qualified_name
        )?;
        write_dims(dimensions, &mut s)?;
        comma_sep(aggregates, dimensions.is_empty(), &mut s, |w, agg| {
            write!(w, "\"{}\"", agg.agg_column.name)
        })?;
        write!(s, ") ")?;
        // We need the id of the aggregation to be something unique, and use
        // the `max(id)` from the rows that we are aggregating over.
        // Surprisingly, Postgres doesn't have a `max(bytea)`, so we do a
        // funky roundtrip between bytea and string
        let max_id = match agg_table.primary_key().column_type.id_type() {
            Ok(IdType::Bytes) => "max(id::text)::bytea",
            Ok(IdType::String) | Ok(IdType::Int8) => "max(id)",
            Err(_) => unreachable!("we make sure that the primary key has an id_type"),
        };
        write!(s, "select {max_id} as id, timestamp, $3, ")?;
        write_dims(dimensions, &mut s)?;
        comma_sep(aggregates, dimensions.is_empty(), &mut s, |w, agg| {
            agg.aggregate(w)
        })?;
        let secs = interval.as_duration().as_secs();
        write!(
            s,
            " from (select id, timestamp/{secs}*{secs} as timestamp, "
        )?;
        write_dims(dimensions, &mut s)?;

        // List of unique source column names
        let agg_srcs = {
            let mut agg_srcs: Vec<_> = aggregates
                .iter()
                .filter_map(|agg| agg.src_column.map(|col| col.name.as_str()))
                .collect();
            agg_srcs.sort();
            agg_srcs.dedup();
            agg_srcs
        };
        comma_sep(agg_srcs, dimensions.is_empty(), &mut s, |w, col: &str| {
            write!(w, "\"{}\"", col)
        })?;

        write!(
            s,
            " from {src_table} where {src_table}.timestamp >= $1 and {src_table}.timestamp < $2",
        )?;
        write!(
            s,
            " order by {src_table}.timestamp) data group by timestamp"
        )?;
        if !dimensions.is_empty() {
            write!(s, ", ")?;
            write_dims(dimensions, &mut s)?;
        }
        Ok(s)
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
        assert_eq!(4, layout.rollups.len());

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
    }
}
