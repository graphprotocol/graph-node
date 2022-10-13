use std::{fmt::Write, sync::Arc, time::Instant};

use diesel::{
    connection::SimpleConnection,
    sql_query,
    sql_types::{BigInt, Integer, Nullable},
    Connection, PgConnection, RunQueryDsl,
};
use graph::{
    components::store::PruneReporter,
    prelude::{BlockNumber, CancelHandle, CancelToken, CancelableError, CheapClone, StoreError},
    slog::Logger,
};
use itertools::Itertools;

use crate::{
    catalog,
    copy::AdaptiveBatchSize,
    deployment,
    relational::{Table, VID_COLUMN},
};

use super::{Layout, SqlName};

/// Utility to copy relevant data out of a source table and into a new
/// destination table and replace the source table with the destination
/// table
struct TablePair {
    src: Arc<Table>,
    dst: Arc<Table>,
}

impl TablePair {
    /// Create a `TablePair` for `src`. This creates a new table `dst` with
    /// the same structure as the `src` table in the database, but without
    /// various indexes. Those are created with `switch`
    fn create(conn: &PgConnection, layout: &Layout, src: Arc<Table>) -> Result<Self, StoreError> {
        let new_name = SqlName::verbatim(format!("{}_n$", src.name));
        let nsp = &layout.site.namespace;

        let dst = src.new_like(&layout.site.namespace, &new_name);

        let mut query = String::new();
        if catalog::table_exists(conn, layout.site.namespace.as_str(), &dst.name)? {
            writeln!(query, "truncate table {nsp}.{new_name};")?;
        } else {
            dst.create_table(&mut query, layout)?;

            // Have the new table use the same vid sequence as the source
            // table
            writeln!(
                query,
                "\
      alter table {nsp}.{new_name} \
        alter column {VID_COLUMN} \
          set default nextval('{nsp}.{src_name}_vid_seq'::regclass);",
                src_name = src.name
            )?;
            writeln!(query, "drop sequence {nsp}.{new_name}_vid_seq;")?;
            writeln!(
                query,
                "alter sequence {nsp}.{src_name}_vid_seq owned by {nsp}.{new_name}.vid",
                src_name = src.name
            )?;
        }
        conn.batch_execute(&query)?;

        Ok(TablePair { src, dst })
    }

    fn copy_final_entities(
        &self,
        conn: &PgConnection,
        reporter: &mut dyn PruneReporter,
        earliest_block: BlockNumber,
        final_block: BlockNumber,
        cancel: &CancelHandle,
    ) -> Result<usize, CancelableError<StoreError>> {
        #[derive(QueryableByName)]
        struct VidRange {
            #[sql_type = "Nullable<BigInt>"]
            min_vid: Option<i64>,
            #[sql_type = "Nullable<BigInt>"]
            max_vid: Option<i64>,
        }

        #[derive(QueryableByName)]
        struct LastVid {
            #[sql_type = "BigInt"]
            rows: i64,
            #[sql_type = "BigInt"]
            last_vid: i64,
        }

        let (min_vid, max_vid) = match sql_query(&format!(
            "select min(vid) as min_vid, max(vid) as max_vid from {src} \
              where coalesce(upper(block_range), 2147483647) > $1 \
                and coalesce(upper(block_range), 2147483647) <= $2",
            src = self.src.qualified_name
        ))
        .bind::<Integer, _>(earliest_block)
        .bind::<Integer, _>(final_block)
        .get_result::<VidRange>(conn)?
        {
            VidRange {
                min_vid: None,
                max_vid: None,
            } => {
                return Ok(0);
            }
            VidRange {
                min_vid: Some(min),
                max_vid: Some(max),
            } => (min, max),
            _ => unreachable!("min and max are Some or None at the same time"),
        };
        cancel.check_cancel()?;

        let column_list = self.column_list();

        let mut batch_size = AdaptiveBatchSize::new(&self.src);
        let mut next_vid = min_vid;
        let mut total_rows: usize = 0;
        loop {
            let start = Instant::now();
            let LastVid { last_vid, rows } = conn.transaction(|| {
                sql_query(&format!(
                    "with cp as (insert into {dst}({column_list}) \
                         select {column_list} from {src} \
                          where lower(block_range) <= $2 \
                            and coalesce(upper(block_range), 2147483647) > $1 \
                            and coalesce(upper(block_range), 2147483647) <= $2 \
                            and vid >= $3 \
                            and vid <= $4 \
                          order by vid \
                          limit $5 \
                          returning vid) \
                         select max(cp.vid) as last_vid, count(*) as rows from cp",
                    src = self.src.qualified_name,
                    dst = self.dst.qualified_name
                ))
                .bind::<Integer, _>(earliest_block)
                .bind::<Integer, _>(final_block)
                .bind::<BigInt, _>(next_vid)
                .bind::<BigInt, _>(max_vid)
                .bind::<BigInt, _>(&batch_size)
                .get_result::<LastVid>(conn)
            })?;
            cancel.check_cancel()?;

            total_rows += rows as usize;
            reporter.copy_final_batch(
                self.src.name.as_str(),
                rows as usize,
                total_rows,
                last_vid >= max_vid,
            );

            if last_vid >= max_vid {
                break;
            }

            batch_size.adapt(start.elapsed());
            next_vid = last_vid + 1;
        }

        Ok(total_rows)
    }

    fn copy_nonfinal_entities(
        &self,
        conn: &PgConnection,
        final_block: BlockNumber,
    ) -> Result<usize, StoreError> {
        let column_list = self.column_list();

        sql_query(&format!(
            "insert into {dst}({column_list}) \
             select {column_list} from {src} \
              where coalesce(upper(block_range), 2147483647) > $1 \
                and block_range && int4range($1, null) \
              order by vid",
            dst = self.dst.qualified_name,
            src = self.src.qualified_name,
        ))
        .bind::<Integer, _>(final_block)
        .execute(conn)
        .map_err(StoreError::from)
    }

    /// Replace the `src` table with the `dst` table. This makes sure (as
    /// does the rest of the code in `TablePair`) that the table and all
    /// associated objects (indexes, constraints, etc.) have the same names
    /// as they had initially so that pruning can be performed again in the
    /// future without any name clashes in the database.
    fn switch(self, conn: &PgConnection, layout: &Layout) -> Result<(), StoreError> {
        sql_query(&format!("drop table {}", self.src.qualified_name)).execute(conn)?;

        let uses_excl =
            catalog::has_exclusion_constraint(conn, &layout.site.namespace, &self.dst.name)?;
        let mut query = String::new();
        Table::rename_sql(&mut query, &layout, &self.dst, &self.src, uses_excl)?;
        self.src.create_time_travel_indexes(&mut query, layout)?;
        self.src.create_attribute_indexes(&mut query, layout)?;

        conn.batch_execute(&query)?;

        Ok(())
    }

    fn column_list(&self) -> String {
        self.src
            .column_names()
            .map(|name| format!("\"{name}\""))
            .join(", ")
    }
}

impl Layout {
    /// Remove all data from the underlying deployment that is not needed to
    /// respond to queries before block `earliest_block`. The strategy
    /// implemented here works well for situations in which pruning will
    /// remove a large amount of data from the subgraph (at least 50%)
    ///
    /// Blocks before `final_block` are considered final and it is assumed
    /// that they will not be modified in any way while pruning is running.
    /// Only tables where the ratio of entities to entity versions is below
    /// `prune_ratio` will actually be pruned.
    ///
    /// The strategy for `prune_by_copying` is to copy all data that is
    /// needed to respond to queries at block heights at or after
    /// `earliest_block` to a new table and then to replace the existing
    /// tables with these new tables atomically in a transaction. Copying
    /// happens in two stages: we first copy data for final blocks without
    /// blocking writes, and then copy data for nonfinal blocks. The latter
    /// blocks writes by taking a lock on the row for the deployment in
    /// `subgraph_deployment` (via `deployment::lock`) The process for
    /// switching to the new tables needs to take the naming of various
    /// database objects that Postgres creates automatically into account so
    /// that they all have the same names as the original objects to ensure
    /// that pruning can be done again without risking name clashes.
    ///
    /// The reason this strategy works well when a lot (or even the
    /// majority) of the data needs to be removed is that in the more
    /// straightforward strategy of simply deleting unneeded data, accessing
    /// the remaining data becomes very inefficient since it is scattered
    /// over a large number of pages, often with just one row per page. We
    /// would therefore need to do a full vacuum of the tables after
    /// deleting which effectively copies the remaining data into new
    /// tables. But a full vacuum takes an `access exclusive` lock which
    /// prevents both reads and writes to the table, which means it would
    /// also block queries to the deployment, often for extended periods of
    /// time. The `prune_by_copying` strategy never blocks reads, it only
    /// ever blocks writes.
    pub fn prune_by_copying(
        &self,
        _logger: &Logger,
        reporter: &mut dyn PruneReporter,
        conn: &PgConnection,
        earliest_block: BlockNumber,
        final_block: BlockNumber,
        prune_ratio: f64,
        cancel: &CancelHandle,
    ) -> Result<(), CancelableError<StoreError>> {
        // Analyze all tables and get statistics for them
        let mut tables: Vec<_> = self.tables.values().collect();
        reporter.start_analyze();
        tables.sort_by_key(|table| table.name.as_str());
        for table in tables {
            reporter.start_analyze_table(table.name.as_str());
            table.analyze(conn)?;
            reporter.finish_analyze_table(table.name.as_str());
            cancel.check_cancel()?;
        }
        let stats = catalog::stats(conn, &self.site.namespace)?;
        reporter.finish_analyze(stats.as_slice());

        // Determine which tables are prunable and create a shadow table for
        // them via `TablePair::create`
        let prunable_tables = {
            let mut prunable_tables: Vec<TablePair> = self
                .tables
                .values()
                .filter_map(|table| {
                    stats
                        .iter()
                        .find(|s| s.tablename == table.name.as_str())
                        .map(|s| (table, s))
                })
                .filter(|(_, stats)| stats.ratio <= prune_ratio)
                .map(|(table, _)| TablePair::create(conn, self, table.cheap_clone()))
                .collect::<Result<_, _>>()?;
            prunable_tables.sort_by(|a, b| a.src.name.as_str().cmp(b.src.name.as_str()));
            prunable_tables
        };
        cancel.check_cancel()?;

        // Copy final entities. This can happen in parallel to indexing as
        // that part of the table will not change
        reporter.copy_final_start(earliest_block, final_block);
        for table in &prunable_tables {
            table.copy_final_entities(conn, reporter, earliest_block, final_block, cancel)?;
        }
        reporter.copy_final_finish();

        let prunable_src: Vec<_> = prunable_tables
            .iter()
            .map(|table| table.src.clone())
            .collect();

        // Copy nonfinal entities, and replace the original `src` table with
        // the smaller `dst` table
        reporter.start_switch();
        conn.transaction(|| -> Result<(), CancelableError<StoreError>> {
            //  see also: deployment-lock-for-update
            deployment::lock(conn, &self.site)?;

            for table in &prunable_tables {
                reporter.copy_nonfinal_start(table.src.name.as_str());
                let rows = table.copy_nonfinal_entities(conn, final_block)?;
                reporter.copy_nonfinal_finish(table.src.name.as_str(), rows);
                cancel.check_cancel()?;
            }

            for table in prunable_tables {
                table.switch(conn, self)?;
                cancel.check_cancel()?;
            }

            Ok(())
        })?;
        reporter.finish_switch();

        // Analyze the new tables
        reporter.start_analyze();
        for table in &prunable_src {
            reporter.start_analyze_table(table.name.as_str());
            table.analyze(conn)?;
            reporter.finish_analyze_table(table.name.as_str());
            cancel.check_cancel()?;
        }
        let stats: Vec<_> = catalog::stats(conn, &self.site.namespace)?
            .into_iter()
            .filter(|s| {
                prunable_src
                    .iter()
                    .find(|table| table.name.as_str() == s.tablename)
                    .is_some()
            })
            .collect();
        reporter.finish_analyze(stats.as_slice());

        reporter.finish_prune();

        Ok(())
    }
}
