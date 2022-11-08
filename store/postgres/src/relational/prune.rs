use std::{fmt::Write, sync::Arc, time::Instant};

use diesel::{
    connection::SimpleConnection,
    sql_query,
    sql_types::{BigInt, Integer},
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

use super::{Layout, Namespace};

/// Utility to copy relevant data out of a source table and into a new
/// destination table and replace the source table with the destination
/// table
struct TablePair {
    // The original unpruned table
    src: Arc<Table>,
    // The temporary table to which we copy the data we'd like to keep. It
    // has the same name as `src` but is in a different namespace
    dst: Arc<Table>,
    src_nsp: Namespace,
    dst_nsp: Namespace,
}

impl TablePair {
    /// Create a `TablePair` for `src`. This creates a new table `dst` with
    /// the same structure as the `src` table in the database, but in a
    /// different namespace so that the names of indexes etc. don't clash
    fn create(
        conn: &PgConnection,
        src: Arc<Table>,
        src_nsp: Namespace,
        dst_nsp: Namespace,
    ) -> Result<Self, StoreError> {
        let dst = src.new_like(&dst_nsp, &src.name);

        let mut query = String::new();
        if catalog::table_exists(conn, dst_nsp.as_str(), &dst.name)? {
            writeln!(query, "truncate table {};", dst.qualified_name)?;
        } else {
            dst.as_ddl(&mut query)?;
        }
        conn.batch_execute(&query)?;

        Ok(TablePair {
            src,
            dst,
            src_nsp,
            dst_nsp,
        })
    }

    /// Copy all entity versions visible between `earliest_block` and
    /// `final_block` in batches, where each batch is a separate transaction
    fn copy_final_entities(
        &self,
        conn: &PgConnection,
        reporter: &mut dyn PruneReporter,
        earliest_block: BlockNumber,
        final_block: BlockNumber,
        cancel: &CancelHandle,
    ) -> Result<usize, CancelableError<StoreError>> {
        #[derive(QueryableByName)]
        struct LastVid {
            #[sql_type = "BigInt"]
            rows: i64,
            #[sql_type = "BigInt"]
            last_vid: i64,
        }

        let column_list = self.column_list();

        let mut batch_size = AdaptiveBatchSize::new(&self.src);
        // The first vid we still need to copy. When we start, we start with
        // 0 so that we don't constrain the set of rows based on their vid
        let mut next_vid = 0;
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
                            and block_range && int4range($1, $2, '[]') \
                            and vid >= $3 \
                          order by vid \
                          limit $4 \
                          returning vid) \
                         select coalesce(max(cp.vid), 0) as last_vid, count(*) as rows from cp",
                    src = self.src.qualified_name,
                    dst = self.dst.qualified_name
                ))
                .bind::<Integer, _>(earliest_block)
                .bind::<Integer, _>(final_block)
                .bind::<BigInt, _>(next_vid)
                .bind::<BigInt, _>(&batch_size)
                .get_result::<LastVid>(conn)
            })?;
            cancel.check_cancel()?;

            total_rows += rows as usize;
            let done = rows == 0;
            reporter.copy_final_batch(self.src.name.as_str(), rows as usize, total_rows, done);

            if done {
                return Ok(total_rows);
            }

            batch_size.adapt(start.elapsed());
            next_vid = last_vid + 1;
        }
    }

    /// Copy all entity versions visible after `final_block` in batches,
    /// where each batch is a separate transaction
    fn copy_nonfinal_entities(
        &self,
        conn: &PgConnection,
        reporter: &mut dyn PruneReporter,
        final_block: BlockNumber,
    ) -> Result<usize, StoreError> {
        let column_list = self.column_list();

        #[derive(QueryableByName)]
        struct LastVid {
            #[sql_type = "BigInt"]
            rows: i64,
            #[sql_type = "BigInt"]
            last_vid: i64,
        }

        let mut batch_size = AdaptiveBatchSize::new(&self.src);
        // The first vid we still need to copy. When we start, we start with
        // 0 so that we don't constrain the set of rows based on their vid
        let mut next_vid = 0;
        let mut total_rows = 0;
        loop {
            let start = Instant::now();
            let LastVid { rows, last_vid } = conn.transaction(|| {
                sql_query(&format!(
                    "with cp as (insert into {dst}({column_list}) \
                         select {column_list} from {src} \
                          where coalesce(upper(block_range), 2147483647) > $1 \
                            and block_range && int4range($1, null) \
                            and vid >= $2 \
                          order by vid \
                          limit $3
                          returning vid) \
                         select coalesce(max(cp.vid), 0) as last_vid, count(*) as rows from cp",
                    dst = self.dst.qualified_name,
                    src = self.src.qualified_name,
                ))
                .bind::<Integer, _>(final_block)
                .bind::<BigInt, _>(next_vid)
                .bind::<BigInt, _>(&batch_size)
                .get_result::<LastVid>(conn)
                .map_err(StoreError::from)
            })?;
            total_rows += rows as usize;
            reporter.copy_nonfinal_batch(
                self.src.name.as_str(),
                rows as usize,
                total_rows,
                rows == 0,
            );
            if rows == 0 {
                return Ok(total_rows);
            }
            batch_size.adapt(start.elapsed());
            next_vid = last_vid + 1;
        }
    }

    /// Replace the `src` table with the `dst` table
    fn switch(self, conn: &PgConnection) -> Result<(), StoreError> {
        let src_qname = &self.src.qualified_name;
        let dst_qname = &self.dst.qualified_name;
        let src_nsp = &self.src_nsp;
        let dst_nsp = &self.dst_nsp;

        let vid_seq = format!("{}_{VID_COLUMN}_seq", self.src.name);

        let mut query = String::new();

        // Make sure the vid sequence continues from where it was
        writeln!(
            query,
            "select setval('{dst_nsp}.{vid_seq}', nextval('{src_nsp}.{vid_seq}'));"
        )?;

        writeln!(query, "drop table {src_qname};")?;
        writeln!(query, "alter table {dst_qname} set schema {src_nsp}")?;
        conn.transaction(|| conn.batch_execute(&query))?;

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
        let dst_nsp = Namespace::prune(self.site.id);
        let prunable_tables = conn.transaction(|| -> Result<_, StoreError> {
            catalog::recreate_schema(conn, dst_nsp.as_str())?;

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
                .map(|(table, _)| {
                    TablePair::create(
                        conn,
                        table.cheap_clone(),
                        self.site.namespace.clone(),
                        dst_nsp.clone(),
                    )
                })
                .collect::<Result<_, _>>()?;
            prunable_tables.sort_by(|a, b| a.src.name.as_str().cmp(b.src.name.as_str()));
            Ok(prunable_tables)
        })?;
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
        //  see also: deployment-lock-for-update
        deployment::with_lock(conn, &self.site, || -> Result<_, StoreError> {
            for table in &prunable_tables {
                reporter.copy_nonfinal_start(table.src.name.as_str());
                table.copy_nonfinal_entities(conn, reporter, final_block)?;
                cancel.check_cancel().map_err(CancelableError::from)?;
            }

            for table in prunable_tables {
                conn.transaction(|| table.switch(conn))?;
                cancel.check_cancel().map_err(CancelableError::from)?;
            }

            Ok(())
        })?;
        reporter.finish_switch();

        // Get rid of the temporary prune schema
        catalog::drop_schema(conn, dst_nsp.as_str())?;

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
