use std::{collections::HashMap, fmt::Write, sync::Arc};

use diesel::{
    connection::SimpleConnection,
    sql_query,
    sql_types::{BigInt, Integer},
    Connection, PgConnection, RunQueryDsl,
};
use graph::{
    components::store::{PrunePhase, PruneReporter, PruneRequest, PruningStrategy, VersionStats},
    prelude::{
        BlockNumber, CancelHandle, CancelToken, CancelableError, CheapClone, StoreError,
        BLOCK_NUMBER_MAX,
    },
    schema::InputSchema,
    slog::{warn, Logger},
};
use itertools::Itertools;

use crate::{
    catalog, deployment,
    relational::{Table, VID_COLUMN},
    vid_batcher::{VidBatcher, VidRange},
};

use super::{
    index::{load_indexes_from_table, CreateIndex, IndexList},
    Catalog, Layout, Namespace,
};

pub use status::{Phase, PruneState, PruneTableState, Viewer};

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
        conn: &mut PgConnection,
        src: Arc<Table>,
        src_nsp: Namespace,
        dst_nsp: Namespace,
        schema: &InputSchema,
        catalog: &Catalog,
    ) -> Result<Self, StoreError> {
        let dst = src.new_like(&dst_nsp, &src.name);

        let mut query = String::new();
        if catalog::table_exists(conn, dst_nsp.as_str(), &dst.name)? {
            writeln!(query, "truncate table {};", dst.qualified_name)?;
        } else {
            let mut list = IndexList {
                indexes: HashMap::new(),
            };
            let indexes = load_indexes_from_table(conn, &src, src_nsp.as_str())?
                .into_iter()
                .map(|index| index.with_nsp(dst_nsp.to_string()))
                .collect::<Result<Vec<CreateIndex>, _>>()?;
            list.indexes.insert(src.name.to_string(), indexes);

            // In case of pruning we don't do delayed creation of indexes,
            // as the asumption is that there is not that much data inserted.
            dst.as_ddl(schema, catalog, Some(&list), &mut query)?;
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
    /// `final_block` in batches, where each batch is a separate
    /// transaction. Write activity for nonfinal blocks can happen
    /// concurrently to this copy
    fn copy_final_entities(
        &self,
        conn: &mut PgConnection,
        reporter: &mut dyn PruneReporter,
        tracker: &status::Tracker,
        earliest_block: BlockNumber,
        final_block: BlockNumber,
        cancel: &CancelHandle,
    ) -> Result<(), CancelableError<StoreError>> {
        let column_list = self.column_list();

        // Determine the last vid that we need to copy
        let range = VidRange::for_prune(conn, &self.src, earliest_block, final_block)?;
        let mut batcher = VidBatcher::load(conn, &self.src_nsp, &self.src, range)?;
        tracker.start_copy_final(conn, &self.src, range)?;

        while !batcher.finished() {
            let (_, rows) = batcher.step(|start, end| {
                conn.transaction(|conn| {
                    // Page through all rows in `src` in batches of `batch_size`
                    // and copy the ones that are visible to queries at block
                    // heights between `earliest_block` and `final_block`, but
                    // whose block_range does not extend past `final_block`
                    // since they could still be reverted while we copy.
                    // The conditions on `block_range` are expressed redundantly
                    // to make more indexes useable
                    sql_query(format!(
                    "/* controller=prune,phase=final,start_vid={start},batch_size={batch_size} */ \
                     insert into {dst}({column_list}) \
                     select {column_list} from {src} \
                      where lower(block_range) <= $2 \
                        and coalesce(upper(block_range), 2147483647) > $1 \
                        and coalesce(upper(block_range), 2147483647) <= $2 \
                        and block_range && int4range($1, $2, '[]') \
                        and vid >= $3 and vid <= $4 \
                      order by vid",
                    src = self.src.qualified_name,
                    dst = self.dst.qualified_name,
                    batch_size = end - start + 1,
                ))
                    .bind::<Integer, _>(earliest_block)
                    .bind::<Integer, _>(final_block)
                    .bind::<BigInt, _>(start)
                    .bind::<BigInt, _>(end)
                    .execute(conn)
                    .map_err(StoreError::from)
                })
            })?;
            let rows = rows.unwrap_or(0);
            tracker.copy_final_batch(conn, &self.src, rows, &batcher)?;
            cancel.check_cancel()?;

            reporter.prune_batch(
                self.src.name.as_str(),
                rows,
                PrunePhase::CopyFinal,
                batcher.finished(),
            );
        }
        Ok(())
    }

    /// Copy all entity versions visible after `final_block` in batches,
    /// where each batch is a separate transaction. This assumes that all
    /// other write activity to the source table is blocked while we copy
    fn copy_nonfinal_entities(
        &self,
        conn: &mut PgConnection,
        reporter: &mut dyn PruneReporter,
        tracker: &status::Tracker,
        final_block: BlockNumber,
    ) -> Result<(), StoreError> {
        let column_list = self.column_list();

        // Determine the last vid that we need to copy
        let range = VidRange::for_prune(conn, &self.src, final_block + 1, BLOCK_NUMBER_MAX)?;
        let mut batcher = VidBatcher::load(conn, &self.src.nsp, &self.src, range)?;
        tracker.start_copy_nonfinal(conn, &self.src, range)?;

        while !batcher.finished() {
            let (_, rows) = batcher.step(|start, end| {
                // Page through all the rows in `src` in batches of
                // `batch_size` that are visible to queries at block heights
                // starting right after `final_block`. The conditions on
                // `block_range` are expressed redundantly to make more
                // indexes useable
                conn.transaction(|conn| {
                    sql_query(format!(
                        "/* controller=prune,phase=nonfinal,start_vid={start},batch_size={batch_size} */ \
                     insert into {dst}({column_list}) \
                     select {column_list} from {src} \
                      where coalesce(upper(block_range), 2147483647) > $1 \
                        and block_range && int4range($1, null) \
                        and vid >= $2 and vid <= $3 \
                      order by vid",
                        dst = self.dst.qualified_name,
                        src = self.src.qualified_name,
                        batch_size = end - start + 1,
                    ))
                    .bind::<Integer, _>(final_block)
                    .bind::<BigInt, _>(start)
                    .bind::<BigInt, _>(end)
                    .execute(conn)
                    .map_err(StoreError::from)
                })
            })?;
            let rows = rows.unwrap_or(0);

            tracker.copy_nonfinal_batch(conn, &self.src, rows as i64, &batcher)?;

            reporter.prune_batch(
                self.src.name.as_str(),
                rows,
                PrunePhase::CopyNonfinal,
                batcher.finished(),
            );
        }
        Ok(())
    }

    /// Replace the `src` table with the `dst` table
    fn switch(self, logger: &Logger, conn: &mut PgConnection) -> Result<(), StoreError> {
        let src_qname = &self.src.qualified_name;
        let dst_qname = &self.dst.qualified_name;
        let src_nsp = &self.src_nsp;
        let dst_nsp = &self.dst_nsp;

        let vid_seq = format!("{}_{VID_COLUMN}_seq", self.src.name);

        let mut query = String::new();

        // What we are about to do would get blocked by autovacuum on our
        // tables, so just kill the autovacuum
        if let Err(e) = catalog::cancel_vacuum(conn, src_nsp) {
            warn!(logger, "Failed to cancel vacuum during pruning; trying to carry on regardless";
                  "src" => src_nsp.as_str(), "error" => e.to_string());
        }

        // Make sure the vid sequence continues from where it was in case
        // that we use autoincrementing order of the DB
        if !self.src.object.has_vid_seq() {
            writeln!(
                query,
                "select setval('{dst_nsp}.{vid_seq}', nextval('{src_nsp}.{vid_seq}'));"
            )?;
        }

        writeln!(query, "drop table {src_qname};")?;
        writeln!(query, "alter table {dst_qname} set schema {src_nsp}")?;
        conn.transaction(|conn| conn.batch_execute(&query))?;

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
    /// Analyze the `tables` and return `VersionStats` for all tables in
    /// this `Layout`
    fn analyze_tables(
        &self,
        conn: &mut PgConnection,
        reporter: &mut dyn PruneReporter,
        mut tables: Vec<&Arc<Table>>,
        cancel: &CancelHandle,
    ) -> Result<Vec<VersionStats>, CancelableError<StoreError>> {
        reporter.start_analyze();
        tables.sort_by_key(|table| table.name.as_str());
        for table in &tables {
            reporter.start_analyze_table(table.name.as_str());
            table.analyze(conn)?;
            reporter.finish_analyze_table(table.name.as_str());
            cancel.check_cancel()?;
        }
        let stats = catalog::stats(conn, &self.site)?;

        let analyzed: Vec<_> = tables.iter().map(|table| table.name.as_str()).collect();
        reporter.finish_analyze(&stats, &analyzed);

        Ok(stats)
    }

    /// Return statistics for the tables in this `Layout`. If `analyze_all`
    /// is `true`, analyze all tables before getting statistics. If it is
    /// `false`, only analyze tables that Postgres' autovacuum daemon would
    /// consider needing analysis.
    fn version_stats(
        &self,
        conn: &mut PgConnection,
        reporter: &mut dyn PruneReporter,
        analyze_all: bool,
        cancel: &CancelHandle,
    ) -> Result<Vec<VersionStats>, CancelableError<StoreError>> {
        let needs_analyze = if analyze_all {
            vec![]
        } else {
            catalog::needs_autoanalyze(conn, &self.site.namespace)?
        };
        let tables: Vec<_> = self
            .tables
            .values()
            .filter(|table| analyze_all || needs_analyze.contains(&table.name))
            .collect();

        self.analyze_tables(conn, reporter, tables, cancel)
    }

    /// Return all tables and the strategy to prune them withir stats whose ratio of distinct entities
    /// to versions is less than `prune_ratio`
    fn prunable_tables(
        &self,
        stats: &[VersionStats],
        req: &PruneRequest,
    ) -> Vec<(&Arc<Table>, PruningStrategy)> {
        let mut prunable_tables = self
            .tables
            .values()
            .filter(|table| !table.immutable)
            .filter_map(|table| {
                stats
                    .iter()
                    .find(|stats| stats.tablename == table.name.as_str())
                    .map(|stats| (table, stats))
            })
            .filter_map(|(table, stats)| req.strategy(stats).map(|strat| (table, strat)))
            .collect::<Vec<_>>();
        prunable_tables.sort_by(|(a, _), (b, _)| a.name.as_str().cmp(b.name.as_str()));
        prunable_tables
    }

    /// Remove all data from the underlying deployment that is not needed to
    /// respond to queries before block `earliest_block`. The `req` is used
    /// to determine which strategy should be used for pruning, rebuild or
    /// delete.
    ///
    /// Blocks before `req.final_block` are considered final and it is
    /// assumed that they will not be modified in any way while pruning is
    /// running.
    ///
    /// The rebuild strategy implemented here works well for situations in
    /// which pruning will remove a large amount of data from the subgraph
    /// (say, at least 50%)
    ///
    /// The strategy for rebuilding is to copy all data that is needed to
    /// respond to queries at block heights at or after `earliest_block` to
    /// a new table and then to replace the existing tables with these new
    /// tables atomically in a transaction. Rebuilding happens in two stages
    /// that are performed for each table in turn: we first copy data for
    /// final blocks without blocking writes, and then copy data for
    /// nonfinal blocks. The latter blocks writes by taking an advisory lock
    /// on the deployment (via `deployment::lock`) The process for switching
    /// to the new tables needs to take the naming of various database
    /// objects that Postgres creates automatically into account so that
    /// they all have the same names as the original objects to ensure that
    /// pruning can be done again without risking name clashes.
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
    /// time. The rebuild strategy never blocks reads, it only ever blocks
    /// writes.
    pub fn prune(
        self: Arc<Self>,
        logger: &Logger,
        reporter: &mut dyn PruneReporter,
        conn: &mut PgConnection,
        req: &PruneRequest,
        cancel: &CancelHandle,
    ) -> Result<(), CancelableError<StoreError>> {
        let tracker = status::Tracker::new(conn, self.clone())?;

        reporter.start(req);

        let stats = self.version_stats(conn, reporter, true, cancel)?;

        let prunable_tables: Vec<_> = self.prunable_tables(&stats, req).into_iter().collect();
        tracker.start(conn, req, &prunable_tables)?;

        // create a shadow namespace where we will put the copies of our
        // tables, but only create it in the database if we really need it
        let dst_nsp = Namespace::prune(self.site.id);
        let mut recreate_dst_nsp = true;

        // Go table by table; note that the subgraph writer can write in
        // between the execution of the `with_lock` block below, and might
        // therefore work with tables where some are pruned and some are not
        // pruned yet. That does not affect correctness since we make no
        // assumption about where the subgraph head is. If the subgraph
        // advances during this loop, we might have an unnecessarily
        // pessimistic but still safe value for `final_block`. We do assume
        // that `final_block` is far enough from the subgraph head that it
        // stays final even if a revert happens during this loop, but that
        // is the definition of 'final'
        for (table, strat) in &prunable_tables {
            reporter.start_table(table.name.as_str());
            tracker.start_table(conn, table)?;
            match strat {
                PruningStrategy::Rebuild => {
                    if recreate_dst_nsp {
                        catalog::recreate_schema(conn, dst_nsp.as_str())?;
                        recreate_dst_nsp = false;
                    }
                    let pair = TablePair::create(
                        conn,
                        table.cheap_clone(),
                        self.site.namespace.clone(),
                        dst_nsp.clone(),
                        &self.input_schema,
                        &self.catalog,
                    )?;
                    // Copy final entities. This can happen in parallel to indexing as
                    // that part of the table will not change
                    pair.copy_final_entities(
                        conn,
                        reporter,
                        &tracker,
                        req.earliest_block,
                        req.final_block,
                        cancel,
                    )?;
                    // Copy nonfinal entities, and replace the original `src` table with
                    // the smaller `dst` table
                    // see also: deployment-lock-for-update
                    reporter.start_switch();
                    deployment::with_lock(conn, &self.site, |conn| -> Result<_, StoreError> {
                        pair.copy_nonfinal_entities(conn, reporter, &tracker, req.final_block)?;
                        cancel.check_cancel().map_err(CancelableError::from)?;

                        conn.transaction(|conn| pair.switch(logger, conn))?;
                        cancel.check_cancel().map_err(CancelableError::from)?;

                        Ok(())
                    })?;
                    reporter.finish_switch();
                }
                PruningStrategy::Delete => {
                    // Delete all entity versions whose range was closed
                    // before `req.earliest_block`
                    let range = VidRange::for_prune(conn, &table, 0, req.earliest_block)?;
                    let mut batcher = VidBatcher::load(conn, &self.site.namespace, &table, range)?;

                    tracker.start_delete(conn, table, range, &batcher)?;
                    while !batcher.finished() {
                        let (_, rows) = batcher.step(|start, end| {sql_query(format!(
                            "/* controller=prune,phase=delete,start_vid={start},batch_size={batch_size} */ \
                             delete from {qname} \
                                          where coalesce(upper(block_range), 2147483647) <= $1 \
                                            and vid >= $2 and vid <= $3",
                            qname = table.qualified_name,
                            batch_size = end - start + 1
                        ))
                        .bind::<Integer, _>(req.earliest_block)
                        .bind::<BigInt, _>(start)
                        .bind::<BigInt, _>(end)
                        .execute(conn).map_err(StoreError::from)})?;
                        let rows = rows.unwrap_or(0);

                        tracker.delete_batch(conn, table, rows, &batcher)?;

                        reporter.prune_batch(
                            table.name.as_str(),
                            rows,
                            PrunePhase::Delete,
                            batcher.finished(),
                        );
                    }
                }
            }
            reporter.finish_table(table.name.as_str());
            tracker.finish_table(conn, table)?;
        }
        // Get rid of the temporary prune schema if we actually created it
        if !recreate_dst_nsp {
            catalog::drop_schema(conn, dst_nsp.as_str())?;
        }

        for (table, _) in &prunable_tables {
            catalog::set_last_pruned_block(conn, &self.site, &table.name, req.earliest_block)?;
        }

        // Analyze the new tables
        let tables = prunable_tables.iter().map(|(table, _)| *table).collect();
        self.analyze_tables(conn, reporter, tables, cancel)?;

        reporter.finish();
        tracker.finish(conn)?;

        Ok(())
    }
}

mod status {
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use diesel::{
        deserialize::FromSql,
        dsl::insert_into,
        pg::{Pg, PgValue},
        query_builder::QueryFragment,
        serialize::{Output, ToSql},
        sql_types::Text,
        table, update, AsChangeset, Connection, ExpressionMethods as _, OptionalExtension,
        PgConnection, QueryDsl as _, RunQueryDsl as _,
    };
    use graph::{
        components::store::{PruneRequest, PruningStrategy, StoreResult},
        env::ENV_VARS,
        prelude::StoreError,
    };

    use crate::{
        relational::{Layout, Table},
        vid_batcher::{VidBatcher, VidRange},
        ConnectionPool,
    };

    table! {
        subgraphs.prune_state(vid) {
            vid -> Integer,
            // Deployment id (sgd)
            id -> Integer,
            run -> Integer,
            // The first block in the subgraph when the prune started
            first_block -> Integer,
            final_block -> Integer,
            latest_block -> Integer,
            // The amount of history configured
            history_blocks -> Integer,

            started_at -> Timestamptz,
            finished_at -> Nullable<Timestamptz>,
        }
    }

    table! {
        subgraphs.prune_table_state(vid) {
            vid -> Integer,
            // Deployment id (sgd)
            id -> Integer,
            run -> Integer,
            table_name -> Text,

            strategy -> Char,
            // see enum Phase
            phase -> Text,

            start_vid -> Nullable<BigInt>,
            final_vid -> Nullable<BigInt>,
            nonfinal_vid -> Nullable<BigInt>,
            rows -> Nullable<BigInt>,

            next_vid -> Nullable<BigInt>,
            batch_size -> Nullable<BigInt>,

            started_at -> Nullable<Timestamptz>,
            finished_at -> Nullable<Timestamptz>,
        }
    }

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow)]
    #[diesel(sql_type = Text)]
    pub enum Phase {
        Queued,
        Started,
        /// Only used when strategy is Rebuild
        CopyFinal,
        /// Only used when strategy is Rebuild
        CopyNonfinal,
        /// Only used when strategy is Delete
        Delete,
        Done,
        /// Not a real phase, indicates that the database has an invalid
        /// value
        Unknown,
    }

    impl Phase {
        pub fn from_str(phase: &str) -> Self {
            use Phase::*;
            match phase {
                "queued" => Queued,
                "started" => Started,
                "copy_final" => CopyFinal,
                "copy_nonfinal" => CopyNonfinal,
                "delete" => Delete,
                "done" => Done,
                _ => Unknown,
            }
        }

        pub fn as_str(&self) -> &str {
            use Phase::*;
            match self {
                Queued => "queued",
                Started => "started",
                CopyFinal => "copy_final",
                CopyNonfinal => "copy_nonfinal",
                Delete => "delete",
                Done => "done",
                Unknown => "*unknown*",
            }
        }
    }

    impl ToSql<diesel::sql_types::Text, Pg> for Phase {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
            let phase = self.as_str();
            <str as ToSql<Text, Pg>>::to_sql(phase, &mut out.reborrow())
        }
    }

    impl FromSql<Text, Pg> for Phase {
        fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
            Ok(Phase::from_str(std::str::from_utf8(bytes.as_bytes())?))
        }
    }

    /// Information about one pruning run for a deployment
    #[derive(Queryable)]
    pub struct PruneState {
        pub vid: i32,
        pub id: i32,
        pub run: i32,
        pub first_block: i32,
        pub final_block: i32,
        pub latest_block: i32,
        pub history_blocks: i32,

        pub started_at: DateTime<Utc>,
        pub finished_at: Option<DateTime<Utc>>,
    }

    /// Per-table information about the pruning run for a deployment
    #[derive(Queryable)]
    pub struct PruneTableState {
        pub vid: i32,
        pub id: i32,
        pub run: i32,
        pub table_name: String,

        // 'r' for rebuild or 'd' for delete
        pub strategy: String,
        pub phase: Phase,

        pub start_vid: Option<i64>,
        pub final_vid: Option<i64>,
        pub nonfinal_vid: Option<i64>,
        pub rows: Option<i64>,

        pub next_vid: Option<i64>,
        pub batch_size: Option<i64>,

        pub started_at: Option<DateTime<Utc>>,
        pub finished_at: Option<DateTime<Utc>>,
    }

    /// A helper to persist pruning progress in the database
    pub(super) struct Tracker {
        layout: Arc<Layout>,
        run: i32,
    }

    impl Tracker {
        pub(super) fn new(conn: &mut PgConnection, layout: Arc<Layout>) -> StoreResult<Self> {
            use prune_state as ps;
            let run = ps::table
                .filter(ps::id.eq(layout.site.id))
                .order(ps::run.desc())
                .select(ps::run)
                .get_result::<i32>(conn)
                .optional()
                .map_err(StoreError::from)?
                .unwrap_or(0)
                + 1;

            // Delete old prune state. Keep the initial run and the last
            // `prune_keep_history` runs (including this one)
            diesel::delete(ps::table)
                .filter(ps::id.eq(layout.site.id))
                .filter(ps::run.gt(1))
                .filter(ps::run.lt(run - (ENV_VARS.store.prune_keep_history - 1) as i32))
                .execute(conn)
                .map_err(StoreError::from)?;

            Ok(Tracker { layout, run })
        }

        pub(super) fn start(
            &self,
            conn: &mut PgConnection,
            req: &PruneRequest,
            prunable_tables: &[(&Arc<Table>, PruningStrategy)],
        ) -> StoreResult<()> {
            use prune_state as ps;
            use prune_table_state as pts;

            conn.transaction(|conn| {
                insert_into(ps::table)
                    .values((
                        ps::id.eq(self.layout.site.id),
                        ps::run.eq(self.run),
                        ps::first_block.eq(req.first_block),
                        ps::final_block.eq(req.final_block),
                        ps::latest_block.eq(req.latest_block),
                        ps::history_blocks.eq(req.history_blocks),
                        ps::started_at.eq(diesel::dsl::now),
                    ))
                    .execute(conn)?;

                for (table, strat) in prunable_tables {
                    let strat = match strat {
                        PruningStrategy::Rebuild => "r",
                        PruningStrategy::Delete => "d",
                    };
                    insert_into(pts::table)
                        .values((
                            pts::id.eq(self.layout.site.id),
                            pts::run.eq(self.run),
                            pts::table_name.eq(table.name.as_str()),
                            pts::strategy.eq(strat),
                            pts::phase.eq(Phase::Queued),
                        ))
                        .execute(conn)?;
                }
                Ok(())
            })
        }

        pub(crate) fn start_table(
            &self,
            conn: &mut PgConnection,
            table: &Table,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            self.update_table_state(
                conn,
                table,
                (
                    pts::started_at.eq(diesel::dsl::now),
                    pts::phase.eq(Phase::Started),
                ),
            )?;

            Ok(())
        }

        pub(crate) fn start_copy_final(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            range: VidRange,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::phase.eq(Phase::CopyFinal),
                pts::start_vid.eq(range.min),
                pts::next_vid.eq(range.min),
                pts::final_vid.eq(range.max),
                pts::rows.eq(0),
            );

            self.update_table_state(conn, table, values)
        }

        pub(crate) fn copy_final_batch(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            rows: usize,
            batcher: &VidBatcher,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::next_vid.eq(batcher.next_vid()),
                pts::batch_size.eq(batcher.batch_size() as i64),
                pts::rows.eq(pts::rows + (rows as i64)),
            );

            self.update_table_state(conn, table, values)
        }

        pub(crate) fn start_copy_nonfinal(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            range: VidRange,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::phase.eq(Phase::CopyNonfinal),
                pts::nonfinal_vid.eq(range.max),
            );
            self.update_table_state(conn, table, values)
        }

        pub(crate) fn copy_nonfinal_batch(
            &self,
            conn: &mut PgConnection,
            src: &Table,
            rows: i64,
            batcher: &VidBatcher,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::next_vid.eq(batcher.next_vid()),
                pts::batch_size.eq(batcher.batch_size() as i64),
                pts::rows.eq(pts::rows + rows),
            );

            self.update_table_state(conn, src, values)
        }

        pub(crate) fn finish_table(
            &self,
            conn: &mut PgConnection,
            table: &Table,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::finished_at.eq(diesel::dsl::now),
                pts::phase.eq(Phase::Done),
            );

            self.update_table_state(conn, table, values)
        }

        pub(crate) fn start_delete(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            range: VidRange,
            batcher: &VidBatcher,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::phase.eq(Phase::Delete),
                pts::start_vid.eq(range.min),
                pts::final_vid.eq(range.max),
                pts::nonfinal_vid.eq(range.max),
                pts::rows.eq(0),
                pts::next_vid.eq(range.min),
                pts::batch_size.eq(batcher.batch_size() as i64),
            );

            self.update_table_state(conn, table, values)
        }

        pub(crate) fn delete_batch(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            rows: usize,
            batcher: &VidBatcher,
        ) -> StoreResult<()> {
            use prune_table_state as pts;

            let values = (
                pts::next_vid.eq(batcher.next_vid()),
                pts::batch_size.eq(batcher.batch_size() as i64),
                pts::rows.eq(pts::rows - (rows as i64)),
            );

            self.update_table_state(conn, table, values)
        }

        fn update_table_state<V, C>(
            &self,
            conn: &mut PgConnection,
            table: &Table,
            values: V,
        ) -> StoreResult<()>
        where
            V: AsChangeset<Target = prune_table_state::table, Changeset = C>,
            C: QueryFragment<diesel::pg::Pg>,
        {
            use prune_table_state as pts;

            update(pts::table)
                .filter(pts::id.eq(self.layout.site.id))
                .filter(pts::run.eq(self.run))
                .filter(pts::table_name.eq(table.name.as_str()))
                .set(values)
                .execute(conn)?;
            Ok(())
        }

        pub(crate) fn finish(&self, conn: &mut PgConnection) -> StoreResult<()> {
            use prune_state as ps;

            update(ps::table)
                .filter(ps::id.eq(self.layout.site.id))
                .filter(ps::run.eq(self.run))
                .set((ps::finished_at.eq(diesel::dsl::now),))
                .execute(conn)?;
            Ok(())
        }
    }

    /// A helper to read pruning progress from the database
    pub struct Viewer {
        pool: ConnectionPool,
        layout: Arc<Layout>,
    }

    impl Viewer {
        pub fn new(pool: ConnectionPool, layout: Arc<Layout>) -> Self {
            Self { pool, layout }
        }

        pub fn runs(&self) -> StoreResult<Vec<usize>> {
            use prune_state as ps;

            let mut conn = self.pool.get()?;
            let runs = ps::table
                .filter(ps::id.eq(self.layout.site.id))
                .select(ps::run)
                .order(ps::run.asc())
                .load::<i32>(&mut conn)
                .map_err(StoreError::from)?;
            let runs = runs.into_iter().map(|run| run as usize).collect::<Vec<_>>();
            Ok(runs)
        }

        pub fn state(&self, run: usize) -> StoreResult<Option<(PruneState, Vec<PruneTableState>)>> {
            use prune_state as ps;
            use prune_table_state as pts;

            let mut conn = self.pool.get()?;

            let ptss = pts::table
                .filter(pts::id.eq(self.layout.site.id))
                .filter(pts::run.eq(run as i32))
                .order(pts::table_name.asc())
                .load::<PruneTableState>(&mut conn)
                .map_err(StoreError::from)?;

            ps::table
                .filter(ps::id.eq(self.layout.site.id))
                .filter(ps::run.eq(run as i32))
                .first::<PruneState>(&mut conn)
                .optional()
                .map_err(StoreError::from)
                .map(|state| state.map(|state| (state, ptss)))
        }
    }
}
