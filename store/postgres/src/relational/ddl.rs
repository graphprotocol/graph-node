use std::{
    fmt::{self, Write},
    iter,
};

use graph::{prelude::ENV_VARS, schema::InputSchema};

use crate::relational::{
    BLOCK_COLUMN, BLOCK_RANGE_COLUMN, BYTE_ARRAY_PREFIX_SIZE, ColumnType, STRING_PREFIX_SIZE,
    VID_COLUMN,
};
use crate::{block_range::CAUSALITY_REGION_COLUMN, relational::index::Cond};

use super::{
    Column, Layout, SqlName, Table,
    index::{CreateIndex, Expr, IndexCreator, Method, PrefixKind},
};

// In debug builds (for testing etc.) unconditionally create exclusion constraints, in release
// builds for production, skip them
#[cfg(debug_assertions)]
const CREATE_EXCLUSION_CONSTRAINT: bool = true;
#[cfg(not(debug_assertions))]
const CREATE_EXCLUSION_CONSTRAINT: bool = false;

impl Layout {
    /// Generate the DDL for the entire layout, i.e., all `create table`
    /// and `create index` etc. statements needed in the database schema
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();

        // Output enums first so table definitions can reference them
        self.write_enum_ddl(&mut out)?;

        // We sort tables here solely because the unit tests rely on
        // 'create table' statements appearing in a fixed order
        let mut tables = self.tables.values().collect::<Vec<_>>();
        tables.sort_by_key(|table| table.position);
        // Output 'create table' statements for all tables
        let creat = self.index_creator(false, false);
        for table in tables {
            table.as_ddl(&self.input_schema, &creat, &mut out)?;
        }

        Ok(out)
    }

    pub(crate) fn write_enum_ddl(&self, out: &mut dyn Write) -> Result<(), fmt::Error> {
        for name in self.input_schema.enum_types() {
            let values = self.input_schema.enum_values(name).unwrap();
            let mut sep = "";
            let name = SqlName::from(name);
            write!(
                out,
                "create type {}.{}\n    as enum (",
                self.catalog.site.namespace,
                name.quoted()
            )?;
            for value in values.iter() {
                write!(out, "{}'{}'", sep, value)?;
                sep = ", "
            }
            writeln!(out, ");")?;
        }
        Ok(())
    }
}

impl Table {
    /// Return an iterator over all the column names of this table
    ///
    // This needs to stay in sync with `create_table`
    pub(crate) fn column_names(&self) -> impl Iterator<Item = &str> {
        let block_column = if self.immutable {
            BLOCK_COLUMN
        } else {
            BLOCK_RANGE_COLUMN
        };
        let data_cols = self.columns.iter().map(|col| col.name.as_str());
        iter::once(VID_COLUMN)
            .chain(data_cols)
            .chain(iter::once(block_column))
    }

    // Changes to this function require changing `column_names`, too
    pub(crate) fn create_table(&self, out: &mut String) -> fmt::Result {
        fn columns_ddl(table: &Table) -> Result<String, fmt::Error> {
            let mut cols = String::new();
            let mut first = true;

            if table.has_causality_region {
                first = false;
                write!(
                    cols,
                    "{causality_region}     int not null",
                    causality_region = CAUSALITY_REGION_COLUMN
                )?;
            }

            for column in &table.columns {
                if !first {
                    writeln!(cols, ",")?;
                    write!(cols, "        ")?;
                }
                column.as_ddl(&mut cols)?;
                first = false;
            }

            Ok(cols)
        }

        let vid_type = if self.object.has_vid_seq() {
            "bigint"
        } else {
            "bigserial"
        };

        if self.immutable {
            writeln!(
                out,
                "
    create table {qname} (
        {vid}                  {vid_type} primary key,
        {block}                int not null,\n\
        {cols},
        unique({id})
    );",
                qname = self.qualified_name,
                cols = columns_ddl(self)?,
                vid = VID_COLUMN,
                vid_type = vid_type,
                block = BLOCK_COLUMN,
                id = self.primary_key().name
            )
        } else {
            writeln!(
                out,
                r#"
    create table {qname} (
        {vid}                  {vid_type} primary key,
        {block_range}          int4range not null,
        {cols}
    );"#,
                qname = self.qualified_name,
                cols = columns_ddl(self)?,
                vid = VID_COLUMN,
                vid_type = vid_type,
                block_range = BLOCK_RANGE_COLUMN
            )?;

            self.exclusion_ddl(out)
        }
    }

    /// Create a `CreateIndex` for an index on this table with the given
    /// name over the given columns. The index will be a non-unique BTree
    /// index
    fn create_index(&self, name: &str, columns: Vec<Expr>) -> CreateIndex {
        CreateIndex::create(
            name,
            &format!("\"{}\"", self.nsp),
            &self.name.quoted(),
            false,
            Method::BTree,
            columns,
            None,
            None,
        )
    }

    fn time_travel_indexes(&self) -> Vec<CreateIndex> {
        let mut idxs = Vec::new();
        if self.immutable {
            // For immutable entities, a simple BTree on block$ is sufficient
            let idx = self.create_index(&format!("{}_block", self.name), vec![Expr::Block]);
            idxs.push(idx);
        } else {
            // Add a BRIN index on the block_range bounds to exploit the fact
            // that block ranges closely correlate with where in a table an
            // entity appears physically. This index is incredibly efficient for
            // reverts where we look for very recent blocks, so that this index
            // is highly selective. See https://github.com/graphprotocol/graph-node/issues/1415#issuecomment-630520713
            // for details on one experiment.
            //
            // We do not index the `block_range` as a whole, but rather the lower
            // and upper bound separately, since experimentation has shown that
            // Postgres will not use the index on `block_range` for clauses like
            // `block_range @> $block` but rather falls back to a full table scan.
            //
            // We also make sure that we do not put `NULL` in the index for
            // the upper bound since nulls can not be compared to anything and
            // will make the index less effective.
            //
            // To make the index usable, queries need to have clauses using
            // `lower(block_range)` and `coalesce(..)` verbatim.
            //
            // We also index `vid` as that correlates with the order in which
            // entities are stored.

            let idx = self
                .create_index(
                    &format!("brin_{table_name}", table_name = self.name),
                    vec![Expr::BlockRangeLower, Expr::BlockRangeUpper, Expr::Vid],
                )
                .method(Method::Brin);
            idxs.push(idx);

            // Add a BTree index that helps with the `RevertClampQuery` by making
            // it faster to find entity versions that have been modified
            let idx = self
                .create_index(
                    &format!("{table_name}_block_range_closed", table_name = self.name),
                    vec![Expr::BlockRangeUpper],
                )
                .cond(Cond::Closed);
            idxs.push(idx);
        }
        idxs
    }

    pub fn calculate_index_method_and_expression(column: &Column) -> (String, String) {
        let index_expr = if column.use_prefix_comparison {
            match column.column_type {
                ColumnType::String => {
                    format!("left({}, {})", column.name.quoted(), STRING_PREFIX_SIZE)
                }
                ColumnType::Bytes => format!(
                    "substring({}, 1, {})",
                    column.name.quoted(),
                    BYTE_ARRAY_PREFIX_SIZE
                ),
                // Handle other types if necessary, or maintain the unreachable statement
                _ => unreachable!("only String and Bytes can have arbitrary size"),
            }
        } else {
            column.name.quoted()
        };

        let method = if column.is_list() || column.is_fulltext() {
            "gin".to_string()
        } else {
            "btree".to_string()
        };

        (method, index_expr)
    }

    fn columns_to_index(&self) -> impl Iterator<Item = &Column> {
        // Skip columns whose type is an array of enum, since there is no
        // good way to index them with Postgres 9.6. Once we move to
        // Postgres 11, we can enable that (tracked in graph-node issue
        // #1330)
        let not_enum_list = |col: &&Column| !(col.is_list() && col.is_enum());

        // We create a unique index on `id` in `create_table`
        // and don't need an explicit attribute index
        let not_immutable_pk = |col: &&Column| !(self.immutable && col.is_primary_key());

        // GIN indexes on numeric types are not very useful, but expensive
        // to build
        let not_numeric_list = |col: &&Column| {
            !(col.is_list()
                && [ColumnType::BigDecimal, ColumnType::BigInt, ColumnType::Int]
                    .contains(&col.column_type))
        };

        self.columns
            .iter()
            .filter(not_enum_list)
            .filter(not_immutable_pk)
            .filter(not_numeric_list)
    }

    /// Return the index method and expressions for an attribute column.
    fn attr_index_spec(immutable: bool, column: &Column) -> (Method, Vec<Expr>) {
        if column.is_reference() && !column.is_list() {
            if immutable {
                (
                    Method::BTree,
                    vec![Expr::Column(column.name.as_str().to_string()), Expr::Block],
                )
            } else {
                (
                    Method::Gist,
                    vec![
                        Expr::Column(column.name.as_str().to_string()),
                        Expr::BlockRange,
                    ],
                )
            }
        } else if column.use_prefix_comparison {
            match column.column_type {
                ColumnType::String => (
                    Method::BTree,
                    vec![Expr::Prefix(
                        column.name.as_str().to_string(),
                        PrefixKind::Left,
                    )],
                ),
                ColumnType::Bytes => (
                    Method::BTree,
                    vec![Expr::Prefix(
                        column.name.as_str().to_string(),
                        PrefixKind::Substring,
                    )],
                ),
                _ => unreachable!("only String and Bytes can have arbitrary size"),
            }
        } else if column.is_list() || column.is_fulltext() {
            (
                Method::Gin,
                vec![Expr::Column(column.name.as_str().to_string())],
            )
        } else {
            (
                Method::BTree,
                vec![Expr::Column(column.name.as_str().to_string())],
            )
        }
    }

    fn add_attribute_indexes(&self, indexes: &mut Vec<CreateIndex>) {
        for (column_index, column) in self.columns_to_index().enumerate() {
            if column.is_list() && !ENV_VARS.store.create_gin_indexes {
                continue;
            }
            let (method, columns) = Self::attr_index_spec(self.immutable, column);
            let name = format!(
                "attr_{}_{}_{}_{}",
                self.position, column_index, self.name, column.name
            );
            indexes.push(CreateIndex::create(
                &name,
                &format!("\"{}\"", self.nsp),
                &self.name.quoted(),
                false,
                method,
                columns,
                None,
                None,
            ));
        }
    }

    /// Add an index `<table>_dims` for aggregation tables if the
    /// aggregation has cumulative aggregates; the index is needed to speed
    /// up the rollup query as that has to, for cumulative aggregates,
    /// access this table when we calculate the aggregations for the next
    /// bucket.
    ///
    /// The index we create is made up of the dimensions of the aggregation
    /// plus the `timestamp` column as the last entry in the index to make
    /// it usable for rollup queries.
    ///
    /// see also: #rollup-query-indexing for the exact query
    fn add_aggregate_indexes(
        &self,
        schema: &InputSchema,
        indexes: &mut Vec<CreateIndex>,
    ) -> Result<(), fmt::Error> {
        let agg = schema
            .agg_mappings()
            .find(|mapping| mapping.agg_type(schema) == self.object)
            .map(|mapping| mapping.aggregation(schema))
            .filter(|agg| agg.aggregates.iter().any(|a| a.cumulative))
            .filter(|agg| agg.dimensions().count() > 0);

        let Some(agg) = agg else {
            return Ok(());
        };

        let mut columns: Vec<Expr> = agg
            .dimensions()
            .map(|dim| {
                self.column_for_field(&dim.name)
                    .map(|col| Expr::Column(col.name.as_str().to_string()))
                    .map_err(|_| fmt::Error)
            })
            .collect::<Result<_, _>>()?;
        columns.push(Expr::Column("timestamp".to_string()));

        let name = format!("{}_dims", self.name);
        indexes.push(CreateIndex::create(
            &name,
            &format!("\"{}\"", self.nsp),
            &self.name.quoted(),
            false,
            Method::BTree,
            columns,
            None,
            None,
        ));
        Ok(())
    }

    /// Return all indexes for this table as `CreateIndex` objects
    pub(crate) fn indexes(&self, schema: &InputSchema) -> Result<Vec<CreateIndex>, fmt::Error> {
        let mut indexes = Vec::new();
        indexes.extend(self.time_travel_indexes());
        self.add_attribute_indexes(&mut indexes);
        self.add_aggregate_indexes(schema, &mut indexes)?;
        Ok(indexes)
    }

    /// Generate the DDL for one table, i.e. one `create table` statement
    /// and all `create index` statements for the table's columns
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    pub(crate) fn as_ddl(
        &self,
        schema: &InputSchema,
        creat: &IndexCreator,
        out: &mut String,
    ) -> fmt::Result {
        self.create_table(out)?;
        for idx in self.indexes(schema)? {
            if !idx.to_postpone() {
                writeln!(out, "{};", creat.to_sql(&idx)?)?;
            }
        }
        Ok(())
    }

    pub fn exclusion_ddl(&self, out: &mut String) -> fmt::Result {
        // Tables with causality regions need to use exclusion constraints for correctness,
        // to catch violations of write isolation.
        let as_constraint = self.has_causality_region || CREATE_EXCLUSION_CONSTRAINT;

        self.exclusion_ddl_inner(out, as_constraint)
    }

    // `pub` for tests.
    pub(crate) fn exclusion_ddl_inner(&self, out: &mut String, as_constraint: bool) -> fmt::Result {
        if as_constraint {
            writeln!(
                out,
                "
    alter table {qname}
        add constraint {bare_name}_{id}_{block_range}_excl exclude using gist ({id} with =, {block_range} with &&);",
                qname = self.qualified_name,
                bare_name = self.name,
                id = self.primary_key().name,
                block_range = BLOCK_RANGE_COLUMN
            )?;
        } else {
            writeln!(
                out,
                "
        create index {bare_name}_{id}_{block_range}_excl on {qname}
         using gist ({id}, {block_range});
               ",
                qname = self.qualified_name,
                bare_name = self.name,
                id = self.primary_key().name,
                block_range = BLOCK_RANGE_COLUMN
            )?;
        }
        Ok(())
    }
}

impl Column {
    /// Generate the DDL for one column, i.e. the part of a `create table`
    /// statement for this column.
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String) -> fmt::Result {
        write!(out, "{:20} {}", self.name.quoted(), self.sql_type())?;
        if self.is_list() {
            write!(out, "[]")?;
        }
        if self.is_primary_key() || !self.is_nullable() {
            write!(out, " not null")?;
        }
        Ok(())
    }
}
