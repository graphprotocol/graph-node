use std::fmt::{self, Write};

use graph::prelude::BLOCK_NUMBER_MAX;

use crate::relational::{
    ColumnType, BLOCK_COLUMN, BLOCK_RANGE_COLUMN, BYTE_ARRAY_PREFIX_SIZE, STRING_PREFIX_SIZE,
    VID_COLUMN,
};

use super::{Column, Layout, SqlName, Table};

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
        for table in tables {
            table.as_ddl(&mut out, self)?;
        }

        Ok(out)
    }

    pub(crate) fn write_enum_ddl(&self, out: &mut dyn Write) -> Result<(), fmt::Error> {
        for (name, values) in &self.enums {
            let mut sep = "";
            let name = SqlName::from(name.as_str());
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
    /// Generate the DDL for one table, i.e. one `create table` statement
    /// and all `create index` statements for the table's columns
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String, layout: &Layout) -> fmt::Result {
        fn columns_ddl(table: &Table) -> Result<String, fmt::Error> {
            let mut cols = String::new();
            let mut first = true;
            for column in &table.columns {
                if !first {
                    writeln!(cols, ",")?;
                } else {
                    writeln!(cols)?;
                }
                write!(cols, "    ")?;
                column.as_ddl(&mut cols)?;
                first = false;
            }
            Ok(cols)
        }

        fn create_table(table: &Table, out: &mut String, layout: &Layout) -> fmt::Result {
            if table.immutable {
                writeln!(
                    out,
                    r#"
                create table {nsp}.{name} (
                    {vid}                  bigserial primary key,
                    {block}                int not null,
                    {cols},
                    unique({id})
                );
                "#,
                    nsp = layout.catalog.site.namespace,
                    name = table.name.quoted(),
                    cols = columns_ddl(table)?,
                    vid = VID_COLUMN,
                    block = BLOCK_COLUMN,
                    id = table.primary_key().name
                )
            } else {
                writeln!(
                    out,
                    r#"
                create table {nsp}.{name} (
                    {vid}                  bigserial primary key,
                    {block_range}          int4range not null,
                    {cols}
                );
                "#,
                    nsp = layout.catalog.site.namespace,
                    name = table.name.quoted(),
                    cols = columns_ddl(table)?,
                    vid = VID_COLUMN,
                    block_range = BLOCK_RANGE_COLUMN
                )?;

                table.exclusion_ddl(
                    out,
                    layout.catalog.site.namespace.as_str(),
                    layout.catalog.create_exclusion_constraint(),
                )
            }
        }

        fn create_time_travel_indexes(
            table: &Table,
            out: &mut String,
            layout: &Layout,
        ) -> fmt::Result {
            if table.immutable {
                write!(
                    out,
                    "create index brin_{table_name}\n    \
                    on {schema_name}.{table_name}\n \
                       using brin({block}, vid);\n",
                    table_name = table.name,
                    schema_name = layout.catalog.site.namespace,
                    block = BLOCK_COLUMN
                )
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
                write!(out,"create index brin_{table_name}\n    \
                    on {schema_name}.{table_name}\n \
                       using brin(lower(block_range), coalesce(upper(block_range), {block_max}), vid);\n",
                    table_name = table.name,
                    schema_name = layout.catalog.site.namespace,
                    block_max = BLOCK_NUMBER_MAX)?;

                // Add a BTree index that helps with the `RevertClampQuery` by making
                // it faster to find entity versions that have been modified
                write!(
                    out,
                    "create index {table_name}_block_range_closed\n    \
                     on {schema_name}.{table_name}(coalesce(upper(block_range), {block_max}))\n \
                     where coalesce(upper(block_range), {block_max}) < {block_max};\n",
                    table_name = table.name,
                    schema_name = layout.catalog.site.namespace,
                    block_max = BLOCK_NUMBER_MAX
                )
            }
        }

        fn create_attribute_indexes(
            table: &Table,
            out: &mut String,
            layout: &Layout,
        ) -> fmt::Result {
            // Create indexes. Skip columns whose type is an array of enum,
            // since there is no good way to index them with Postgres 9.6.
            // Once we move to Postgres 11, we can enable that
            // (tracked in graph-node issue #1330)
            for (i, column) in table
                .columns
                .iter()
                .filter(|col| !(col.is_list() && col.is_enum()))
                .enumerate()
            {
                if table.immutable && column.is_primary_key() {
                    // We create a unique index on `id` in `create_table`
                    // and don't need an explicit attribute index
                    continue;
                }

                let (method, index_expr) = if column.is_reference() && !column.is_list() {
                    // For foreign keys, index the key together with the block range
                    // since we almost always also have a block_range clause in
                    // queries that look for specific foreign keys
                    if table.immutable {
                        let index_expr = format!("{}, {}", column.name.quoted(), BLOCK_COLUMN);
                        ("btree", index_expr)
                    } else {
                        let index_expr =
                            format!("{}, {}", column.name.quoted(), BLOCK_RANGE_COLUMN);
                        ("gist", index_expr)
                    }
                } else {
                    // Attributes that are plain strings or bytes are
                    // indexed with a BTree; but they can be too large for
                    // Postgres' limit on values that can go into a BTree.
                    // For those attributes, only index the first
                    // STRING_PREFIX_SIZE or BYTE_ARRAY_PREFIX_SIZE characters
                    // see: attr-bytea-prefix
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
                            _ => unreachable!("only String and Bytes can have arbitrary size"),
                        }
                    } else {
                        column.name.quoted()
                    };

                    let method = if column.is_list() || column.is_fulltext() {
                        "gin"
                    } else {
                        "btree"
                    };

                    (method, index_expr)
                };
                write!(
                out,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.\"{table_name}\" using {method}({index_expr});\n",
                table_index = table.position,
                table_name = table.name,
                column_index = i,
                column_name = column.name,
                schema_name = layout.catalog.site.namespace,
                method = method,
                index_expr = index_expr,
            )?;
            }
            writeln!(out)
        }

        create_table(self, out, layout)?;
        create_time_travel_indexes(self, out, layout)?;
        create_attribute_indexes(self, out, layout)
    }

    pub fn exclusion_ddl(&self, out: &mut String, nsp: &str, as_constraint: bool) -> fmt::Result {
        if as_constraint {
            writeln!(
                out,
                r#"
        alter table {nsp}.{name}
          add constraint {bare_name}_{id}_{block_range}_excl exclude using gist ({id} with =, {block_range} with &&);
               "#,
                name = self.name.quoted(),
                bare_name = self.name,
                id = self.primary_key().name,
                block_range = BLOCK_RANGE_COLUMN
            )?;
        } else {
            writeln!(
                out,
                r#"
        create index {bare_name}_{id}_{block_range}_excl on {nsp}.{name}
         using gist ({id}, {block_range});
               "#,
                name = self.name.quoted(),
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
        write!(out, "    ")?;
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
