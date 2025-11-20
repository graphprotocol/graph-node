use std::{
    collections::BTreeMap,
    hash::{BuildHasher, Hash, Hasher},
    ops::{ControlFlow, RangeInclusive},
};

use ahash::RandomState;
use alloy::primitives::BlockNumber;
use sqlparser_latest::ast::{self, VisitMut, VisitorMut};

use super::{extract_tables, parse_query, TableReference};

/// Limits the query execution to the specified block range.
///
/// Wraps the `query` in a CTE, and creates CTEs for every table it references.
/// These CTEs load data from the referenced tables only on the specified block range.
/// All the table references in the original SQL query are replaced with the created CTE names.
///
/// The output is ordered by block numbers.
pub(super) fn new_block_range_query<'a>(
    query: &ast::Query,
    block_number_column: &str,
    block_range: &RangeInclusive<BlockNumber>,
) -> ast::Query {
    // CTE names are unique within a SQL query.
    // The hasher ensures that CTEs created for block range do not collide with user-defined CTEs.
    // Constant seeds ensure consistent block range queries for the same input parameters.
    let mut hasher = RandomState::with_seeds(0, 0, 0, 0).build_hasher();

    let tables_to_ctes_mapping = new_tables_to_ctes_mapping(query, &mut hasher);
    assert!(!tables_to_ctes_mapping.is_empty());

    let mut cte_tables = Vec::with_capacity(tables_to_ctes_mapping.len());
    for (table, cte_table) in &tables_to_ctes_mapping {
        cte_tables.push(format!(
            "{cte_table} AS (SELECT * FROM {table} WHERE _block_num BETWEEN {start_block} AND {end_block})",
            start_block = block_range.start(),
            end_block = block_range.end()
        ))
    }

    let mut query = query.clone();
    let mut table_replacer = TableReplacer::new(tables_to_ctes_mapping);
    let _: ControlFlow<()> = VisitMut::visit(&mut query, &mut table_replacer);

    let block_range_query = format!(
        "WITH {cte_tables}, {source} AS ({query}) SELECT {source}.* FROM {source} ORDER BY {source}.{block_number_column}",
        cte_tables = cte_tables.join(", "),
        source = format!("source_{}", hasher.finish())
    );

    parse_query(block_range_query).unwrap()
}

/// Creates unique CTE names for every table referenced by the SQL query.
fn new_tables_to_ctes_mapping(
    query: &ast::Query,
    hasher: &mut impl Hasher,
) -> BTreeMap<TableReference, String> {
    extract_tables(query)
        .into_iter()
        .map(|table| {
            table.hash(hasher);

            (table, format!("block_range_{}", hasher.finish()))
        })
        .collect()
}

/// Visits the SQL query AST and replaces referenced table names with CTE names.
struct TableReplacer {
    tables_to_ctes_mapping: BTreeMap<TableReference, String>,
}

impl TableReplacer {
    /// Creates a new table replacer.
    fn new(tables_to_ctes_mapping: BTreeMap<TableReference, String>) -> Self {
        Self {
            tables_to_ctes_mapping,
        }
    }

    /// Replaces the table name of the current `table_factor` with the associated CTE name.
    fn visit_table_factor(&mut self, table_factor: &mut ast::TableFactor) {
        let ast::TableFactor::Table { name, alias, .. } = table_factor else {
            return;
        };

        let Some(cte_table) = self
            .tables_to_ctes_mapping
            .get(&TableReference::with_object_name(name))
        else {
            return;
        };

        // Set the alias to the original table name so that queries like `SELECT table.column FROM table` do not break
        if alias.is_none() {
            let last_name_part = name.0.last().unwrap();

            *alias = Some(ast::TableAlias {
                name: last_name_part.as_ident().unwrap().clone(),
                columns: Vec::new(),
            })
        }

        *name = ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
            cte_table,
        ))]);
    }
}

impl VisitorMut for TableReplacer {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        self.visit_table_factor(table_factor);
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    #[test]
    fn query_with_one_table_reference_is_wrapped_with_block_range() {
        let query = parse_query("SELECT a, b, c FROM d").unwrap();
        let block_number_column = "b";
        let block_range = 0..=1_000_000;
        let block_range_query = new_block_range_query(&query, block_number_column, &block_range);

        assert_eq!(
            block_range_query,
            parse_query(
                r#"
                WITH block_range_1164572571450379730 AS (
                    SELECT * FROM "d" WHERE _block_num BETWEEN 0 AND 1000000
                ),
                source_1164572571450379730 AS (
                    SELECT a, b, c FROM block_range_1164572571450379730 AS d
                )
                SELECT
                    source_1164572571450379730.*
                FROM
                    source_1164572571450379730
                ORDER BY
                    source_1164572571450379730.b
                "#
            )
            .unwrap(),
        )
    }

    #[test]
    fn query_with_multiple_table_references_is_wrapped_with_block_range() {
        let query = parse_query("SELECT a, b, c FROM d JOIN e ON e.e = d.d").unwrap();
        let block_number_column = "b";
        let block_range = 0..=1_000_000;
        let block_range_query = new_block_range_query(&query, block_number_column, &block_range);

        assert_eq!(
            block_range_query,
            parse_query(
                r#"
                WITH block_range_1164572571450379730 AS (
                    SELECT * FROM "d" WHERE _block_num BETWEEN 0 AND 1000000
                ),
                block_range_13063992259633584610 AS (
                    SELECT * FROM "e" WHERE _block_num BETWEEN 0 AND 1000000
                ),
                source_13063992259633584610 AS (
                    SELECT a, b, c FROM block_range_1164572571450379730 AS d JOIN block_range_13063992259633584610 AS e ON e.e = d.d
                )
                SELECT
                    source_13063992259633584610.*
                FROM
                    source_13063992259633584610
                ORDER BY
                    source_13063992259633584610.b
                "#
            )
            .unwrap(),
        )
    }
}
