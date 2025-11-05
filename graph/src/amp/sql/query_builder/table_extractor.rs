use std::{collections::BTreeSet, ops::ControlFlow};

use itertools::Itertools;
use sqlparser_latest::ast::{self, Visit, Visitor};

/// Returns all tables that are referenced by the SQL query.
///
/// The table names are lowercased and quotes are ignored.
pub(super) fn extract_tables(query: &ast::Query) -> BTreeSet<String> {
    let mut table_extractor = TableExtractor::new();
    let _: ControlFlow<()> = Visit::visit(query, &mut table_extractor);

    table_extractor.tables
}

/// Returns the normalized table name.
///
/// The table name is lowercased and quotes are ignored.
pub(super) fn normalize_table(object_name: &ast::ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
        })
        .join(".")
}

/// Visits the SQL query AST and extracts referenced table names, ignoring CTEs.
struct TableExtractor {
    tables: BTreeSet<String>,
    cte_stack: CteStack,
}

impl TableExtractor {
    /// Creates a new empty table extractor.
    fn new() -> Self {
        Self {
            tables: BTreeSet::new(),
            cte_stack: CteStack::new(),
        }
    }

    /// Extracts and stores the table name from the current `table_factor`.
    fn visit_table_factor(&mut self, table_factor: &ast::TableFactor) {
        let ast::TableFactor::Table { name, .. } = table_factor else {
            return;
        };

        let table = normalize_table(name);

        if self.cte_stack.contains(&table) {
            return;
        }

        self.tables.insert(table);
    }
}

impl Visitor for TableExtractor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &ast::Query) -> ControlFlow<Self::Break> {
        self.cte_stack.pre_visit_query(query);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &ast::Query) -> ControlFlow<Self::Break> {
        self.cte_stack.post_visit_query();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        self.visit_table_factor(table_factor);
        ControlFlow::Continue(())
    }
}

/// Maintains a list of active CTEs for each subquery scope.
struct CteStack {
    stack: Vec<BTreeSet<String>>,
}

impl CteStack {
    /// Creates a new empty CTE stack.
    fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Returns `true` if the `table_name` is present in the CTE list at any scope.
    fn contains(&self, table_name: &str) -> bool {
        self.stack.iter().any(|scope| scope.contains(table_name))
    }

    /// Creates a new subquery scope with all the CTEs of the current `query`.
    fn pre_visit_query(&mut self, query: &ast::Query) {
        let cte_tables = match &query.with {
            Some(with) => with
                .cte_tables
                .iter()
                .map(|cte_table| cte_table.alias.name.value.to_lowercase())
                .collect(),
            None => BTreeSet::new(),
        };

        self.stack.push(cte_tables);
    }

    /// Removes all the CTEs from the most recent subquery scope.
    fn post_visit_query(&mut self) {
        self.stack.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    macro_rules! test_extract_tables {
        ($($name:ident: $input:expr => $expected:expr),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let query = parse_query($input).unwrap();
                    assert_eq!(extract_tables(&query), $expected.into_iter().map(Into::into).collect());
                }
            )*
        };
    }

    test_extract_tables! {
        one_table: "SELECT a FROM b" => ["b"],
        multiple_tables_with_one_join: "SELECT a FROM b JOIN c ON c.c = b.b" => ["b", "c"],
        multiple_tables_with_multiple_joins: "SELECT a FROM b JOIN c ON c.c = b.b JOIN d ON d.d = b.b" => ["b", "c", "d"],
        one_table_with_one_cte: "WITH a AS (SELECT * FROM b) SELECT * FROM a" => ["b"],
        one_table_with_multiple_ctes: "WITH a AS (SELECT * FROM b), c AS (SELECT * FROM a) SELECT * FROM c" => ["b"],
        multiple_tables_with_multiple_ctes: "WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM a JOIN c ON c.c = a.a" => ["b", "d"],
        multiple_tables_with_nested_ctes: "WITH a AS (WITH b AS (SELECT * FROM c) SELECT * FROM d JOIN b ON b.b = d.d) SELECT * FROM a" => ["c", "d"],
        multiple_tables_with_union: "SELECT a FROM b UNION SELECT c FROM d" => ["b", "d"],
        multiple_tables_with_union_all: "SELECT a FROM b UNION ALL SELECT c FROM d" => ["b", "d"],

        namespace_is_preserved: "SELECT a FROM b.c" => ["b.c"],
        catalog_is_preserved: "SELECT a FROM b.c.d" => ["b.c.d"],
        tables_are_lowercased: "SELECT a FROM B.C" => ["b.c"],
        single_quotes_in_tables_are_ignored: "SELECT a FROM 'B'.'C'" => ["b.c"],
        double_quotes_in_tables_are_ignored: r#"SELECT a FROM "B"."C""# => ["b.c"],
        backticks_in_tables_are_ignored: "SELECT a FROM `B`.`C`" => ["b.c"],
    }
}
