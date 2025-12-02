use std::{collections::BTreeSet, fmt, ops::ControlFlow};

use sqlparser_latest::ast::{self, Visit, Visitor};

/// Returns all tables that are referenced by the SQL query.
///
/// The table names are lowercased and quotes are ignored.
pub(super) fn extract_tables(query: &ast::Query) -> BTreeSet<TableReference> {
    let mut table_extractor = TableExtractor::new();
    let _: ControlFlow<()> = Visit::visit(query, &mut table_extractor);

    table_extractor.tables
}

/// Contains a normalized table reference.
///
/// Used to compare physical table references with CTE names and custom tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct TableReference(ast::ObjectName);

impl TableReference {
    const QUOTE_STYLE: char = '"';

    /// Creates a new table reference from a custom dataset and table.
    pub(super) fn new(dataset: &str, table: &str) -> Self {
        Self(
            vec![
                ast::Ident::with_quote(Self::QUOTE_STYLE, dataset),
                ast::Ident::with_quote(Self::QUOTE_STYLE, table),
            ]
            .into(),
        )
    }

    /// Creates a new table reference from an object name.
    pub(super) fn with_object_name(object_name: &ast::ObjectName) -> Self {
        Self::with_idents(
            object_name
                .0
                .iter()
                .map(|object_name_part| match object_name_part {
                    ast::ObjectNamePart::Identifier(ident) => ident,
                }),
        )
    }

    /// Creates a new table reference from a list of identifiers.
    pub(super) fn with_idents<'a>(idents: impl IntoIterator<Item = &'a ast::Ident>) -> Self {
        Self(
            idents
                .into_iter()
                .map(|ident| {
                    let ast::Ident {
                        value,
                        quote_style,
                        span: _,
                    } = ident;

                    ast::Ident::with_quote(Self::QUOTE_STYLE, {
                        if quote_style.is_none() {
                            value.to_lowercase()
                        } else {
                            value.to_owned()
                        }
                    })
                })
                .collect::<Vec<_>>()
                .into(),
        )
    }
}

impl fmt::Display for TableReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Visits the SQL query AST and extracts referenced table names, ignoring CTEs.
struct TableExtractor {
    tables: BTreeSet<TableReference>,
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

        let table_reference = TableReference::with_object_name(name);
        if self.cte_stack.contains(&table_reference) {
            return;
        }

        self.tables.insert(table_reference);
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
    stack: Vec<BTreeSet<TableReference>>,
}

impl CteStack {
    /// Creates a new empty CTE stack.
    fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Returns `true` if the `table_reference` is present in the CTE list at any scope.
    fn contains(&self, table_reference: &TableReference) -> bool {
        self.stack
            .iter()
            .any(|scope| scope.contains(table_reference))
    }

    /// Creates a new subquery scope with all the CTEs of the current `query`.
    fn pre_visit_query(&mut self, query: &ast::Query) {
        let cte_tables = match &query.with {
            Some(with) => with
                .cte_tables
                .iter()
                .map(|cte_table| TableReference::with_idents([&cte_table.alias.name]))
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
                    assert_eq!(
                        extract_tables(&query).into_iter().map(|table| table.to_string()).collect::<Vec<_>>(),
                        $expected.into_iter().map(|table| table.to_string()).collect::<Vec<_>>()
                    );
                }
            )*
        };
    }

    test_extract_tables! {
        one_table: "SELECT a FROM b" => [r#""b""#],
        multiple_tables_with_one_join: "SELECT a FROM b JOIN c ON c.c = b.b" => [r#""b""#, r#""c""#],
        multiple_tables_with_multiple_joins: "SELECT a FROM b JOIN c ON c.c = b.b JOIN d ON d.d = b.b" => [r#""b""#, r#""c""#, r#""d""#],
        one_table_with_one_cte: "WITH a AS (SELECT * FROM b) SELECT * FROM a" => [r#""b""#],
        one_table_with_multiple_ctes: "WITH a AS (SELECT * FROM b), c AS (SELECT * FROM a) SELECT * FROM c" => [r#""b""#],
        multiple_tables_with_multiple_ctes: "WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM a JOIN c ON c.c = a.a" => [r#""b""#, r#""d""#],
        multiple_tables_with_nested_ctes: "WITH a AS (WITH b AS (SELECT * FROM c) SELECT * FROM d JOIN b ON b.b = d.d) SELECT * FROM a" => [r#""c""#, r#""d""#],
        multiple_tables_with_union: "SELECT a FROM b UNION SELECT c FROM d" => [r#""b""#, r#""d""#],
        multiple_tables_with_union_all: "SELECT a FROM b UNION ALL SELECT c FROM d" => [r#""b""#, r#""d""#],

        namespace_is_preserved: "SELECT a FROM b.c" => [r#""b"."c""#],
        catalog_is_preserved: "SELECT a FROM b.c.d" => [r#""b"."c"."d""#],
        unquoted_tables_are_lowercased: "SELECT a FROM B.C" => [r#""b"."c""#],
        single_quotes_in_tables_are_converted_to_double_quotes: "SELECT a FROM 'B'.'C'" => [r#""B"."C""#],
        double_quotes_in_tables_are_preserved: r#"SELECT a FROM "B"."C""# => [r#""B"."C""#],
        backticks_in_tables_are_converted_to_double_quotes: "SELECT a FROM `B`.`C`" => [r#""B"."C""#],
    }
}
