use std::{collections::BTreeMap, ops::ControlFlow};

use alloy::primitives::BlockNumber;
use sqlparser_latest::ast::{self, VisitMut, VisitorMut};

use super::{format::Ident, parse};

/// Applies a block range filter to the SQL query.
///
/// Creates temporary ordered result sets for each table in the dataset, limiting
/// the blocks processed during execution.
///
/// The temporary result sets replace the tables referenced in the SQL query.
///
/// This ensures deterministic output during query execution and enables resuming
/// after failures or when new blocks are available.
pub(super) fn filter_blocks(
    query: &mut ast::Query,
    dataset: &Ident,
    tables: &[Ident],
    start_block: BlockNumber,
    end_block: BlockNumber,
) {
    let tables_to_cte_mapping = tables_to_cte_mapping(dataset, tables);

    let mut table_to_cte_replacer = TableToCteReplacer::new(&tables_to_cte_mapping);
    let _: ControlFlow<()> = VisitMut::visit(query, &mut table_to_cte_replacer);

    match &mut query.with {
        Some(with) => {
            remove_cte_filters(&mut with.cte_tables, &tables_to_cte_mapping);

            add_cte_filters(
                &mut with.cte_tables,
                &tables_to_cte_mapping,
                start_block,
                end_block,
            );
        }
        None => {
            let mut cte_tables = Vec::new();

            add_cte_filters(
                &mut cte_tables,
                &tables_to_cte_mapping,
                start_block,
                end_block,
            );

            query.with = Some(ast::With {
                with_token: ast::helpers::attached_token::AttachedToken::empty(),
                recursive: false,
                cte_tables,
            })
        }
    }
}

// Maps `dataset` and `tables` to consistent names for temporary result sets.
fn tables_to_cte_mapping(dataset: &Ident, tables: &[Ident]) -> BTreeMap<String, String> {
    tables
        .into_iter()
        .map(|table| {
            let dataset_table = format!("{dataset}.{table}");
            let cte_table = format!("sg_{dataset}_{table}");

            (dataset_table, cte_table)
        })
        .collect()
}

/// Removes previously added temporary result sets from the SQL query.
fn remove_cte_filters(ctes: &mut Vec<ast::Cte>, tables_to_cte_mapping: &BTreeMap<String, String>) {
    ctes.retain(|cte| {
        !tables_to_cte_mapping
            .values()
            .any(|cte_table| *cte_table == cte.alias.name.value)
    });
}

/// Creates temporary result sets for each table in the dataset and adds them to the SQL query.
fn add_cte_filters(
    ctes: &mut Vec<ast::Cte>,
    tables_to_cte_mapping: &BTreeMap<String, String>,
    start_block: BlockNumber,
    end_block: BlockNumber,
) {
    let mut output_ctes = Vec::with_capacity(ctes.len() + tables_to_cte_mapping.len());

    for (table, cte_table) in tables_to_cte_mapping {
        let query = parse::query(format!(
            "SELECT * FROM {table} WHERE _block_num BETWEEN {start_block} AND {end_block} ORDER BY _block_num ASC"
        ))
        .unwrap();

        output_ctes.push(ast::Cte {
            alias: ast::TableAlias {
                name: ast::Ident::new(cte_table),
                columns: Vec::new(),
            },
            query: Box::new(query),
            from: None,
            materialized: None,
            closing_paren_token: ast::helpers::attached_token::AttachedToken::empty(),
        });
    }

    output_ctes.append(ctes);
    let _empty = std::mem::replace(ctes, output_ctes);
}

/// Walks the SQL AST and replaces each table reference with a temporary result set name.
struct TableToCteReplacer<'a> {
    tables_to_cte_mapping: &'a BTreeMap<String, String>,
}

impl<'a> TableToCteReplacer<'a> {
    /// Creates a new replacer.
    fn new(tables_to_cte_mapping: &'a BTreeMap<String, String>) -> Self {
        Self {
            tables_to_cte_mapping,
        }
    }

    /// Makes the `table_factor` reference a temporary result set instead of a table.
    ///
    /// Ignores unrelated table factors and table references without a namespace because
    /// they might reference other CTEs.
    fn visit_table_factor(&self, table_factor: &mut ast::TableFactor) {
        let ast::TableFactor::Table { name, alias, .. } = table_factor else {
            return;
        };

        let mut iter = name.0.iter().rev().map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        });

        let Some(table) = iter.next() else {
            return;
        };

        let Some(dataset) = iter.next() else {
            return;
        };

        let dataset_table = format!("{}.{}", Ident::new(dataset), Ident::new(table));
        let Some(cte_table) = self.tables_to_cte_mapping.get(&dataset_table) else {
            return;
        };

        if alias.is_none() {
            *alias = Some(ast::TableAlias {
                name: ast::Ident::new(table),
                columns: Vec::new(),
            })
        }

        *name = ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
            cte_table,
        ))]);
    }
}

impl<'a> VisitorMut for TableToCteReplacer<'a> {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        self.visit_table_factor(table_factor);
        ControlFlow::Continue(())
    }
}
