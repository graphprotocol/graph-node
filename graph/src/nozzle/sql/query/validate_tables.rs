use std::ops::ControlFlow;

use anyhow::{anyhow, bail, Error, Result};
use sqlparser_latest::ast::{self, Visit, Visitor};

use super::format::Ident;

/// Validates the dataset and tables used by the SQL query to ensure consistency with the explicitly declared ones.
///
/// Checks every table reference in the SQL query and verifies that they match the `dataset` and `tables`.
/// Ignores table references not in `namespace.table` format as they may reference CTEs.
///
/// # Errors
///
/// Returns an error if:
/// - The SQL query references a dataset that is not equal to `dataset`
/// - The SQL query references a table that is not in the `tables` list
///
/// The returned error is deterministic.
pub(super) fn validate_tables(query: &ast::Query, dataset: &Ident, tables: &[Ident]) -> Result<()> {
    let mut table_validator = TableValidator { dataset, tables };
    if let ControlFlow::Break(e) = Visit::visit(query, &mut table_validator) {
        return Err(e);
    }
    Ok(())
}

/// Walks the SQL AST and validates every table reference.
struct TableValidator<'a> {
    dataset: &'a Ident,
    tables: &'a [Ident],
}

impl<'a> TableValidator<'a> {
    /// Validates that the `table_factor` references the explicitly declared dataset and tables.
    ///
    /// Ignores unrelated table factors and table references without a namespace as they may reference CTEs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `table_factor` references a dataset that is not equal to `dataset`
    /// - The `table_factor` references a table that is not in the `tables` list
    ///
    /// The returned error is deterministic.
    fn visit_table_factor(&self, table_factor: &ast::TableFactor) -> Result<()> {
        let ast::TableFactor::Table { name, .. } = table_factor else {
            return Ok(());
        };

        let mut ident_iter = name.0.iter().rev().map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => Ident::new(ident.value.as_str()),
        });

        let Some(table) = ident_iter.next() else {
            return Ok(());
        };

        let Some(dataset) = ident_iter.next() else {
            return Ok(());
        };

        if *self.dataset != dataset {
            bail!("'{name}': invalid dataset '{dataset}'");
        }

        if !self.tables.iter().any(|t| *t == table) {
            bail!("'{name}': invalid table '{table}'");
        }

        Ok(())
    }
}

impl<'a> Visitor for TableValidator<'a> {
    type Break = Error;

    fn post_visit_table_factor(
        &mut self,
        table_factor: &ast::TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let Err(e) = self.visit_table_factor(table_factor) {
            return ControlFlow::Break(anyhow!("failed to validate table {e:#}"));
        }

        ControlFlow::Continue(())
    }
}
