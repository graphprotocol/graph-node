use sqlparser::ast::{ObjectName, Statement, TableFactor, VisitMut, VisitorMut};
use std::ops::ControlFlow;

use super::Schema;

pub struct Formatter<'a> {
    prelude: &'a str,
    schema: &'a Schema,
}

impl<'a> Formatter<'a> {
    pub fn new(prelude: &'a str, schema: &'a Schema) -> Self {
        Self { prelude, schema }
    }

    fn prepend_prefix_to_object_name_mut(&self, name: &mut ObjectName) {
        let table_identifier = &mut name.0;
        // remove all but the last identifier
        table_identifier.drain(0..table_identifier.len() - 1);

        // Ensure schema tables has quotation to match up with prelude generated cte.
        if let Some(table_name) = table_identifier.last_mut() {
            if self.schema.contains_key(&table_name.value) {
                table_name.quote_style = Some('"');
            }
        }
    }

    pub fn format(&mut self, statement: &mut Statement) -> String {
        statement.visit(self);

        format!(
            "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub",
            self.prelude, statement
        )
    }
}

impl VisitorMut for Formatter<'_> {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, .. } = table_factor {
            self.prepend_prefix_to_object_name_mut(name);
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;
    use crate::sql::constants::SQL_DIALECT;
    const CTE_PREFIX: &str = "WITH \"swap\" AS (
            SELECT 
            id,
            amount_in,
            amount_out,
            concat('0x',encode(token_in,'hex') as token_in,
            concat('0x',token_out,'hex') AS token_out 
            FROM 
            sdg1.swap
        )";

    #[test]
    fn format_sql() {
        let mut schema = Schema::new();
        schema.insert(
            "swap".to_string(),
            HashSet::from_iter(
                ["id", "amount_in", "amount_out", "token_in", "token_out"]
                    .into_iter()
                    .map(|s| s.to_string()),
            ),
        );

        let mut formatter = Formatter::new(CTE_PREFIX, &schema);

        let sql = "SELECT token_in, SUM(amount_in) AS amount FROM unknown.swap GROUP BY token_in";

        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql).unwrap();

        let mut statement = statements.get_mut(0).unwrap();

        let result = formatter.format(&mut statement);

        assert_eq!(
            result,
            format!(
                "{} SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub",
                CTE_PREFIX,
                "SELECT token_in, SUM(amount_in) AS amount FROM \"swap\" GROUP BY token_in"
            )
        );
    }
}
