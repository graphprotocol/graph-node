use std::ops::ControlFlow;

use sqlparser::ast::{Ident, ObjectName, Statement, TableFactor, VisitMut, VisitorMut};

pub struct SqlFormatter<'a> {
    prefix: &'a str,
}

impl VisitorMut for SqlFormatter<'_> {
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

impl<'a> SqlFormatter<'a> {
    pub fn new(prefix: &'a str) -> Self {
        Self { prefix }
    }

    pub fn format(&mut self, statement: &mut Statement) -> String {
        statement.visit(self);
        format!(
            "SELECT to_jsonb(sub.*) AS data FROM ( {} ) AS sub;",
            statement
        )
    }

    fn prepend_prefix_to_object_name_mut(&self, name: &mut ObjectName) {
        name.0.insert(0, Ident::new(self.prefix));
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::VisitMut;

    #[test]
    fn run_formatter_visitor() {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};

        let sql = "SELECT * FROM foo";
        let mut formatter = super::SqlFormatter::new("sgd1");
        let mut query = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        query.visit(&mut formatter);

        let result = query
            .into_iter()
            .map(|stmt| stmt.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        assert_eq!(result, "SELECT * FROM sgd1.foo");
    }
}
