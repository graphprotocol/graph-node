use anyhow::{anyhow, Ok, Result};
use sqlparser::ast::{Statement, Visit};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::formatter::SqlFormatter;
use crate::validators::create_postgres_function_validator;

pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_and_validate(&self, sql: &str, deployment_id: i32) -> Result<String> {
        let mut result = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
        let statement = result
            .get_mut(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        match statement {
            Statement::Query(query) => {
                let prefix = format!("sgd{}", deployment_id);
                let mut formatter = SqlFormatter::new(&prefix);

                let mut function_validator = create_postgres_function_validator(); 

                function_validator.validate_query(query)?;

                let result = formatter.format(&mut *query);

                Ok(result)
            }
            _ => Err(anyhow!("Only SELECT queries are supported")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SqlParser;

    #[test]
    fn parse_sql() {
        let parser = SqlParser::new();

        let sql = include_str!("../test.sql");

        let ast = parser.parse_and_validate(sql, 1).unwrap();

        assert_eq!(ast, "SELECT to_jsonb(sub.*) AS data FROM ( SELECT e1.employee_id, (SELECT AVG(salary) FROM (SELECT salary FROM sgd1.employees AS e2 WHERE e2.manager_id = e1.manager_id AND e2.department_id IN (SELECT department_id FROM sgd1.departments AS d1 WHERE EXISTS (SELECT department_name FROM sgd1.departments AS d2 WHERE d1.parent_department_id = d2.department_id AND d2.location_id = (SELECT location_id FROM sgd1.locations WHERE country_id = 'US')))) AS DepartmentSalary) AS AverageDepartmentSalary FROM sgd1.employees AS e1 WHERE e1.employee_id IN (SELECT employee_id FROM sgd1.job_history WHERE start_date > (SELECT MIN(start_date) FROM sgd1.job_history WHERE department_id = (SELECT department_id FROM sgd1.employees WHERE employee_id = e1.manager_id))) AND e1.salary > (SELECT AVG(salary) FROM sgd1.employees WHERE department_id = e1.department_id) ORDER BY e1.employee_id ) AS sub;");
    }
}
