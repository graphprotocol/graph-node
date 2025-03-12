use super::{constants::SQL_DIALECT, validation::Validator};
use crate::relational::Layout;
use anyhow::{anyhow, Ok, Result};
use graph::{env::ENV_VARS, prelude::BlockNumber};
use std::sync::Arc;

pub struct Parser {
    layout: Arc<Layout>,
    block: BlockNumber,
}

impl Parser {
    pub fn new(layout: Arc<Layout>, block: BlockNumber) -> Self {
        Self { layout, block }
    }

    pub fn parse_and_validate(&self, sql: &str) -> Result<String> {
        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql)?;

        let max_offset = ENV_VARS.graphql.max_skip;
        let max_limit = ENV_VARS.graphql.max_first;

        let mut validator = Validator::new(&self.layout, self.block, max_limit, max_offset);
        validator.validate_statements(&mut statements)?;

        let statement = statements
            .get(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        Ok(statement.to_string())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::sql::{parser::SQL_DIALECT, test::make_layout};
    use graph::prelude::{lazy_static, serde_yaml, BLOCK_NUMBER_MAX};
    use serde::{Deserialize, Serialize};

    use pretty_assertions::assert_eq;

    use super::Parser;

    const TEST_GQL: &str = "
        type Swap @entity(immutable: true) {
            id: Bytes!
            timestamp: BigInt!
            pool: Bytes!
            token0: Bytes!
            token1: Bytes!
            sender: Bytes!
            recipient: Bytes!
            origin: Bytes! # the EOA that initiated the txn
            amount0: BigDecimal!
            amount1: BigDecimal!
            amountUSD: BigDecimal!
            sqrtPriceX96: BigInt!
            tick: BigInt!
            logIndex: BigInt
        }

        type Token @entity {
            id: ID!
            address: Bytes! # address
            symbol: String!
            name: String!
            decimals: Int!
        }
    ";

    fn parse_and_validate(sql: &str) -> Result<String, anyhow::Error> {
        let parser = Parser::new(Arc::new(make_layout(TEST_GQL)), BLOCK_NUMBER_MAX);

        parser.parse_and_validate(sql)
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestCase {
        name: Option<String>,
        sql: String,
        ok: Option<String>,
        err: Option<String>,
    }

    impl TestCase {
        fn fail(
            &self,
            name: &str,
            msg: &str,
            exp: impl std::fmt::Display,
            actual: impl std::fmt::Display,
        ) {
            panic!(
                "case {name} failed: {}\n  expected: {}\n  actual: {}",
                msg, exp, actual
            );
        }

        fn run(&self, num: usize) {
            fn normalize(query: &str) -> String {
                sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, query)
                    .unwrap()
                    .pop()
                    .unwrap()
                    .to_string()
            }

            let name = self
                .name
                .as_ref()
                .map(|name| format!("{num} ({name})"))
                .unwrap_or_else(|| num.to_string());
            let result = parse_and_validate(&self.sql);

            match (&self.ok, &self.err, result) {
                (Some(expected), None, Ok(actual)) => {
                    let actual = normalize(&actual);
                    let expected = normalize(expected);
                    assert_eq!(actual, expected, "case {} failed", name);
                }
                (None, Some(expected), Err(actual)) => {
                    let actual = actual.to_string();
                    if !actual.contains(expected) {
                        self.fail(&name, "expected error message not found", expected, actual);
                    }
                }
                (Some(_), Some(_), _) => {
                    panic!("case {} has both ok and err", name);
                }
                (None, None, _) => {
                    panic!("case {} has neither ok nor err", name)
                }
                (None, Some(exp), Ok(actual)) => {
                    self.fail(&name, "expected an error", exp, actual);
                }
                (Some(exp), None, Err(actual)) => self.fail(&name, "expected success", exp, actual),
            }
        }
    }

    lazy_static! {
        static ref TESTS: Vec<TestCase> = {
            let file = std::path::PathBuf::from_iter([
                env!("CARGO_MANIFEST_DIR"),
                "src",
                "sql",
                "parser_tests.yaml",
            ]);
            let tests = std::fs::read_to_string(file).unwrap();
            serde_yaml::from_str(&tests).unwrap()
        };
    }

    #[test]
    fn parse_sql() {
        for (num, case) in TESTS.iter().enumerate() {
            case.run(num);
        }
    }
}
