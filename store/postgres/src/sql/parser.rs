use super::{constants::SQL_DIALECT, validation::Validator};
use crate::relational::Layout;
use anyhow::{anyhow, Ok, Result};
use graph::prelude::BlockNumber;
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

        let mut validator = Validator::new(&self.layout, self.block);
        validator.validate_statements(&mut statements)?;

        let statement = statements
            .get_mut(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        let sql = format!(
            "select to_jsonb(sub.*) as data from ( {} ) as sub",
            statement
        );
        Ok(sql)
    }
}

#[cfg(test)]
mod test {

    use graph::prelude::BLOCK_NUMBER_MAX;

    use crate::sql::test::make_layout;

    use super::*;

    const TEST_GQL: &str = "
        type SwapMulti @entity(immutable: true) {
            id: Bytes!
            sender: Bytes! # address
            amountsIn: [BigInt!]! # uint256[]
            tokensIn: [Bytes!]! # address[]
            amountsOut: [BigInt!]! # uint256[]
            tokensOut: [Bytes!]! # address[]
            referralCode: BigInt! # uint32
            blockNumber: BigInt!
            blockTimestamp: BigInt!
            transactionHash: Bytes!
        }

        type Token @entity {
            id: ID!
            address: Bytes! # address
            symbol: String!
            name: String!
            decimals: Int!
        }
    ";

    const SQL_QUERY: &str = "
        with tokens as (
            select * from (values
            ('0x0000000000000000000000000000000000000000','ETH','Ethereum',18),
            ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48','USDC','USD Coin',6)
            ) as t(address,symbol,name,decimals)
        )

        select
        date,
        t.symbol,
        SUM(amount)/pow(10,t.decimals) as amount
        from (select
        date(to_timestamp(block_timestamp) at time zone 'utc') as date,
        token,
        amount
        from swap_multi as sm
        ,unnest(sm.amounts_in,sm.tokens_in) as smi(amount,token)
        union all
        select
        date(to_timestamp(block_timestamp) at time zone 'utc') as date,
        token,
        amount
        from sgd1.swap_multi as sm
        ,unnest(sm.amounts_out,sm.tokens_out) as smo(amount,token)
        ) as tp
        inner join tokens as t on t.address = '0x' || encode(tp.token,'hex')
        group by tp.date,t.symbol,t.decimals
        order by tp.date desc ,amount desc

        ";

    fn parse_and_validate(sql: &str) -> Result<String, anyhow::Error> {
        let parser = Parser::new(Arc::new(make_layout(TEST_GQL)), BLOCK_NUMBER_MAX);

        parser.parse_and_validate(sql)
    }

    #[test]
    fn parse_sql() {
        let query = parse_and_validate(SQL_QUERY).unwrap();

        assert_eq!(
            query,
            r#"WITH "swap_multi" AS (SELECT concat('0x', encode("id", 'hex')) AS "id", concat('0x', encode("sender", 'hex')) AS "sender", "amounts_in", "tokens_in", "amounts_out", "tokens_out", "referral_code", "block_number", "block_timestamp", concat('0x', encode("transaction_hash", 'hex')) AS "transaction_hash", "block$" FROM "sgd0815"."swap_multi"),
"token" AS (SELECT "id", concat('0x', encode("address", 'hex')) AS "address", "symbol", "name", "decimals", "block_range" FROM "sgd0815"."token" WHERE "block_range" @> 2147483647) SELECT to_jsonb(sub.*) AS data FROM ( WITH tokens AS (SELECT * FROM (VALUES ('0x0000000000000000000000000000000000000000', 'ETH', 'Ethereum', 18), ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'USDC', 'USD Coin', 6)) AS t (address, symbol, name, decimals)) SELECT date, t.symbol, SUM(amount) / pow(10, t.decimals) AS amount FROM (SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM "swap_multi" AS sm, UNNEST(sm.amounts_in, sm.tokens_in) AS smi (amount, token) UNION ALL SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM "swap_multi" AS sm, UNNEST(sm.amounts_out, sm.tokens_out) AS smo (amount, token)) AS tp JOIN tokens AS t ON t.address = '0x' || encode(tp.token, 'hex') GROUP BY tp.date, t.symbol, t.decimals ORDER BY tp.date DESC, amount DESC ) AS sub"#
        );
    }

    #[test]
    fn parse_simple_sql() {
        let query =
            parse_and_validate("select symbol, address from token where decimals > 10").unwrap();

        assert_eq!(
            query,
            r#"select to_jsonb(sub.*) as data from ( SELECT symbol, address FROM (SELECT * FROM "sgd0815"."token" WHERE block_range @> 2147483647) AS token WHERE decimals > 10 ) as sub"#
        );
        println!("{}", query);
    }
}
