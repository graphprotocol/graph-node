use super::{constants::SQL_DIALECT, formatter::Formatter, validation::Validator};
use crate::relational::{ColumnType, Layout};
use anyhow::{anyhow, Ok, Result};
use graph::components::store::BLOCK_NUMBER_MAX;
use itertools::Itertools;
use std::sync::Arc;

pub fn generate_table_prelude_from_layout(layout: &Layout) -> String {
    let schema = &layout.catalog.site.namespace;
    let ctes = layout
        .tables
        .iter()
        .filter(|(entity, _)| !entity.is_poi())
        .map(|(_, table)| {
            let table_name = table.name.as_str();

            let (block_column, filter) = if !table.immutable {
                (
                    "block_range",
                    Some(format!(" WHERE block_range @> {}", BLOCK_NUMBER_MAX)),
                )
            } else {
                ("block$", None)
            };

            let columns = table
                .columns
                .iter()
                .map(|col| {
                    if !col.is_list() && col.column_type == ColumnType::Bytes {
                        format!(
                            "concat('0x', encode({}, 'hex')) AS {}",
                            col.name.as_str(),
                            col.name.as_str()
                        )
                    } else {
                        col.name.to_string()
                    }
                })
                .chain(std::iter::once(block_column.to_string()))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{table_name} AS (SELECT {columns} FROM {schema}.{table_name}{})",
                filter.unwrap_or_default()
            )
        })
        .sorted()
        .collect::<Vec<_>>()
        .join(",\n");
    format!("WITH {ctes}")
}

pub struct Parser {
    schema: super::Schema,
    prelude: String,
}

impl Parser {
    pub fn new(layout: Arc<Layout>) -> Self {
        Self {
            schema: layout
                .tables
                .iter()
                .filter(|(entity, _)| !entity.is_poi())
                .map(|(_, table)| {
                    (
                        table.name.to_string(),
                        table
                            .columns
                            .iter()
                            .map(|column| column.name.to_string())
                            .collect(),
                    )
                })
                .collect(),
            prelude: generate_table_prelude_from_layout(&layout),
        }
    }

    pub fn parse_and_validate(&self, sql: &str) -> Result<String> {
        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql)?;

        let mut validator = Validator::new(&self.schema);
        validator.validate_statements(&statements)?;

        let mut formatter = Formatter::new(&self.prelude);

        let statement = statements
            .get_mut(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        let result = formatter.format(statement);

        Ok(result)
    }
}

#[cfg(test)]
mod test {

    use crate::layout_for_tests::{make_dummy_site, Catalog, Namespace};

    use super::*;
    use graph::{data::subgraph::DeploymentHash, schema::InputSchema};

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

    const NAMESPACE: &str = "sgd0815";

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

    fn test_layout() -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse(TEST_GQL, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new(NAMESPACE.to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog =
            Catalog::for_tests(site.clone(), Default::default()).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    #[test]
    fn parse_sql() {
        let parser = Parser::new(Arc::new(test_layout()));

        let result = parser.parse_and_validate(SQL_QUERY);

        assert!(result.is_ok());

        let query = result.unwrap();

        assert_eq!(
            query,
            "WITH swap_multi AS (SELECT concat('0x', encode(id, 'hex')) AS id, concat('0x', encode(sender, 'hex')) AS sender, amounts_in, tokens_in, amounts_out, tokens_out, referral_code, block_number, block_timestamp, concat('0x', encode(transaction_hash, 'hex')) AS transaction_hash, block$ FROM sgd0815.swap_multi),\ntoken AS (SELECT id, concat('0x', encode(address, 'hex')) AS address, symbol, name, decimals, block_range FROM sgd0815.token WHERE block_range @> 2147483647) SELECT to_jsonb(sub.*) AS data FROM ( WITH tokens AS (SELECT * FROM (VALUES ('0x0000000000000000000000000000000000000000', 'ETH', 'Ethereum', 18), ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'USDC', 'USD Coin', 6)) AS t (address, symbol, name, decimals)) SELECT date, t.symbol, SUM(amount) / pow(10, t.decimals) AS amount FROM (SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM swap_multi AS sm, UNNEST(sm.amounts_in, sm.tokens_in) AS smi (amount, token) UNION ALL SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM swap_multi AS sm, UNNEST(sm.amounts_out, sm.tokens_out) AS smo (amount, token)) AS tp JOIN tokens AS t ON t.address = '0x' || encode(tp.token, 'hex') GROUP BY tp.date, t.symbol, t.decimals ORDER BY tp.date DESC, amount DESC ) AS sub"
        );
    }
}
