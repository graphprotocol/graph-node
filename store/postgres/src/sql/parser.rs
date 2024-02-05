use crate::relational::Layout;
use anyhow::{anyhow, Ok, Result};
use std::sync::Arc;

use super::{constants::SQL_DIALECT, formatter::Formatter, validation::Validator};

pub struct Parser {
    namespace: String,
    schema: super::Schema,
    cte_prefix: String,
}

impl Parser {
    pub fn new(layout: Arc<Layout>) -> Self {
        Self {
            namespace: layout.site.namespace.to_string(),
            schema: layout
                .tables
                .iter()
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
            cte_prefix: "-- CTE --".to_owned(),
        }
    }

    pub fn parse_and_validate(&self, sql: &str) -> Result<String> {
        let mut statements = sqlparser::parser::Parser::parse_sql(&SQL_DIALECT, sql)?;

        let mut validator = Validator::new(&self.schema);
        validator.validate_statements(&mut statements)?;

        let mut formatter = Formatter::new(&self.namespace, &self.cte_prefix);

        let mut statement = statements
            .get_mut(0)
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        let result = formatter.format(&mut statement);

        Ok(result)
    }
}

#[cfg(test)]
mod test {

    use crate::layout_for_tests::{make_dummy_site, Catalog, Namespace};

    use super::*;
    use graph::{data::subgraph::DeploymentHash, schema::InputSchema};

    const TEST_GQL: &str = "
        type OwnershipTransferred @entity(immutable: true) {
            id: Bytes!
            previousOwner: Bytes! # address
            newOwner: Bytes! # address
            blockNumber: BigInt!
            blockTimestamp: BigInt!
            transactionHash: Bytes!
        }
        
        type Swap @entity(immutable: true) {
            id: Bytes!
            sender: Bytes! # address
            inputAmount: BigInt! # uint256
            inputToken: Bytes! # address
            amountOut: BigInt! # uint256
            outputToken: Bytes! # address
            slippage: BigInt! # int256
            referralCode: BigInt! # uint32
            blockNumber: BigInt!
            blockTimestamp: BigInt!
            transactionHash: Bytes!
        }
        
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
    ";

    const NAMESPACE: &str = "sgd0815";

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

        let sql = include_str!("./test.sql");

        let result = parser.parse_and_validate(sql);

        assert!(result.is_ok());

        let query = result.unwrap();

        assert_eq!(query,"-- CTE -- SELECT to_jsonb(sub.*) AS data FROM ( WITH tokens (address, symbol, name, decimals) AS (SELECT lower(t.address) AS address, symbol, name, decimals FROM (VALUES ('0x4Fabb145d64652a948d72533023f6E7A623C7C53', 'BUSD', 'Binance USD', 18), ('0x0f51bb10119727a7e5eA3538074fb341F56B09Ad', 'DAO', 'DAO Maker', 18), ('0x0000000000000000000000000000000000000000', 'ETH', 'Ethereum', 18), ('0xdB25f211AB05b1c97D595516F45794528a807ad8', 'EURS', 'STASIS EURS Token', 2), ('0xc944E90C64B2c07662A292be6244BDf05Cda44a7', 'GRT', 'Graph Token', 18), ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'USDC', 'USD Coin', 6), ('0xdAC17F958D2ee523a2206206994597C13D831ec7', 'USDT', 'Tether USD', 6), ('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599', 'WBTC', 'Wrapped BTC', 8), ('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 'WETH', 'Wrapped Ether', 18)) AS t (address, symbol, name, decimals)) SELECT date, t.symbol, SUM(amount) / pow(10, t.decimals) AS amount FROM (SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM sgd0815.swap_multi AS sm, UNNEST(sm.amounts_in, sm.tokens_in) AS smi (amount, token) UNION ALL SELECT date(to_timestamp(block_timestamp) AT TIME ZONE 'utc') AS date, token, amount FROM sgd0815.sgd1.swap_multi AS sm, UNNEST(sm.amounts_out, sm.tokens_out) AS smo (amount, token)) AS tp JOIN sgd0815.tokens AS t ON t.address = '0x' || encode(tp.token, 'hex') GROUP BY tp.date, t.symbol, t.decimals ORDER BY tp.date DESC, amount DESC ) AS sub;");
    }
}
