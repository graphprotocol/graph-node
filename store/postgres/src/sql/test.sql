with tokens(address,symbol,name,decimals) as (
select 
lower(t.address) as address,
symbol,
name,
decimals
from (values 
('0x4Fabb145d64652a948d72533023f6E7A623C7C53','BUSD','Binance USD',18),
('0x0f51bb10119727a7e5eA3538074fb341F56B09Ad','DAO','DAO Maker',18),
('0x0000000000000000000000000000000000000000','ETH','Ethereum',18),
('0xdB25f211AB05b1c97D595516F45794528a807ad8','EURS','STASIS EURS Token',2),
('0xc944E90C64B2c07662A292be6244BDf05Cda44a7','GRT','Graph Token',18),
('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48','USDC','USD Coin',6),
('0xdAC17F958D2ee523a2206206994597C13D831ec7','USDT','Tether USD',6),
('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599','WBTC','Wrapped BTC',8),
('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2','WETH','Wrapped Ether',18)
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

