use crate::{
    abi::{
        factory::events::PoolCreated,
        pool::events::{Burn, Flash, Initialize, Mint, Swap},
        positionmanager::events::{Collect, DecreaseLiquidity, IncreaseLiquidity, Transfer},
    },
    proto::edgeandnode::uniswap::v1 as uniswap,
};

impl Into<uniswap::PoolCreated> for PoolCreated {
    fn into(self) -> uniswap::PoolCreated {
        let PoolCreated {
            token0,
            token1,
            fee,
            tick_spacing,
            pool,
        } = self;

        uniswap::PoolCreated {
            token0: hex::encode(token0),
            token1: hex::encode(token1),
            fee: fee.to_string(),
            tick_spacing: tick_spacing.to_string(),
            pool: hex::encode(pool),
        }
    }
}

// Position manager events
impl Into<uniswap::IncreaseLiquidity> for IncreaseLiquidity {
    fn into(self) -> uniswap::IncreaseLiquidity {
        let IncreaseLiquidity {
            token_id,
            liquidity,
            amount0,
            amount1,
        } = self;

        uniswap::IncreaseLiquidity {
            token_id: token_id.to_string(),
            liquidity: liquidity.to_string(),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
        }
    }
}
impl Into<uniswap::DecreaseLiquidity> for DecreaseLiquidity {
    fn into(self) -> uniswap::DecreaseLiquidity {
        let DecreaseLiquidity {
            token_id,
            liquidity,
            amount0,
            amount1,
        } = self;

        uniswap::DecreaseLiquidity {
            token_id: token_id.to_string(),
            liquidity: liquidity.to_string(),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
        }
    }
}
impl Into<uniswap::Collect> for Collect {
    fn into(self) -> uniswap::Collect {
        let Collect {
            token_id,
            amount0,
            amount1,
            recipient,
        } = self;

        uniswap::Collect {
            token_id: token_id.to_string(),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            recipient: hex::encode(recipient),
        }
    }
}
impl Into<uniswap::Transfer> for Transfer {
    fn into(self) -> uniswap::Transfer {
        let Transfer { from, to, token_id } = self;

        uniswap::Transfer {
            from: hex::encode(from),
            to: hex::encode(to),
            token_id: token_id.to_string(),
        }
    }
}

// Pool events
impl Into<uniswap::Initialize> for Initialize {
    fn into(self) -> uniswap::Initialize {
        let Initialize {
            sqrt_price_x96,
            tick,
        } = self;

        uniswap::Initialize {
            sqrt_price_x96: sqrt_price_x96.to_string(),
            tick: tick.to_string(),
        }
    }
}
impl From<(Vec<u8>, u32, Swap)> for uniswap::Swap {
    fn from(val: (Vec<u8>, u32, Swap)) -> Self {
        let Swap {
            sender,
            recipient,
            amount0,
            amount1,
            sqrt_price_x96,
            liquidity,
            tick,
        } = val.2;

        uniswap::Swap {
            sender: hex::encode(sender),
            recipient: hex::encode(recipient),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            sqrt_price_x96: sqrt_price_x96.to_string(),
            liquidity: liquidity.to_string(),
            tick: tick.to_string(),
            log_index: val.1.try_into().unwrap(),
            transaction_from: hex::encode(val.0),
        }
    }
}
impl From<(Vec<u8>, u32, Mint)> for uniswap::Mint {
    fn from(val: (Vec<u8>, u32, Mint)) -> Self {
        let Mint {
            sender,
            owner,
            amount0,
            amount1,
            tick_lower,
            tick_upper,
            amount,
        } = val.2;

        uniswap::Mint {
            sender: hex::encode(sender),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            owner: hex::encode(owner),
            tick_lower: tick_lower.to_string(),
            tick_upper: tick_upper.to_string(),
            amount: amount.to_string(),
            log_index: val.1.try_into().unwrap(),
            transaction_from: hex::encode(val.0),
        }
    }
}
impl From<(Vec<u8>, u32, Burn)> for uniswap::Burn {
    fn from(val: (Vec<u8>, u32, Burn)) -> Self {
        let Burn {
            owner,
            amount0,
            amount1,
            tick_lower,
            tick_upper,
            amount,
        } = val.2;

        uniswap::Burn {
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            owner: hex::encode(owner),
            tick_lower: tick_lower.to_string(),
            tick_upper: tick_upper.to_string(),
            amount: amount.to_string(),
            log_index: val.1.try_into().unwrap(),
            transaction_from: hex::encode(val.0),
        }
    }
}
impl Into<uniswap::Flash> for Flash {
    fn into(self) -> uniswap::Flash {
        let Flash {
            sender,
            recipient,
            amount0,
            amount1,
            paid0,
            paid1,
        } = self;

        uniswap::Flash {
            sender: hex::encode(sender),
            recipient: hex::encode(recipient),
            amount0: amount0.to_string(),
            amount1: amount1.to_string(),
            paid0: paid0.to_string(),
            paid1: paid1.to_string(),
        }
    }
}
