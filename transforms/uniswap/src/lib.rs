pub mod abi;
pub mod proto;
mod types;

use std::str::FromStr;

use abi::positionmanager::events::{DecreaseLiquidity, IncreaseLiquidity, Transfer};
use borsh::{BorshDeserialize, BorshSerialize};
use ethabi::Address;
use num_bigint::BigInt;
use prost::Message;
use prost_types::Any;
use proto::edgeandnode::uniswap::v1::event::Event2;
use substreams_ethereum::block_view::LogView;
use substreams_ethereum::pb::eth::v2::Block;

use graph::indexer::{BlockTransform, EncodedBlock, EncodedTriggers, State};

use crate::abi::factory::events::PoolCreated;
use crate::abi::pool::events::{Burn, Flash, Initialize, Mint, Swap};
use crate::abi::positionmanager::events::Collect;
use crate::proto::edgeandnode::uniswap::v1 as uniswap;

const UNISWAP_V3_FACTORY: &str = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
pub const POOL_TAG: &str = "pool";

#[derive(BorshSerialize, BorshDeserialize)]
pub struct Pool {
    address: Vec<u8>,
    token0: Vec<u8>,
    token1: Vec<u8>,
    owner: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct UniswapTransform {
    factory_addr: Address,
}

impl UniswapTransform {
    #[allow(unused)]
    pub fn new() -> Self {
        Self {
            factory_addr: Address::from_str(UNISWAP_V3_FACTORY).unwrap(),
        }
    }
}

fn parse_log(log_view: &LogView) -> Option<(uniswap::EventType, Event2)> {
    let log = &log_view.log;
    if Collect::match_log(&log) {
        let evt: uniswap::Collect = Collect::decode(log).unwrap().into();
        return Some((uniswap::EventType::Collect, Event2::Collect(evt)));
    }

    if IncreaseLiquidity::match_log(&log) {
        let evt: uniswap::IncreaseLiquidity = IncreaseLiquidity::decode(log).unwrap().into();
        return Some((
            uniswap::EventType::IncreaseLiquidity,
            Event2::Increaseliquidity(evt),
        ));
    }
    if DecreaseLiquidity::match_log(&log) {
        let evt: uniswap::DecreaseLiquidity = DecreaseLiquidity::decode(log).unwrap().into();
        return Some((
            uniswap::EventType::DecreaseLiquidity,
            Event2::Decreaseliquidity(evt),
        ));
    }
    if Collect::match_log(&log) {
        let evt: uniswap::Collect = Collect::decode(log).unwrap().into();
        return Some((uniswap::EventType::Collect, Event2::Collect(evt)));
    }
    if Transfer::match_log(&log) {
        let evt: uniswap::Transfer = Transfer::decode(log).unwrap().into();
        return Some((uniswap::EventType::Transfer, Event2::Transfer(evt)));
    }
    if Initialize::match_log(&log) {
        let evt: uniswap::Initialize = Initialize::decode(log).unwrap().into();
        return Some((uniswap::EventType::Initialize, Event2::Initialize(evt)));
    }
    if Swap::match_log(&log) {
        let evt = Swap::decode(log).unwrap();
        let evt: uniswap::Swap = (log_view.receipt.transaction.from.clone(), log.index, evt).into();
        return Some((uniswap::EventType::Swap, Event2::Swap(evt)));
    }
    if Mint::match_log(&log) {
        let evt = Mint::decode(log).unwrap();
        let evt: uniswap::Mint = (log_view.receipt.transaction.from.clone(), log.index, evt).into();
        return Some((uniswap::EventType::Mint, Event2::Mint(evt)));
    }
    if Burn::match_log(&log) {
        let evt = Burn::decode(log).unwrap();
        let evt: uniswap::Burn = (log_view.receipt.transaction.from.clone(), log.index, evt).into();
        return Some((uniswap::EventType::Burn, Event2::Burn(evt)));
    }
    if Flash::match_log(&log) {
        let evt: uniswap::Flash = Flash::decode(log).unwrap().into();
        return Some((uniswap::EventType::Flash, Event2::Flash(evt)));
    }

    if PoolCreated::match_log(&log) {
        let evt: uniswap::PoolCreated = PoolCreated::decode(log).unwrap().into();
        return Some((uniswap::EventType::PoolCreated, Event2::Poolcreated(evt)));
    }

    None
}

impl BlockTransform for UniswapTransform {
    fn transform(&self, block: EncodedBlock, mut state: State) -> (State, EncodedTriggers) {
        let mut events = vec![];
        let block = Block::decode(block.0.as_ref()).unwrap();

        for log in block.logs().into_iter() {
            // skip reverted blocks
            if log.log.block_index == 0 {
                continue;
            }

            // // TODO: Improve perf .
            let evt = parse_log(&log);
            match evt {
                Some((et, event)) => events.push(uniswap::Event {
                    owner: hex::encode(log.address()),
                    r#type: et.into(),
                    event: None,
                    address: hex::encode(log.address()),
                    block_number: block.header.as_ref().unwrap().number.try_into().unwrap(),
                    block_timestamp: block.timestamp_seconds().to_string(),
                    tx_hash: hex::encode(&log.receipt.transaction.hash),
                    tx_gas_used: log.receipt.transaction.gas_used.to_string(),
                    tx_gas_price: BigInt::from_bytes_le(
                        num_bigint::Sign::NoSign,
                        &log.receipt
                            .transaction
                            .gas_price
                            .clone()
                            .unwrap_or_default()
                            .bytes,
                    )
                    .to_string(),
                    event2: Some(event),
                }),
                None => continue,
            }
        }

        // let bs = borsh::to_vec(&Events { events }).unwrap();
        let events = uniswap::Events { events };

        (
            state,
            EncodedTriggers(events.encode_to_vec().into_boxed_slice()),
        )
    }
}
