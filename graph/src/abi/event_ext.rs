use alloy::json_abi::Event;
use alloy::primitives::LogData;
use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use itertools::Itertools;
use web3::types::Log;

use crate::abi::DynSolParam;

pub trait EventExt {
    fn decode_log(&self, log: &Log) -> Result<Vec<DynSolParam>>;
}

impl EventExt for Event {
    fn decode_log(&self, log: &Log) -> Result<Vec<DynSolParam>> {
        let log_data = log_to_log_data(log)?;
        let decoded_event = alloy::dyn_abi::EventExt::decode_log(self, &log_data, true)?;

        if self.inputs.len() != decoded_event.indexed.len() + decoded_event.body.len() {
            return Err(anyhow!(
                "unexpected number of decoded event inputs; expected {}, got {}",
                self.inputs.len(),
                decoded_event.indexed.len() + decoded_event.body.len(),
            ));
        }

        let decoded_params = decoded_event
            .indexed
            .into_iter()
            .chain(decoded_event.body.into_iter())
            .enumerate()
            .map(|(i, value)| DynSolParam {
                name: self.inputs[i].name.clone(),
                value,
            })
            .collect();

        Ok(decoded_params)
    }
}

fn log_to_log_data(log: &Log) -> Result<LogData> {
    let topics = log
        .topics
        .iter()
        .map(|x| x.to_fixed_bytes().into())
        .collect_vec();

    let data = log.data.0.clone().into();

    LogData::new(topics, data).context("log has an invalid number of topics")
}

#[cfg(test)]
mod tests {
    use alloy::dyn_abi::DynSolValue;
    use alloy::primitives::U256;

    use super::*;

    fn make_log(topics: &[[u8; 32]], data: Vec<u8>) -> Log {
        Log {
            address: [1; 20].into(),
            topics: topics.iter().map(Into::into).collect(),
            data: data.into(),
            block_hash: None,
            block_number: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            transaction_log_index: None,
            log_type: None,
            removed: None,
        }
    }

    #[test]
    fn decode_log_no_topic_0() {
        let event = Event::parse("event X(uint256 indexed a, bytes32 b)").unwrap();
        let a = U256::from(10).to_be_bytes::<32>();
        let b = DynSolValue::FixedBytes([10; 32].into(), 32).abi_encode();

        let log = make_log(&[a], b);
        let err = event.decode_log(&log).unwrap_err();

        assert_eq!(
            err.to_string(),
            "invalid log topic list length: expected 2 topics, got 1",
        );
    }

    #[test]
    fn decode_log_invalid_topic_0() {
        let event = Event::parse("event X(uint256 indexed a, bytes32 b)").unwrap();
        let a = U256::from(10).to_be_bytes::<32>();
        let b = DynSolValue::FixedBytes([10; 32].into(), 32).abi_encode();

        let log = make_log(&[[0; 32], a], b);
        let err = event.decode_log(&log).unwrap_err();

        assert!(err.to_string().starts_with("invalid event signature:"));
    }

    #[test]
    fn decode_log_success() {
        let event = Event::parse("event X(uint256 indexed a, bytes32 b)").unwrap();
        let topic_0 = event.selector().0;
        let a = U256::from(10).to_be_bytes::<32>();
        let b = DynSolValue::FixedBytes([10; 32].into(), 32).abi_encode();

        let log = make_log(&[topic_0, a], b);
        let resp = event.decode_log(&log).unwrap();

        assert_eq!(
            resp,
            vec![
                DynSolParam {
                    name: "a".to_owned(),
                    value: DynSolValue::Uint(U256::from(10), 256),
                },
                DynSolParam {
                    name: "b".to_owned(),
                    value: DynSolValue::FixedBytes([10; 32].into(), 32),
                }
            ],
        );
    }

    #[test]
    fn decode_log_too_many_topics() {
        let event = Event::parse("event X(uint256 indexed a, bytes32 b)").unwrap();
        let topic_0 = event.selector().0;
        let a = U256::from(10).to_be_bytes::<32>();
        let b = DynSolValue::FixedBytes([10; 32].into(), 32).abi_encode();

        let log = make_log(&[topic_0, a, a, a, a], b);
        let err = event.decode_log(&log).unwrap_err();

        assert_eq!(err.to_string(), "log has an invalid number of topics");
    }
}
