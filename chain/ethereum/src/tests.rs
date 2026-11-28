use std::sync::Arc;

use graph::{
    blockchain::{BlockPtr, Trigger, block_stream::BlockWithTriggers},
    prelude::{
        EthereumCall, LightEthereumBlock,
        alloy::{
            self,
            primitives::{Address, B256, Bytes, LogData},
            rpc::types::{Block, Log},
        },
        rand::{self, Rng},
    },
    slog::{self, Logger, o},
};

use crate::{
    chain::BlockFinality,
    trigger::{EthereumBlockTriggerType, EthereumTrigger, LogRef},
};

pub trait Random {
    fn random() -> Self;
}

impl Random for B256 {
    fn random() -> Self {
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        Self::from(bytes)
    }
}

impl Random for Address {
    fn random() -> Self {
        let mut rng = rand::rng();
        let mut bytes = [0u8; 20];
        rng.fill(&mut bytes);
        Self::from(bytes)
    }
}

fn create_log(tx_index: u64, log_index: u64) -> Arc<Log> {
    let log = Log {
        inner: alloy::primitives::Log {
            address: Address::default(),
            data: LogData::new_unchecked(vec![], Bytes::from(vec![])),
        },
        block_hash: Some(B256::ZERO),
        block_number: Some(0),
        block_timestamp: Some(0),
        transaction_hash: Some(B256::ZERO),
        transaction_index: Some(tx_index),
        log_index: Some(log_index),
        removed: false,
    };

    Arc::new(log)
}

#[test]
fn test_trigger_ordering() {
    let block1 = EthereumTrigger::Block(
        BlockPtr::from((B256::random(), 1u64)),
        EthereumBlockTriggerType::End,
    );

    let block2 = EthereumTrigger::Block(
        BlockPtr::from((B256::random(), 0u64)),
        EthereumBlockTriggerType::WithCallTo(Address::random()),
    );

    let call1 = EthereumCall {
        transaction_index: 1,
        ..Default::default()
    };
    let call1 = EthereumTrigger::Call(Arc::new(call1));

    let call2 = EthereumCall {
        transaction_index: 2,
        input: Bytes::from(vec![0]),
        ..Default::default()
    };
    let call2 = EthereumTrigger::Call(Arc::new(call2));

    let call3 = EthereumCall {
        transaction_index: 3,
        ..Default::default()
    };
    let call3 = EthereumTrigger::Call(Arc::new(call3));

    // Call with the same tx index as call2
    // different than call2 so they don't get mistaken as the same
    let call4 = EthereumCall {
        transaction_index: 2,
        input: Bytes::from(vec![1]),
        ..Default::default()
    };
    let call4 = EthereumTrigger::Call(Arc::new(call4));

    // Event with transaction_index 1 and log_index 0;
    // should be the first element after sorting
    let log1 = EthereumTrigger::Log(LogRef::FullLog(create_log(1, 0), None));

    // Event with transaction_index 1 and log_index 1;
    // should be the second element after sorting
    let log2 = EthereumTrigger::Log(LogRef::FullLog(create_log(1, 1), None));

    // Event with transaction_index 2 and log_index 5;
    // should come after call1 and before call2 after sorting
    let log3 = EthereumTrigger::Log(LogRef::FullLog(create_log(2, 5), None));

    let triggers = vec![
        // Call triggers; these should be in the order 1, 2, 4, 3 after sorting
        call3.clone(),
        call1.clone(),
        call2.clone(),
        call4.clone(),
        // Block triggers; these should appear at the end after sorting
        // but with their order unchanged
        block2.clone(),
        block1.clone(),
        // Event triggers
        log3.clone(),
        log2.clone(),
        log1.clone(),
    ];

    let logger = Logger::root(slog::Discard, o!());

    let b = Block::default();

    let b = LightEthereumBlock::new(graph::components::ethereum::AnyBlock::from(b));

    // Test that `BlockWithTriggers` sorts the triggers.
    let block_with_triggers = BlockWithTriggers::<crate::Chain>::new(
        BlockFinality::Final(Arc::new(b)),
        triggers,
        &logger,
    );

    let expected = vec![log1, log2, call1, log3, call2, call4, call3, block2, block1]
        .into_iter()
        .map(Trigger::Chain)
        .collect::<Vec<_>>();

    assert_eq!(block_with_triggers.trigger_data, expected);
}

#[test]
fn test_trigger_dedup() {
    let block1 = EthereumTrigger::Block(
        BlockPtr::from((B256::random(), 1u64)),
        EthereumBlockTriggerType::End,
    );

    let block2 = EthereumTrigger::Block(
        BlockPtr::from((B256::random(), 0u64)),
        EthereumBlockTriggerType::WithCallTo(Address::random()),
    );

    // duplicate block2
    let block3 = block2.clone();

    let call1 = EthereumCall {
        transaction_index: 1,
        ..Default::default()
    };
    let call1 = EthereumTrigger::Call(Arc::new(call1));

    let call2 = EthereumCall {
        transaction_index: 2,
        ..Default::default()
    };
    let call2 = EthereumTrigger::Call(Arc::new(call2));

    let call3 = EthereumCall {
        transaction_index: 3,
        ..Default::default()
    };
    let call3 = EthereumTrigger::Call(Arc::new(call3));

    // duplicate call2
    let call4 = EthereumCall {
        transaction_index: 2,
        ..Default::default()
    };
    let call4 = EthereumTrigger::Call(Arc::new(call4));

    let log1 = EthereumTrigger::Log(LogRef::FullLog(create_log(1, 0), None));
    let log2 = EthereumTrigger::Log(LogRef::FullLog(create_log(1, 1), None));
    let log3 = EthereumTrigger::Log(LogRef::FullLog(create_log(2, 5), None));
    // duplicate logs 2 and 3
    let log4 = log2.clone();
    let log5 = log3.clone();

    let triggers = vec![
        // Call triggers
        call3.clone(),
        call1.clone(),
        call2.clone(),
        call4,
        // Block triggers
        block3,
        block2.clone(),
        block1.clone(),
        // Event triggers
        log5,
        log4,
        log3.clone(),
        log2.clone(),
        log1.clone(),
    ];

    let logger = Logger::root(slog::Discard, o!());

    #[allow(unused_variables)]
    let b = Block::default();

    #[allow(unreachable_code)]
    let b = LightEthereumBlock::new(graph::components::ethereum::AnyBlock::from(b));

    // Test that `BlockWithTriggers` sorts the triggers.
    let block_with_triggers = BlockWithTriggers::<crate::Chain>::new(
        BlockFinality::Final(Arc::new(b)),
        triggers,
        &logger,
    );

    let expected = vec![log1, log2, call1, log3, call2, call3, block2, block1]
        .into_iter()
        .map(Trigger::Chain)
        .collect::<Vec<_>>();

    assert_eq!(block_with_triggers.trigger_data, expected);
}

#[test]
fn test_trigger_ordering_is_transitive() {
    // Regression test: `EthereumTrigger::cmp` compared `Call` triggers (and `Call`/`Log` pairs)
    // by `transaction_index`, but compared `Log`/`Log` pairs by `log_index` alone, ignoring
    // `transaction_index`. Those two comparisons only agree when `log_index` happens to increase
    // in lockstep with `transaction_index`, which does not hold for every trigger source (for
    // example log/call triggers that were fetched independently and merged). When it doesn't
    // hold, the combined ordering isn't transitive, which crashes Rust's sort with "user-provided
    // comparison function does not correctly implement a total order".
    //
    // Here, `log_a` is in an earlier transaction than `log_c` but has a higher `log_index`:
    let log_a = EthereumTrigger::Log(LogRef::FullLog(create_log(5, 50), None));
    let call_b = EthereumTrigger::Call(Arc::new(EthereumCall {
        transaction_index: 5,
        ..Default::default()
    }));
    let log_c = EthereumTrigger::Log(LogRef::FullLog(create_log(6, 10), None));

    // `log_a` and `call_b` share a transaction, so `log_a < call_b` (events before calls in the
    // same transaction). `call_b` is in an earlier transaction than `log_c`, so `call_b < log_c`.
    // Transitivity requires `log_a < log_c`.
    assert_eq!(log_a.cmp(&call_b), std::cmp::Ordering::Less);
    assert_eq!(call_b.cmp(&log_c), std::cmp::Ordering::Less);
    assert_eq!(
        log_a.cmp(&log_c),
        std::cmp::Ordering::Less,
        "transitivity violated: log_a < call_b < log_c but log_a is not < log_c"
    );

    // The actual regression: sorting a `Vec` containing this combination used to panic.
    let mut triggers = vec![log_c, call_b, log_a];
    triggers.sort();
}
