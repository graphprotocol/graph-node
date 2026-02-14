//! Criterion benchmarks for entity cache data structures.
//!
//! These benchmarks measure the performance of LfuCache, EntityOp::apply_to,
//! Entity::sorted_ref, and Entity::validate — the key data structures on the
//! hot path of subgraph trigger processing.
//!
//! Run:
//!   cargo bench -p graph --bench entity_cache
//!
//! Save a baseline for before/after comparison:
//!   cargo bench -p graph --bench entity_cache -- --save-baseline before
//!   # ... make changes ...
//!   cargo bench -p graph --bench entity_cache -- --baseline before

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

use graph::data::store::Value;
use graph::data::subgraph::{DeploymentHash, LATEST_VERSION};
use graph::entity;
use graph::schema::InputSchema;
use graph::util::lfu_cache::LfuCache;

const SCHEMA_GQL: &str = "type Transfer @entity {
    id: String!
    from: String!
    to: String!
    amount: BigInt!
    blockNumber: BigInt!
    timestamp: BigInt!
    gasUsed: BigInt!
    gasPrice: BigInt!
    logIndex: BigInt!
    transactionHash: String!
    token: String!
    sender: String!
    receiver: String!
    memo: String!
    status: String!
    fee: BigInt!
    nonce: BigInt!
    value: BigInt!
    data: String!
    confirmed: Boolean!
}";

fn make_schema() -> InputSchema {
    let id = DeploymentHash::new("QmBenchTest00000000000000000000000000000000000").unwrap();
    InputSchema::parse(LATEST_VERSION, SCHEMA_GQL, id).unwrap()
}

fn make_entity(schema: &InputSchema, i: u64) -> graph::prelude::Entity {
    entity! { schema =>
        id: format!("0x{i:064x}"),
        from: format!("0xfrom{i:060x}"),
        to: format!("0xto{i:062x}"),
        amount: Value::BigInt(i.into()),
        blockNumber: Value::BigInt(i.into()),
        timestamp: Value::BigInt(1_700_000_000u64.into()),
        gasUsed: Value::BigInt(21000u64.into()),
        gasPrice: Value::BigInt(20_000_000_000u64.into()),
        logIndex: Value::BigInt(i.into()),
        transactionHash: format!("0xtx{i:061x}"),
        token: format!("0xtoken{i:058x}"),
        sender: format!("0xsender{i:057x}"),
        receiver: format!("0xreceiver{i:055x}"),
        memo: format!("transfer memo #{i}"),
        status: "confirmed",
        fee: Value::BigInt(100u64.into()),
        nonce: Value::BigInt(i.into()),
        value: Value::BigInt((i * 1_000_000).into()),
        data: format!("0xdata{i:060x}"),
        confirmed: true
    }
}

fn bench_lfu_cache_get(c: &mut Criterion) {
    let schema = make_schema();
    let entity_type = schema.entity_type("Transfer").unwrap();
    let n = 10_000;

    let mut cache: LfuCache<graph::schema::EntityKey, Option<Arc<graph::prelude::Entity>>> =
        LfuCache::new();
    let mut keys = Vec::with_capacity(n);
    for i in 0..n as u64 {
        let key = entity_type.parse_key(format!("0x{i:064x}")).unwrap();
        let entity = make_entity(&schema, i);
        cache.insert(key.clone(), Some(Arc::new(entity)));
        keys.push(key);
    }

    c.bench_with_input(BenchmarkId::new("lfu_cache_get", n), &keys, |b, keys| {
        b.iter(|| {
            // Access 1000 random-ish keys from the cache
            for i in (0..1000).map(|j| (j * 7 + 13) % keys.len()) {
                black_box(cache.get(&keys[i]));
            }
        });
    });
}

fn bench_lfu_cache_insert(c: &mut Criterion) {
    let schema = make_schema();
    let entity_type = schema.entity_type("Transfer").unwrap();
    let n = 1_000;

    let entries: Vec<_> = (0..n as u64)
        .map(|i| {
            let key = entity_type.parse_key(format!("0x{i:064x}")).unwrap();
            let entity = make_entity(&schema, i);
            (key, Some(Arc::new(entity)))
        })
        .collect();

    c.bench_with_input(
        BenchmarkId::new("lfu_cache_insert", n),
        &entries,
        |b, entries| {
            b.iter(|| {
                let mut cache: LfuCache<
                    graph::schema::EntityKey,
                    Option<Arc<graph::prelude::Entity>>,
                > = LfuCache::new();
                for (key, entity) in entries {
                    cache.insert(key.clone(), entity.clone());
                }
                black_box(&cache);
            });
        },
    );
}

fn bench_lfu_cache_evict(c: &mut Criterion) {
    let schema = make_schema();
    let entity_type = schema.entity_type("Transfer").unwrap();
    let n = 10_000;

    let entries: Vec<_> = (0..n as u64)
        .map(|i| {
            let key = entity_type.parse_key(format!("0x{i:064x}")).unwrap();
            let entity = make_entity(&schema, i);
            (key, Some(Arc::new(entity)))
        })
        .collect();

    c.bench_with_input(
        BenchmarkId::new("lfu_cache_evict", n),
        &entries,
        |b, entries| {
            b.iter_batched(
                || {
                    let mut cache: LfuCache<
                        graph::schema::EntityKey,
                        Option<Arc<graph::prelude::Entity>>,
                    > = LfuCache::new();
                    for (key, entity) in entries {
                        cache.insert(key.clone(), entity.clone());
                    }
                    cache
                },
                |mut cache| {
                    // Evict with a small target weight to force significant eviction
                    black_box(cache.evict(1));
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );
}

fn bench_entity_sorted_ref(c: &mut Criterion) {
    let schema = make_schema();
    let entity = make_entity(&schema, 42);

    c.bench_function("entity_sorted_ref_20_fields", |b| {
        b.iter(|| {
            black_box(entity.sorted_ref());
        });
    });
}

fn bench_entity_validate(c: &mut Criterion) {
    let schema = make_schema();
    let entity_type = schema.entity_type("Transfer").unwrap();
    let entity = make_entity(&schema, 42);
    let key = entity_type.parse_key("0x42").unwrap();

    c.bench_function("entity_validate_20_fields", |b| {
        b.iter(|| {
            black_box(entity.validate(black_box(&key))).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_lfu_cache_get,
    bench_lfu_cache_insert,
    bench_lfu_cache_evict,
    bench_entity_sorted_ref,
    bench_entity_validate,
);
criterion_main!(benches);
