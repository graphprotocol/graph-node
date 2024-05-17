use std::{collections::HashSet, sync::Arc, time::Instant};

use anyhow::anyhow;
use clap::Parser;
use graph::{
    components::store::write::{EntityModification, RowGroupForPerfTest as RowGroup},
    data::{
        store::{Id, Value},
        subgraph::DeploymentHash,
        value::Word,
    },
    schema::{EntityType, InputSchema},
};
use lazy_static::lazy_static;
use rand::{rng, Rng};

#[derive(Parser)]
#[clap(
    name = "append_row",
    about = "Measure time it takes to append rows to a row group"
)]
struct Opt {
    /// Number of repetitions of the test
    #[clap(short, long, default_value = "5")]
    niter: usize,
    /// Number of rows
    #[clap(short, long, default_value = "10000")]
    rows: usize,
    /// Number of blocks
    #[clap(short, long, default_value = "300")]
    blocks: usize,
    /// Number of ids
    #[clap(short, long, default_value = "500")]
    ids: usize,
}

// A very fake schema that allows us to get the entity types we need
const GQL: &str = r#"
      type Thing @entity { id: ID!, count: Int! }
      type RowGroup @entity { id: ID! }
      type Entry @entity { id: ID! }
    "#;
lazy_static! {
    static ref DEPLOYMENT: DeploymentHash = DeploymentHash::new("batchAppend").unwrap();
    static ref SCHEMA: InputSchema = InputSchema::parse_latest(GQL, DEPLOYMENT.clone()).unwrap();
    static ref THING_TYPE: EntityType = SCHEMA.entity_type("Thing").unwrap();
    static ref ROW_GROUP_TYPE: EntityType = SCHEMA.entity_type("RowGroup").unwrap();
    static ref ENTRY_TYPE: EntityType = SCHEMA.entity_type("Entry").unwrap();
}

pub fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    let next_block = opt.blocks as f64 / opt.rows as f64;
    for _ in 0..opt.niter {
        let ids = (0..opt.ids)
            .map(|n| Id::String(Word::from(format!("00{n}010203040506"))))
            .collect::<Vec<_>>();
        let mut existing: HashSet<Id> = HashSet::new();
        let mut mods = Vec::new();
        let mut block = 0;
        let mut block_pos = Vec::new();
        for _ in 0..opt.rows {
            if rng().random_bool(next_block) {
                block += 1;
                block_pos.clear();
            }

            let mut attempt = 0;
            let pos = loop {
                if attempt > 20 {
                    return Err(anyhow!(
                        "Failed to find a position in 20 attempts. Increase `ids`"
                    ));
                }
                attempt += 1;
                let pos = rng().random_range(0..opt.ids);
                if block_pos.contains(&pos) {
                    continue;
                }
                block_pos.push(pos);
                break pos;
            };
            let id = &ids[pos];
            let data = vec![
                (Word::from("id"), Value::String(id.to_string())),
                (Word::from("count"), Value::Int(block as i32)),
            ];
            let data = Arc::new(SCHEMA.make_entity(data).unwrap());
            let md = if existing.contains(id) {
                EntityModification::Overwrite {
                    key: THING_TYPE.key(id.clone()),
                    data,
                    block,
                    end: None,
                }
            } else {
                existing.insert(id.clone());
                EntityModification::Insert {
                    key: THING_TYPE.key(id.clone()),
                    data,
                    block,
                    end: None,
                }
            };
            mods.push(md);
        }
        let mut group = RowGroup::new(THING_TYPE.clone(), false);

        let start = Instant::now();
        for md in mods {
            group.append_row(md).unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "Adding {} rows with {} ids across {} blocks took {:?}",
            opt.rows,
            existing.len(),
            block,
            elapsed
        );
    }
    Ok(())
}
