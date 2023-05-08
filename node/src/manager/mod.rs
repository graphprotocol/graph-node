use std::collections::BTreeSet;

use graph::{
    components::store::{SubscriptionManager, UnitStream},
    prelude::{anyhow, StoreEventStreamBox, SubscriptionFilter},
};

pub mod cli;
pub mod color;
pub mod core;
pub mod deployment;
pub mod display;
pub mod prompt;
pub mod utils;

/// A dummy subscription manager that always panics
pub struct PanicSubscriptionManager;

impl SubscriptionManager for PanicSubscriptionManager {
    fn subscribe(&self, _: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox {
        panic!("we were never meant to call `subscribe`");
    }

    fn subscribe_no_payload(&self, _: BTreeSet<SubscriptionFilter>) -> UnitStream {
        panic!("we were never meant to call `subscribe_no_payload`");
    }
}

pub type CmdResult = Result<(), anyhow::Error>;
