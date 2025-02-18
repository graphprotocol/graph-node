use std::collections::BTreeSet;

use graph::{
    components::store::SubscriptionManager,
    prelude::{anyhow, StoreEventStreamBox, SubscriptionFilter},
};

pub mod catalog;
pub mod color;
pub mod commands;
pub mod deployment;
mod display;
pub mod prompt;

/// A dummy subscription manager that always panics
pub struct PanicSubscriptionManager;

impl SubscriptionManager for PanicSubscriptionManager {
    fn subscribe(&self, _: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox {
        panic!("we were never meant to call `subscribe`");
    }
}

pub type CmdResult = Result<(), anyhow::Error>;
