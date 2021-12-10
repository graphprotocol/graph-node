use graph::{
    components::store::{SubscriptionManager, UnitStream},
    prelude::{StoreEventStreamBox, SubscriptionFilter},
};

pub mod catalog;
pub mod commands;
pub mod deployment;
mod display;

/// A dummy subscription manager that always panics
pub struct PanicSubscriptionManager;

impl SubscriptionManager for PanicSubscriptionManager {
    fn subscribe(&self, _: Vec<SubscriptionFilter>) -> StoreEventStreamBox {
        panic!("we were never meant to call `subscribe`");
    }

    fn subscribe_no_payload(&self, _: Vec<SubscriptionFilter>) -> UnitStream {
        panic!("we were never meant to call `subscribe_no_payload`");
    }
}
