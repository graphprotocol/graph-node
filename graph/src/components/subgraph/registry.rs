use data::subgraph::SubgraphId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct SubgraphRegistry<T> {
    ids_to_values: Arc<RwLock<HashMap<SubgraphId, T>>>,
}

impl<T> Clone for SubgraphRegistry<T> {
    fn clone(&self) -> Self {
        SubgraphRegistry {
            ids_to_values: self.ids_to_values.clone(),
        }
    }
}

impl<T> SubgraphRegistry<T> {
    pub fn new() -> Self {
        SubgraphRegistry {
            ids_to_values: Default::default(),
        }
    }

    pub fn resolve_map<U, F: FnOnce(&T) -> U>(&self, id: &SubgraphId, by: F) -> Option<U> {
        let ids_to_values = self.ids_to_values.read().unwrap();
        ids_to_values.get(id).map(by)
    }

    pub fn resolve(&self, id: &SubgraphId) -> Option<T>
    where
        T: Clone,
    {
        let ids_to_values = self.ids_to_values.read().unwrap();
        ids_to_values.get(id).cloned()
    }

    pub fn mutate(&self, id: &SubgraphId, op: impl FnOnce(&mut T)) {
        let mut ids_to_values = self.ids_to_values.write().unwrap();
        ids_to_values.get_mut(id).map(op);
    }

    pub fn insert(&mut self, id: SubgraphId, value: T) {
        self.ids_to_values.write().unwrap().insert(id, value);
    }

    pub fn remove_id(&mut self, id: SubgraphId) {
        self.ids_to_values.write().unwrap().remove(&id);
    }
}
