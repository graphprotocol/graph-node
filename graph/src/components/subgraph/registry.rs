use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct SubgraphRegistry<T> {
    names_to_ids: Arc<RwLock<HashMap<String, String>>>,
    ids_to_values: Arc<RwLock<HashMap<String, T>>>,
}

impl<T> Clone for SubgraphRegistry<T> {
    fn clone(&self) -> Self {
        SubgraphRegistry {
            names_to_ids: self.names_to_ids.clone(),
            ids_to_values: self.ids_to_values.clone(),
        }
    }
}

impl<T> SubgraphRegistry<T> {
    pub fn new() -> Self {
        SubgraphRegistry {
            names_to_ids: Default::default(),
            ids_to_values: Default::default(),
        }
    }

    pub fn resolve_map<U, F: FnOnce(&T) -> U>(&self, id_or_name: &String, by: F) -> Option<U> {
        let names_to_ids = self.names_to_ids.read().unwrap();
        let ids_to_values = self.ids_to_values.read().unwrap();

        let id = names_to_ids.get(id_or_name).unwrap_or(id_or_name);
        ids_to_values.get(id).map(by)
    }

    pub fn resolve(&self, id_or_name: &String) -> Option<T>
    where
        T: Clone,
    {
        let names_to_ids = self.names_to_ids.read().unwrap();
        let ids_to_values = self.ids_to_values.read().unwrap();

        let id = names_to_ids.get(id_or_name).unwrap_or(id_or_name);
        ids_to_values.get(id).cloned()
    }

    pub fn mutate(&self, id_or_name: &String, op: impl FnOnce(&mut T)) {
        let names_to_ids = self.names_to_ids.read().unwrap();
        let mut ids_to_values = self.ids_to_values.write().unwrap();

        let id = names_to_ids.get(id_or_name).unwrap_or(id_or_name);
        ids_to_values.get_mut(id).map(op);
    }

    pub fn insert(&mut self, name: Option<String>, id: String, value: T) {
        if let Some(name) = name {
            self.names_to_ids.write().unwrap().insert(name, id.clone());
        }
        self.ids_to_values.write().unwrap().insert(id, value);
    }

    pub fn remove_name(&mut self, name: String) {
        let id = self.names_to_ids.write().unwrap().remove(&name);

        if let Some(id) = id {
            self.ids_to_values.write().unwrap().remove(&id);
        }
    }

    pub fn remove_id(&mut self, id: String) {
        self.ids_to_values.write().unwrap().remove(&id);

        let names: Vec<_> = self
            .names_to_ids
            .read()
            .unwrap()
            .iter()
            .filter_map(|(name, value)| {
                if value == &id {
                    Some(name.clone())
                } else {
                    None
                }
            }).collect();

        for name in names {
            self.names_to_ids.write().unwrap().remove(&name);
        }
    }
}
