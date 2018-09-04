use std::collections::HashMap;

pub struct SubgraphRegistry<T> {
    names_to_ids: HashMap<String, String>,
    ids_to_values: HashMap<String, T>,
}

impl<T> SubgraphRegistry<T>
where
    T: Send,
{
    pub fn new() -> Self {
        SubgraphRegistry {
            names_to_ids: Default::default(),
            ids_to_values: Default::default(),
        }
    }

    pub fn resolve(&self, id_or_name: &String) -> Option<&T> {
        let id = self.names_to_ids.get(id_or_name).unwrap_or(id_or_name);
        self.ids_to_values.get(id)
    }

    pub fn insert(&mut self, name: Option<String>, id: String, value: T) {
        if let Some(name) = name {
            self.names_to_ids.insert(name, id.clone());
        }
        self.ids_to_values.insert(id, value);
    }

    pub fn remove(&mut self, id: String) {
        self.ids_to_values.remove(&id);

        let names: Vec<_> = self
            .names_to_ids
            .iter()
            .filter_map(|(name, value)| {
                if value == &id {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        for name in names {
            self.names_to_ids.remove(&name);
        }
    }
}
