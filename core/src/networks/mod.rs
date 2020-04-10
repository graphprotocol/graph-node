use std::sync::Arc;

use graph::prelude::{NetworkInstance, NetworkInstanceId, NetworkRegistry as NetworkRegistryTrait};

pub struct NetworkRegistry {
  instances: Vec<Arc<dyn NetworkInstance>>,
}

impl NetworkRegistry {
  pub fn new() -> Self {
    Self {
      instances: Default::default(),
    }
  }
}

impl NetworkRegistryTrait for NetworkRegistry {
  fn register_instance(&mut self, instance: Arc<dyn NetworkInstance>) {
    self.instances.push(instance);
  }

  fn instances(&self, network: &str) -> Vec<Arc<dyn NetworkInstance>> {
    self
      .instances
      .iter()
      .filter(|instance| instance.id().network.eq(network))
      .cloned()
      .collect()
  }

  fn instance(&self, id: &NetworkInstanceId) -> Option<Arc<dyn NetworkInstance>> {
    self
      .instances
      .iter()
      .find(|instance| instance.id() == id)
      .map(|instance| instance.clone())
  }
}
