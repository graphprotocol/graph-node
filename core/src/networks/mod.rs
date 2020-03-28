use graph::prelude::{NetworkInstance, NetworkInstanceId, NetworkRegistry as NetworkRegistryTrait};

pub struct NetworkRegistry {
  instances: Vec<Box<dyn NetworkInstance>>,
}

impl NetworkRegistry {
  pub fn new() -> Self {
    Self {
      instances: Default::default(),
    }
  }
}

impl NetworkRegistryTrait for NetworkRegistry {
  fn register_instance(&mut self, instance: Box<dyn NetworkInstance>) {
    self.instances.push(instance);
  }

  fn instances(&self, network: &str) -> Vec<&Box<dyn NetworkInstance>> {
    self
      .instances
      .iter()
      .filter(|instance| instance.id().network.eq(network))
      .collect()
  }

  fn instance(&self, id: &NetworkInstanceId) -> Option<&Box<dyn NetworkInstance>> {
    self.instances.iter().find(|instance| instance.id() == id)
  }
}
