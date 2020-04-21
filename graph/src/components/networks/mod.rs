use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use async_trait::*;

use crate::prelude::{format_err, Error, SubgraphManifest};
use futures03::future::AbortHandle;

pub mod blockchains;
use blockchains::Blockchain;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NetworkInstanceId {
  pub network: String,
  pub name: String,
}

impl fmt::Display for NetworkInstanceId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}/{}", self.network, self.name)
  }
}

impl TryFrom<&str> for NetworkInstanceId {
  type Error = Error;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    let segments: Vec<&str> = s.split('/').collect();

    if segments.len() != 2 {
      return Err(format_err!(
        "Network instance ID `{}` must be of the form `<network>/<instance>'",
        s
      ));
    }

    let network = segments[0];
    let name = segments[1];

    if !network.chars().all(|c| c.is_alphanumeric() || c == '-') {
      return Err(format_err!(
        "Network `{}` must only contain alpha-numeric characters and dashes",
        network
      ));
    }

    if !name.chars().all(|c| c.is_alphanumeric() || c == '-') {
      return Err(format_err!(
        "Network instance `{}` must only contain alpha-numeric characters and dashes",
        name
      ));
    }

    Ok(NetworkInstanceId {
      network: network.into(),
      name: name.into(),
    })
  }
}

#[async_trait]
pub trait NetworkInstance: Blockchain + Send + Sync + 'static {
  fn id(&self) -> &NetworkInstanceId;
  fn url(&self) -> &str;

  /// Indexes a subgraph, returning a guard that allows to cancel it later.
  async fn start_subgraph(&self, subgraph: SubgraphManifest) -> Result<AbortHandle, Error>;
}

#[async_trait]
pub trait NetworkRegistry: Send + Sync + 'static {
  fn register_instance(&mut self, chain: Arc<dyn NetworkInstance>);
  fn instances(&self, network: &str) -> Vec<Arc<dyn NetworkInstance>>;
  fn instance(&self, id: &NetworkInstanceId) -> Option<Arc<dyn NetworkInstance>>;
}
