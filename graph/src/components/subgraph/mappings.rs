// use std::collections::HashMap;
// use std::sync::Arc;
//
// use async_trait::async_trait;
// use failure::Error;
//
// use crate::prelude::*;
// use wasmi::ModuleImportResolver;
//
// /// Common trait for runtime host implementations.
// #[async_trait]
// pub trait MappingHost: Send + Sync + Debug + 'static {
//     type InputValue;
//     type OutputValue;
//
//     async fn call_export(
//         &self,
//         name: &str,
//         params: Vec<Self::InputValue>,
//     ) -> Result<Self::OutputValue, Error>;
// }
//
// pub trait MappingHostBuilder: Clone + Send + Sync + 'static {
//     type Host: MappingHost;
//
//     /// Build a new runtime host for a subgraph data source.
//     fn build(
//         &self,
//         logger: &Logger,
//         host_modules: HashMap<String, Box<dyn ModuleImportResolver>>,
//         metrics_registry: Arc<dyn MetricsRegistry>,
//     ) -> Result<Self::Host, Error>;
// }
//
