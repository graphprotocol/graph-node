pub mod data_source;

use crate::schema::InputSchema;

pub use self::data_source::DataSource;

/// Represents a valid Nozzle Subgraph manifest.
///
/// This manifest contains parsed, formatted, and resolved data.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// The schema of the Subgraph.
    ///
    /// Contains all the entities, aggregations, and relationships between them.
    pub schema: InputSchema,

    /// The Nozzle data sources of the Subgraph.
    ///
    /// A Nozzle Subgraph can only contain Nozzle data sources.
    pub data_sources: Vec<DataSource>,
}
