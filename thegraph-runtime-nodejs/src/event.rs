use serde;

use thegraph::prelude::*;

#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeEvent {
    pub entity: String,
    pub operation: String,
    pub data: Entity,
}
