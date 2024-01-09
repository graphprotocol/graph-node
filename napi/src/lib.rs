use napi::bindgen_prelude as n;
use napi_derive::napi;

use graph::{data::subgraph::DeploymentHash, schema::InputSchema};

#[napi]
pub fn validate_schema(schema: String, id: String) -> n::Result<Vec<String>> {
    let id = match DeploymentHash::new(id) {
        Ok(id) => id,
        Err(e) => {
            return Err(n::Error::new(
                n::Status::InvalidArg,
                format!("Invalid deployment hash {e}"),
            ))
        }
    };
    let errs = InputSchema::validate(&schema, id)
        .into_iter()
        .map(|e| e.to_string())
        .collect();
    Ok(errs)
}
