use napi::bindgen_prelude as n;
use napi_derive::napi;

use graph::{data::subgraph::DeploymentHash, schema::InputSchema};

#[napi]
pub fn validate_schema(schema: String, id: String) -> n::Result<String> {
    let id = match DeploymentHash::new(id) {
        Ok(id) => id,
        Err(e) => {
            return Err(n::Error::new(
                n::Status::InvalidArg,
                format!("Invalid deployment hash {e}"),
            ))
        }
    };
    let res = InputSchema::parse(&schema, id);
    match res {
        Ok(_) => Ok("ok".to_string()),
        Err(e) => Err(n::Error::new(n::Status::GenericFailure, e.to_string())),
    }
}
