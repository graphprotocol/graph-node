/// Internal structures and logic specific to Pinata API

use serde::Deserialize;

#[derive(Deserialize)]
/// Error response structure from pinata
pub(crate) struct PinataApiError {
  error: String
}

impl PinataApiError {
    pub fn message(&self) -> String {
      self.error.clone()
    }
}
