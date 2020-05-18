use graph::prelude::StoreError;

use crate::relational::SqlName;

/// Information about what tables and columns we have in the database
#[derive(Debug, Clone)]
pub struct Catalog {
    pub schema: String,
}

impl Catalog {
    pub fn new(schema: String) -> Result<Self, StoreError> {
        SqlName::check_valid_identifier(&schema, "database schema")?;
        Ok(Catalog { schema })
    }
}
