use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Demo {
    pub event: String,
    pub value: i32,
}
