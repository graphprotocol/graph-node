use std::convert::TryFrom;
use std::error::Error;
use std::fmt::{self, Display};

#[derive(Default)]
pub struct Cache;

impl Cache {
    pub fn trigger_error(&mut self) -> Result<(), anyhow::Error> {
        Result::<u64, Err>::Ok(0)?;
        Ok(())
    }
}

/// Error caused while executing a [Query](struct.Query.html).
#[derive(Debug)]
pub enum Err {
    Err(q::Value),
}

impl Display for Err {
    fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl Error for Err {}

impl From<anyhow::Error> for Err {
    fn from(_: anyhow::Error) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    List(Vec<Value>),
}

impl TryFrom<q::Value> for Value {
    type Error = q::Value;

    fn try_from(value: q::Value) -> Result<Self, Self::Error> {
        match value {
            q::Value::List(vals) => vals
                .into_iter()
                .map(Value::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List),
            _ => todo!(),
        }
    }
}

pub mod q {
    use std::collections::BTreeMap;

    pub type Value = ValueInner<'static, String>;

    pub trait AsValue<'a> {
        type Value;
    }

    impl<'a> AsValue<'a> for String {
        type Value = String;
    }

    #[derive(Debug)]
    pub enum ValueInner<'a, T: AsValue<'a>> {
        Variable(T::Value),
        List(Vec<ValueInner<'a, T>>),
        Object(BTreeMap<T::Value, ValueInner<'a, T>>),
    }
}
