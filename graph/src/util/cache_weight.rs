use crate::prelude::{BigDecimal, BigInt, Entity, Value};

/// Estimate of how much memory a value consumes.
/// Useful for measuring the size of caches.
pub trait CacheWeight {
    /// Total weight of the value.
    fn weight(&self) -> usize {
        std::mem::size_of_val(&self) + self.indirect_weight()
    }

    /// The weight of values pointed to by this value but logically owned by it, which is not
    /// accounted for by `size_of`.
    fn indirect_weight(&self) -> usize;
}

impl<T: CacheWeight> CacheWeight for Option<T> {
    fn indirect_weight(&self) -> usize {
        match self {
            Some(x) => x.indirect_weight(),
            None => 0,
        }
    }
}

impl<T: CacheWeight> CacheWeight for Vec<T> {
    fn indirect_weight(&self) -> usize {
        self.iter().map(CacheWeight::indirect_weight).sum()
    }
}

impl<T: CacheWeight, U: CacheWeight> CacheWeight for std::collections::BTreeMap<T, U> {
    fn indirect_weight(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.weight() + value.weight())
            .sum()
    }
}

impl CacheWeight for &'_ [u8] {
    fn indirect_weight(&self) -> usize {
        self.len()
    }
}

impl CacheWeight for String {
    fn indirect_weight(&self) -> usize {
        self.len()
    }
}

impl CacheWeight for BigDecimal {
    fn indirect_weight(&self) -> usize {
        (self.digits() as f32).log2() as usize
    }
}

impl CacheWeight for BigInt {
    fn indirect_weight(&self) -> usize {
        self.bits() / 8
    }
}

impl CacheWeight for Value {
    fn indirect_weight(&self) -> usize {
        match self {
            Value::String(s) => s.indirect_weight(),
            Value::BigDecimal(d) => d.indirect_weight(),
            Value::List(values) => values.indirect_weight(),
            Value::Bytes(bytes) => bytes.as_slice().indirect_weight(),
            Value::BigInt(n) => n.indirect_weight(),
            Value::Int(_) | Value::Bool(_) | Value::Null => 0,
        }
    }
}

impl CacheWeight for Entity {
    fn indirect_weight(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.weight() + value.weight())
            .sum()
    }
}

impl CacheWeight for graphql_parser::query::Value {
    fn indirect_weight(&self) -> usize {
        use graphql_parser::query as q;

        match self {
            q::Value::Boolean(_) | q::Value::Int(_) | q::Value::Null | q::Value::Float(_) => 0,
            q::Value::Enum(s) | q::Value::String(s) | q::Value::Variable(s) => s.indirect_weight(),
            q::Value::List(l) => l.indirect_weight(),
            q::Value::Object(o) => o.indirect_weight(),
        }
    }
}
