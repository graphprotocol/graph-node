use crate::prelude::{BigDecimal, BigInt, Value};
use std::mem;

/// Estimate of how much memory a value consumes.
/// Useful for measuring the size of caches.
pub trait CacheWeight {
    /// Total weight of the value.
    fn weight(&self) -> usize {
        mem::size_of_val(&self) + self.indirect_weight()
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
        self.iter().map(CacheWeight::indirect_weight).sum::<usize>()
            + self.capacity() * mem::size_of::<T>()
    }
}

impl<T: CacheWeight, U: CacheWeight> CacheWeight for std::collections::BTreeMap<T, U> {
    fn indirect_weight(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.weight() + value.weight())
            .sum()
    }
}

impl<T: CacheWeight, U: CacheWeight> CacheWeight for std::collections::HashMap<T, U> {
    fn indirect_weight(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.indirect_weight() + value.indirect_weight())
            .sum::<usize>()
            + self.capacity() * mem::size_of::<(T, U, u64)>()
    }
}

impl CacheWeight for String {
    fn indirect_weight(&self) -> usize {
        self.capacity()
    }
}

impl CacheWeight for BigDecimal {
    fn indirect_weight(&self) -> usize {
        ((self.digits() as f32 * std::f32::consts::LOG2_10) / 8.0).ceil() as usize
    }
}

impl CacheWeight for BigInt {
    fn indirect_weight(&self) -> usize {
        self.bits() / 8
    }
}

impl CacheWeight for crate::data::store::scalar::Bytes {
    fn indirect_weight(&self) -> usize {
        self.as_slice().len()
    }
}

impl CacheWeight for Value {
    fn indirect_weight(&self) -> usize {
        match self {
            Value::String(s) => s.indirect_weight(),
            Value::BigDecimal(d) => d.indirect_weight(),
            Value::List(values) => values.indirect_weight(),
            Value::Bytes(bytes) => bytes.indirect_weight(),
            Value::BigInt(n) => n.indirect_weight(),
            Value::Int(_) | Value::Bool(_) | Value::Null => 0,
        }
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

#[test]
fn big_decimal_cache_weight() {
    use std::str::FromStr;

    // 22.4548 has 18 bits as binary, so 3 bytes.
    let n = BigDecimal::from_str("22.454800000000").unwrap();
    assert_eq!(n.indirect_weight(), 3);
}
