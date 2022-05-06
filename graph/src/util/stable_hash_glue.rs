use stable_hash::{StableHash, StableHasher};
use stable_hash_legacy::prelude::{
    StableHash as StableHashLegacy, StableHasher as StableHasherLegacy,
};

/// Implements StableHash and StableHashLegacy. This macro supports two forms:
/// Struct { field1, field2, ... } and Tuple(transparent). Each field supports
/// an optional modifier. For example: Tuple(transparent: AsBytes)
#[macro_export]
macro_rules! _impl_stable_hash {
    ($T:ident$(<$lt:lifetime>)? {$($field:ident$(:$e:path)?),*}) => {
        // Only this first impl commented, the same comments apply throughout
        impl stable_hash::StableHash for $T$(<$lt>)? {
            // This suppressed warning is for the final index + 1, which is unused
            // in the next "iteration of the loop"
            #[allow(unused_assignments)]
            fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
                // Destructuring ensures we have all of the fields. If a field is added to the struct,
                // it must be added to the macro or it will fail to compile.
                let $T { $($field,)* } = self;
                let mut index = 0;
                $(
                    // We might need to "massage" the value, for example, to wrap
                    // it in AsBytes. So we provide a way to inject those.
                    $(let $field = $e($field);)?
                    stable_hash::StableHash::stable_hash(&$field, stable_hash::FieldAddress::child(&field_address, index), state);
                    index += 1;
                )*
            }
        }

        impl stable_hash_legacy::StableHash for $T$(<$lt>)? {
            fn stable_hash<H: stable_hash_legacy::StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
                let $T { $($field,)* } = self;
                $(
                    $(let $field = $e($field);)*
                    stable_hash_legacy::StableHash::stable_hash(&$field, stable_hash_legacy::SequenceNumber::next_child(&mut sequence_number), state);
                )*
            }
        }
    };
    ($T:ident$(<$lt:lifetime>)? (transparent$(:$e:path)?)) => {
        impl stable_hash::StableHash for $T$(<$lt>)? {
            #[allow(unused_assignments)]
            fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
                let Self(transparent) = self;
                $(let transparent = $e(transparent);)?
                stable_hash::StableHash::stable_hash(&transparent, field_address, state);
            }
        }

        impl stable_hash_legacy::StableHash for $T$(<$lt>)? {
            fn stable_hash<H: stable_hash_legacy::StableHasher>(&self, sequence_number: H::Seq, state: &mut H,
            ) {
                let Self(transparent) = self;
                $(let transparent = $e(transparent);)*
                stable_hash_legacy::StableHash::stable_hash(&transparent, sequence_number, state);
            }
        }
    };
}
pub use crate::_impl_stable_hash as impl_stable_hash;

pub struct AsBytes<T>(pub T);

impl<T> StableHashLegacy for AsBytes<T>
where
    T: AsRef<[u8]>,
{
    fn stable_hash<H: StableHasherLegacy>(&self, sequence_number: H::Seq, state: &mut H) {
        stable_hash_legacy::utils::AsBytes(self.0.as_ref()).stable_hash(sequence_number, state);
    }
}

impl<T> StableHash for AsBytes<T>
where
    T: AsRef<[u8]>,
{
    fn stable_hash<H: StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        stable_hash::utils::AsBytes(self.0.as_ref()).stable_hash(field_address, state);
    }
}
