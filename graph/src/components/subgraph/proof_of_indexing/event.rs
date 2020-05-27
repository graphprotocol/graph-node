use crate::prelude::Value;
use stable_hash::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use strum::AsStaticRef as _;
use strum_macros::AsStaticStr;

#[derive(AsStaticStr)]
pub enum ProofOfIndexingEvent<'a> {
    RemoveEntity {
        entity_type: &'a str,
        id: &'a str,
    },
    SetEntity {
        entity_type: &'a str,
        id: &'a str,
        data: &'a HashMap<String, Value>,
    },
}

impl StableHash for ProofOfIndexingEvent<'_> {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        use ProofOfIndexingEvent::*;
        self.as_static()
            .stable_hash(sequence_number.next_child(), state);
        match self {
            RemoveEntity { entity_type, id } => {
                entity_type.stable_hash(sequence_number.next_child(), state);
                id.stable_hash(sequence_number.next_child(), state);
            }
            SetEntity {
                entity_type,
                id,
                data,
            } => {
                entity_type.stable_hash(sequence_number.next_child(), state);
                id.stable_hash(sequence_number.next_child(), state);
                data.stable_hash(sequence_number.next_child(), state);
            }
        }
    }
}

/// Different than #[derive(Debug)] in order to be deterministic so logs can be
/// diffed easily. In particular, we swap out the HashMap for a BTreeMap when
/// printing the data field of the SetEntity variant so that the keys are
/// sorted.
impl fmt::Debug for ProofOfIndexingEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct(self.as_static());
        match self {
            Self::RemoveEntity { entity_type, id } => {
                builder.field("entity_type", entity_type);
                builder.field("id", id);
            }
            Self::SetEntity {
                entity_type,
                id,
                data,
            } => {
                builder.field("entity_type", entity_type);
                builder.field("id", id);
                builder.field("data", &data.iter().collect::<BTreeMap<_, _>>());
            }
        }
        builder.finish()
    }
}

impl slog::Value for ProofOfIndexingEvent<'_> {
    fn serialize(
        &self,
        record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        format!("{:?}", self).serialize(record, key, serializer)
    }
}
