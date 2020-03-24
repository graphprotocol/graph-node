use crate::prelude::Value;
use stable_hash::{prelude::*, utils::StableHasherWrapper, SequenceNumberInt};
use std::collections::HashMap;
use std::fmt;
use strum::AsStaticRef as _;
use strum_macros::AsStaticStr;
use twox_hash::XxHash64;

#[derive(Debug)]
pub struct ProofOfIndexingDigest(pub String);

impl StableHash for ProofOfIndexingDigest {
    fn stable_hash(&self, sequence_number: impl SequenceNumber, state: &mut impl StableHasher) {
        self.0.stable_hash(sequence_number, state)
    }
}

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
    fn stable_hash(&self, mut sequence_number: impl SequenceNumber, state: &mut impl StableHasher) {
        use ProofOfIndexingEvent::*;
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
        // Include the discriminant
        self.as_static().stable_hash(sequence_number, state);
    }
}

/// The POI is the StableHash of:
/// (Vec<ProofOfIndexingEvent>, PreviousDigest)
/// This struct contains the necessary state to construct that value in a streaming manner
pub struct ProofOfIndexingStream {
    previous_digest_sequence_number: SequenceNumberInt<u64>,
    vec_sequence_number: SequenceNumberInt<u64>,
    vec_length: usize,
    digest: StableHasherWrapper<XxHash64>,
}

impl ProofOfIndexingStream {
    fn new() -> Self {
        let mut tuple_sequence_number = SequenceNumberInt::<u64>::root();
        let vec_sequence_number = tuple_sequence_number.next_child();
        let previous_digest_sequence_number = tuple_sequence_number.next_child();
        Self {
            previous_digest_sequence_number,
            vec_sequence_number,
            vec_length: 0,
            digest: Default::default(),
        }
    }

    fn write(&mut self, event: &ProofOfIndexingEvent) {
        event.stable_hash(self.vec_sequence_number.next_child(), &mut self.digest);
        self.vec_length += 1;
    }

    pub fn finish(self, previous: &Option<ProofOfIndexingDigest>) -> ProofOfIndexingDigest {
        let Self {
            previous_digest_sequence_number,
            vec_sequence_number,
            vec_length,
            mut digest,
        } = self;

        // Finish out the vec digest
        vec_length.stable_hash(vec_sequence_number, &mut digest);

        // Add the previous digest to the end of the tuple
        previous.stable_hash(previous_digest_sequence_number, &mut digest);

        let hash = format!("{:x}", digest.finish());
        ProofOfIndexingDigest(hash)
    }
}

#[derive(Default)]
pub struct ProofOfIndexing {
    /// The POI is updated for each data source independently. This is necessary because
    /// some data sources (eg: IPFS files) may be unreliable and therefore cannot mix
    /// state with other data sources. This may also give us some freedom to change
    /// the order of triggers in the future.
    per_causality_region: HashMap<String, ProofOfIndexingStream>,
}

impl fmt::Debug for ProofOfIndexing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProofOfIndexing").field(&"...").finish()
    }
}

impl ProofOfIndexing {
    /// Adds an event to the digest of the ProofOfIndexingStream local to the DataSource
    pub fn write(&mut self, causality_region: &str, event: &ProofOfIndexingEvent<'_>) {
        // This may be better with the raw_entry API, once that is stabilized
        if let Some(data_source) = self.per_causality_region.get_mut(causality_region) {
            data_source.write(event);
        } else {
            let mut entry = ProofOfIndexingStream::new();
            entry.write(event);
            self.per_causality_region
                .insert(causality_region.to_owned(), entry);
        }
    }

    /// Swaps the internals out for an empty one
    /// Returns None if there are no changes.
    pub fn take(&mut self) -> Option<HashMap<String, ProofOfIndexingStream>> {
        if self.per_causality_region.is_empty() {
            None
        } else {
            Some(std::mem::replace(
                &mut self.per_causality_region,
                HashMap::new(),
            ))
        }
    }
}
