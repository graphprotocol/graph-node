mod array_decoder;
mod decoder;
mod list_decoder;
mod mapping_decoder;
mod name_cache;
mod value_decoder;

pub mod utils;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, RecordBatch};

use self::{list_decoder::ListDecoder, mapping_decoder::MappingDecoder, name_cache::NameCache};
use crate::{
    data::{
        graphql::TypeExt,
        store::{Id, IdType, Value},
        value::Word,
    },
    schema::{EntityKey, EntityType, Field, InputSchema},
};

pub use self::{array_decoder::ArrayDecoder, decoder::Decoder};

/// Handles decoding of record batches to subgraph entities.
pub struct Codec {
    input_schema: InputSchema,
    name_cache: NameCache,
}

/// Contains the entities decoded from a record batch.
pub struct DecodeOutput {
    /// The type of entities in this batch.
    pub entity_type: EntityType,

    /// The type of the ID of entities in this batch.
    pub id_type: IdType,

    /// A list of decoded entities of the same type.
    pub decoded_entities: Vec<DecodedEntity>,
}

/// Contains a single entity decoded from a record batch.
pub struct DecodedEntity {
    /// The unique ID of the entity.
    ///
    /// When set to `None`, the ID is expected to be auto-generated before a new entity is persisted.
    pub key: Option<EntityKey>,

    /// A list of entity field names and their values.
    ///
    /// This list could contain a subset of fields of an entity.
    pub entity_data: Vec<(Word, Value)>,
}

impl Codec {
    /// Creates a new decoder for the `input_schema`.
    pub fn new(input_schema: InputSchema) -> Self {
        let name_cache = NameCache::new();

        Self {
            input_schema,
            name_cache,
        }
    }

    /// Decodes a `record_batch` according to the schema of the entity with name `entity_name`.
    ///
    /// # Errors
    ///
    /// Returns an error if `record_batch` is not compatible with the schema of the entity with name `entity_name`.
    ///
    /// The returned error is deterministic.
    pub fn decode(&mut self, record_batch: RecordBatch, entity_name: &str) -> Result<DecodeOutput> {
        let entity_type = self.entity_type(entity_name)?;
        let id_type = entity_type.id_type()?;
        let value_decoders = self.value_decoders(&entity_type, &record_batch)?;
        let mut decoded_entities = Vec::with_capacity(record_batch.num_rows());

        for i in 0..record_batch.num_rows() {
            let err_ctx = |s: &str| format!("field '{s}' at row {i}");
            let mut entity_id: Option<Value> = None;
            let mut entity_data = Vec::with_capacity(value_decoders.len());

            for (&field_name, value_decoder) in &value_decoders {
                let value = value_decoder
                    .decode(i)
                    .with_context(|| err_ctx(field_name))?;

                if field_name.eq_ignore_ascii_case("id") {
                    entity_id = Some(value.clone());
                }

                entity_data.push((Word::from(field_name), value));
            }

            let entity_key = entity_id
                .map(Id::try_from)
                .transpose()
                .with_context(|| err_ctx("id"))?
                .map(|entity_id| entity_type.key(entity_id));

            decoded_entities.push(DecodedEntity {
                key: entity_key,
                entity_data,
            });
        }

        drop(value_decoders);

        Ok(DecodeOutput {
            entity_type,
            id_type,
            decoded_entities,
        })
    }

    /// Returns the type of the entity with name `entity_name`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - There is no entity with name `entity_name`
    /// - The entity is not an object
    /// - The entity is a POI entity
    ///
    /// The returned error is deterministic.
    fn entity_type(&self, entity_name: &str) -> Result<EntityType> {
        let entity_type = self
            .input_schema
            .entity_type(entity_name)
            .context("entity not found")?;

        if !entity_type.is_object_type() {
            return Err(anyhow!("entity is not an object"));
        }

        if entity_type.is_poi() {
            return Err(anyhow!("entity is POI entity"));
        }

        Ok(entity_type)
    }

    /// Creates and returns value decoders for the fields of the entity with name `entity_name`.
    ///
    /// # Errors
    ///
    /// Returns an error if a decoder could not be created for a required field.
    ///
    /// The returned error is deterministic.
    fn value_decoders<'a>(
        &mut self,
        entity_type: &'a EntityType,
        record_batch: &'a RecordBatch,
    ) -> Result<BTreeMap<&'a str, Box<dyn Decoder<Value> + 'a>>> {
        let object_type = entity_type.object_type().unwrap();
        let columns = record_batch
            .schema_ref()
            .fields()
            .into_iter()
            .zip(record_batch.columns())
            .map(|(field, array)| Ok((self.ident(field.name()), array.as_ref())))
            .collect::<Result<HashMap<_, _>>>()?;

        let mut value_decoders = BTreeMap::new();
        for field in &object_type.fields {
            let Some(value_decoder) = self.value_decoder(field, &columns)? else {
                continue;
            };

            value_decoders.insert(field.name.as_str(), value_decoder);
        }

        Ok(value_decoders)
    }

    /// Creates and returns a value decoder for the `field`.
    ///
    /// Returns `None` when the `field` does not require a decoder.
    /// This happens for derived fields, reserved fields, and when there is no associated
    /// Arrow array for a nullable `field` or a `field` that could be auto-generated.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - There is no associated Arrow array for a required `field`
    /// - The `field` type is not compatible with the Arrow array
    ///
    /// The returned error is deterministic.
    fn value_decoder<'a>(
        &mut self,
        field: &'a Field,
        columns: &HashMap<Arc<str>, &'a dyn Array>,
    ) -> Result<Option<Box<dyn Decoder<Value> + 'a>>> {
        // VIDs are auto-generated
        if field.name.eq_ignore_ascii_case("vid") {
            return Ok(None);
        }

        // Derived fields are handled automatically
        if field.is_derived() {
            return Ok(None);
        }

        let normalized_name = self.ident(&field.name);
        let array = match columns.get(&normalized_name) {
            Some(&array) => array,
            None => {
                // Allow ID auto-generation
                if field.name.eq_ignore_ascii_case("id") {
                    return Ok(None);
                }

                // Allow partial entities
                if !field.field_type.is_non_null() {
                    return Ok(None);
                }

                bail!("failed to get column for field '{}'", field.name);
            }
        };

        let decoder = value_decoder::value_decoder(field.value_type, field.is_list(), array)
            .with_context(|| format!("failed to create decoder for field '{}'", field.name))?;

        Ok(Some(decoder))
    }

    fn ident(&mut self, name: impl AsRef<str>) -> Arc<str> {
        self.name_cache.ident(name.as_ref())
    }
}
