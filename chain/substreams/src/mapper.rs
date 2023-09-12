use std::collections::HashMap;
use std::str::FromStr;

use crate::codec::entity_change;
use crate::{codec, Block, Chain, EntityChanges, ParsedChanges, TriggerData};
use graph::blockchain::block_stream::{
    BlockStreamEvent, BlockWithTriggers, FirehoseCursor, SubstreamsError, SubstreamsMapper,
};
use graph::data::store::scalar::Bytes;
use graph::data::store::IdType;
use graph::data::value::Word;
use graph::data_source::CausalityRegion;
use graph::prelude::BigDecimal;
use graph::prelude::{async_trait, BigInt, BlockHash, BlockNumber, BlockPtr, Logger, Value};
use graph::schema::InputSchema;
use graph::slog::o;
use graph::substreams::Clock;
use graph::substreams_rpc::response::Message as SubstreamsMessage;
use prost::Message;

// Mapper will transform the proto content coming from substreams in the graph-out format
// into the internal Block representation. If schema is passed then additional transformation
// into from the substreams block representation is performed into the Entity model used by
// the store. If schema is None then only the original block is passed. This None should only
// be used for block ingestion where entity content is empty and gets discarded.
pub struct Mapper {
    pub schema: Option<InputSchema>,
}

#[async_trait]
impl SubstreamsMapper<Chain> for Mapper {
    async fn to_block_stream_event(
        &self,
        logger: &mut Logger,
        message: Option<SubstreamsMessage>,
    ) -> Result<Option<BlockStreamEvent<Chain>>, SubstreamsError> {
        match message {
            Some(SubstreamsMessage::Session(session_init)) => {
                *logger = logger.new(o!("trace_id" => session_init.trace_id));
                return Ok(None);
            }
            Some(SubstreamsMessage::BlockUndoSignal(undo)) => {
                let valid_block = match undo.last_valid_block {
                    Some(clock) => clock,
                    None => return Err(SubstreamsError::InvalidUndoError),
                };
                let valid_ptr = BlockPtr {
                    hash: valid_block.id.trim_start_matches("0x").try_into()?,
                    number: valid_block.number as i32,
                };
                return Ok(Some(BlockStreamEvent::Revert(
                    valid_ptr,
                    FirehoseCursor::from(undo.last_valid_cursor.clone()),
                )));
            }

            Some(SubstreamsMessage::BlockScopedData(block_scoped_data)) => {
                let module_output = match &block_scoped_data.output {
                    Some(out) => out,
                    None => return Ok(None),
                };

                let clock = match block_scoped_data.clock {
                    Some(clock) => clock,
                    None => return Err(SubstreamsError::MissingClockError),
                };

                let cursor = &block_scoped_data.cursor;

                let Clock {
                    id: hash,
                    number,
                    timestamp: _,
                } = clock;

                let hash: BlockHash = hash.as_str().try_into()?;
                let number: BlockNumber = number as BlockNumber;

                let changes: EntityChanges = match module_output.map_output.as_ref() {
                    Some(msg) => Message::decode(msg.value.as_slice())
                        .map_err(SubstreamsError::DecodingError)?,
                    None => EntityChanges {
                        entity_changes: [].to_vec(),
                    },
                };

                let parsed_changes = match self.schema.as_ref() {
                    Some(schema) => parse_changes(&changes, schema)?,
                    None => vec![],
                };
                let mut triggers = vec![];
                if changes.entity_changes.len() >= 1 {
                    triggers.push(TriggerData {});
                }

                // Even though the trigger processor for substreams doesn't care about TriggerData
                // there are a bunch of places in the runner that check if trigger data
                // empty and skip processing if so. This will prolly breakdown
                // close to head so we will need to improve things.

                // TODO(filipe): Fix once either trigger data can be empty
                // or we move the changes into trigger data.
                Ok(Some(BlockStreamEvent::ProcessBlock(
                    BlockWithTriggers::new(
                        Block {
                            hash,
                            number,
                            changes,
                            parsed_changes,
                        },
                        triggers,
                        logger,
                    ),
                    FirehoseCursor::from(cursor.clone()),
                )))
            }

            // ignoring Progress messages and SessionInit
            // We are only interested in Data and Undo signals
            _ => Ok(None),
        }
    }
}

fn parse_changes(
    changes: &EntityChanges,
    schema: &InputSchema,
) -> anyhow::Result<Vec<ParsedChanges>> {
    let mut parsed_changes = vec![];
    for entity_change in changes.entity_changes.iter() {
        let mut parsed_data: HashMap<Word, Value> = HashMap::default();
        let entity_type = schema.entity_type(&entity_change.entity)?;

        // Make sure that the `entity_id` gets set to a value
        // that is safe for roundtrips through the database. In
        // particular, if the type of the id is `Bytes`, we have
        // to make sure that the `entity_id` starts with `0x` as
        // that will be what the key for such an entity have
        // when it is read from the database.
        //
        // Needless to say, this is a very ugly hack, and the
        // real fix is what's described in [this
        // issue](https://github.com/graphprotocol/graph-node/issues/4663)
        let entity_id: String = match entity_type.id_type()? {
            IdType::String => entity_change.id.clone(),
            IdType::Bytes => {
                if entity_change.id.starts_with("0x") {
                    entity_change.id.clone()
                } else {
                    format!("0x{}", entity_change.id)
                }
            }
        };
        // Substreams don't currently support offchain data
        let key = entity_type.parse_key_in(Word::from(entity_id), CausalityRegion::ONCHAIN)?;

        let id = key.id_value();
        parsed_data.insert(Word::from("id"), id);

        let changes = match entity_change.operation() {
            entity_change::Operation::Create | entity_change::Operation::Update => {
                for field in entity_change.fields.iter() {
                    let new_value: &codec::value::Typed = match &field.new_value {
                        Some(codec::Value {
                            typed: Some(new_value),
                        }) => &new_value,
                        _ => continue,
                    };

                    let value: Value = decode_value(new_value)?;
                    *parsed_data
                        .entry(Word::from(field.name.as_str()))
                        .or_insert(Value::Null) = value;
                }
                let entity = schema
                    .make_entity(parsed_data)
                    .map_err(anyhow::Error::from)?;

                ParsedChanges::Upsert { key, entity }
            }
            entity_change::Operation::Delete => ParsedChanges::Delete(key),
            entity_change::Operation::Unset => ParsedChanges::Unset,
        };
        parsed_changes.push(changes);
    }

    Ok(parsed_changes)
}

fn decode_value(value: &crate::codec::value::Typed) -> anyhow::Result<Value> {
    use codec::value::Typed;

    match value {
        Typed::Int32(new_value) => Ok(Value::Int(*new_value)),

        Typed::Bigdecimal(new_value) => BigDecimal::from_str(new_value)
            .map(Value::BigDecimal)
            .map_err(|err| anyhow::Error::from(err)),

        Typed::Bigint(new_value) => BigInt::from_str(new_value)
            .map(Value::BigInt)
            .map_err(|err| anyhow::Error::from(err)),

        Typed::String(new_value) => {
            let mut string = new_value.clone();

            // Strip null characters since they are not accepted by Postgres.
            if string.contains('\u{0000}') {
                string = string.replace('\u{0000}', "");
            }
            Ok(Value::String(string))
        }

        Typed::Bytes(new_value) => base64::decode(new_value)
            .map(|bs| Value::Bytes(Bytes::from(bs)))
            .map_err(|err| anyhow::Error::from(err)),

        Typed::Bool(new_value) => Ok(Value::Bool(*new_value)),

        Typed::Array(arr) => arr
            .value
            .iter()
            .filter_map(|item| item.typed.as_ref().map(decode_value))
            .collect::<anyhow::Result<Vec<Value>>>()
            .map(Value::List),
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Add, str::FromStr};

    use super::decode_value;
    use crate::codec::value::Typed;
    use crate::codec::{Array, Value};
    use graph::{
        data::store::scalar::Bytes,
        prelude::{BigDecimal, BigInt, Value as GraphValue},
    };

    #[test]
    fn validate_substreams_field_types() {
        struct Case {
            name: String,
            value: Value,
            expected_value: GraphValue,
        }

        let cases = vec![
            Case {
                name: "string value".to_string(),
                value: Value {
                    typed: Some(Typed::String(
                        "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d"
                            .to_string(),
                    )),
                },
                expected_value: GraphValue::String(
                    "d4325ee72c39999e778a9908f5fb0803f78e30c441a5f2ce5c65eee0e0eba59d".to_string(),
                ),
            },
            Case {
                name: "bytes value".to_string(),
                value: Value {
                    typed: Some(Typed::Bytes(
                        base64::encode(
                            hex::decode(
                                "445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f",
                            )
                            .unwrap(),
                        )
                        .into_bytes(),
                    )),
                },
                expected_value: GraphValue::Bytes(
                    Bytes::from_str(
                        "0x445247fe150195bd866516594e087e1728294aa831613f4d48b8ec618908519f",
                    )
                    .unwrap(),
                ),
            },
            Case {
                name: "int value for block".to_string(),
                value: Value {
                    typed: Some(Typed::Int32(12369760)),
                },
                expected_value: GraphValue::Int(12369760),
            },
            Case {
                name: "negative int value".to_string(),
                value: Value {
                    typed: Some(Typed::Int32(-12369760)),
                },
                expected_value: GraphValue::Int(-12369760),
            },
            Case {
                name: "big int".to_string(),
                value: Value {
                    typed: Some(Typed::Bigint("123".to_string())),
                },
                expected_value: GraphValue::BigInt(BigInt::from(123u64)),
            },
            Case {
                name: "big int > u64".to_string(),
                value: Value {
                    typed: Some(Typed::Bigint(
                        BigInt::from(u64::MAX).add(BigInt::from(1)).to_string(),
                    )),
                },
                expected_value: GraphValue::BigInt(BigInt::from(u64::MAX).add(BigInt::from(1))),
            },
            Case {
                name: "big decimal value".to_string(),
                value: Value {
                    typed: Some(Typed::Bigdecimal("3133363633312e35".to_string())),
                },
                expected_value: GraphValue::BigDecimal(BigDecimal::new(
                    BigInt::from(3133363633312u64),
                    35,
                )),
            },
            Case {
                name: "bool value".to_string(),
                value: Value {
                    typed: Some(Typed::Bool(true)),
                },
                expected_value: GraphValue::Bool(true),
            },
            Case {
                name: "string array".to_string(),
                value: Value {
                    typed: Some(Typed::Array(Array {
                        value: vec![
                            Value {
                                typed: Some(Typed::String("1".to_string())),
                            },
                            Value {
                                typed: Some(Typed::String("2".to_string())),
                            },
                            Value {
                                typed: Some(Typed::String("3".to_string())),
                            },
                        ],
                    })),
                },
                expected_value: GraphValue::List(vec!["1".into(), "2".into(), "3".into()]),
            },
        ];

        for case in cases.into_iter() {
            let value: GraphValue = decode_value(&case.value.typed.unwrap()).unwrap();
            assert_eq!(case.expected_value, value, "failed case: {}", case.name)
        }
    }
}
