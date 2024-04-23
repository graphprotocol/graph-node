use crate::pinata::PinataApi;
use anyhow::{anyhow, Error};
use base64::{engine::general_purpose, Engine as _};
use byteorder::{BigEndian, ByteOrder};
use ethabi::{ParamType, Token};
use inflector::Inflector;
use itertools::Itertools;
use json::{array, JsonValue};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{ser::SerializeMap, ser::SerializeSeq, Deserialize, Serialize};
use serde_json::Value;
use serde_json_path::JsonPath;

use tiny_keccak::keccak256;
use web3::types::{H160, U256};

use crate::packed_decode::decode_packed;

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ERC725DynamicParam {
    Index(u32),
    Length(),
    Name(String),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ERC725JsonSchema {
    pub name: String,
    pub key: String,
    pub key_type: String,
    pub value_type: String,
    pub value_content: String,
    pub short_name: Option<String>,
    pub dynamic: Option<ERC725DynamicParam>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ERC725Value {
    Null(),
    Array(Vec<ERC725Value>),
    Address(H160),
    Bytes(Vec<u8>),
    Boolean(bool),
    Int(U256),
    UInt(U256),
    String(String),
    VerifiableURI {
        url: String,
        method: Vec<u8>,
        data: Vec<u8>,
    },
    BitArray(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ERC725DecodedValue {
    Null(),
    ArrayLength {
        name: String,
        length: u32,
    },
    ArrayItem {
        name: String,
        index: u32,
        value: ERC725Value,
    },
    Value {
        name: String,
        dynamic: Value,
        value: ERC725Value,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ERC725Error {
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("IPFS file {0} is too large. It can be at most {1} bytes")]
    FileTooLarge(String, usize),
    #[error("JSON format error")]
    Json(#[from] serde_json::Error),
    #[error("ethabi error")]
    Abi(#[from] ethabi::Error),
    #[error("character encoding")]
    Encoding(#[from] std::string::FromUtf8Error),
    #[error("Generic error {0}")]
    Error(String),
}

impl Serialize for ERC725Value {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ERC725Value::Null() => ser.serialize_none(),
            ERC725Value::Boolean(value) => ser.serialize_bool(*value),
            ERC725Value::Address(address) => {
                let data = hex::encode(address.as_bytes());
                let data = format!("0x{}", &data);
                ser.serialize_str(&data)
            }
            ERC725Value::Bytes(bytes) => {
                let data = hex::encode(bytes);
                let data = format!("0x{}", &data);
                ser.serialize_str(&data)
            }
            ERC725Value::UInt(number) => {
                if number.0[1..] == [0u64; 3] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 8];
                    number.copy_from_slice(&bytes[(32 - 8)..]);
                    let val = u64::from_le_bytes(number);
                    return ser.serialize_u64(val);
                }
                if number.0[2..] == [0u64; 2] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 16];
                    number.copy_from_slice(&bytes[16..]);
                    let val = u128::from_le_bytes(number);
                    return ser.serialize_u128(val);
                }
                let mut bytes = [0u8; 32];
                number.to_big_endian(&mut bytes);
                ser.serialize_str(format!("0x{}", hex::encode(bytes)).as_str())
            }
            ERC725Value::Int(number) => {
                if number.0[1..] == [0xffffffffffffffffu64; 3] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 8];
                    number.copy_from_slice(&bytes[(32 - 8)..]);
                    let val = i64::from_le_bytes(number);
                    return ser.serialize_i64(val);
                }
                if number.0[2..] == [0xffffffffffffffffu64; 2] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 16];
                    number.copy_from_slice(&bytes[16..]);
                    let val = i128::from_le_bytes(number);
                    return ser.serialize_i128(val);
                }
                if number.0[1..] == [0u64; 3] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 8];
                    number.copy_from_slice(&bytes[(32 - 8)..]);
                    let val = u64::from_le_bytes(number);
                    return ser.serialize_u64(val);
                }
                if number.0[2..] == [0u64; 2] {
                    let mut bytes = [0u8; 32];
                    number.to_little_endian(&mut bytes);
                    let mut number = [0u8; 16];
                    number.copy_from_slice(&bytes[16..]);
                    let val = u128::from_le_bytes(number);
                    return ser.serialize_u128(val);
                }
                let mut bytes = [0u8; 32];
                number.to_big_endian(&mut bytes);
                ser.serialize_str(format!("0x{}", hex::encode(bytes)).as_str())
            }
            ERC725Value::String(string) => ser.serialize_str(string),
            ERC725Value::VerifiableURI { url, method, data } => {
                let mut map = ser.serialize_map(Some(3))?;
                map.serialize_entry("url", url)?;
                map.serialize_entry("method", method)?;
                map.serialize_entry("data", data)?;
                map.end()
            }
            ERC725Value::Array(array) => {
                let mut seq = ser.serialize_seq(Some(array.len()))?;
                for item in array {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            ERC725Value::BitArray(array) => {
                let data = hex::encode(array);
                let data = format!("0x{}", &data);
                ser.serialize_str(&data)
            }
        }
    }
}

pub const KECCAK256_UTF8: &str = "keccak256(utf8)";
pub const KECCAK256_BYTES: &str = "keccak256(bytes)";
pub const HASH_KECCAK256_UTF8: &str = "6f357c6a";
pub const HASH_KECCAK256_BYTES: &str = "8019f9b1";
pub const HASHBYTES_KECCAK256_UTF8: &[u8] = &[0x6f, 0x35, 0x7c, 0x6a];
pub const HASHBYTES_KECCAK256_BYTES: &[u8] = &[0x80, 0x19, 0xf9, 0xb1];

lazy_static! {
  // Declare a global constant array using json::array!
    static ref SCHEMAS: JsonValue = array![
  {
    "name": "LSP1UniversalReceiverDelegate",
    "key": "0x0cfc51aec37c55a4d0b1a65c6255c4bf2fbdf6277f3cc0730c45b828b6db8b47",
    "keyType": "Singleton",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP1UniversalReceiverDelegate:<bytes32>",
    "key": "0x0cfc51aec37c55a4d0b10000<bytes32>",
    "keyType": "Mapping",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "SupportedStandards:LSP3Profile",
    "key": "0xeafec4d89fa9619884b600005ef83ad9559033e6e941db7d7c495acdce616347",
    "keyType": "Mapping",
    "valueType": "bytes4",
    "valueContent": "0x5ef83ad9"
  },
  {
    "name": "LSP3Profile",
    "key": "0x5ef83ad9559033e6e941db7d7c495acdce616347d28e90c7ce47cbfcfcad3bc5",
    "keyType": "Singleton",
    "valueType": "bytes",
    "valueContent": "VerifiableURI"
  },
  {
    "name": "LSP12IssuedAssets[]",
    "key": "0x7c8c3416d6cda87cd42c71ea1843df28ac4850354f988d55ee2eaa47b6dc05cd",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP12IssuedAssetsMap:<address>",
    "key": "0x74ac2555c10b9349e78f0000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "LSP5ReceivedAssets[]",
    "key": "0x6460ee3c0aac563ccbf76d6e1d07bada78e3a9514e6382b736ed3f478ab7b90b",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP5ReceivedAssetsMap:<address>",
    "key": "0x812c4334633eb816c80d0000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "LSP1UniversalReceiverDelegate",
    "key": "0x0cfc51aec37c55a4d0b1a65c6255c4bf2fbdf6277f3cc0730c45b828b6db8b47",
    "keyType": "Singleton",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP1UniversalReceiverDelegate:<bytes32>",
    "key": "0x0cfc51aec37c55a4d0b10000<bytes32>",
    "keyType": "Mapping",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP17Extension:<bytes4>",
    "key": "0xcee78b4094da860110960000<bytes4>",
    "keyType": "Mapping",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "SupportedStandards:LSP4DigitalAsset",
    "key": "0xeafec4d89fa9619884b60000a4d96624a38f7ac2d8d9a604ecf07c12c77e480c",
    "keyType": "Mapping",
    "valueType": "bytes4",
    "valueContent": "0xa4d96624"
  },
  {
    "name": "LSP4TokenName",
    "key": "0xdeba1e292f8ba88238e10ab3c7f88bd4be4fac56cad5194b6ecceaf653468af1",
    "keyType": "Singleton",
    "valueType": "string",
    "valueContent": "String"
  },
  {
    "name": "LSP4TokenSymbol",
    "key": "0x2f0a68ab07768e01943a599e73362a0e17a63a72e94dd2e384d2c1d4db932756",
    "keyType": "Singleton",
    "valueType": "string",
    "valueContent": "String"
  },
  {
    "name": "LSP4TokenType",
    "key": "0xe0261fa95db2eb3b5439bd033cda66d56b96f92f243a8228fd87550ed7bdfdb3",
    "keyType": "Singleton",
    "valueType": "uint256",
    "valueContent": "Number"
  },
  {
    "name": "LSP4Metadata",
    "key": "0x9afb95cacc9f95858ec44aa8c3b685511002e30ae54415823f406128b85b238e",
    "keyType": "Singleton",
    "valueType": "bytes",
    "valueContent": "VerifiableURI"
  },
  {
    "name": "LSP4Creators[]",
    "key": "0x114bd03b3a46d48759680d81ebb2b414fda7d030a7105a851867accf1c2352e7",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP4CreatorsMap:<address>",
    "key": "0x6de85eaf5d982b4e5da00000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "LSP5ReceivedAssets[]",
    "key": "0x6460ee3c0aac563ccbf76d6e1d07bada78e3a9514e6382b736ed3f478ab7b90b",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP5ReceivedAssetsMap:<address>",
    "key": "0x812c4334633eb816c80d0000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "AddressPermissions[]",
    "key": "0xdf30dba06db6a30e65354d9a64c609861f089545ca58c6b4dbe31a5f338cb0e3",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "AddressPermissions:Permissions:<address>",
    "key": "0x4b80742de2bf82acb3630000<address>",
    "keyType": "MappingWithGrouping",
    "valueType": "bytes32",
    "valueContent": "BitArray"
  },
  {
    "name": "AddressPermissions:AllowedCalls:<address>",
    "key": "0x4b80742de2bf393a64c70000<address>",
    "keyType": "MappingWithGrouping",
    "valueType": "(bytes4,address,bytes4,bytes4)[CompactBytesArray]",
    "valueContent": "(BitArray,Address,Bytes4,Bytes4)"
  },
  {
    "name": "AddressPermissions:AllowedERC725YDataKeys:<address>",
    "key": "0x4b80742de2bf866c29110000<address>",
    "keyType": "MappingWithGrouping",
    "valueType": "bytes[CompactBytesArray]",
    "valueContent": "Bytes"
  },
  {
    "name": "LSP8TokenIdFormat",
    "key": "0xf675e9361af1c1664c1868cfa3eb97672d6b1a513aa5b81dec34c9ee330e818d",
    "keyType": "Singleton",
    "valueType": "uint256",
    "valueContent": "Number"
  },
  {
    "name": "LSP8TokenMetadataBaseURI",
    "key": "0x1a7628600c3bac7101f53697f48df381ddc36b9015e7d7c9c5633d1252aa2843",
    "keyType": "Singleton",
    "valueType": "bytes",
    "valueContent": "VerifiableURI"
  },
  {
    "name": "LSP8ReferenceContract",
    "key": "0x708e7b881795f2e6b6c2752108c177ec89248458de3bf69d0d43480b3e5034e6",
    "keyType": "Singleton",
    "valueType": "(address,bytes32)",
    "valueContent": "(Address,bytes32)"
  },
  {
    "name": "LSP10VaultsMap:<address>",
    "key": "0x192448c3c0f88c7f238c0000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "LSP10Vaults[]",
    "key": "0x55482936e01da86729a45d2b87a6b1d3bc582bea0ec00e38bdb340e3af6f9f06",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP12IssuedAssets[]",
    "key": "0x7c8c3416d6cda87cd42c71ea1843df28ac4850354f988d55ee2eaa47b6dc05cd",
    "keyType": "Array",
    "valueType": "address",
    "valueContent": "Address"
  },
  {
    "name": "LSP12IssuedAssetsMap:<address>",
    "key": "0x74ac2555c10b9349e78f0000<address>",
    "keyType": "Mapping",
    "valueType": "(bytes4,uint128)",
    "valueContent": "(Bytes4,Number)"
  },
  {
    "name": "LSP17Extension:<bytes4>",
    "key": "0xcee78b4094da860110960000<bytes4>",
    "keyType": "Mapping",
    "valueType": "address",
    "valueContent": "Address"
  }
];
}

const DATA_REGEX: &str = r"^data:(.*?);(.*?),(.*)$";

pub fn resolve_data_url(url: &str) -> Result<Vec<u8>, Error> {
    // data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAABjElEQVRIS+2VwQ3CMAxFc
    let regex = Regex::new(DATA_REGEX).unwrap();
    if let Some(parts) = regex.captures(url) {
        let encoding = parts.get(2).unwrap().as_str();
        if encoding.ends_with("base64") {
            return Ok(general_purpose::STANDARD
                .decode(parts.get(3).unwrap().as_str())
                .unwrap());
        }
        return Ok(Vec::from(parts.get(3).unwrap().as_str()));
    }
    Err(anyhow!("Invalid data URL"))
}

pub fn decode_key(key: Vec<u8>) -> Option<Value> {
    let input_key = &key;
    let key = format!("0x{}", hex::encode(input_key));
    (*SCHEMAS).members().find_map(|schema| {
        let schema = schema.clone();
        let mut schema_key = schema["key"].as_str().unwrap();
        let mut is_key = false;
        let short_name = schema["name"]
            .as_str()
            .unwrap()
            .split(':')
            .filter(|x| !x.starts_with('<'))
            .join(":");
        if let Some(index) = schema_key.find('<') {
            schema_key = &schema_key[..index];
        } else if schema["name"].as_str().unwrap().ends_with("[]") {
            schema_key = &schema_key[..34];
            is_key = true;
        }
        if key.starts_with(schema_key) {
            let mut schema = schema.clone();
            schema["short_name"] = JsonValue::String(short_name.to_string());
            let current_key = schema["key"].as_str().unwrap();
            if is_key {
                if current_key == key {
                    schema["dynamic"] = JsonValue::String("length".to_string());
                } else {
                    let data = &input_key[(input_key.len() - 4)..];
                    let index = BigEndian::read_u32(data);
                    schema["dynamic"] = JsonValue::Number(index.into());
                }
            } else if schema_key.len() < 66 {
                let arg = key.strip_prefix(schema_key).unwrap();
                schema["dynamic"] = JsonValue::String(format!("0x{}", arg));
            }
            let str = schema.dump();
            Some(serde_json::from_str(&str).unwrap())
        } else {
            None
        }
    })
}

pub fn decode_token(token: Token, value_content: &str) -> Result<ERC725Value, ERC725Error> {
    if value_content == "Address" {
        if let Token::Address(address) = token {
            return Ok(ERC725Value::Address(address));
        }
    } else if value_content.starts_with("Bytes") {
        let size = value_content.strip_prefix("Bytes");
        if let Some(size) = size {
            let num = size.parse::<u8>().unwrap();
            if let Token::FixedBytes(token) = token {
                if token.len() == num as usize {
                    return Ok(ERC725Value::Bytes(token));
                }
            }
        }
    } else if value_content == "Number" {
        match token {
            Token::Uint(token) => {
                return Ok(ERC725Value::UInt(token));
            }
            Token::Int(token) => {
                return Ok(ERC725Value::Int(token));
            }
            _ => return Err(ERC725Error::Error("Invalid Number".to_string())),
        }
    } else if value_content == "String" {
        if let Token::String(token) = token {
            return Ok(ERC725Value::String(token));
        } else {
            return Err(ERC725Error::Error("Invalid String".to_string()));
        }
    } else if value_content == "VerifiableURI" {
        if let Token::Bytes(bytes) = token {
            return decode_verifiable_uri(bytes);
        } else {
            return Err(ERC725Error::Error("Invalid VerifiableURI".to_string()));
        }
    } else if value_content == "BitArray" {
        if let Token::FixedBytes(token) = token {
            return Ok(ERC725Value::BitArray(token));
        } else if let Token::Bytes(token) = token {
            return Ok(ERC725Value::BitArray(token));
        }
    }
    Err(ERC725Error::Error(format!(
        "Invalid Data: {}",
        value_content
    )))
}

pub fn decode_value(key_item: Value, value: Vec<u8>) -> Result<ERC725DecodedValue, ERC725Error> {
    if let Value::Object(map) = &key_item {
        let mut value_content = map.get("valueContent").unwrap().as_str().unwrap();
        if value_content.starts_with('(') {
            value_content = value_content.strip_prefix('(').unwrap();
            value_content = value_content.strip_suffix(')').unwrap();
        }
        let value_content = value_content.split(',').collect::<Vec<&str>>();
        let value_type = map.get("valueType").unwrap().as_str().unwrap();
        let short_name = map
            .get("short_name")
            .unwrap()
            .as_str()
            .unwrap()
            .replace(':', "")
            .to_camel_case();
        let dynamic = map.get("dynamic").unwrap_or(&Value::Null);
        if value_type.starts_with('(') && value_type.ends_with(')') {
            let types = value_type.strip_prefix('(').unwrap();
            let types = types.strip_suffix(')').unwrap();
            let types = types.split(',').map(|x| format!("\"{}\"", x)).join(",");
            let json_types = format!("[{}]", types);
            let types: Vec<ParamType> = serde_json::from_str(&json_types)?;
            let tokens = decode_packed(&types, &value)?;
            if value_content.len() == 1 {
                let is_array = map.get("keyType").unwrap().as_str().unwrap() == "Array";
                if is_array {
                    let dynamic = map.get("dynamic").unwrap();
                    return match dynamic {
                        Value::String(name) => {
                            if name == "length" {
                                let length = BigEndian::read_u32(&value);
                                return Ok(ERC725DecodedValue::ArrayLength {
                                    name: short_name.to_string(),
                                    length,
                                });
                            }
                            Err(ERC725Error::Error("Invalid Array Length".to_string()))
                        }
                        Value::Number(index) => {
                            let index = index.as_u64().unwrap() as u32;
                            if let Some(tokens) = tokens {
                                Ok(ERC725DecodedValue::ArrayItem {
                                    name: short_name.to_string(),
                                    index,
                                    value: decode_token(tokens[0].clone(), value_content[0])?,
                                })
                            } else {
                                // Clear entry
                                Ok(ERC725DecodedValue::ArrayItem {
                                    name: short_name.to_string(),
                                    index,
                                    value: ERC725Value::Null(),
                                })
                            }
                        }
                        _ => return Err(ERC725Error::Error("Invalid Array Index".to_string())),
                    };
                }
                let value_content = value_content[0];
                if let Some(tokens) = tokens {
                    let token = tokens[0].clone();
                    let value = decode_token(token, value_content)?;
                    return Ok(ERC725DecodedValue::Value {
                        name: short_name.to_string(),
                        dynamic: dynamic.clone(),
                        value,
                    });
                } else {
                    // Clear value
                    return Ok(ERC725DecodedValue::Value {
                        name: short_name.to_string(),
                        dynamic: dynamic.clone(),
                        value: ERC725Value::Null(),
                    });
                }
            }
            if let Some(tokens) = tokens {
                let output = value_content
                    .iter()
                    .zip(tokens.iter())
                    .map(|(value_content, token)| decode_token(token.clone(), value_content))
                    .collect::<Result<Vec<ERC725Value>, ERC725Error>>()?;
                return Ok(ERC725DecodedValue::Value {
                    name: short_name.to_string(),
                    value: ERC725Value::Array(output),
                    dynamic: dynamic.clone(),
                });
            } else {
                // Clear value
                return Ok(ERC725DecodedValue::Value {
                    name: short_name.to_string(),
                    value: ERC725Value::Null(),
                    dynamic: dynamic.clone(),
                });
            }
        } else if value_type == "bytes" {
            let value_content = value_content[0];
            let value = decode_token(Token::Bytes(value), value_content)?;
            return Ok(ERC725DecodedValue::Value {
                name: short_name.to_string(),
                value,
                dynamic: dynamic.clone(),
            });
        } else {
            let is_array = map.get("keyType").unwrap().as_str().unwrap() == "Array";
            if is_array {
                match map.get("dynamic") {
                    Some(Value::String(name)) => {
                        if name == "length" && value.len() >= 4 {
                            let value = value[(value.len() - 4)..].to_vec();
                            let length = BigEndian::read_u32(&value);
                            return Ok(ERC725DecodedValue::ArrayLength {
                                name: short_name.to_string(),
                                length,
                            });
                        }
                        return Err(ERC725Error::Error("Invalid Array Length".to_string()));
                    }
                    Some(Value::Number(index)) => {
                        let index = index.as_u64().unwrap() as u32;
                        let as_array = format!("[\"{}\"]", value_type);
                        let types: Vec<ParamType> = serde_json::from_str(&as_array)?;
                        let tokens = decode_packed(&types, &value)?;
                        if let Some(tokens) = tokens {
                            let token = tokens[0].clone();
                            let value = decode_token(token, value_content[0])?;
                            return Ok(ERC725DecodedValue::ArrayItem {
                                name: short_name.to_string(),
                                index,
                                value,
                            });
                        } else {
                            // Clear value
                            return Ok(ERC725DecodedValue::ArrayItem {
                                name: short_name.to_string(),
                                index,
                                value: ERC725Value::Null(),
                            });
                        }
                    }
                    _ => {
                        return Err(ERC725Error::Error("Invalid Array Index".to_string()));
                    }
                }
            }
            let types: Vec<ParamType> = serde_json::from_str(&format!("[\"{}\"]", value_type))?;
            let tokens = decode_packed(&types, &value)?;
            if let Some(tokens) = tokens {
                let token_value = tokens[0].clone();
                if let Token::FixedBytes(token_value) = token_value {
                    if value_content[0].starts_with("0x") {
                        let value = hex::decode(&value_content[0][2..]).unwrap();
                        return Ok(ERC725DecodedValue::Value {
                            name: short_name.to_string(),
                            dynamic: dynamic.clone(),
                            value: ERC725Value::Boolean(value == token_value),
                        });
                    }
                }
                let value = decode_token(tokens[0].clone(), value_content[0])?;
                return Ok(ERC725DecodedValue::Value {
                    name: short_name.to_string(),
                    dynamic: dynamic.clone(),
                    value,
                });
            } else {
                // Clear value
                return Ok(ERC725DecodedValue::Value {
                    name: short_name.to_string(),
                    dynamic: dynamic.clone(),
                    value: ERC725Value::Null(),
                });
            }
        }
    }
    Ok(ERC725DecodedValue::Null())
}

pub fn decode_key_value(key: Vec<u8>, value: Vec<u8>) -> Result<ERC725DecodedValue, ERC725Error> {
    if let Some(key_item) = decode_key(key) {
        return decode_value(key_item, value);
    }
    Ok(ERC725DecodedValue::Null())
}

const IPFS_PREFIX: &[u8] = "ipfs://".as_bytes();
const HTTPS_PREFIX: &[u8] = "https://".as_bytes();
const DATA_PREFIX: &[u8] = "data:".as_bytes();

pub fn decode_verifiable_uri(bytes: Vec<u8>) -> Result<ERC725Value, ERC725Error> {
    if bytes.len() < 2 {
        return Err(ERC725Error::Error("Invalid VerifiableURI".to_string()));
    }
    let code: u16 = BigEndian::read_u16(&bytes);
    if code == 0 {
        // VerifiableURI
        let method = &bytes[2..6];
        let length: u16 = BigEndian::read_u16(&bytes[6..]);
        if length > bytes.len() as u16 - 8 {
            return Err(ERC725Error::Error("Invalid VerifiableURI".to_string()));
        }
        let data = &bytes[8..(8 + length as usize)];
        let url = String::from_utf8(bytes[(8 + length as usize)..].to_vec().clone())?;
        let url = url.trim_end().to_string(); // Not sure why, but there are some URLs with a space at the end.
        return Ok(ERC725Value::VerifiableURI {
            url,
            method: method.to_vec(),
            data: data.to_vec(),
        });
    }
    let method = &bytes[0..4]; // 0..3
    let out = &bytes[4..];
    if bytes.len() < 36
        || (out.starts_with(IPFS_PREFIX)
            || out.starts_with(HTTPS_PREFIX)
            || out.starts_with(DATA_PREFIX))
    {
        let url = String::from_utf8(bytes[4..].to_vec().clone())?;
        let url = url.trim_end().to_string(); // Not sure why, but there are some URLs with a space at the end.
        return Ok(ERC725Value::VerifiableURI {
            url,
            method: vec![0u8; 4],
            data: vec![],
        });
    }
    let data = &bytes[4..36];
    let url = String::from_utf8(bytes[36..].to_vec().clone())?;
    let url = url.trim_end().to_string(); // Not sure why, but there are some URLs with a space at the end.
    Ok(ERC725Value::VerifiableURI {
        url,
        method: method.to_vec(),
        data: data.to_vec(),
    })
}

async fn upload_data(api_key: &str, api_key_secret: &str, data: Vec<u8>, mime: String) -> String {
    let api = PinataApi::new(api_key, api_key_secret).expect("failed to create pinata api");

    // /Users/andy/Development/rust-web3/target/debug/rust-web3
    let result = api.pin_file_content(data, mime).await;

    match result {
        Ok(pinned_object) => pinned_object.ipfs_hash,
        Err(e) => e.to_string(),
    }
}

async fn translate_url(
    api_key: &str,
    api_key_secret: &str,
    url: String,
    desired_hash: Option<Vec<u8>>,
) -> (String, Option<bool>) {
    if url.starts_with("data:") {
        // data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAABjElEQVRIS+2VwQ3CMAxFc
        let regex = Regex::new(DATA_REGEX).unwrap();
        let parts = regex.captures(url.as_str()).unwrap();
        let mime = parts.get(3).unwrap().as_str();
        let encoding = parts.get(2).unwrap().as_str();
        let value: Vec<u8>;
        let hash: Option<Vec<u8>>;
        if encoding.ends_with("base64") {
            value = general_purpose::STANDARD
                .decode(parts.get(3).unwrap().as_str())
                .unwrap();
            hash = Some(keccak256(&value).to_vec());
        } else {
            value = Vec::from(parts.get(3).unwrap().as_str());
            hash = Some(keccak256(&value).to_vec());
        }
        let verified = match desired_hash {
            Some(desired_hash) => match hash {
                Some(hash) => {
                    if hash == desired_hash {
                        Some(true)
                    } else if mime.ends_with("json") {
                        let value = String::from_utf8(value.clone()).unwrap();
                        let value: Value = serde_json::from_str(&value).unwrap();
                        let value = serde_json::to_string(&value).unwrap();
                        let hash = keccak256(value.as_bytes()).to_vec();
                        if hash == desired_hash {
                            Some(true)
                        } else {
                            Some(false)
                        }
                    } else {
                        Some(false)
                    }
                }
                None => None,
            },
            None => None,
        };

        let cid = upload_data(api_key, api_key_secret, value, mime.to_owned()).await;
        (format!("ipfs://{}", cid), verified)
    } else if url.starts_with("ipfs://") || url.starts_with("https://") {
        let src = url.replace("ipfs://", "https://api.universalprofile.cloud/ipfs/");
        let res = match reqwest::get(&src).await {
            Ok(res) => res,
            Err(_e) => {
                return (url.clone(), None);
            }
        };
        let data = match res.bytes().await {
            Ok(data) => data,
            Err(_e) => {
                return (url.clone(), None);
            }
        };
        let hash = keccak256(&data);
        let verified = match desired_hash {
            Some(desired_hash) => {
                if hash.to_vec() == desired_hash {
                    Some(true)
                } else {
                    Some(false)
                }
            }
            None => None,
        };
        (url, verified)
    } else {
        (url, None)
    }
}

pub async fn rewrite_json(
    api_key: &str,
    api_key_secret: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let path = JsonPath::parse("$..url").unwrap();
    let list = path.query_located(&data);
    let mut output = data.clone();
    for location in list {
        let loc = location.location().to_json_pointer();
        let val = data.pointer(&loc).unwrap();
        if let Value::String(url) = val.clone() {
            let _parent: Vec<&str> = loc.split('/').collect();
            let parent = _parent[0.._parent.len() - 1].join("/");
            let hash = if let Value::Object(root) = data.pointer(&parent).unwrap() {
                if let Some(Value::Object(verification)) = root.get("verification") {
                    if let Some(Value::String(hash)) = verification.get("data") {
                        Some(hex::decode(&hash[2..]).unwrap())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let (new_url, verified) =
                translate_url(&api_key, &api_key_secret, url.clone(), hash).await;
            if new_url != url || verified.is_some() {
                let mut data = data.clone();
                let parent_value = data.pointer_mut(&parent).unwrap();
                if let Value::Object(parent_value) = parent_value {
                    parent_value.insert("url".to_string(), Value::String(new_url.clone()));
                    if let Some(verified) = verified {
                        if let Some(serde_json::Value::Object(verification)) =
                            parent_value.get_mut("verification")
                        {
                            verification.insert("verified".to_string(), Value::Bool(verified));
                        }
                    }
                }
                output = data.clone();
            }
        }
    }
    Ok(output)
}

#[cfg(test)]
mod test {

    use serde_json::Value;
    use web3::types::H160;

    use crate::erc725::{decode_key_value, ERC725DecodedValue, ERC725Value, HASH_KECCAK256_UTF8};

    #[test]
    fn test_erc725_decode() {
        struct Case<'a> {
            name: &'a str,
            key: &'a str,
            data: &'a str,
            expected: Result<ERC725DecodedValue, anyhow::Error>,
        }

        let cases = vec![
            Case {
                name: "decode verifiable uri",
                key: "5ef83ad9559033e6e941db7d7c495acdce616347d28e90c7ce47cbfcfcad3bc5",
                data: "00006f357c6a002081dadadadadadadadadadadadadadadf00a4bdfa8fcaf3791d25f69b497abf88687474703a2f2f6461792e6e696768742f61737365742e676c62",
                expected: Ok(ERC725DecodedValue::Value {
                  name: "lsp3Profile".to_string(),
                  dynamic: Value::Null,
                  value: ERC725Value::VerifiableURI {
                    url: "http://day.night/asset.glb".to_string(),
                    method: hex::decode(HASH_KECCAK256_UTF8).unwrap(),
                    data: hex::decode("81dadadadadadadadadadadadadadadf00a4bdfa8fcaf3791d25f69b497abf88").unwrap()
                  }
                }),
            },
            Case {
              name: "decode array length",
              key: "6460ee3c0aac563ccbf76d6e1d07bada78e3a9514e6382b736ed3f478ab7b90b",
              data: "00000000000000000000000000000005",
              expected: Ok(ERC725DecodedValue::ArrayLength {
                name: "lsp5ReceivedAssets".to_string(),
                length: 5
              })
            },
            Case {
              name: "decode array item",
              key:  "6460ee3c0aac563ccbf76d6e1d07bada00000000000000000000000000000004",
              data: "d6c68c2c94af899ce43ff1863693016a711ae7c7",
              expected: Ok(ERC725DecodedValue::ArrayItem {
                name: "lsp5ReceivedAssets".to_string(),
                index: 4,
                value: ERC725Value::Address(H160::from_slice(&hex::decode("d6c68c2c94af899ce43ff1863693016a711ae7c7").unwrap()))
              })
            },
            Case {
                name: "decode test mapping",
                key: "6de85eaf5d982b4e5da00000d6c68c2c94af899ce43ff1863693016a711ae7c7",
                data: "0123456700000000000000000000000000000005",
                expected: Ok(ERC725DecodedValue::Value {
                    name: "lsp4CreatorsMap".to_string(),
                    value: ERC725Value::Array(vec![
                        ERC725Value::Bytes(hex::decode("01234567").unwrap()),
                        ERC725Value::Int(5.into()),
                    ]),
                    dynamic: Value::Null,
                }),
            },
        ];

        for case in cases {
            let key = hex::decode(case.key).unwrap();
            let data = hex::decode(case.data).unwrap();

            let output = decode_key_value(key, data);

            match case.expected {
                Ok(correct) => {
                    let output = output.unwrap();
                    assert_eq!(output, correct, "case: {}", case.name);
                }
                Err(err) => assert_eq!(
                    output.unwrap_err().to_string(),
                    err.to_string(),
                    "case: {}",
                    case.name
                ),
            }
        }
    }
}
