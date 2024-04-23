use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
/// Possible MetadaValues
pub enum MetadataValue {
  /// Represents a String metadata value
  String(String),
  /// Represents a float metadata value
  Float(f64),
  /// Represents an integer value
  Integer(u64),
  /// Only valid when used with a ChangePinMetadata request
  Delete,
}

/// alias type for HashMap<String, MetadataValue>
pub type MetadataKeyValues = HashMap<String, MetadataValue>;

#[derive(Debug, Serialize)]
/// Pin metadata stored along with files pinned.
pub struct PinMetadata {
  #[serde(skip_serializing_if = "Option::is_none")]
  /// Custom name used for referencing your pinned content.
  pub name: Option<String>,
  /// List of key value items to attach with the pinned content
  pub keyvalues: MetadataKeyValues,
}

#[derive(Debug, Deserialize)]
/// Pin metadata returns from PinList query
/// 
/// This is different from [PinMetadata](struct.PinListMetadata.html) because
/// keyvalues can also be optional in this result
pub struct PinListMetadata {
  /// Custom name used for referencing your pinned content.
  pub name: Option<String>,
  /// List of key value items to attach with the pinned content
  pub keyvalues: Option<MetadataKeyValues>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// Pin metadata struct to update metadata of pinned items.
/// 
pub struct ChangePinMetadata {
  /// Customx
  pub ipfs_pin_hash: String,

  #[serde(flatten)]
  /// Updates to metadata for ipfs_pin_hash
  pub metadata: PinMetadata,
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use serde_json::Value;
  use super::{PinMetadata, MetadataValue};

  #[test]
  fn test_serialization_of_metadata() {
    let mut keyvalues = HashMap::new();
    keyvalues.insert("string".to_string(), MetadataValue::String("value".to_string()));
    keyvalues.insert("number".to_string(), MetadataValue::Integer(10));
    keyvalues.insert("delete".to_string(), MetadataValue::Delete);

    let data = PinMetadata {
      name: None,
      keyvalues,
    };

    let json_value: Value = serde_json::from_str(
      &serde_json::to_string(&data).unwrap()
    ).unwrap();

    // verify the structure of the json generated is similar to
    // {
    //   "keyvalues": {
    //     "number": 10
    //     "delete": Null,
    //     "string": "value"
    //   }
    // }
    if let Value::Object(object) = json_value {
        if object.contains_key("name") {
          assert!(false, "name fields of metadata should not exists")
        }

        if let Value::Object(keyvalues) = object.get("keyvalues").unwrap() {
          if let Value::Null = keyvalues.get("delete").unwrap() { } else {
            assert!(false, "keyvalues.delete should be null");
          }

          if let Value::String(string) = keyvalues.get("string").unwrap() {
            assert_eq!("value", string);
          } else {
            assert!(false, "keyvalues.string is not a string");
          }

          if let Value::Number(number) = keyvalues.get("number").unwrap() {
            assert_eq!(10, number.as_u64().unwrap());
          } else {
            assert!(false, "keyvalues.number is not a number");
          }


        } else {
          assert!(false, "keyvalues fields of metadata should be an object");
        }
    } else {
      assert!(false, "metadata not serialized as object");
    }
  }
}