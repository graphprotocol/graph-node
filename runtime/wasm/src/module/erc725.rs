use byteorder::{BigEndian, ByteOrder};
use graph::{itertools::Itertools, prelude::lazy_static};
use json::{array, JsonValue};

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

pub fn decode_key(key: Vec<u8>) -> Option<JsonValue> {
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
                    schema["arg"] = JsonValue::String("length".to_string());
                } else {
                    let data = &input_key[(input_key.len() - 8)..];
                    let index = BigEndian::read_u32(data);
                    schema["arg"] = JsonValue::Number(index.into());
                }
            } else if schema_key.len() < 66 {
                let arg = key.strip_prefix(schema_key).unwrap();
                schema["arg"] = JsonValue::String(format!("0x{}", arg));
            }
            Some(schema)
        } else {
            None
        }
    })
}
