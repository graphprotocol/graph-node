use std::{convert::TryFrom};
use subtle_encoding::{Encoding, Hex};
use anyhow::{anyhow, Error};
use std::{
    fmt::{self, Debug, Display},
    str::FromStr,
};


use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};


#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct Hash([u8; 32]);

impl TryFrom<Vec<u8>> for Hash {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Hash::from_bytes(&value)
    }
}

impl From<Hash> for Vec<u8> {
    fn from(value: Hash) -> Self {
       return Vec::from(value.to_owned())
    }
}

impl Hash {
    pub fn from_bytes(bytes: &[u8]) -> Result<Hash, Error> {
        if bytes.is_empty() {
            return Ok(Hash([0u8; 32]));
        }

        if bytes.len() != 32 {
           return Err(anyhow!("Hash is not 32 byte long"))
        }

        let mut h = [0u8; 32];
        h.copy_from_slice(bytes);
        return Ok(Hash(h))
    }

    pub fn from_hex(s: &str) -> Result<Hash, Error> {
        if s.is_empty() {
            return Ok(Hash([0u8; 32]));
        }

        let mut h = [0u8; 32];
        Hex::upper_case()
            .decode_to_slice(s.as_bytes(), &mut h)
            .map_err( Error::new)?;
        return Ok(Hash(h))
    }

    pub fn as_vec(&self) -> Vec<u8> {
       Vec::from(self.to_owned())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash::Sha256({})", self)
    }
}

impl Default for Hash {
    fn default() -> Self {
        Hash([0u8; 32])
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        let hex = Hex::upper_case().encode_to_string(self.0).unwrap();
        //Hash::Sha256(ref h) =>
    //    let hex = match self {
      //      Hash::Sha256(ref h) => Hex::upper_case().encode_to_string(h).unwrap(),
//            Hash::None => String::new(),
  //      };

        write!(f, "{}", hex)
    }
}

impl FromStr for Hash {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::from_hex(s)
    }
}


// Serialization is used in light-client config
impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let h = String::deserialize(deserializer)?;

        if h.is_empty() {
            Err(D::Error::custom("empty hash"))
        } else {
            Ok(Self::from_str(&h).map_err(|e| D::Error::custom(format!("{}", e)))?)
        }
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

