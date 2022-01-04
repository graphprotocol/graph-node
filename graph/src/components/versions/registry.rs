use crate::prelude::FeatureFlag;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

lazy_static! {
    pub static ref VERSIONS: HashMap<VersionNumber, Vec<FeatureFlag>> = {
        let supported_versions: Vec<(&str, Vec<FeatureFlag>)> = vec![
            // baseline version
            ("1.0.0", vec![]),
            // Versions with feature flags
            ("1.1.0", vec![FeatureFlag::BasicOrdering])
        ];

        let mut map = HashMap::new();

        for (version, flags) in supported_versions {
            map.insert(VersionNumber::from(version), flags);
        }

        map
    };

    static ref LATEST_VERSION: String = {
        let keys: Vec<VersionNumber> = VERSIONS.clone().into_keys().collect();

        let last_version = keys.last().unwrap();

        last_version.0.clone()
    };
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct VersionNumber(String);

impl Default for VersionNumber {
    fn default() -> Self {
        Self(LATEST_VERSION.to_string())
    }
}

impl fmt::Display for VersionNumber {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.0)?;
        Ok(())
    }
}

impl VersionNumber {
    pub fn validate(version: String) -> Result<(), String> {
        let chunks: Vec<&str> = version.split(".").collect();

        if chunks.len() != 3 {
            return Err(format!("Invalid version number: {}", version));
        }

        let major = chunks[0].parse::<u32>();
        let minor = chunks[1].parse::<u32>();
        let patch = chunks[2].parse::<u32>();

        if major.is_err() || minor.is_err() || patch.is_err() {
            return Err(format!("Invalid version number: {}", version));
        }

        if !VERSIONS.contains_key(&VersionNumber::from(version)) {
            return Err("No versions found".to_string());
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Version {
    pub version: VersionNumber,
    features: Vec<FeatureFlag>,
}

impl From<String> for VersionNumber {
    fn from(version: String) -> Self {
        Self(version)
    }
}

impl From<&str> for VersionNumber {
    fn from(version: &str) -> Self {
        Self(version.to_string())
    }
}

impl Version {
    pub fn new(current_version: VersionNumber) -> Self {
        Self {
            version: current_version.clone(),
            features: VERSIONS
                .get(&current_version)
                .expect(format!("Version {:?} is not supported", current_version).as_str()) // At this point, we know that the version exists (thanks to VersionNumber::validate)
                .to_vec(),
        }
    }

    pub fn supports(&self, feature: FeatureFlag) -> bool {
        self.features.contains(&feature)
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new(VersionNumber::default())
    }
}
