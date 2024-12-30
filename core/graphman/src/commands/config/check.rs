use crate::config::Config;
use anyhow::Error;
use graph::{components::subgraph::Settings, env::EnvVars};

pub struct ConfigCheckResult {
    pub validated: bool,
    pub validated_subgraph_settings: bool,
    pub config_json: Option<String>,
}

pub fn check(config: &Config, print: bool) -> Result<ConfigCheckResult, Error> {
    let mut result = ConfigCheckResult {
        validated: false,
        validated_subgraph_settings: false,
        config_json: None,
    };
    if print {
        match config.to_json() {
            Ok(txt) => {
                result.config_json = Some(txt);
            }
            Err(err) => return Err(anyhow::format_err!("error serializing config: {}", err)),
        };
    }
    let env_vars = EnvVars::from_env().unwrap();
    if let Some(path) = &env_vars.subgraph_settings {
        match Settings::from_file(path) {
            Ok(_) => {
                result.validated_subgraph_settings = true;
            }
            Err(e) => {
                return Err(anyhow::format_err!(
                    "configuration error in subgraph settings {}: {}",
                    path,
                    e
                ));
            }
        }
    };
    result.validated = true;
    Ok(result)
}
