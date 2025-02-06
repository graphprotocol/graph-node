use anyhow::Error;
use graphman::{commands::config::check::check, config::Config};

pub fn run(config: &Config, print: bool) -> Result<(), Error> {
    let check = check(config, print);

    match check {
        Ok(res) => {
            if res.validated {
                println!("Successfully validated configuration");
            }
            if res.validated_subgraph_settings {
                println!("Successfully validated subgraph settings");
            }
            if let Some(txt) = res.config_json {
                println!("{}", txt);
            }
        }
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
