use fluvio_smartmodule::{smartmodule, Record, Result};

#[smartmodule(filter, params)]
pub fn filter(record: &Record, params: &SmartModuleOpt) -> Result<bool> {
    // let string = std::str::from_utf8(record.value.as_ref())?;
    // Ok(string.contains('a'))

    let data: serde_json::Value = serde_json::from_slice(record.value.as_ref())?;
    Ok(data
        .as_object()
        .and_then(|data| data.get("block"))
        .and_then(|block| block.as_object())
        .and_then(|block| block.get("number"))
        .and_then(|number| number.as_u64())
        .map_or(false, |number| {
            number >= params.start_block && number <= params.end_block
        }))
}

#[derive(fluvio_smartmodule::SmartOpt, Default)]
pub struct SmartModuleOpt {
    start_block: u64,
    end_block: u64,
}
