use graph::prelude::*;

pub fn validate_manifest(manifest: SubgraphManifest) -> Result<SubgraphManifest, SubgraphRegistrarError> {
    // Validate that the manifest has a `source` address in each data source
    // which has call or block handlers
    let has_invalid_data_source = manifest
        .data_sources
        .iter()
        .any(|data_source| {
            let no_source_address = data_source
                .source
                .address
                .is_none();
            let has_call_handlers = data_source
                .mapping
                .call_handlers
                .len() > 0;
            let has_block_handlers = data_source
                .mapping
                .block_handler
                .is_some();
            no_source_address && (has_call_handlers || has_block_handlers)
        });
    if has_invalid_data_source {
        return Err(SubgraphRegistrarError::ManifestValidationError(
            SubgraphManifestValidationError::SourceAddressRequired
        ))
    }
    Ok(manifest)
}
