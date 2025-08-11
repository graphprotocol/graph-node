// Test the Address.fromString string corruption bug
use super::*;

// Test string conversion to/from AscString to verify the fix for issue #6067
async fn test_address_from_string_string_conversion(api_version: Version) {
    let mut instance = test_module(
        "stringConversionTest",
        mock_data_source(
            &wasm_file_path("abi_classes.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    )
    .await;

    // Test the problematic address from issue #6067
    let problematic_address = "0x3b44b2a187a7b3824131f8db5a74194d0a42fc15";

    // Convert to AscString and back to test the string handling
    let asc_string_ptr = instance.asc_new(problematic_address).unwrap();
    let returned_str: String = instance.asc_get(asc_string_ptr).unwrap();

    assert_eq!(
        returned_str,
        problematic_address,
        "String was corrupted during AscString conversion: expected '{}', got '{}', length: {}",
        problematic_address,
        returned_str,
        returned_str.len()
    );

    // Test if the string contains any null characters (which could cause corruption)
    assert!(
        !returned_str.contains('\0'),
        "String contains null characters: {:?}",
        returned_str.as_bytes()
    );

    // Test if it's empty (the main symptom reported in the issue)
    assert!(
        !returned_str.is_empty(),
        "String became empty - this is the main bug!"
    );

    // Test that the length is correct (42 chars for 0x + 40 hex chars)
    assert_eq!(
        returned_str.len(),
        42,
        "Address should be exactly 42 characters"
    );
}

#[tokio::test]
async fn address_from_string_string_conversion_v0_0_5() {
    test_address_from_string_string_conversion(API_VERSION_0_0_5).await;
}

#[tokio::test]
async fn address_from_string_string_conversion_v0_0_4() {
    test_address_from_string_string_conversion(API_VERSION_0_0_4).await;
}
