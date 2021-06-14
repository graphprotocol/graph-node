use graph::prelude::{ethabi::Token, web3::types::U256};
use graph_runtime_wasm::{
    asc_abi::class::{
        ArrayBuffer, AscEnum, AscEnumArray, EthereumValueKind, StoreValueKind, TypedArray,
    },
    TRAP_TIMEOUT,
};

use super::*;

#[tokio::test(threaded_scheduler)]
async fn unbounded_loop() {
    // Set handler timeout to 3 seconds.
    let module = test_valid_module_and_store_with_timeout(
        "unboundedLoop",
        mock_data_source("wasm_test/non_terminating.wasm"),
        Some(Duration::from_secs(3)),
    )
    .0;
    let res: Result<(), _> = module.get_func("loop").typed().unwrap().call(());
    assert!(res.unwrap_err().to_string().contains(TRAP_TIMEOUT));
}

#[tokio::test]
async fn unbounded_recursion() {
    let module = test_module(
        "unboundedRecursion",
        mock_data_source("wasm_test/non_terminating.wasm"),
    );
    let res: Result<(), _> = module.get_func("rabbit_hole").typed().unwrap().call(());
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("call stack exhausted"), "{:#?}", err_msg);
}

#[tokio::test]
async fn abi_array() {
    let mut module = test_module("abiArray", mock_data_source("wasm_test/abi_classes.wasm"));

    let vec = vec![
        "1".to_owned(),
        "2".to_owned(),
        "3".to_owned(),
        "4".to_owned(),
    ];
    let vec_obj: AscPtr<Array<AscPtr<AscString>>> = asc_new(&mut module, &*vec).unwrap();

    let new_vec_obj: AscPtr<Array<AscPtr<AscString>>> = module.invoke_export("test_array", vec_obj);
    let new_vec: Vec<String> = asc_get(&module, new_vec_obj).unwrap();

    assert_eq!(
        new_vec,
        vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
            "4".to_owned(),
            "5".to_owned()
        ]
    )
}

#[tokio::test]
async fn abi_subarray() {
    let mut module = test_module(
        "abiSubarray",
        mock_data_source("wasm_test/abi_classes.wasm"),
    );

    let vec: Vec<u8> = vec![1, 2, 3, 4];
    let vec_obj: AscPtr<TypedArray<u8>> = asc_new(&mut module, &*vec).unwrap();

    let new_vec_obj: AscPtr<TypedArray<u8>> =
        module.invoke_export("byte_array_third_quarter", vec_obj);
    let new_vec: Vec<u8> = asc_get(&module, new_vec_obj).unwrap();

    assert_eq!(new_vec, vec![3])
}

#[tokio::test]
async fn abi_bytes_and_fixed_bytes() {
    let mut module = test_module(
        "abiBytesAndFixedBytes",
        mock_data_source("wasm_test/abi_classes.wasm"),
    );
    let bytes1: Vec<u8> = vec![42, 45, 7, 245, 45];
    let bytes2: Vec<u8> = vec![3, 12, 0, 1, 255];

    let bytes1_ptr = asc_new::<Uint8Array, _, _>(&mut module, &*bytes1).unwrap();
    let bytes2_ptr = asc_new::<Uint8Array, _, _>(&mut module, &*bytes2).unwrap();
    let new_vec_obj: AscPtr<Uint8Array> = module.invoke_export2("concat", bytes1_ptr, bytes2_ptr);

    // This should be bytes1 and bytes2 concatenated.
    let new_vec: Vec<u8> = asc_get(&module, new_vec_obj).unwrap();

    let mut concated = bytes1.clone();
    concated.extend(bytes2.clone());
    assert_eq!(new_vec, concated);
}

/// Test a roundtrip Token -> Payload -> Token identity conversion through asc,
/// and assert the final token is the same as the starting one.
#[tokio::test]
async fn abi_ethabi_token_identity() {
    let mut module = test_module(
        "abiEthabiTokenIdentity",
        mock_data_source("wasm_test/abi_token.wasm"),
    );

    // Token::Address
    let address = H160([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let token_address = Token::Address(address);

    let token_address_ptr = asc_new(&mut module, &token_address).unwrap();
    let new_address_obj: AscPtr<ArrayBuffer> =
        module.invoke_export("token_to_address", token_address_ptr);

    let new_token_ptr = module.invoke_export("token_from_address", new_address_obj);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(token_address, new_token);

    // Token::Bytes
    let token_bytes = Token::Bytes(vec![42, 45, 7, 245, 45]);

    let token_bytes_ptr = asc_new(&mut module, &token_bytes).unwrap();
    let new_bytes_obj: AscPtr<ArrayBuffer> =
        module.invoke_export("token_to_bytes", token_bytes_ptr);

    let new_token_ptr = module.invoke_export("token_from_bytes", new_bytes_obj);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(token_bytes, new_token);

    // Token::Int
    let int_token = Token::Int(U256([256, 453452345, 0, 42]));

    let int_token_ptr = asc_new(&mut module, &int_token).unwrap();
    let new_int_obj: AscPtr<ArrayBuffer> = module.invoke_export("token_to_int", int_token_ptr);

    let new_token_ptr = module.invoke_export("token_from_int", new_int_obj);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(int_token, new_token);

    // Token::Uint
    let uint_token = Token::Uint(U256([256, 453452345, 0, 42]));

    let uint_token_ptr = asc_new(&mut module, &uint_token).unwrap();
    let new_uint_obj: AscPtr<ArrayBuffer> = module.invoke_export("token_to_uint", uint_token_ptr);

    let new_token_ptr = module.invoke_export("token_from_uint", new_uint_obj);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(uint_token, new_token);
    assert_ne!(uint_token, int_token);

    // Token::Bool
    let token_bool = Token::Bool(true);

    let token_bool_ptr = asc_new(&mut module, &token_bool).unwrap();
    let func = module.get_func("token_to_bool").typed().unwrap().clone();
    let boolean: i32 = func.call(token_bool_ptr.wasm_ptr()).unwrap();

    let new_token_ptr = module.takes_val_returns_ptr("token_from_bool", boolean);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(token_bool, new_token);

    // Token::String
    let token_string = Token::String("漢字Go🇧🇷".into());

    let token_string_ptr = asc_new(&mut module, &token_string).unwrap();
    let new_string_obj: AscPtr<AscString> =
        module.invoke_export("token_to_string", token_string_ptr);

    let new_token_ptr = module.invoke_export("token_from_string", new_string_obj);
    let new_token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(token_string, new_token);

    // Token::Array
    let token_array = Token::Array(vec![token_address, token_bytes, token_bool]);
    let token_array_nested = Token::Array(vec![token_string, token_array]);

    let new_array_ptr = asc_new(&mut module, &token_array_nested).unwrap();
    let new_array_obj: AscEnumArray<EthereumValueKind> =
        module.invoke_export("token_to_array", new_array_ptr);

    let new_token_ptr = module.invoke_export("token_from_array", new_array_obj);
    let new_token: Token = asc_get(&module, new_token_ptr).unwrap();

    assert_eq!(new_token, token_array_nested);
}

#[tokio::test]
async fn abi_store_value() {
    use graph::data::store::Value;

    let mut module = test_module(
        "abiStoreValue",
        mock_data_source("wasm_test/abi_store_value.wasm"),
    );

    // Value::Null
    let func = module.get_func("value_null").typed().unwrap().clone();
    let ptr: u32 = func.call(()).unwrap();
    let null_value_ptr: AscPtr<AscEnum<StoreValueKind>> = ptr.into();
    let null_value: Value = try_asc_get(&module, null_value_ptr).unwrap();
    assert_eq!(null_value, Value::Null);

    // Value::String
    let string = "some string";
    let string_ptr = asc_new(&mut module, string).unwrap();
    let new_value_ptr = module.invoke_export("value_from_string", string_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::from(string));

    // Value::Int
    let int = i32::min_value();
    let new_value_ptr = module.takes_val_returns_ptr("value_from_int", int);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::Int(int));

    // Value::BigDecimal
    let big_decimal = BigDecimal::from_str("3.14159001").unwrap();
    let big_decimal_ptr = asc_new(&mut module, &big_decimal).unwrap();
    let new_value_ptr = module.invoke_export("value_from_big_decimal", big_decimal_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::BigDecimal(big_decimal));

    let big_decimal = BigDecimal::new(10.into(), 5);
    let big_decimal_ptr = asc_new(&mut module, &big_decimal).unwrap();
    let new_value_ptr = module.invoke_export("value_from_big_decimal", big_decimal_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::BigDecimal(1_000_000.into()));

    // Value::Bool
    let boolean = true;
    let new_value_ptr =
        module.takes_val_returns_ptr("value_from_bool", if boolean { 1 } else { 0 });
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::Bool(boolean));

    // Value::List
    let func = module
        .get_func("array_from_values")
        .typed()
        .unwrap()
        .clone();
    let new_value_ptr: u32 = func
        .call((asc_new(&mut module, string).unwrap().wasm_ptr(), int))
        .unwrap();
    let new_value_ptr = AscPtr::from(new_value_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(
        new_value,
        Value::List(vec![Value::from(string), Value::Int(int)])
    );

    let array: &[Value] = &[
        Value::String("foo".to_owned()),
        Value::String("bar".to_owned()),
    ];
    let array_ptr = asc_new(&mut module, array).unwrap();
    let new_value_ptr = module.invoke_export("value_from_array", array_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(
        new_value,
        Value::List(vec![
            Value::String("foo".to_owned()),
            Value::String("bar".to_owned()),
        ])
    );

    // Value::Bytes
    let bytes: &[u8] = &[0, 2, 5];
    let bytes_ptr: AscPtr<Uint8Array> = asc_new(&mut module, bytes).unwrap();
    let new_value_ptr = module.invoke_export("value_from_bytes", bytes_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(new_value, Value::Bytes(bytes.into()));

    // Value::BigInt
    let bytes: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    let bytes_ptr: AscPtr<Uint8Array> = asc_new(&mut module, bytes).unwrap();
    let new_value_ptr = module.invoke_export("value_from_bigint", bytes_ptr);
    let new_value: Value = try_asc_get(&module, new_value_ptr).unwrap();
    assert_eq!(
        new_value,
        Value::BigInt(::graph::data::store::scalar::BigInt::from_unsigned_bytes_le(bytes))
    );
}

#[tokio::test]
async fn abi_h160() {
    let mut module = test_module("abiH160", mock_data_source("wasm_test/abi_classes.wasm"));
    let address = H160::zero();

    // As an `Uint8Array`
    let array_buffer: AscPtr<Uint8Array> = asc_new(&mut module, &address).unwrap();
    let new_address_obj: AscPtr<Uint8Array> = module.invoke_export("test_address", array_buffer);

    // This should have 1 added to the first and last byte.
    let new_address: H160 = asc_get(&module, new_address_obj).unwrap();

    assert_eq!(
        new_address,
        H160([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    )
}

#[tokio::test]
async fn string() {
    let mut module = test_module("string", mock_data_source("wasm_test/abi_classes.wasm"));
    let string = "    漢字Double_Me🇧🇷  ";
    let trimmed_string_ptr = asc_new(&mut module, string).unwrap();
    let trimmed_string_obj: AscPtr<AscString> =
        module.invoke_export("repeat_twice", trimmed_string_ptr);
    let doubled_string: String = asc_get(&module, trimmed_string_obj).unwrap();
    assert_eq!(doubled_string, string.repeat(2))
}

#[tokio::test]
async fn abi_big_int() {
    let mut module = test_module("abiBigInt", mock_data_source("wasm_test/abi_classes.wasm"));

    // Test passing in 0 and increment it by 1
    let old_uint = U256::zero();
    let array_buffer: AscPtr<AscBigInt> =
        asc_new(&mut module, &BigInt::from_unsigned_u256(&old_uint)).unwrap();
    let new_uint_obj: AscPtr<AscBigInt> = module.invoke_export("test_uint", array_buffer);
    let new_uint: BigInt = asc_get(&module, new_uint_obj).unwrap();
    assert_eq!(new_uint, BigInt::from(1 as i32));
    let new_uint = new_uint.to_unsigned_u256();
    assert_eq!(new_uint, U256([1, 0, 0, 0]));

    // Test passing in -50 and increment it by 1
    let old_uint = BigInt::from(-50);
    let array_buffer: AscPtr<AscBigInt> = asc_new(&mut module, &old_uint).unwrap();
    let new_uint_obj: AscPtr<AscBigInt> = module.invoke_export("test_uint", array_buffer);
    let new_uint: BigInt = asc_get(&module, new_uint_obj).unwrap();
    assert_eq!(new_uint, BigInt::from(-49 as i32));
    let new_uint_from_u256 = BigInt::from_signed_u256(&new_uint.to_signed_u256());
    assert_eq!(new_uint, new_uint_from_u256);
}

#[tokio::test]
async fn big_int_to_string() {
    let mut module = test_module(
        "bigIntToString",
        mock_data_source("wasm_test/big_int_to_string.wasm"),
    );

    let big_int_str = "30145144166666665000000000000000000";
    let big_int = BigInt::from_str(big_int_str).unwrap();
    let ptr: AscPtr<AscBigInt> = asc_new(&mut module, &big_int).unwrap();
    let string_obj: AscPtr<AscString> = module.invoke_export("big_int_to_string", ptr);
    let string: String = asc_get(&module, string_obj).unwrap();
    assert_eq!(string, big_int_str);
}

// This should panic rather than exhibiting UB. It's hard to test for UB, but
// when reproducing a SIGILL was observed which would be caught by this.
#[tokio::test]
#[should_panic]
async fn invalid_discriminant() {
    let module = test_module(
        "invalidDiscriminant",
        mock_data_source("wasm_test/abi_store_value.wasm"),
    );

    let func = module
        .get_func("invalid_discriminant")
        .typed()
        .unwrap()
        .clone();
    let ptr: u32 = func.call(()).unwrap();
    let _value: Value = try_asc_get(&module, ptr.into()).unwrap();
}
