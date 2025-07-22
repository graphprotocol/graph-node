# Plan: Implement Extensible Compression for RPC Requests

## Overview
Add extensible compression support for Graph Node's outgoing RPC requests to upstream providers, configurable on a per-provider basis with future compression methods in mind.

## Implementation Steps (COMPLETED)

### 1. ✅ Create Compression Enum (`node/src/config.rs`)
- Added `Compression` enum with `None` and `Gzip` variants
- Commented placeholders for future compression methods (Brotli, Deflate)
- Default implementation returns `Compression::None`

### 2. ✅ Update Configuration Structure (`node/src/config.rs`)
- Replaced `compression_enabled: bool` with `compression: Compression` field in `Web3Provider` struct
- Updated all existing code to use new enum
- Added unit tests for both "gzip" and "none" compression options

### 3. ✅ Modify HTTP Transport (`chain/ethereum/src/transport.rs`)
- Updated `Transport::new_rpc()` to accept `Compression` enum parameter
- Implemented match statement for different compression types
- Added comments showing where future compression methods can be added
- Uses reqwest's `.gzip(true)` for automatic compression/decompression

### 4. ✅ Update Transport Creation (`node/src/chain.rs`)
- Pass compression enum from config to transport
- Updated logging to show compression method using debug format

### 5. ✅ Update Dependencies (`graph/Cargo.toml`)
- Added "gzip" feature to reqwest dependency

### 6. ✅ Update Test Configuration
- Updated `full_config.toml` example to use new enum format
- Added comprehensive unit tests for compression parsing

## Configuration Examples

### Gzip Compression
```toml
[chains.mainnet]
provider = [
  { 
    label = "mainnet-rpc", 
    details = { 
      type = "web3", 
      url = "http://rpc.example.com", 
      features = ["archive"],
      compression = "gzip"
    }
  }
]
```

### No Compression (Default)
```toml
[chains.mainnet]
provider = [
  { 
    label = "mainnet-rpc", 
    details = { 
      type = "web3", 
      url = "http://rpc.example.com", 
      features = ["archive"],
      compression = "none"  # or omit entirely
    }
  }
]
```

### Future Extension Example
```rust
// Future compression methods can be easily added:
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Compression {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "gzip")]
    Gzip,
    #[serde(rename = "brotli")]
    Brotli,
    #[serde(rename = "deflate")]
    Deflate,
}

// And handled in transport:
match compression {
    Compression::Gzip => client_builder = client_builder.gzip(true),
    Compression::Brotli => client_builder = client_builder.brotli(true),
    Compression::Deflate => client_builder = client_builder.deflate(true),
    Compression::None => {} // No compression
}
```

## Benefits of This Implementation
- **Extensible**: Easy to add new compression methods without breaking changes
- **Backward Compatible**: Defaults to no compression, existing configs work unchanged
- **Type Safe**: Enum prevents invalid compression method strings
- **Future Proof**: Clear pattern for adding Brotli, Deflate, etc.
- **Per-Provider**: Each RPC provider can have different compression settings