# gnd test

Mock-based subgraph test runner that feeds JSON-defined blocks through real graph-node infrastructure (store, WASM runtime, trigger processing) with only the blockchain layer mocked.

## Quick Start

```bash
# Run all tests in tests/ directory
gnd test

# Run a specific test file
gnd test tests/transfer.json

# Skip automatic build (if subgraph already built)
gnd test --skip-build

# Use legacy Matchstick runner
gnd test --matchstick
```

## Test File Format

Tests are JSON files that define:
- Mock blockchain blocks with events
- Mock `eth_call` responses
- GraphQL assertions to validate entity state

Place test files in a `tests/` directory with `.json` or `.test.json` extension.

### Basic Example

```json
{
  "name": "Transfer creates entity",
  "blocks": [
    {
      "number": 1,
      "timestamp": 1672531200,
      "events": [
        {
          "address": "0x1234...",
          "event": "Transfer(address indexed from, address indexed to, uint256 value)",
          "params": {
            "from": "0xaaaa...",
            "to": "0xbbbb...",
            "value": "1000"
          }
        }
      ],
      "ethCalls": [
        {
          "address": "0x1234...",
          "function": "balanceOf(address)(uint256)",
          "params": ["0xaaaa..."],
          "returns": ["1000000000000000000"]
        }
      ]
    }
  ],
  "assertions": [
    {
      "query": "{ transfer(id: \"1\") { from to value } }",
      "expected": {
        "transfer": {
          "from": "0xaaaa...",
          "to": "0xbbbb...",
          "value": "1000"
        }
      }
    }
  ]
}
```

## Block Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `number` | No | Auto-increments from lowest defined `startBlock` in the manifest file, or from  `0` if no `startBlock` are defined | Block number |
| `hash` | No | `keccak256(block_number)` | Block hash |
| `timestamp` | No | `block_number * 12` | Unix timestamp |
| `baseFeePerGas` | No | None (pre-EIP-1559) | Base fee in wei |
| `events` | No | Empty array | Log events in this block |
| `ethCalls` | No | Empty array | Mock `eth_call` responses |

### Empty Blocks

Empty blocks (no events) still trigger block handlers:

```json
{
  "name": "Test block handlers",
  "blocks": [
    {
      "number": 1,
      "events": [...]
    },
    {}  // Block 2 with no events - block handlers still fire
  ]
}
```

## Event Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `address` | Yes | — | Contract address (lowercase hex with 0x prefix) |
| `event` | Yes | — | Full event signature with `indexed` keywords |
| `params` | No | Empty object | Event parameter values |
| `txHash` | No | `keccak256(block_number \|\| log_index)` | Transaction hash |

### Event Signature Format

**Important:** Include `indexed` keywords in the signature:

```json
{
  "event": "Transfer(address indexed from, address indexed to, uint256 value)"
}
```

Not:
```json
{
  "event": "Transfer(address,address,uint256)"  // ❌ Missing indexed keywords
}
```

### Parameter Types

Event parameters are automatically ABI-encoded based on the signature. Supported formats:

```json
{
  "params": {
    "from": "0xaaaa...",           // address
    "to": "0xbbbb...",              // address
    "value": "1000",                // uint256 (string or number)
    "amount": 1000,                 // uint256 (number)
    "enabled": true,                // bool
    "data": "0x1234...",           // bytes
    "name": "Token"                 // string
  }
}
```

## Block Handlers

Block handlers are **automatically triggered** for every block. You don't need to specify block triggers in the JSON.

### How Block Handlers Work

The test runner auto-injects both `Start` and `End` block triggers for each block, ensuring all block handler filters work correctly:

- **`once` filter** → Fires once at `startBlock` (via `Start` trigger)
- **No filter** → Fires on every block (via `End` trigger)
- **`polling` filter** → Fires every N blocks based on formula: `(block_number - startBlock) % every == 0`

### Example: Basic Block Handlers

```json
{
  "name": "Block handlers test",
  "blocks": [
    {},  // Block 0 - both 'once' and regular block handlers fire
    {}   // Block 1 - only regular block handlers fire
  ],
  "assertions": [
    {
      "query": "{ blocks { number } }",
      "expected": {
        "blocks": [
          {"number": "0"},
          {"number": "1"}
        ]
      }
    },
    {
      "query": "{ blockOnces { msg } }",
      "expected": {
        "blockOnces": [
          {"msg": "This fires only once at block 0"}
        ]
      }
    }
  ]
}
```

### Polling Block Handlers

Polling handlers fire at regular intervals specified by the `every` parameter. The handler fires when:

```
(block_number - startBlock) % every == 0
```

**Manifest example:**
```yaml
blockHandlers:
  - handler: handleEveryThreeBlocks
    filter:
      kind: polling
      every: 3
```

**Test example (startBlock: 0):**
```json
{
  "name": "Polling handler test",
  "blocks": [
    {},  // Block 0 - handler fires (0 % 3 == 0)
    {},  // Block 1 - handler doesn't fire
    {},  // Block 2 - handler doesn't fire
    {},  // Block 3 - handler fires (3 % 3 == 0)
    {},  // Block 4 - handler doesn't fire
    {},  // Block 5 - handler doesn't fire
    {}   // Block 6 - handler fires (6 % 3 == 0)
  ],
  "assertions": [
    {
      "query": "{ pollingBlocks(orderBy: number) { number } }",
      "expected": {
        "pollingBlocks": [
          {"number": "0"},
          {"number": "3"},
          {"number": "6"}
        ]
      }
    }
  ]
}
```

**With non-zero startBlock:**

When your data source has `startBlock > 0`, the polling interval is calculated from that starting point.

**Manifest:**
```yaml
dataSources:
  - name: Token
    source:
      startBlock: 100
    mapping:
      blockHandlers:
        - handler: handlePolling
          filter:
            kind: polling
            every: 5
```

**Test:**
```json
{
  "name": "Polling from block 100",
  "blocks": [
    {"number": 100},  // Fires: (100-100) % 5 == 0
    {"number": 101},  // Doesn't fire
    {"number": 102},  // Doesn't fire
    {"number": 103},  // Doesn't fire
    {"number": 104},  // Doesn't fire
    {"number": 105},  // Fires: (105-100) % 5 == 0
    {"number": 106},  // Doesn't fire
    {"number": 107},  // Doesn't fire
    {"number": 108},  // Doesn't fire
    {"number": 109},  // Doesn't fire
    {"number": 110}   // Fires: (110-100) % 5 == 0
  ],
  "assertions": [
    {
      "query": "{ pollingBlocks(orderBy: number) { number } }",
      "expected": {
        "pollingBlocks": [
          {"number": "100"},
          {"number": "105"},
          {"number": "110"}
        ]
      }
    }
  ]
}
```

**Note:** The test runner automatically handles `startBlock > 0`, so blocks default to numbering from the manifest's `startBlock`.

## eth_call Mocking

Mock contract calls made from mapping handlers using `contract.call()`:

```json
{
  "ethCalls": [
    {
      "address": "0x1234...",
      "function": "balanceOf(address)(uint256)",
      "params": ["0xaaaa..."],
      "returns": ["1000000000000000000"]
    }
  ]
}
```

### ethCall Fields

| Field | Required | Description |
|-------|----------|-------------|
| `address` | Yes | Contract address |
| `function` | Yes | Full signature: `"functionName(inputTypes)(returnTypes)"` |
| `params` | Yes | Array of input parameters (as strings) |
| `returns` | Yes | Array of return values (as strings, ignored if `reverts: true`) |
| `reverts` | No | Default `false`. If `true`, the call is cached as `Retval::Null` |

### Function Signature Format

Use full signatures with input and return types:

```json
{
  "function": "symbol()(string)",           // No inputs, returns string
  "function": "balanceOf(address)(uint256)", // One input, returns uint256
  "function": "decimals()(uint8)"           // No inputs, returns uint8
}
```

### Mocking Reverts

```json
{
  "address": "0x1234...",
  "function": "transfer(address,uint256)(bool)",
  "params": ["0xaaaa...", "1000"],
  "returns": [],
  "reverts": true
}
```

### Real-World Example

From the ERC20 test:

```json
{
  "ethCalls": [
    {
      "address": "0x731a10897d267e19b34503ad902d0a29173ba4b1",
      "function": "symbol()(string)",
      "params": [],
      "returns": ["GRT"]
    },
    {
      "address": "0x731a10897d267e19b34503ad902d0a29173ba4b1",
      "function": "name()(string)",
      "params": [],
      "returns": ["TheGraph"]
    },
    {
      "address": "0x731a10897d267e19b34503ad902d0a29173ba4b1",
      "function": "balanceOf(address)(uint256)",
      "params": ["0xaaaa000000000000000000000000000000000000"],
      "returns": ["3000000000000000000"]
    }
  ]
}
```

## Assertions

GraphQL queries to validate the indexed entity state after processing all blocks.

### Assertion Fields

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | GraphQL query string |
| `expected` | Yes | Expected JSON response |

### Comparison Behavior

| Aspect | Behavior |
|--------|----------|
| Objects | Key-compared, order-insensitive |
| Arrays | **Order-insensitive** (set comparison) |
| String vs Number | Coerced — `"123"` matches `123` |
| Nulls/Booleans | Strict equality |

**Important:** Arrays are compared as sets (order doesn't matter). If you need ordered results, use `orderBy` in your GraphQL query:

```json
{
  "query": "{ transfers(orderBy: timestamp, orderDirection: asc) { id from to value } }",
  "expected": { ... }
}
```

### Multiple Assertions

You can have multiple assertions per test. They run sequentially after all blocks are processed:

```json
{
  "assertions": [
    {
      "query": "{ tokens { id name symbol } }",
      "expected": { ... }
    },
    {
      "query": "{ accounts { id balance } }",
      "expected": { ... }
    }
  ]
}
```

### Nested Entity Queries

Test relationships and nested entities:

```json
{
  "query": "{ accounts { id balances { token { symbol } amount } } }",
  "expected": {
    "accounts": [
      {
        "id": "0xbbbb...",
        "balances": [
          {
            "token": { "symbol": "GRT" },
            "amount": "5000000000000000000"
          }
        ]
      }
    ]
  }
}
```

## startBlock Handling

The test runner automatically reads `startBlock` from your subgraph manifest and handles it correctly — **no real blockchain connection needed**.

### How It Works

1. Extracts the **minimum `startBlock`** across all data sources in your manifest
2. If min > 0, creates a `start_block_override` to bypass graph-node's on-chain block validation
3. Test blocks without explicit `"number"` auto-increment starting from that minimum `startBlock`

### Default Block Numbering

The starting block number depends on your manifest:

| Manifest Configuration | Test Block Numbers |
|----------------------|-------------------|
| `startBlock: 0` (or unset) | 0, 1, 2, ... |
| `startBlock: 100` | 100, 101, 102, ... |
| Multiple data sources: `startBlock: 50` and `startBlock: 200` | 50, 51, 52, ... (uses minimum) |

### Example: Single Data Source

**Manifest:**
```yaml
dataSources:
  - name: Token
    source:
      startBlock: 1000
```

**Test:**
```json
{
  "blocks": [
    {},  // Block 1000 (auto-numbered)
    {}   // Block 1001 (auto-numbered)
  ]
}
```

### Example: Explicit Block Numbers

Override auto-numbering by specifying `"number"`:

```json
{
  "blocks": [
    {
      "number": 5000,
      "events": [...]
    },
    {
      "number": 5001,
      "events": [...]
    }
  ]
}
```

### Multi-Data Source Testing

When your subgraph has multiple data sources with different `startBlock` values, you may need to use explicit block numbers.

**Scenario:** DataSource A at `startBlock: 50` (Transfer events), DataSource B at `startBlock: 200` (Approval events). You want to test only DataSource B.

**Manifest:**
```yaml
dataSources:
  - name: TokenTransfers
    source:
      startBlock: 50
    mapping:
      eventHandlers:
        - event: Transfer(...)
          handler: handleTransfer
  - name: TokenApprovals
    source:
      startBlock: 200
    mapping:
      eventHandlers:
        - event: Approval(...)
          handler: handleApproval
```

**Test:**
```json
{
  "name": "Test Approval handler",
  "blocks": [
    {
      "number": 200,  // Explicit number >= DataSource B's startBlock
      "events": [
        {
          "address": "0x5678...",
          "event": "Approval(address indexed owner, address indexed spender, uint256 value)",
          "params": {
            "owner": "0xaaaa...",
            "spender": "0xbbbb...",
            "value": "500"
          }
        }
      ]
    },
    {
      "number": 201,
      "events": [...]
    }
  ]
}
```

**Why explicit numbers are needed:**
- Default numbering starts at the **minimum** `startBlock` across all data sources (50 in this case)
- Blocks 50-199 are below DataSource B's `startBlock: 200`, so its handlers won't fire
- Use explicit `"number": 200` to ensure the block is in DataSource B's active range

**Note:** DataSource A is still "active" from block 50 onward, but it simply sees no matching Transfer events in blocks 200-201, so no handlers fire for it. This is normal behavior — graph-node doesn't error on inactive handlers.

## Test Organization

### Directory Structure

```
my-subgraph/
├── subgraph.yaml
├── schema.graphql
├── src/
│   └── mapping.ts
└── tests/
    ├── transfer.json
    ├── approval.json
    └── edge-cases.test.json
```

### Naming Conventions

- Use `.json` or `.test.json` extension
- Descriptive names: `transfer.json`, `mint-burn.json`, `edge-cases.json`
- The test runner discovers all `*.json` and `*.test.json` files in the test directory

## Known Limitations

| Feature | Status |
|---------|--------|
| Log events | ✅ Supported |
| Block handlers (all filters) | ✅ Supported |
| eth_call mocking | ✅ Supported |
| Dynamic/template data sources | (Untested)
| Transaction receipts (`receipt: true`) | ❌ Not implemented — handlers get `null` |
| File data sources / IPFS mocking | ❌ Not implemented |
| Call triggers (traces) | ❌ Not implemented |
| `--json` CI output | ❌ Not implemented |
| Parallel test execution | ❌ Not implemented |
| Test name filtering (`--filter`) | ❌ Not implemented |

## Tips & Best Practices

### Use Lowercase Addresses

Always use lowercase hex addresses with `0x` prefix:

```json
{
  "address": "0x731a10897d267e19b34503ad902d0a29173ba4b1"  // ✅ Correct
}
```

Not:
```json
{
  "address": "0x731A10897D267E19B34503Ad902d0A29173Ba4B1"  // ❌ Mixed case
}
```

### Test One Thing at a Time

Write focused tests that validate a single behavior:

```json
// ✅ Good - tests one scenario
{
  "name": "Transfer event creates TransferEvent entity",
  "blocks": [...],
  "assertions": [...]
}
```

```json
// ❌ Avoid - tests too many things
{
  "name": "Test everything",
  "blocks": [/* 50 blocks */],
  "assertions": [/* 20 assertions */]
}
```

### Order GraphQL Results

If your assertion needs specific ordering, use `orderBy`:

```json
{
  "query": "{ transfers(first: 10, orderBy: timestamp, orderDirection: asc) { id } }",
  "expected": { ... }
}
```

### Test Block Handlers with Empty Blocks

Use empty blocks to test that block handlers fire even without events:

```json
{
  "blocks": [
    {},  // Empty block - block handlers still fire
    {}
  ]
}
```

### Split Complex Tests

Instead of one large test with many blocks, split into multiple focused test files:

```
tests/
├── transfer-basic.json        # Basic transfer functionality
├── transfer-zero-value.json   # Edge case: zero value
└── transfer-same-account.json # Edge case: self-transfer
```

## Architecture

The test runner reuses real graph-node infrastructure:

```
test.json
    ↓
Parse & ABI encode events
    ↓
Mock block stream (StaticStreamBuilder)
    ↓
Real graph-node indexer
    ├── WASM runtime
    ├── Trigger processing
    └── Entity storage (pgtemp database)
    ↓
GraphQL queries → Assertions
```

**Key design principles:**

- **Isolated database per test:** Each test gets a pgtemp database dropped on completion (default), or a shared persistent database with post-test cleanup (`--postgres-url`)
- **Real WASM runtime:** Uses `EthereumRuntimeAdapterBuilder` with real `ethereum.call` host function
- **Pre-populated call cache:** `eth_call` responses are cached before indexing starts
- **No IPFS for manifest:** Uses `FileLinkResolver` to load manifest/WASM from build directory
- **Dummy RPC adapter:** Registered at `http://0.0.0.0:0` for capability lookup; never actually called

## Troubleshooting

### Test Fails: "Entity not found"

**Cause:** Handler didn't create the expected entity.

**Fix:**
1. Check event signature matches ABI (include `indexed` keywords)
2. Verify contract address matches manifest
3. Check block number is >= data source's `startBlock`
4. Add debug logging to your mapping handler

### Test Timeout

**Cause:** Indexer took longer than 60 seconds (default timeout).

**Fix:**
1. Reduce number of blocks in test
2. Simplify mapping logic
3. Check for infinite loops in handler code

### eth_call Returns Wrong Value

**Cause:** Call cache miss — no matching mock in `ethCalls`.

**Fix:**
1. Verify `address`, `function`, and `params` exactly match the call from your mapping
2. Check function signature format: `"functionName(inputTypes)(returnTypes)"`
3. Ensure parameters are in correct order

### Block Handler Not Firing

**Cause:** Block handlers auto-fire, but might be outside data source's active range.

**Fix:**
1. Check data source's `startBlock` in manifest
2. Use explicit `"number"` in test blocks to ensure they're >= `startBlock`
3. Verify handler is defined in manifest's `blockHandlers` section

## Legacy Matchstick Mode

Fall back to the external Matchstick test runner for backward compatibility:

```bash
gnd test --matchstick
```

This is useful if:
- You have existing Matchstick tests
- You need features not yet supported by the mock-based runner
- You're migrating gradually from Matchstick to the new test format

## See Also

- [Subgraph Manifest Documentation](https://thegraph.com/docs/en/developing/creating-a-subgraph/)
- [AssemblyScript Mapping API](https://thegraph.com/docs/en/developing/assemblyscript-api/)
- [GraphQL Schema](https://thegraph.com/docs/en/developing/creating-a-subgraph/#the-graph-ql-schema)
