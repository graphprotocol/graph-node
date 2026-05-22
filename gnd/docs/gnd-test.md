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
- Mock Ethereum RPC responses (`eth_call`, `eth_getBalance`, `eth_getCode`)
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
| `timestamp` | No | `block_number` | Unix timestamp |
| `baseFeePerGas` | No | None (pre-EIP-1559) | Base fee in wei |
| `events` | No | Empty array | Log events in this block |
| `ethCalls` | No | Empty array | Mock `eth_call` responses |
| `getBalanceCalls` | No | Empty array | Mock `eth_getBalance` responses for `ethereum.getBalance()` |
| `hasCodeCalls` | No | Empty array | Mock `eth_getCode` responses for `ethereum.hasCode()` |

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

## Transaction Receipts

Mock receipts are constructed for every log trigger and attached only to handlers that declare `receipt: true` in the manifest, mirroring production behaviour. Handlers without `receipt: true` receive a null receipt — the same as on a real node.

**Limitation:** Only `receipt.logs` reflects your test data. All other receipt fields (`from`, `to`, `gas_used`, `status`, etc.) are hardcoded stubs and do not correspond to real transaction data. If your handler reads those fields, the values will be fixed defaults regardless of what you put in the test JSON.

### How receipts are built

Every event gets a mock receipt attached automatically. The key rule is **`txHash` grouping**:

- Events sharing the same `txHash` share **one receipt** — `event.receipt!.logs` contains all of their logs in declaration order.
- Events without an explicit `txHash` each get a unique auto-generated hash (`keccak256(block_number || log_index)`), so each gets its own single-log receipt.

### Example: Two events sharing a receipt

```json
{
  "events": [
    {
      "address": "0x1234...",
      "event": "Transfer(address indexed from, address indexed to, uint256 value)",
      "params": { "from": "0xaaaa...", "to": "0xbbbb...", "value": "100" },
      "txHash": "0xdeadbeef0000000000000000000000000000000000000000000000000000000"
    },
    {
      "address": "0x1234...",
      "event": "Transfer(address indexed from, address indexed to, uint256 value)",
      "params": { "from": "0xbbbb...", "to": "0xcccc...", "value": "50" },
      "txHash": "0xdeadbeef0000000000000000000000000000000000000000000000000000000"
    }
  ]
}
```

Both handlers receive a receipt where `receipt.logs` has two entries, in declaration order.

### Mock receipt defaults

| Field | Value |
|-------|-------|
| `status` | success |
| `cumulative_gas_used` | `21000` |
| `gas_used` | `21000` |
| transaction type | `2` (EIP-1559) |
| `from` | `0x000...000` |
| `to` | `null` |
| `effective_gas_price` | `0` |

Handlers without `receipt: true` in the manifest are unaffected — they never access `event.receipt`.

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
| `reverts` | No | Default `false`. If `true`, the mock transport returns an RPC error |

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

## ethereum.getBalance() Mocking

Mock balance lookups made from mapping handlers using `ethereum.getBalance()`:

```json
{
  "getBalanceCalls": [
    {
      "address": "0xaaaa000000000000000000000000000000000000",
      "value": "1000000000000000000"
    }
  ]
}
```

### getBalanceCalls Fields

| Field | Required | Description |
|-------|----------|-------------|
| `address` | Yes | Account address (checksummed or lowercase hex) |
| `value` | Yes | Balance in Wei as a decimal string |

## ethereum.hasCode() Mocking

Mock code existence checks made from mapping handlers using `ethereum.hasCode()`:

```json
{
  "hasCodeCalls": [
    {
      "address": "0x1234000000000000000000000000000000000000",
      "hasCode": true
    }
  ]
}
```

### hasCodeCalls Fields

| Field | Required | Description |
|-------|----------|-------------|
| `address` | Yes | Contract address (checksummed or lowercase hex) |
| `hasCode` | Yes | Whether the address has deployed bytecode |

## File Data Sources

Mock IPFS and Arweave file contents for file data source handlers. Files are defined at the top level of the test JSON (not inside blocks).

### IPFS Files

```json
{
  "name": "File data source test",
  "files": [
    {
      "cid": "QmExample...",
      "content": "{\"name\": \"Token\", \"description\": \"A token\"}"
    },
    {
      "cid": "QmAnother...",
      "file": "fixtures/metadata.json"
    }
  ],
  "blocks": [...],
  "assertions": [...]
}
```

#### files Fields

| Field | Required | Description |
|-------|----------|-------------|
| `cid` | Yes | IPFS CID (`Qm...` or `bafy...`). The mock ignores hash/content relationship |
| `content` | One of `content`/`file` | Inline UTF-8 content |
| `file` | One of `content`/`file` | File path, resolved relative to the test JSON |

### Arweave Files

```json
{
  "name": "Arweave data source test",
  "arweaveFiles": [
    {
      "txId": "abc123",
      "content": "{\"name\": \"Token\"}"
    },
    {
      "txId": "def456/metadata.json",
      "file": "fixtures/arweave-data.json"
    }
  ],
  "blocks": [...],
  "assertions": [...]
}
```

#### arweaveFiles Fields

| Field | Required | Description |
|-------|----------|-------------|
| `txId` | Yes | Arweave transaction ID or bundle path (e.g. `"txid/filename.json"`) |
| `content` | One of `content`/`file` | Inline UTF-8 content |
| `file` | One of `content`/`file` | File path, resolved relative to the test JSON |

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
| Hex strings | **Must be lowercase** — graph-node returns all hex values in lowercase |

**Important:** Arrays are compared as sets (order doesn't matter).

**Important:** graph-node returns all hex-encoded values (addresses, transaction hashes, byte arrays) in **lowercase**. Expected values in assertions must match exactly — mixed-case hex will not match:

```json
{
  "expected": {
    "transfer": {
      "from": "0xaaaa000000000000000000000000000000000000",  // ✅ lowercase
      "to":   "0xBBBB000000000000000000000000000000000000"   // ❌ will not match
    }
  }
}
```

Note: hex values in event inputs (`params`, `address`) are normalized automatically and can be mixed case. If you need ordered results, use `orderBy` in your GraphQL query:

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
| `eth_call` mocking | ✅ Supported |
| `ethereum.getBalance()` mocking | ✅ Supported |
| `ethereum.hasCode()` mocking | ✅ Supported |
| Dynamic/template data sources | ✅ Supported |
| Transaction receipts (`receipt: true`) | ⚠️ Partial — `receipt.logs` is populated and grouped by `txHash`; other fields (gas, from, to, etc.) are hardcoded stubs (see [Transaction Receipts](#transaction-receipts)) |
| File data sources (IPFS + Arweave) | ✅ Supported |
| Call triggers (traces) | ❌ Not implemented |
| `--json` CI output | ❌ Not implemented |
| Parallel test execution | ❌ Not implemented |
| Test name filtering (`--filter`) | ❌ Not implemented |

## Tips & Best Practices

### Use Lowercase Hex in Assertions

graph-node returns **all** hex-encoded values in lowercase — addresses, transaction hashes, and any `Bytes`/`ID` fields. Expected values in assertions must use lowercase hex:

```json
{
  "expected": {
    "transfer": {
      "from": "0xaaaa000000000000000000000000000000000000"  // ✅ lowercase
    }
  }
}
```

Not:
```json
{
  "expected": {
    "transfer": {
      "from": "0xAAAA000000000000000000000000000000000000"  // ❌ will not match
    }
  }
}
```

Event inputs (`address`, `params`) are normalized automatically and can be mixed case.

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
- **Mock transport layer:** A mock Alloy transport serves `eth_call`, `eth_getBalance`, and `eth_getCode` from test JSON data. All three flow through the real production code path — only the transport returns mock responses. Unmocked RPC calls fail immediately with a descriptive error.
- **No IPFS for manifest:** Uses `FileLinkResolver` to load manifest/WASM from build directory

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

### Unmocked RPC Call

**Cause:** A mapping handler calls `ethereum.call`, `ethereum.getBalance`, or `ethereum.hasCode` for a call that has no matching mock entry.

**Symptom:** Test fails immediately with a descriptive error like:
```
gnd test: unmocked eth_call to 0x1234... at block hash 0xabcd...
Add a matching 'ethCalls' entry to this block in your test JSON.
```

**Fix:**
1. Add the missing mock to the appropriate field in your test block (`ethCalls`, `getBalanceCalls`, or `hasCodeCalls`)
2. If the call is not supposed to happen, check the mapping logic — a code path may be executing unexpectedly

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
