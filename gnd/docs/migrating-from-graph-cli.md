# Migrating from graph-cli to gnd

`gnd` is designed as a drop-in replacement for `graph-cli`. This guide covers the differences and how to migrate your workflow.

## TL;DR

For most users, migration is straightforward:

```bash
# Instead of:
graph codegen
graph build
graph deploy --studio my-subgraph

# Use:
gnd codegen
gnd build
gnd deploy my-subgraph  # defaults to Studio
```

## Command Mapping

| graph-cli | gnd | Notes |
|-----------|-----|-------|
| `graph init` | `gnd init` | Identical flags |
| `graph codegen` | `gnd codegen` | Identical flags |
| `graph build` | `gnd build` | Identical flags |
| `graph deploy` | `gnd deploy` | Defaults to Studio if `--node` not provided |
| `graph create` | `gnd create` | Identical flags |
| `graph remove` | `gnd remove` | Identical flags |
| `graph auth` | `gnd auth` | Identical flags |
| `graph add` | `gnd add` | Identical flags |
| `graph test` | `gnd test` | Identical flags |
| `graph clean` | `gnd clean` | Identical flags (new in graph-cli 0.80+) |
| `graph publish` | `gnd publish` | Identical flags |
| `graph local` | N/A | Not implemented - use graph-node's test infrastructure |
| `graph node` | N/A | Not implemented - use `graphman` |

## What's the Same

### Flag Compatibility

All flags use the same names, short forms, and defaults:

```bash
# Both work identically
graph codegen -o generated/ --skip-migrations
gnd codegen -o generated/ --skip-migrations

graph build -t wasm --network mainnet
gnd build -t wasm --network mainnet

graph deploy -l v1.0.0 --ipfs http://localhost:5001 my-subgraph
gnd deploy -l v1.0.0 --ipfs http://localhost:5001 my-subgraph
```

### Output Format

Same success checkmarks, step descriptions, and progress indicators:

```
✔ Generate types for data source: Gravity (...)
✔ Write types to generated/Gravity/Gravity.ts (...)
```

### Exit Codes

- `0` for success
- `1` for any error

### Configuration Files

`gnd` uses the same configuration files as `graph-cli`:

- `~/.graph-cli.json` - Deploy keys (from `gnd auth`)
- `networks.json` - Network-specific addresses

Your existing auth keys work with both tools.

### Generated Code

Code generation produces identical AssemblyScript output (after formatting). Your existing subgraph code will work without changes.

## What's Different

### Commands Not Available

#### `graph local`

Not implemented in `gnd`. Use graph-node's built-in integration test infrastructure or Docker compose setups instead.

#### `graph node`

Not implemented. Use `graphman` for node management operations:

```bash
# Instead of graph node commands, use graphman:
graphman info subgraph-name
graphman reassign subgraph-name shard
```

### Debug Logging

```bash
# graph-cli uses:
DEBUG=graph-cli:* graph codegen

# gnd uses:
RUST_LOG=gnd=debug gnd codegen
```

### The `--uncrashable` Flag

The `--uncrashable` flag on `codegen` (Float Capital's uncrashable helper generation) is not implemented. This is a third-party feature that most subgraphs don't use.

### Minor Code Generation Differences

These differences are intentional and documented:

1. **Int8 import**: `gnd` always imports `Int8` in generated code for simplicity, even when not used. This doesn't affect functionality.

2. **Trailing commas**: `gnd` uses trailing commas in multi-line constructs:
   ```typescript
   // gnd output
   new Foo(
     param1,
     param2,  // trailing comma
   )

   // graph-cli output
   new Foo(
     param1,
     param2
   )
   ```

3. **2D array accessors**: `gnd` uses the correct `toStringMatrix()` method for 2D GraphQL arrays, fixing a bug in graph-cli that used `toStringArray()`.

## Migration Steps

### Step 1: Install gnd

Build from source in the graph-node repository:

```bash
cargo build -p gnd --release
# Binary at target/release/gnd
```

### Step 2: Verify Your Auth

Your existing `~/.graph-cli.json` works with both tools. Verify with:

```bash
# Check stored keys
cat ~/.graph-cli.json

# Or just run a deploy dry-run to see if auth is working
gnd deploy --help
```

### Step 3: Test Locally

Run your normal workflow with gnd:

```bash
gnd codegen
gnd build
```

Compare the output to graph-cli if you want to verify:

```bash
# graph-cli
graph codegen -o generated-graph/
graph build -o build-graph/

# gnd
gnd codegen -o generated-gnd/
gnd build -o build-gnd/

# Compare (should be identical after formatting)
diff -r generated-graph generated-gnd
diff -r build-graph build-gnd
```

### Step 4: Update CI/CD

Replace `graph` with `gnd` in your CI configuration:

```yaml
# Before
- run: graph codegen
- run: graph build
- run: graph deploy --studio ${{ secrets.SUBGRAPH_NAME }}

# After
- run: gnd codegen
- run: gnd build
- run: gnd deploy ${{ secrets.SUBGRAPH_NAME }}
```

### Step 5: Update package.json Scripts (Optional)

If you have npm scripts calling graph-cli:

```json
{
  "scripts": {
    "codegen": "gnd codegen",
    "build": "gnd build",
    "deploy": "gnd deploy"
  }
}
```

## Troubleshooting

### "Command not found: gnd"

Make sure the gnd binary is in your PATH:

```bash
export PATH="$PATH:/path/to/graph-node/target/release"
```

### "prettier: command not found"

`gnd codegen` shells out to `prettier` for formatting. Install it:

```bash
npm install -g prettier
# or
pnpm add -g prettier
```

### "asc: command not found"

`gnd build` shells out to the AssemblyScript compiler. Install it:

```bash
npm install -g assemblyscript
# or have it in your subgraph's node_modules
npm install --save-dev assemblyscript
```

### Different Output After codegen

If you notice differences in generated code:

1. Check if it's one of the documented differences (Int8 import, trailing commas)
2. Run prettier on both outputs to normalize formatting
3. Report any unexpected differences as a bug

### Authentication Issues

Keys stored by graph-cli work with gnd and vice versa. If you have issues:

```bash
# Re-authenticate
gnd auth YOUR_DEPLOY_KEY

# Verify the key was saved
cat ~/.graph-cli.json
```

## Why Migrate?

### Benefits of gnd

- **Native performance**: Rust implementation is faster for large subgraphs
- **Integrated with graph-node**: Same codebase, consistent behavior
- **Better error messages**: Leverages graph-node's validation
- **`gnd dev` command**: Run graph-node in development mode

### When to Keep Using graph-cli

- If you need multi-protocol support (NEAR, Cosmos, Arweave, Substreams) - gnd currently focuses on Ethereum
- If you use the `--uncrashable` flag
- If you need the `graph local` or `graph node` commands

## Getting Help

- File issues at: https://github.com/graphprotocol/graph-node/issues
- Check gnd version: `gnd --version`
