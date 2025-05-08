# Graph Node Dev (`gnd`)

Graph Node Dev (`gnd`) is a tool for subgraph development that makes local testing faster and easier.

## What It Does

`gnd` simplifies the subgraph development process by:

- Automatically spinning up a local Graph Node instance
- Setting up a temporary PostgreSQL database for your subgraph data
- Deploying your subgraph to the local Graph Node
- Providing a watch mode that automatically redeploys when you make changes

Instead of manually configuring and connecting multiple services (PostgreSQL, IPFS, graph-node), `gnd` handles everything with a single command.

## Usage

```bash
gnd [OPTIONS]
```

### Basic Options

| Option | Description | Default |
|--------|-------------|---------|
| `--watch` | Automatically redeploy when files change | `false` |
| `--manifest` | Path to subgraph manifest | `./build/subgraph.yaml` |
| `--ethereum-rpc` | Ethereum network and RPC URL<br>`NETWORK_NAME:[CAPABILITIES]:URL` | - |
| `--ipfs` | IPFS server address | `https://api.thegraph.com/ipfs` |

## Quick Start

1. Build your subgraph:
   ```bash
   graph build
   ```

2. Run gnd:
   ```bash
   gnd
   ```

3. For development with auto-reloading:
   ```bash
   gnd --watch
   ```

## Common Examples

### Basic Usage
```bash
# Run with default settings
gnd

# Watch for changes
gnd --watch

# Specify custom manifest location
gnd --manifest ./subgraphs/my-subgraph/subgraph.yaml
```

### Network Configuration
```bash
# Connect to Ethereum mainnet via custom RPC
gnd --ethereum-rpc mainnet:https://ethereum-rpc.example.com

# Custom IPFS endpoint
gnd --ipfs https://api.thegraph.com/ipfs
```

## Development Workflow

1. Build your subgraph with `graph build`
2. Start `gnd`, optionally with `--watch` flag
3. Make changes to your subgraph code
4. If using `--watch`, changes will auto-deploy; otherwise rebuild and restart
5. Query your subgraph at http://localhost:8000/subgraphs/name/test

