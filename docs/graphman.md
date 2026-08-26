## Graphman Commands

- [Info](#info)
- [Remove](#remove)
- [Unassign](#unassign)
- [Unused Record](#unused-record)
- [Unused Remove](#unused-remove)
- [Drop (removed)](#drop)
- [Copy Create](#copy-create)
- [Copy Activate](#copy-activate)
- [Copy List](#copy-list)
- [Copy Status](#copy-status)
- [Chain Check Blocks](#check-blocks)
- [Chain Call Cache Remove](#chain-call-cache-remove)

<a id="info"></a>
# ⌘ Info

### SYNOPSIS

    Prints the details of a deployment

    The deployment can be specified as either a subgraph name, an IPFS hash `Qm..`, or the database
    namespace `sgdNNN`. Since the same IPFS hash can be deployed in multiple shards, it is possible to
    specify the shard by adding `:shard` to the IPFS hash.

    USAGE:
        graphman --config <CONFIG> info [OPTIONS] <DEPLOYMENT>

    ARGS:
        <DEPLOYMENT>
                The deployment (see above)

    OPTIONS:
        -c, --current
                List only current version

        -h, --help
                Print help information

        -p, --pending
                List only pending versions

        -s, --status
                Include status information

        -u, --used
                List only used (current and pending) versions

### DESCRIPTION

The `info` command fetches details for a given deployment from the database.

By default, it shows the following attributes for the deployment:

-   **name**
-   **status** *(`pending` or `current`)*
-   **id** *(the `Qm...` identifier for the deployment's subgraph)*
-   **namespace** *(The database schema which contains that deployment data tables)*
-   **shard**
-   **active** *(If there are multiple entries for the same subgraph, only one of them will be active. That's the one we use for querying)*
-   **chain**
-   **graph node id**

### OPTIONS

If the `--status` option is enabled, extra attributes are also returned:

-   **synced*** *(Whether or not the subgraph has synced all the way to the current chain head)*
-   **health** *(Can be either `healthy`, `unhealthy` (syncing with errors) or `failed`)*
-   **latest indexed block**
-   **current chain head block**

### EXAMPLES

Describe a deployment by its name:

    graphman --config config.toml info subgraph-name

Describe a deployment by its hash:

    graphman --config config.toml info QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66

Describe a deployment with extra info:

    graphman --config config.toml info QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66 --status

<a id="remove"></a>
# ⌘ Remove

### SYNOPSIS

    Remove a named subgraph

    USAGE:
        graphman --config <CONFIG> remove <NAME>

    ARGS:
        <NAME>    The name of the subgraph to remove

    OPTIONS:
        -h, --help    Print help information

### DESCRIPTION

Removes the association between a subgraph name and a deployment.

No indexed data is lost as a result of this command.

It is used mostly for stopping query traffic based on the subgraph's name, and to release that name for
another deployment to use.

### EXAMPLES

Remove a named subgraph:

    graphman --config config.toml remove subgraph-name

<a id="unassign"></a>
# ⌘ Unassign

#### SYNOPSIS

    Unassign a deployment

    USAGE:
        graphman --config <CONFIG> unassign <DEPLOYMENT>

    ARGS:
        <DEPLOYMENT>    The deployment (see `help info`)

    OPTIONS:
        -h, --help    Print help information

#### DESCRIPTION

Makes `graph-node` stop indexing a deployment permanently.

No indexed data is lost as a result of this command.

Refer to the [Maintenance Documentation](https://github.com/graphprotocol/graph-node/blob/master/docs/maintenance.md#modifying-assignments) for more details about how Graph Node manages its deployment
assignments.

#### EXAMPLES

Unassign a deployment by its name:

    graphman --config config.toml unassign subgraph-name

Unassign a deployment by its hash:

    graphman --config config.toml unassign QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66

<a id="unused-record"></a>
# ⌘ Unused Record

### SYNOPSIS

    graphman-unused-record
    Update and record currently unused deployments

    USAGE:
        graphman unused record

    OPTIONS:
        -h, --help    Print help information


### DESCRIPTION

Inspects every shard for unused deployments and registers them in the `unused_deployments` table in the
primary shard.

No indexed data is lost as a result of this command.

This sub-command is used as previous step towards removing all data from unused subgraphs, followed by
`graphman unused remove`.

A deployment is unused if it fulfills all of these criteria:

1.  It is not assigned to a node.
2.  It is either not marked as active or is neither the current or pending version of a subgraph.
3.  It is not the source of a currently running copy operation

### EXAMPLES

To record all unused deployments:

    graphman --config config.toml unused record

<a id="unused-remove"></a>
# ⌘ Unused Remove

### SYNOPSIS

    Remove deployments that were marked as unused with `record`.

    Deployments are removed in descending order of number of entities, i.e., smaller deployments are
    removed before larger ones

    USAGE:
        graphman unused remove [OPTIONS]

    OPTIONS:
        -c, --count <COUNT>
                How many unused deployments to remove (default: all)

        -d, --deployment <DEPLOYMENT>
                Remove a specific deployment

        -h, --help
                Print help information

        -o, --older <OLDER>
                Remove unused deployments that were recorded at least this many minutes ago

### DESCRIPTION

Removes from database all indexed data from deployments previously marked as unused by the `graphman unused
record` command.

This operation is irreversible.

### EXAMPLES

Remove all unused deployments

    graphman --config config.toml unused remove

Remove all unused deployments older than 12 hours (720 minutes)

    graphman --config config.toml unused remove --older 720

Remove a specific unused deployment

    graphman --config config.toml unused remove --deployment QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66

<a id="drop"></a>
# ⌘ Drop (removed)

`graphman drop` was removed in [#5974](https://github.com/graphprotocol/graph-node/pull/5974).
It is documented here only so that anyone looking for it finds out where it went.

To delete a deployment and all its indexed data, run the sequence `drop` used to
wrap, which is the same one its description always listed:

1. `graphman info <search term>` to find the deployment id and name
2. [`graphman unassign <deployment id>`](#unassign)
3. [`graphman remove <deployment name>`](#remove)
4. [`graphman unused record`](#unused-record)
5. [`graphman unused remove <deployment id>`](#unused-remove)

This operation is irreversible.

<a id="copy-create"></a>
# ⌘ Copy Create

### SYNOPSIS

    Create a copy of an existing subgraph

    The copy will be treated as its own deployment. The deployment with IPFS hash `src` will be
    copied to a new deployment in the database shard `shard` and will be assigned to `node` for
    indexing. The new subgraph will start as a copy of all blocks of `src` that are `offset` behind
    the current subgraph head of `src`. The offset should be chosen such that only final blocks are
    copied

    USAGE:
        graphman --config <CONFIG> copy create [OPTIONS] <SRC> <SHARD> <NODE>

    ARGS:
        <SRC>
                The source deployment (see `help info`)

        <SHARD>
                The name of the database shard into which to copy

        <NODE>
                The name of the node that should index the copy

    OPTIONS:
        -a, --activate
                Activate this copy once it has synced

        -h, --help
                Print help information

        -o, --offset <OFFSET>
                How far behind `src` subgraph head to copy [default: 200]

        -r, --replace
                Replace the source with this copy once it has synced

### DESCRIPTION

Copies the data of an existing deployment into another database shard. The copy becomes a new
deployment with its own `sgdNNN` namespace, assigned to `node`, which performs the copy. The source
deployment is not modified and is not removed.

The copy is made from the state of `src` as of `offset` blocks behind its current head, and the new
deployment then indexes forward from that point on its own. The default offset of 200 exists so that
only final blocks are copied; choosing an offset smaller than the reorg threshold of the chain risks
copying a block that is later reorged away.

`--activate` and `--replace` control what happens once the copy has caught up, and are mutually
exclusive:

| Flag | Behaviour once synced |
| --- | --- |
| neither | Nothing. The copy stays synced and inactive until activated manually. |
| `--activate` | Queries are routed to the copy; the previously active copy becomes inactive. |
| `--replace` | As `--activate`, and additionally unassigns any other copies of the same deployment. |

Use `--replace` when the intent is to move a deployment to another shard rather than to keep a second
copy of it.

Copying is done in batches so that it does not hold long-running transactions open, which would cause
table bloat elsewhere in the system. The batch size adapts so that each batch takes approximately a
fixed target duration, and the copy backs off when database replication lag becomes too large.
Progress is recorded per table in `subgraphs.copy_state` and `subgraphs.copy_table_state`, so a copy
that is interrupted, for example by restarting `graph-node`, resumes where it left off rather than
starting again.

The command refuses to start a copy when:

1.  The source has not indexed any blocks yet.
2.  The source has not yet indexed `offset` blocks.
3.  The block at `offset` behind the source head is earlier than the source's `earliest_block_number`.
    This happens when the source has been pruned past the point being copied from.
4.  The block at that height is not present in the chain block cache.
5.  The block cache holds more than one hash for that height.
6.  `shard` is not one of the configured shards.

After a copy has been activated, the source deployment still exists and still occupies disk. Use
`graphman unused record` followed by `graphman unused remove` to reclaim it. Note that a deployment is
not eligible to be recorded as unused while it is the source of a currently running copy operation.

### EXAMPLES

Copy a deployment into the `shard2` shard, to be indexed by `index_node_1`, keeping the original:

    graphman --config config.toml copy create QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66 shard2 index_node_1

Move a deployment to another shard, activating the copy and unassigning the original once it has
synced:

    graphman --config config.toml copy create --replace QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66 shard2 index_node_1

Copy from further behind the chain head than the default:

    graphman --config config.toml copy create --offset 500 QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66 shard2 index_node_1


<a id="copy-activate"></a>
# ⌘ Copy Activate

### SYNOPSIS

    Activate the copy of a deployment

    This will route queries to that specific copy (with some delay); the previously active copy will
    become inactive. Only copies that have progressed at least as far as the original should be
    activated

    USAGE:
        graphman --config <CONFIG> copy activate <DEPLOYMENT> <SHARD>

    ARGS:
        <DEPLOYMENT>
                The IPFS hash of the deployment to activate

        <SHARD>
                The name of the database shard that holds the copy

    OPTIONS:
        -h, --help
                Print help information

### DESCRIPTION

Makes the copy of a deployment in `shard` the active one, so that queries for that deployment are
served from it. The previously active copy becomes inactive but is not removed.

This is the manual equivalent of having passed `--activate` to `graphman copy create`, and is used
when a copy was created without that flag.

Query routing changes take effect with some delay rather than immediately.

Activating a copy that is behind the currently active one will serve queries from the less advanced
copy. Check progress with `graphman copy status` before activating.

### EXAMPLES

Activate the copy of a deployment held in `shard2`:

    graphman --config config.toml copy activate QmfWRZCjT8pri4Amey3e3mb2Bga75Vuh2fPYyNVnmPYL66 shard2


<a id="copy-list"></a>
# ⌘ Copy List

### SYNOPSIS

    List all currently running copy and graft operations

    USAGE:
        graphman --config <CONFIG> copy list

    OPTIONS:
        -h, --help
                Print help information

### DESCRIPTION

Lists the copy and graft operations that are currently in progress across all shards.

### EXAMPLES

    graphman --config config.toml copy list


<a id="copy-status"></a>
# ⌘ Copy Status

### SYNOPSIS

    Print the progress of a copy operation

    USAGE:
        graphman --config <CONFIG> copy status <DST>

    ARGS:
        <DST>
                The destination deployment of the copy operation (see `help info`)

    OPTIONS:
        -h, --help
                Print help information

### DESCRIPTION

Prints the progress of the copy operation whose destination is `dst`, reading the state that the copy
records as it runs.

The output covers the copy as a whole, including the target block, when it started and, if it has
ended, when it finished or was cancelled. It also breaks progress down per entity type, showing how
far through the table's `vid` range the copy has reached, the batch size currently in use and how long
that table has taken so far.

### EXAMPLES

Show the progress of a copy by the destination deployment:

    graphman --config config.toml copy status sgd1234


<a id="check-blocks"></a>
# ⌘ Check Blocks

### SYNOPSIS

    Compares cached blocks with fresh ones and clears the block cache when they differ

    USAGE:
        graphman --config <config> chain check-blocks <chain-name> <SUBCOMMAND>

    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information

    ARGS:
        <chain-name>    Chain name (must be an existing chain, see 'chain list')

    SUBCOMMANDS:
        by-hash      The number of the target block
        by-number    The hash of the target block
        by-range     A block number range, inclusive on both ends

### DESCRIPTION

The `check-blocks` command compares cached blocks with blocks from a JSON RPC provider and removes any blocks
from the cache that differ from the ones retrieved from the provider.

Sometimes JSON RPC providers send invalid block data to Graph Node. The `graphman chain check-blocks` command
is useful to diagnose the integrity of cached blocks and eventually fix them.

### OPTIONS

Blocks can be selected by different methods. The `check-blocks` command lets you use the block hash, a single
number or a number range to refer to which blocks it should verify:

#### `by-hash`

    graphman --config <config> chain check-blocks <chain-name> by-hash <hash>

#### `by-number`

    graphman --config <config> chain check-blocks <chain-name> by-number <number> [--delete-duplicates]

#### `by-range`

    graphman --config <config> chain check-blocks <chain-name> by-range [-f|--from <block-number>] [-t|--to <block-number>] [--delete-duplicates]

The `by-range` method lets you scan for numeric block ranges and offers the `--from` and `--to` options for
you to define the search bounds. If one of those options is omitted, `graphman` will consider an open bound
and will scan all blocks up to or after that number.

Over time, it can happen that a JSON RPC provider offers different blocks for the same block number. In those
cases, `graphman` will not decide which block hash is the correct one and will abort the operation. Because of
this, the `by-number` and `by-range` methods also provide a `--delete-duplicates` flag, which orients
`graphman` to delete all duplicated blocks for the given number and resume its operation.

### EXAMPLES

Inspect a single Ethereum Mainnet block by hash:

    graphman --config config.toml chain check-blocks mainnet by-hash 0xd56a9f64c7e696cfeb337791a7f4a9e81841aaf4fcad69f9bf2b2e50ad72b972

Inspect a block using its number:

    graphman --config config.toml chain check-blocks mainnet by-number 15626962

Inspect a block range, deleting any duplicated blocks:

    graphman --config config.toml chain check-blocks mainnet by-range --from 15626900 --to 15626962 --delete-duplicates

Inspect all blocks after block `13000000`:

    graphman --config config.toml chain check-blocks mainnet by-range --from 13000000

<a id="chain-call-cache-remove"></a>
# ⌘ Chain Call Cache Remove

### SYNOPSIS

Remove the call cache of the specified chain.

Either remove entries in the range `--from` and `--to`, remove stale contracts which have not been accessed for a specified duration `--ttl_days`, or remove the entire cache with `--remove-entire-cache`. Removing the entire cache can reduce indexing performance significantly and should generally be avoided.

    Usage: graphman chain call-cache <CHAIN_NAME> remove [OPTIONS]

    Options:
        --remove-entire-cache
            Remove the entire cache

        --ttl-days <TTL_DAYS>
            Remove stale contracts based on call_meta table

        --ttl-max-contracts <TTL_MAX_CONTRACTS>
            Limit the number of contracts to consider for stale contract removal

        -f, --from <FROM>
            Starting block number

        -t, --to <TO>
            Ending block number

        -h, --help
            Print help (see a summary with '-h')


### DESCRIPTION

Remove the call cache of a specified chain.

### OPTIONS

The `from` and `to` options are used to decide the block range of the call cache that needs to be removed.

#### `from`

The `from` option is used to specify the starting block number of the block range. In the absence of `from` option,
the first block number will be used as the starting block number.

#### `to`

The `to` option is used to specify the ending block number of the block range. In the absence of `to` option,
the last block number will be used as the ending block number.

#### `--remove-entire-cache`
The `--remove-entire-cache` option is used to remove the entire call cache of the specified chain.

#### `--ttl-days <TTL_DAYS>`
The `--ttl-days` option is used to remove stale contracts based on the `call_meta.accessed_at` field. For example, if `--ttl-days` is set to 7, all calls to a contract that has not been accessed in the last 7 days will be removed from the call cache.

#### `--ttl-max-contracts <TTL_MAX_CONTRACTS>`
The `--ttl-max-contracts` option is used to limit the maximum number of contracts to be removed when using the `--ttl-days` option. For example, if `--ttl-max-contracts` is set to 100, at most 100 contracts will be removed from the call cache even if more contracts meet the TTL criteria.

### EXAMPLES

Remove the call cache for all blocks numbered from 10 to 20:

    graphman --config config.toml chain call-cache ethereum remove --from 10 --to 20

Remove all the call cache of the specified chain:

    graphman --config config.toml chain call-cache ethereum remove --remove-entire-cache

Remove stale contracts from the call cache that have not been accessed in the last 7 days:

    graphman --config config.toml chain call-cache ethereum remove --ttl-days 7

Remove stale contracts from the call cache that have not been accessed in the last 7 days, limiting the removal to a maximum of 100 contracts:
    graphman --config config.toml chain call-cache ethereum remove --ttl-days 7 --ttl-max-contracts 100

