# Pre-Indexer for subgraphs

## Design
The pre indexer will traverse all the blocks, according to some filters which are currently defined
per chain. For each block it will run the mappings with the block and a state kv as input and store
the resulting triggers for the block, these will later be used as input for subgraphs instead of 
raw blockchain data. 

By exposing the state, it will allow users to maintain their own logic for handling dynamic data sources
as well as any derived data. 

The state is expected to be returned after every block and passed on to the next. The state will
not be available for querying and only the latest version is kept between blocks and it will be 
limited in size, through a mechanism we will defined later on.

If state is not used then all the processing will happen in parallel until it reaches the chain head. 

## State
State refers to intermediate state between blocks (think the state for fold operations). It is only 
queryable from the pre-indexer, subgraphs and graphql don't have access to it.

In order to support reverts, it is necessary to be able to retrieve a previous state. State should
store a log of delta operations as well as a snapshot every TBD amount of blocks. Retrieving an old state
will be possible by getting the latest snapshot and applying the delta operations between that block and 
the block it is needed at. 

This state is necessary so that users can keep track of things like created contracts on ethereum.

State is indexed by a string key and an optional tag and will store a Vec<u8>. This means that anything
stored in the state should ideally use a serializable binary format like borsh or protobuf.

The key is designed to be an ID or unique value and tag helps query items by tag. As an example:

```
  store.set("123", "token", ...)
  store.set("321", "token", ...)
  store.get_all("token") // Should yield both the previous values.
```

## Processing
The pre-indexer will iterate over all the blocks coming from firehose/substreams, this means it is
possible to apply filters to the incoming data so that the processing is quicker. The main note
about the processing is that if state is not used, the entire block space being scanned can be 
partitioned and handled in parallel.

## Parallel Processing
The worker will calculate the range between the last stable block (if present) and the chain head
minus the Reorg threshold. For a given number of workers, each worker will get a range starting the 
oldest stable block or start block. 

As confirmation of completion arrives from the older block ranges the last stable block is updated
and the db is flushed to ensure data is written to disk before continuing.

### Last Stable Block
Last Stable block is the property that can be observed from subgraphs block streams in order to know
if the data that follows is ready for processing so it acts as a barrier to protect from consuming
state that is still in flight. 

### Recovery
In case of a failure, only blocks before LSB are considered valid and the rest will be overwritten 
by running the same process again. 

### Cancellation
If an error occurs within one of the ranges, the error should propagate to the orchestration function
which should cancel all the ranges as soon as possible. 

## Store
The store is a mapping of BlockNumber to the list of triggers for that block, where the order will be 
preserved. 

## Transformations
The transformations is similar to the mappings on subgraphs, they provide the code that performs the 
data extraction from blocks. Transformations take as input the previous state and the block, returning
the new state and a list of encoded triggers, that is, the values to be stored for the processed block,
which are later used as inputs for subgraphs.
