/**************************************************************
* CREATE TABLE
**************************************************************/
-- Stores list of immutable subgraphs
CREATE TABLE IF NOT EXISTS subgraphs (
    id VARCHAR PRIMARY KEY,
    network_name VARCHAR NOT NULL,
    latest_block_hash VARCHAR NOT NULL,
    latest_block_number BIGINT NOT NULL
);

-- Maps subgraph names to immutable subgraph versions (IDs).
CREATE TABLE IF NOT EXISTS subgraph_names (
    subgraph_name VARCHAR PRIMARY KEY,
    subgraph_id VARCHAR,
    access_token VARCHAR
);
