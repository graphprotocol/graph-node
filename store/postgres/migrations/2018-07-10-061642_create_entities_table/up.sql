/**************************************************************
* CREATE TABLE
**************************************************************/
CREATE TABLE IF NOT EXISTS entities (
     id VARCHAR NOT NULL,
     data_source VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data jsonb NOT NULL,
     latest_block_hash VARCHAR(40) DEFAULT NULL,
     PRIMARY KEY (id, data_source, entity)
 );
