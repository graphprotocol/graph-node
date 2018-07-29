/**************************************************************
* CREATE TABLE
**************************************************************/
CREATE TABLE IF NOT EXISTS entities (
     id VARCHAR NOT NULL,
     subgraph VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data jsonb NOT NULL,
     event_source VARCHAR DEFAULT NULL,
     PRIMARY KEY (id, subgraph, entity)
 );
