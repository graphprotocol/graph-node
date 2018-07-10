--TABLES
CREATE TABLE IF NOT EXISTS entities (
     id VARCHAR NOT NULL,
     data_source VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data jsonb NOT NULL,
     PRIMARY KEY (id, data_source, entity)
 );

CREATE TABLE IF NOT EXISTS table_event_history (
    id SERIAL PRIMARY KEY,
    transaction_id BIGINT,
    transaction_time TIMESTAMP,
    op_id SMALLINT
);

CREATE TABLE IF NOT EXISTS row_history (
     id SERIAL PRIMARY KEY,
     event_id BIGINT,
     entity_id VARCHAR NOT NULL,
     data_source VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data_before JSONB,
     data_after JSONB
 );