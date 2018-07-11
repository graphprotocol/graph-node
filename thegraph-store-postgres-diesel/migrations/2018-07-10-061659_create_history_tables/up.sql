/**************************************************************
* CREATE TABLES
**************************************************************/
-- Stores the metadata from each sql transaction
CREATE TABLE IF NOT EXISTS table_event_history (
    id SERIAL PRIMARY KEY,
    transaction_id BIGINT,
    transaction_time TIMESTAMP,
    op_id SMALLINT
);

-- Stores the row level data and changes (1 or more per transaction)
CREATE TABLE IF NOT EXISTS row_history (
     id SERIAL PRIMARY KEY,
     event_id BIGINT NOT NULL,
     entity_id VARCHAR NOT NULL,
     data_source VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data_before JSONB,
     data_after JSONB,
     reversion BOOLEAN NOT NULL DEFAULT FALSE
 );

 /**************************************************************
 * ADD FOREIGN KEYS
 **************************************************************/
 -- Define relationship between table_event_history and row_history
ALTER TABLE row_history
    ADD CONSTRAINT row_history_table_event_id_fk
    FOREIGN KEY (event_id)
    REFERENCES table_event_history(id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;
