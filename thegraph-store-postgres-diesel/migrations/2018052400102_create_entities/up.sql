--SEQUENCES
DROP SEQUENCE IF EXISTS id_seq;
CREATE SEQUENCE id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

--TABLES
CREATE TABLE IF NOT EXISTS entities (
     id VARCHAR NOT NULL,
     data_source VARCHAR NOT NULL,
     entity VARCHAR NOT NULL,
     data jsonb NOT NULL,
     history_id INTEGER DEFAULT NEXTVAL('id_seq') UNIQUE NOT NULL,
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
     history_id INTEGER,
     data_before JSONB,
     data_after JSONB
 );