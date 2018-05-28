CREATE TABLE IF NOT EXISTS entities (
     id INT NOT NULL,
     data_source VARCHAR,
     entity VARCHAR,
     data jsonb NOT NULL,
     PRIMARY KEY (id, data_source, entity)
 );
