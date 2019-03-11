-- We put the schema back, but there is no getting the data that we deleted
-- back and it is therefore much better to undo this migration by restoring
-- from a backup.
ALTER TABLE entity_history
  ADD COLUMN data_after jsonb;

CREATE OR REPLACE FUNCTION log_entity_event()
    RETURNS trigger AS
$$
DECLARE
    event_id INTEGER;
    new_event_id INTEGER;
    is_reversion BOOLEAN := FALSE;
    operation_type INTEGER := 10;
    event_source  VARCHAR;
    subgraph VARCHAR;
    entity VARCHAR;
    entity_id VARCHAR;
    data_before JSONB;
    data_after JSONB;
BEGIN
    -- Get operation type and source
    IF (TG_OP = 'INSERT') THEN
        operation_type := 0;
        event_source := NEW.event_source;
        subgraph := NEW.subgraph;
        entity := NEW.entity;
        entity_id := NEW.id;
        data_before := NULL;
        data_after := NEW.data;
    ELSIF (TG_OP = 'UPDATE') THEN
        operation_type := 1;
        event_source := NEW.event_source;
        subgraph := OLD.subgraph;
        entity := OLD.entity;
        entity_id := OLD.id;
        data_before := OLD.data;
        data_after := NEW.data;
    ELSIF (TG_OP = 'DELETE') THEN
        operation_type := 2;
        event_source := current_setting('vars.current_event_source', TRUE);
        subgraph := OLD.subgraph;
        entity := OLD.entity;
        entity_id := OLD.id;
        data_before := OLD.data;
        data_after := NULL;
    ELSE
        RAISE EXCEPTION 'unexpected entity row operation type, %', TG_OP;
    END IF;

    IF event_source = 'REVERSION' THEN
        is_reversion := TRUE;
    END IF;

    SELECT id INTO event_id
    FROM event_meta_data
    WHERE db_transaction_id = txid_current();

    new_event_id := null;

    IF event_id IS NULL THEN
        -- Log information on the postgres transaction for later use in revert operations
        INSERT INTO event_meta_data
            (db_transaction_id, db_transaction_time, source)
        VALUES
            (txid_current(), statement_timestamp(), event_source)
        RETURNING event_meta_data.id INTO new_event_id;
    END IF;

    -- Log row metadata and changes, specify whether event was an original ethereum event or a reversion
    INSERT INTO entity_history
        (event_id, entity_id, subgraph, entity, data_before, data_after, reversion, op_id)
    VALUES
        (COALESCE(new_event_id, event_id), entity_id, subgraph, entity, data_before, data_after, is_reversion, operation_type);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
