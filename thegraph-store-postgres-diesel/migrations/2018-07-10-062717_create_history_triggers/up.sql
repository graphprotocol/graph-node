/**************************************************************
* CREATE TRIGGER FUNCTIONS
**************************************************************/

-- Writes metadata of each transaction on the entities table
-- data written to table_event_history table */
CREATE OR REPLACE FUNCTION log_transaction()
    RETURNS TRIGGER AS
$$
DECLARE
    operation_id SMALLINT;
BEGIN
    CASE TG_OP
        WHEN 'INSERT' THEN operation_id := 0;
        WHEN 'UPDATE' THEN operation_id := 1;
        WHEN 'DELETE' THEN operation_id := 2;
    END CASE;

    -- Insert postgres transaction info into table_event_history
    INSERT INTO table_event_history
        (transaction_id, transaction_time, op_id)
    VALUES
        (txid_current(), statement_timestamp(), operation_id);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Writes row level metadata and before & after state of `data` to row_history
-- Called when after_update_trigger is fired.
CREATE OR REPLACE FUNCTION log_update()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
    is_reversion BOOLEAN;
BEGIN
    -- Get corresponding table event id
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();
    is_reversion := FALSE;
    -- Log row metadata and changes
    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after, reversion)
    VALUES
        (e_id, OLD.id, OLD.data_source, OLD.entity, OLD.data, NEW.data, is_reversion);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Writes out newly inserted row to row_history
-- Called when after_update_trigger is fired.
CREATE OR REPLACE FUNCTION log_insert()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
BEGIN
    -- Get corresponding table event id
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();

    -- Log inserted row
    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (e_id, NEW.id, NEW.data_source, NEW.entity, NULL, NEW.data);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Writes deleted row to row_history
-- Called when after_update_trigger is fired.
CREATE OR REPLACE FUNCTION log_delete()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
BEGIN
    -- Get corresponding table event id
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();

    -- Log content of deleted row
    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (e_id, OLD.id, OLD.data_source, OLD.entity, OlD.data, NULL);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* CREATE TRIGGERS
**************************************************************/
CREATE TRIGGER before_transaction_trigger
    BEFORE INSERT OR UPDATE OR DELETE
    ON entities
    FOR EACH STATEMENT
    EXECUTE PROCEDURE log_transaction();

CREATE TRIGGER after_insert_trigger
    AFTER INSERT
    ON entities
    FOR EACH ROW
    EXECUTE PROCEDURE log_insert();

CREATE TRIGGER after_update_trigger
    AFTER UPDATE
    ON entities
    FOR EACH ROW
    EXECUTE PROCEDURE log_update();

CREATE TRIGGER after_delete_trigger
    AFTER DELETE
    ON entities
    FOR EACH ROW
    EXECUTE PROCEDURE log_delete();
