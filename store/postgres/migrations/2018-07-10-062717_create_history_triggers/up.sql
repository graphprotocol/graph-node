/**************************************************************
* CREATE TRIGGER FUNCTIONS
**************************************************************/

/**************************************************************
* LOG TRANSACTION
*
* Writes metadata of each transaction on the entities table
* Data is written to event_meta_data
* Called when before_transaction_trigger is fired
**************************************************************/
CREATE OR REPLACE FUNCTION log_transaction()
    RETURNS TRIGGER AS
$$
DECLARE
    operation_id SMALLINT;
BEGIN
    CASE TG_OP
        WHEN 'INSERT' THEN
            operation_id := 0;
        WHEN 'UPDATE' THEN
            operation_id := 1;
        WHEN 'DELETE' THEN
            operation_id := 2;
    END CASE;

    -- Insert postgres transaction info into event_meta_data
    INSERT INTO event_meta_data
        (db_transaction_id, db_transaction_time, op_id)
    VALUES
        (txid_current(), statement_timestamp(), operation_id);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* LOG UPDATE
*
* Writes row level metadata and before & after state of `data` to entity_history
* Called when after_update_trigger is fired.
**************************************************************/
CREATE OR REPLACE FUNCTION log_update()
    RETURNS trigger AS
$$
DECLARE
    event_id INTEGER;
    is_reversion BOOLEAN;
BEGIN
    -- Get corresponding event id
    SELECT
        id INTO event_id
    FROM event_meta_data
    WHERE
        db_transaction_id = txid_current();

    IF NEW.event_source IS NULL THEN
        is_reversion := TRUE;
    ELSE
        is_reversion := FALSE;

        UPDATE event_meta_data SET
            source = NEW.event_source
        WHERE db_transaction_id = txid_current();
    END IF;

    -- Log row metadata and changes
    INSERT INTO entity_history
        (event_id, entity_id, data_source, entity, data_before, data_after, reversion)
    VALUES
        (event_id, OLD.id, OLD.data_source, OLD.entity, OLD.data, NEW.data, is_reversion);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* LOG INSERT
*
* Writes out newly inserted entity to entity_history
* Called when after_insert_trigger is fired.
**************************************************************/
CREATE OR REPLACE FUNCTION log_insert()
    RETURNS trigger AS
$$
DECLARE
    event_id INTEGER;
    is_reversion BOOLEAN;
BEGIN
    -- Get corresponding event id
    SELECT
        id INTO event_id
    FROM event_meta_data
    WHERE
        db_transaction_id = txid_current();

    IF NEW.event_source IS NULL THEN
        is_reversion := TRUE;
    ELSE
        is_reversion := FALSE;

        UPDATE event_meta_data SET
            source = NEW.event_source
        WHERE db_transaction_id = txid_current();
    END IF;

    -- Log inserted row
    INSERT INTO entity_history
        (event_id, entity_id, data_source, entity, data_before, data_after, reversion)
    VALUES
        (event_id, NEW.id, NEW.data_source, NEW.entity, NULL, NEW.data, is_reversion);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* LOG DELETE
*
* Writes deleted entity to entity_history
* Called when after_delete_trigger is fired.
**************************************************************/
CREATE OR REPLACE FUNCTION log_delete()
    RETURNS trigger AS
$$
DECLARE
    event_id INTEGER;
BEGIN
    -- Get corresponding event id
    SELECT
        id INTO event_id
    FROM event_meta_data
    WHERE
        db_transaction_id = txid_current();

    -- Log content of deleted entity
    INSERT INTO entity_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (event_id, OLD.id, OLD.data_source, OLD.entity, OlD.data, NULL);
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
