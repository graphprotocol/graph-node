--TRIGGERS
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

--STORED PROCEDURES
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

    INSERT INTO table_event_history
        (transaction_id, transaction_time, op_id)
    VALUES
        (txid_current(), statement_timestamp(), operation_id);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION log_update()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
BEGIN
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();

    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (e_id, OLD.id, OLD.data_source, OLD.entity, OLD.data, NEW.data);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION log_insert()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
BEGIN
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();

    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (e_id, NEW.id, NEW.data_source, NEW.entity, NULL, NEW.data);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION log_delete()
    RETURNS trigger AS
$$
DECLARE
    e_id INTEGER;
    b_hash VARCHAR;
BEGIN
    SELECT
        id INTO e_id
    FROM table_event_history
    WHERE
        transaction_id = txid_current();

    INSERT INTO row_history
        (event_id, entity_id, data_source, entity, data_before, data_after)
    VALUES
        (e_id, OLD.id, OLD.data_source, OLD.entity, OlD.data, NULL);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;