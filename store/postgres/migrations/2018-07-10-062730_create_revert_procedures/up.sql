/**************************************************************
* CREATE REVERT FUNCTIONS
**************************************************************/

/**************************************************************
* REVERT ROW EVENT
*
* Revert a specific row level event
* Parameters: entity_history.id (primary key)
*             operation_id
**************************************************************/
CREATE OR REPLACE FUNCTION revert_row_event(input_entity_history_id INTEGER, input_operation_id INTEGER)
    RETURNS VOID AS
$$
DECLARE
    target_entity_id VARCHAR;
    target_data_source VARCHAR;
    target_entity VARCHAR;
    target_data_before JSONB;
BEGIN
    SELECT
        entity_id,
        data_source,
        entity,
        data_before
    INTO
        target_entity_id,
        target_data_source,
        target_entity,
        target_data_before
    FROM entity_history
    WHERE entity_history.id = input_entity_history_id;

    CASE
        -- INSERT case
        WHEN input_operation_id = 0 THEN
            -- Delete inserted row
            BEGIN
                EXECUTE
                    'DELETE FROM entities WHERE (
                        data_source = $1 AND
                        entity = $2 AND
                        id = $3)'
                USING target_data_source, target_entity, target_entity_id;

                -- Row was already updated
                EXCEPTION
                    WHEN no_data_found THEN
                        NULL;
            END;

        -- UPDATE or DELETE case
        WHEN input_operation_id IN (1,2) THEN
            -- Insert deleted row if not exists
            -- If row exists perform update
            BEGIN
                EXECUTE
                    'INSERT INTO entities (id, data_source, entity, data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (id, data_source, entity) DO UPDATE
                        SET data = $4'
                USING
                    target_entity_id,
                    target_data_source,
                    target_entity,
                    target_data_before;
            END;
    END CASE;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT TRANSACTION
*
* Get all row level events associated with a SQL transaction
* For each row level event call revert_row_event()
* Parameters: event_id
**************************************************************/
CREATE OR REPLACE FUNCTION revert_transaction(input_event_id INTEGER)
    RETURNS VOID AS
$$
DECLARE
    row RECORD;
BEGIN
    FOR row IN
        SELECT
            entity_history.id as id,
            event_meta_data.op_id as op_id
        FROM entity_history
        JOIN event_meta_data ON
            event_meta_data.id=entity_history.event_id
        WHERE event_meta_data.id = input_event_id
        ORDER BY entity_history.id DESC
        LOOP
            PERFORM revert_row_event(row.id, row.op_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT TRANSACTION GROUP
*
* Get all row level events associated with a set of SQL transactions
* For each row level event call revert_row_event()
* Parameters: array of event_id's
**************************************************************/
CREATE OR REPLACE FUNCTION revert_transaction_group(input_event_ids INTEGER[])
    RETURNS VOID AS
$$
DECLARE
    row RECORD;
BEGIN
    FOR row IN
        SELECT
            entity_history.id as id,
            event_meta_data.op_id as op_id
        FROM entity_history
        JOIN event_meta_data ON
            event_meta_data.id=entity_history.event_id
        WHERE event_meta_data.id = ANY(input_event_ids)
        ORDER BY entity_history.id DESC
        LOOP
            PERFORM revert_row_event(row.id, row.op_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT BLOCK
*
* Revert the row store events related to a particular block
* Parameters: block_hash
**************************************************************/
CREATE OR REPLACE FUNCTION revert_block(block_hash VARCHAR)
    RETURNS VOID AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;
