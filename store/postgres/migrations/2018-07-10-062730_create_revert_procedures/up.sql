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
* RERUN ROW EVENT
*
* Rerun a specific row level event
* Parameters: entity_history pkey (id) and operation type
**************************************************************/
CREATE OR REPLACE FUNCTION rerun_row_event(input_entity_history_id INTEGER, input_operation_id INTEGER)
    RETURNS VOID AS
$$
DECLARE
    target_entity_id VARCHAR;
    target_data_source VARCHAR;
    target_entity VARCHAR;
    target_data_after JSONB;
BEGIN
    SELECT
        entity_id,
        data_source,
        entity,
        data_before,
        data_after
    INTO
        target_entity_id,
        target_data_source,
        target_entity,
        target_data_after
    FROM entity_history
    WHERE entity_history.id = input_entity_history_id;

    CASE
        -- INSERT or UPDATE case
        WHEN input_operation_id IN (0,1) THEN
            -- Re insert row
            -- If row exists perform update
            BEGIN
                EXECUTE
                    'INSERT INTO entities (data_source, entity, id, data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (data_source, entity, id) DO UPDATE
                        SET data = $4'
                USING
                    target_data_source,
                    target_entity,
                    target_entity_id,
                    target_data_after;
            END;

        -- DELETE case
        WHEN input_operation_id = 2 THEN
            -- Insert deleted row if not exists
            -- If row exists perform update
            BEGIN
                EXECUTE
                    'DELETE FROM entities WHERE (
                        data_source = $1 AND
                        entity = $2 AND
                        id = $3)'
                USING
                    target_data_source,
                    target_entity;
                    target_entity_id,
            END;
    END CASE;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* RERUN ENTITY
*
* Rerun all events for a specific entity
* avoiding any revert or uncled events
* Parameters: entity pkey -> (entity_id, data_source, entity)
              event_id of revert event
**************************************************************/
CREATE OR REPLACE FUNCTION rerun_entity(
    input_event_id INTEGER, input_data_source VARCHAR, input_entity VARCHAR, input_entity_id VARCHAR)
    RETURNS VOID AS
$$
BEGIN
     FOR row IN
        SELECT
            entity_history.id as id,
            event_meta_data.op_id as op_id
        FROM entity_history
        WHERE (
            entity_history.id = input_entity_id AND
            entity_history.event_id = input_event_id AND
            entity_history.data_source = input_data_source
            AND
            entity_history.event_id > input_event_id
            AND
            entity_history.reversion = FALSE )
        ORDER BY entity_history.id ASC
    LOOP
        PERFORM rerun_row_event(row.id, row.op_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT BLOCK
*
* Revert the row store events related to a particular block
* Rerun all of an entities changes that come after the row store events related to that block
* Parameters: block_hash
**************************************************************/
CREATE OR REPLACE FUNCTION revert_block(input_block_hash VARCHAR)
    RETURNS VOID AS
$$
DECLARE
    event_row RECORD;
    entity_row RECORD;
BEGIN
    FOR event_row IN
        SELECT
            entity_history.event_id as event_id,
            entity_history.data_source as data_source,
            entity_history.entity as entity,
            entity_history.entity_id as entity_id
        FROM entity_history
        JOIN event_meta_data ON
            entity_history.event_id = event_meta_data.id
        WHERE event_meta_data.block_hash = input_block_hash
        GROUP BY
            entity_history.event_id
        ORDER BY entity_history.event_id DESC
    LOOP
        PERFORM revert_transaction(row.event_id);
    END LOOP;

    FOR entity_row IN
        SELECT
            MIN(entity_history.event_id) as event_id,
            entity_history.data_source as data_source,
            entity_history.entity as entity,
            entity_history.entity_id as entity_id
        FROM entity_history
        JOIN event_meta_data ON
            entity_history.event_id = event_meta_data.id
        WHERE event_meta_data.block_hash = input_block_hash
        GROUP BY
            entity_history.data_source,
            entity_history.entity,
            entity_history.entity_id
    LOOP
        PERFORM rerun_entity(row.event_id, row.data_source, row.entity, row.entity_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT BLOCK GROUP
*
* Revert the row store events related to a set of blocks
* for each block in the set run the revert block function
* Parameters: array of block_hash's
**************************************************************/
CREATE OR REPLACE FUNCTION revert_block_group(input_block_hash_group VARCHAR[])
    RETURNS VOID AS
$$
DECLARE
    block_row RECORD;
    event_row RECORD;
    entity_row RECORD;
BEGIN
    FOR block_row IN
        SELECT
            block_hash
        FROM event_meta_data
        WHERE block_hash = ANY(input_block_hash_group)
        GROUP BY block_hash
        ORDER BY id DESC
    LOOP
        FOR event_row IN
            SELECT
                entity_history.event_id as event_id,
                entity_history.data_source as data_source,
                entity_history.entity as entity,
                entity_history.entity_id as entity_id
            FROM entity_history
            JOIN event_meta_data ON
                entity_history.event_id = event_meta_data.id
            WHERE event_meta_data.block_hash = block_row.block_hash
            GROUP BY
                entity_history.event_id
            ORDER BY entity_history.event_id DESC
        LOOP
            PERFORM revert_transaction(row.event_id);
        END LOOP;
    END LOOP;

    FOR entity_row IN
        SELECT
            MIN(entity_history.event_id) as event_id,
            entity_history.data_source as data_source,
            entity_history.entity as entity,
            entity_history.entity_id as entity_id
        FROM entity_history
        JOIN event_meta_data ON
            entity_history.event_id = event_meta_data.id
        WHERE event_meta_data.block_hash = ANY(input_block_hash_group)
        GROUP BY
            entity_history.data_source,
            entity_history.entity,
            entity_history.entity_id
    LOOP
        PERFORM rerun_entity(row.event_id, row.data_source, row.entity, row.entity_id);
    END LOOP;
END;
$$ LANGUAGE plpgsql;