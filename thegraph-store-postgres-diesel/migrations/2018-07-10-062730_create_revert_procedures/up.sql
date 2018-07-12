/**************************************************************
* CREATE REVERT FUNCTIONS
**************************************************************/

/**************************************************************
* REVERT ROW EVENT
*
* Revert a specific row level event
* Parameters: row_history pkey (id) and the operation type
**************************************************************/
CREATE OR REPLACE FUNCTION revert_row_event(input_row_history_id INTEGER, input_operation_id INTEGER)
    RETURNS VOID AS
$$
DECLARE
    target_entity_id VARCHAR;
    target_data_source VARCHAR;
    target_entity VARCHAR;
    target_data_before JSONB;
    target_data_after JSONB;
BEGIN
    SELECT
        rh.entity_id,
        rh.data_source,
        rh.entity,
        rh.data_before,
        rh.data_after
      INTO
        target_entity_id,
        target_data_source,
        target_entity,
        target_data_before,
        target_data_after
    FROM row_history rh
    WHERE rh.id = input_row_history_id;

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

      -- UPDATE case
      WHEN input_operation_id = 1 THEN
        -- Update row to previous state
        BEGIN
          EXECUTE
            'UPDATE entities SET DATA = $1 WHERE (
              data_source = $2 AND
              entity = $3 AND
              id = $4)'
          USING target_data_before, target_data_source, target_entity, target_entity_id;

          -- error
          EXCEPTION
            WHEN others THEN
              RAISE NOTICE 'Could not revert UPDATE';
        END;

      -- DELETE case
      WHEN input_operation_id = 2 THEN
       -- Insert deleted row
        BEGIN
          EXECUTE
            'INSERT INTO entities (id, data_source, entity, data)
              VALUES ($1, $2, $3, $4)'
          USING
            target_entity_id,
            target_data_source,
            target_entity,
            target_data_before;

          -- row is already inserted
          EXCEPTION
            WHEN no_data_found THEN
              NULL;
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
        rh.id as id,
        teh.op_id as op_id
      FROM row_history rh
      JOIN table_event_history teh ON teh.id=rh.event_id
      WHERE teh.id = input_event_id
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
        rh.id as id,
        teh.op_id as op_id
      FROM row_history rh
      JOIN table_event_history teh ON teh.id=rh.event_id
      WHERE teh.id = ANY(input_event_ids)
    LOOP
    PERFORM revert_row_event(row.id, row.op_id);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

/**************************************************************
* REVERT BLOCK
*
* Revert to the row store events related to a particular block
* Parameters: block_hash
**************************************************************/
CREATE OR REPLACE FUNCTION revert_block(block_hash VARCHAR)
    RETURNS VOID AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;
