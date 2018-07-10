CREATE OR REPLACE FUNCTION revert_row_event(INTEGER, INTEGER)
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
    WHERE rh.id = $1;

    CASE
    -- INSERT case
    WHEN $2 = 0 THEN
     -- delete inserted row
      BEGIN
        EXECUTE
          'DELETE FROM entities WHERE (
            data_source = $1 AND
            entity = $2 AND
            id = $3)'
        USING target_data_source, target_entity, target_entity_id;

        -- row is already deleted
        EXCEPTION
          WHEN no_data_found THEN
            NULL;
      END;

      -- UPDATE case
      WHEN $2 = 1 THEN
        -- delete inserted row
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
      WHEN $2 = 2 THEN
       -- delete inserted row
        BEGIN
          EXECUTE
            'INSERT INTO entities (id, data_source, entity, data)
              VALUES ($1, $2, $3, $4)'
          USING
            target_entity_id,
            target_data_source,
            target_entity,
            target_data_before;

          -- row is already deleted
          EXCEPTION
            WHEN no_data_found THEN
              NULL;
        END;
    END CASE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION revert_transaction(event_id INTEGER)
    RETURNS VOID AS
$$
DECLARE
    row_id INTEGER;
    operation_id INTEGER;
    row RECORD;
BEGIN
  FOR row IN
      SELECT
        rh.id as id,
        teh.op_id as op_id
      FROM row_history rh
      JOIN table_event_history teh ON teh.id=rh.event_id
      WHERE teh.id = $1
    LOOP
    PERFORM revert_row_event(row.id, row.op_id);
  END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION revert_transactions(INTEGER, INTEGER)
    RETURNS VOID AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION revert_blocks(VARCHAR, VARCHAR)
    RETURNS VOID AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;
