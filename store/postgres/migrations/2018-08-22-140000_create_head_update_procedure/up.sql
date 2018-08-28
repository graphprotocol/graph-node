/**************************************************************
* CREATE BLOCK INGESTOR FUNCTIONS
**************************************************************/

/**************************************************************
* Attempt to update the head block pointer.
* Raises an exception if ethereum_blocks table is empty.
**************************************************************/
CREATE OR REPLACE FUNCTION attempt_head_update(net_name VARCHAR, ancestor_count BIGINT)
    RETURNS VARCHAR[] AS
$$
DECLARE
    current_head_number BIGINT;
    new_head_hash VARCHAR;
    new_head_number BIGINT;
    missing_parents VARCHAR[];
BEGIN
    -- Get block number of current head block
    SELECT head_block_number
    INTO STRICT current_head_number
    FROM ethereum_networks
    WHERE name = net_name;

    -- Find candidate new head block
    SELECT
        hash,
        number
    INTO STRICT
        new_head_hash,
        new_head_number
    FROM ethereum_blocks
    WHERE network_name = net_name
    ORDER BY
        number DESC,
        hash ASC
    LIMIT 1;

    -- Stop now if it's no better than the current head block
    IF new_head_number <= current_head_number THEN
        RETURN ARRAY[]::VARCHAR[];
    END IF;

    -- Aggregate list of missing parent hashes into missing_parents,
    -- selecting only parents of blocks within ancestor_count of new head
    SELECT array_agg(block1.parent_hash)
    INTO STRICT missing_parents
    FROM ethereum_blocks AS block1
    LEFT OUTER JOIN ethereum_blocks AS block2
    ON block1.parent_hash = block2.hash
    WHERE
        block1.network_name = net_name
        AND block2.hash IS NULL -- cases where no block2 was found
        AND block1.number > (new_head_number - ancestor_count);

    -- Stop now if there are any recent blocks with missing parents
    IF array_length(missing_parents, 1) > 0 THEN
        RETURN missing_parents;
    END IF;

    -- No recent missing parent blocks, therefore candidate new head block has
    -- the necessary minimum number of ancestors present in DB.

    -- Set head block pointer to candidate head block
    UPDATE ethereum_networks
    SET
        head_block_hash = new_head_hash,
        head_block_number = new_head_number
    WHERE name = net_name;

    -- Fire head block update event
    NOTIFY head_block_update;

    -- Done
    RETURN ARRAY[]::VARCHAR[];
END;
$$ LANGUAGE plpgsql;
