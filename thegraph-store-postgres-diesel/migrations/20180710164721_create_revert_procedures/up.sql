CREATE OR REPLACE FUNCTION revert_transaction(event_id INTEGER)
    RETURNS Integer AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION revert_transactions(start INTEGER, end INTEGER)
    RETURNS INTEGER AS
$$
BEGIN
    SELECT start-1;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION revert_blocks(start VARCHAR, end VARCHAR)
    RETURNS VARCHAR AS
$$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;

