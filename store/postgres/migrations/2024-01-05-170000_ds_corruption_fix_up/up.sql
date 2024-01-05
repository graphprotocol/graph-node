/*


This migration exists to fix up DB inconsistencies caused by a bug that was already fixed in this PR:
https://github.com/graphprotocol/graph-node/pull/5083

What it does is:
1. select latest_ethereum_block_number from subgraphs.subgraph_deployment where deployment = '$Qm..';
2. With that value, check if there are any data sources with a higher block:
   select count(*) from sgd*.data_sources$ where lower(block_range) > $latest_block;
3. If there are, then we need to intervene in that subgraph by removing all such data sources:
   delete from sgd*.data_sources$ where lower(block_range) > $latest_block;
4. It also unclamps any data sources that need unclamping (even though I don't think we ever clamp them).
*/

CREATE TEMPORARY TABLE temp_sgd_last_block (
    deployment_schema text,
    last_block_from_registry numeric,
    is_ok boolean,
    is_upper_limit_too_high boolean
);

-- collect the latest block number from SG deployment details
INSERT INTO temp_sgd_last_block (deployment_schema, last_block_from_registry)
SELECT d.name, latest_ethereum_block_number
  FROM subgraphs.subgraph_deployment AS sd
  JOIN deployment_schemas AS d ON sd.deployment = d.subgraph
 WHERE d.name IN (SELECT relnamespace::regnamespace::name
                    FROM pg_class
                   WHERE relname LIKE 'data_sources$%' AND relkind = 'r'
                 );

-- check if the block numbers in the tables are OK
-- if not, it can be
-- - either the lower bound is higher than the last block
-- - or the lower is fine, but the higher is past the last block
DO $do$
DECLARE
    s text;
BEGIN
    FOR s IN SELECT deployment_schema FROM temp_sgd_last_block
    LOOP
        EXECUTE format($$
        UPDATE temp_sgd_last_block
           SET is_ok = NOT EXISTS (SELECT 1
                                     FROM %1$I."data_sources$"
                                    WHERE lower(block_range) > last_block_from_registry
                                    LIMIT 1)
                       AND NOT EXISTS (SELECT 1
                                         FROM %1$I."data_sources$"
                                        WHERE upper(block_range) > last_block_from_registry 
                                          AND lower(block_range) <= last_block_from_registry
                                        LIMIT 1),
               is_upper_limit_too_high = EXISTS (SELECT 1
                                                   FROM %1$I."data_sources$"
                                                  WHERE upper(block_range) > last_block_from_registry 
                                                    AND lower(block_range) <= last_block_from_registry
                                                  LIMIT 1)
         WHERE deployment_schema = '%1$s'  
                $$, s);
    END LOOP;
END;
$do$;

SELECT * FROM temp_sgd_last_block WHERE NOT is_ok;

DO $do$
DECLARE
    schema text;
    last_block_from_registry integer;
    cnt bigint;
BEGIN
    FOR schema, last_block_from_registry IN SELECT deployment_schema, t.last_block_from_registry
                                           FROM temp_sgd_last_block AS t
                                          WHERE NOT is_ok AND NOT is_upper_limit_too_high
    LOOP
        EXECUTE format($$SELECT count(9) FROM %I."data_sources$"$$, schema) INTO cnt;
        RAISE NOTICE 'before DELETE % has % rows', schema, cnt;
        EXECUTE format($$DELETE FROM %I."data_sources$" WHERE lower(block_range) > %s $$, schema, last_block_from_registry);
        GET DIAGNOSTICS cnt = ROW_COUNT;
        RAISE NOTICE 'DELETEd % rows from %', cnt, schema;
    END LOOP;

    FOR schema, last_block_from_registry IN SELECT deployment_schema, t.last_block_from_registry 
                                           FROM temp_sgd_last_block AS t
                                          WHERE NOT is_ok AND is_upper_limit_too_high
    LOOP
        EXECUTE format($$UPDATE %I."data_sources$" 
                            SET block_range = range_merge(block_range, (%2$s,))
                          WHERE upper(block_range) > %2$s 
                                                    AND lower(block_range) <= %2$s $$, schema, last_block_from_registry);
        GET DIAGNOSTICS cnt = ROW_COUNT;
        RAISE NOTICE 'UPDATE affected % rows on %', cnt, schema;
    END LOOP;
END;
$do$;
