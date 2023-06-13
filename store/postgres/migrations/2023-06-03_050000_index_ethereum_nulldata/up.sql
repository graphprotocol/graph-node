CREATE INDEX ethereum_blocks_expr_idx ON ethereum_blocks USING BTREE ((data->'block'->'data')) where data->'block'->'data' = 'null'::jsonb;
