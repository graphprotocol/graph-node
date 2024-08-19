ALTER TABLE subgraphs.subgraph_deployment ADD COLUMN synced_at_block_number NUMERIC;
ALTER TABLE unused_deployments            ADD COLUMN synced_at_block_number INTEGER;
