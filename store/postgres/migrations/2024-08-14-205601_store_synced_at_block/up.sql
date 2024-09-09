ALTER TABLE subgraphs.subgraph_deployment ADD COLUMN synced_at_block_number INT4;
ALTER TABLE unused_deployments            ADD COLUMN synced_at_block_number INT4;
