ALTER TABLE subgraphs.subgraph_deployment DROP COLUMN synced_at_block_number;
ALTER TABLE unused_deployments            DROP COLUMN synced_at_block_number;
