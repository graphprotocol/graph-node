ALTER TABLE subgraphs.subgraph_manifest
    DROP CONSTRAINT graph_node_versions_fk,
    DROP COLUMN graph_node_version_id;

DROP TABLE IF EXISTS subgraphs.graph_node_versions;
