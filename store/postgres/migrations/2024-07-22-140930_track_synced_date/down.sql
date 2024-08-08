DROP VIEW info.subgraph_info;

ALTER TABLE subgraphs.subgraph_deployment ADD COLUMN synced BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE unused_deployments            ADD COLUMN synced BOOLEAN NOT NULL DEFAULT false;

UPDATE subgraphs.subgraph_deployment SET synced = synced_at IS NOT NULL;
UPDATE unused_deployments            SET synced = synced_at IS NOT NULL;

-- NB: We keep the default on unused_deployment, as it was there before.
ALTER TABLE subgraphs.subgraph_deployment ALTER COLUMN synced DROP DEFAULT;

ALTER TABLE subgraphs.subgraph_deployment DROP COLUMN synced_at;
ALTER TABLE unused_deployments            DROP COLUMN synced_at;

CREATE VIEW info.subgraph_info AS
SELECT ds.id AS schema_id,
    ds.name AS schema_name,
    ds.subgraph,
    ds.version,
    s.name,
        CASE
            WHEN s.pending_version = v.id THEN 'pending'::text
            WHEN s.current_version = v.id THEN 'current'::text
            ELSE 'unused'::text
        END AS status,
    d.failed,
    d.synced
   FROM deployment_schemas ds,
    subgraphs.subgraph_deployment d,
    subgraphs.subgraph_version v,
    subgraphs.subgraph s
  WHERE d.deployment = ds.subgraph::text AND v.deployment = d.deployment AND v.subgraph = s.id;
