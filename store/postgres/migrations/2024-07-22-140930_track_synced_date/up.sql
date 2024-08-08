DROP VIEW info.subgraph_info;

ALTER TABLE subgraphs.subgraph_deployment ADD COLUMN synced_at TIMESTAMPTZ;
ALTER TABLE unused_deployments            ADD COLUMN synced_at TIMESTAMPTZ;

UPDATE subgraphs.subgraph_deployment SET synced_at = '1970-01-01 00:00:00 UTC' WHERE synced;
UPDATE unused_deployments            SET synced_at = '1970-01-01 00:00:00 UTC' WHERE synced;

ALTER TABLE subgraphs.subgraph_deployment DROP COLUMN synced;
ALTER TABLE unused_deployments            DROP COLUMN synced;

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
    d.synced_at
   FROM deployment_schemas ds,
    subgraphs.subgraph_deployment d,
    subgraphs.subgraph_version v,
    subgraphs.subgraph s
  WHERE d.deployment = ds.subgraph::text AND v.deployment = d.deployment AND v.subgraph = s.id;
