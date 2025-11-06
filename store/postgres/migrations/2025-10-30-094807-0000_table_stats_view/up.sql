CREATE MATERIALIZED VIEW info.table_stats AS
WITH table_info AS (
    SELECT
        s.schemaname AS schema_name,
        sd.id        AS deployment_id,
        sd.subgraph  AS subgraph,
        s.tablename  AS table_name,
        c.reltuples  AS total_row_count,
        s.n_distinct AS n_distinct
    FROM pg_stats s
    JOIN pg_namespace n ON n.nspname = s.schemaname
    JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
    JOIN subgraphs.deployment sd ON sd.id::text = substring(s.schemaname, 4)
    WHERE
        s.attname   = 'id'
        AND s.schemaname LIKE 'sgd%'
        AND c.relname NOT IN ('poi2$', 'data_sources$')
)
SELECT
    schema_name,
    deployment_id AS deployment,
    subgraph,
    table_name,
    CASE
        WHEN n_distinct < 0 THEN (-n_distinct) * total_row_count
        ELSE n_distinct
    END::bigint AS entities,
    total_row_count::bigint AS versions,
    CASE
        WHEN total_row_count = 0 THEN 0::float8
        WHEN n_distinct < 0 THEN (-n_distinct)::float8
        ELSE n_distinct::numeric / total_row_count::numeric
    END AS ratio
FROM table_info
WITH NO DATA;
