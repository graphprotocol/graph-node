--
-- Views that are useful in understanding details about the data graph-node
-- stores. These views are _only_ for interactive use. The graph-node code
-- must never access these views
--
drop schema if exists meta cascade;
create schema meta;

create view meta.subgraph_info as
select
    ds.id as schema_id,
    ds.name as schema_name,
    ds.subgraph as subgraph,
    ds.version as version,
    s.name,
    (case
        when s.pending_version = v.id then 'pending'
        when s.current_version = v.id then 'current'
        else 'unused' end) status,
    d.failed,
    d.synced
from
    deployment_schemas ds,
    subgraphs.subgraph_deployment d,
    subgraphs.subgraph_version v,
    subgraphs.subgraph s
where   d.id = ds.subgraph
    and v.deployment = d.id
    and v.subgraph = s.id;

-- Size of tables
-- from https://wiki.postgresql.org/wiki/Disk_Usage
create materialized view meta.table_sizes as
select *,
       pg_size_pretty(total_bytes) as total,
       pg_size_pretty(index_bytes) as index,
       pg_size_pretty(toast_bytes) as toast,
       pg_size_pretty(table_bytes) as table
  from (
    select *,
           total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes
      from (
        select nspname as table_schema, relname as table_name,
               'shared'::text as version,
               c.reltuples as row_estimate,
               pg_total_relation_size(c.oid) as total_bytes,
               pg_indexes_size(c.oid) as index_bytes,
               pg_total_relation_size(reltoastrelid) as toast_bytes
          from pg_class c
               join pg_namespace n on n.oid = c.relnamespace
          where relkind = 'r'
            and (nspname in ('public', 'subgraphs'))
  ) a
) a;

create materialized view meta.subgraph_sizes as
select *,
       pg_size_pretty(total_bytes) as total,
       pg_size_pretty(index_bytes) as index,
       pg_size_pretty(toast_bytes) as toast,
       pg_size_pretty(table_bytes) as table
  from (
    select *,
           total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes
      from (
        select nspname as name,
               ds.subgraph as subgraph,
               (ds."version")::text as version,
               sum(c.reltuples) as row_estimate,
               sum(pg_total_relation_size(c.oid)) as total_bytes,
               sum(pg_indexes_size(c.oid)) as index_bytes,
               sum(pg_total_relation_size(reltoastrelid)) as toast_bytes
          from pg_class c
               join pg_namespace n on n.oid = c.relnamespace
               join deployment_schemas ds on ds."name" = n.nspname
          where relkind = 'r'
            and nspname like 'sgd%'
          group by nspname, subgraph, version
  ) a
) a;

create view meta.all_sizes as
select * from meta.subgraph_sizes
union all
select * from meta.table_sizes;

-- Currently active queries
create view meta.activity as
select coalesce(nullif(application_name,''), 'unknown') as application_name,
       pid,
       extract(epoch from age(now(), query_start)) as query_age,
       extract(epoch from age(now(), xact_start)) as txn_age,
       query
  from pg_stat_activity
 where state='active'
order by query_start desc;

create view meta.wraparound as
select oid::regclass::text AS table,
       least(
         (select setting::int
            from pg_settings
            where name = 'autovacuum_freeze_max_age') - age(relfrozenxid),
         (select setting::int
            from pg_settings
           where name = 'autovacuum_multixact_freeze_max_age')
                - mxid_age(relminmxid)) as tx_before_wraparound_vacuum,
       pg_size_pretty(pg_total_relation_size(oid)) AS size,
       pg_stat_get_last_autovacuum_time(oid) AS last_autovacuum,
       age(relfrozenxid) AS xid_age,
       mxid_age(relminmxid) AS mxid_age
  from pg_class
 where relfrozenxid != 0
   and oid > 16384
   and relkind='r'
 order by tx_before_wraparound_vacuum;
