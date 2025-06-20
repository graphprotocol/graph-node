create table subgraphs.subgraph_deployment (
    id int4 primary key,

    deployment text unique not null,

    latest_ethereum_block_hash bytea,
    latest_ethereum_block_number numeric,
    entity_count numeric NOT NULL,
    firehose_cursor text,

    earliest_block_number integer DEFAULT 0 NOT NULL,

    graft_base text,
    graft_block_hash bytea,
    graft_block_number numeric,

    health text NOT NULL,
    failed boolean NOT NULL,
    fatal_error text,
    non_fatal_errors text[] DEFAULT '{}'::text[],

    reorg_count integer DEFAULT 0 NOT NULL,
    current_reorg_depth integer DEFAULT 0 NOT NULL,
    max_reorg_depth integer DEFAULT 0 NOT NULL,

    last_healthy_ethereum_block_hash bytea,
    last_healthy_ethereum_block_number numeric,

    debug_fork text,

    synced_at timestamp with time zone,
    synced_at_block_number integer,

    constraint subgraph_deployment_health_new_check
      check ((health = any (array['failed', 'healthy', 'unhealthy'])))
);

insert into subgraphs.subgraph_deployment
 (id, deployment,
  latest_ethereum_block_hash, latest_ethereum_block_number,
  entity_count, firehose_cursor,
  earliest_block_number,
  graft_base, graft_block_hash, graft_block_number,
  health, failed, fatal_error, non_fatal_errors,
  reorg_count, current_reorg_depth, max_reorg_depth,
  last_healthy_ethereum_block_hash, last_healthy_ethereum_block_number,
  debug_fork,
  synced_at, synced_at_block_number)
select h.id, d.subgraph,
  h.block_hash, h.block_number,
  h.entity_count, h.firehose_cursor,
  earliest_block_number,
  graft_base, graft_block_hash, graft_block_number,
  health, failed, fatal_error, non_fatal_errors,
  reorg_count, current_reorg_depth, max_reorg_depth,
  last_healthy_block_hash, last_healthy_block_number,
  debug_fork,
  synced_at, synced_at_block_number
  from subgraphs.head h, subgraphs.deployment d
 where h.id = d.id;

alter table subgraphs.copy_state
 drop constraint copy_state_dst_fkey,
  add constraint copy_state_dst_fkey
      foreign key (dst) references
      subgraphs.subgraph_deployment(id) on delete cascade;

alter table subgraphs.subgraph_error
 drop constraint subgraph_error_subgraph_id_fkey,
  add constraint subgraph_error_subgraph_id_fkey
      foreign key (subgraph_id) references
      subgraphs.subgraph_deployment(deployment) on delete cascade;

alter table subgraphs.subgraph_manifest
 drop constraint subgraph_manifest_id_fkey,
  add constraint subgraph_manifest_new_id_fkey
      foreign key (id) references
      subgraphs.subgraph_deployment(id) on delete cascade;

alter table subgraphs.table_stats
 drop constraint table_stats_deployment_fkey,
  add constraint table_stats_deployment_fkey
      foreign key (deployment) references
      subgraphs.subgraph_deployment(id) on delete cascade;

drop view info.subgraph_info;

create view info.subgraph_info as
select ds.id AS schema_id,
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
  from deployment_schemas ds,
       subgraphs.subgraph_deployment d,
       subgraphs.subgraph_version v,
       subgraphs.subgraph s
  where d.id = ds.id
    and v.deployment = d.deployment
    and v.subgraph = s.id;

drop table subgraphs.deployment;
drop table subgraphs.head;
