create table subgraphs.head (
    id                    int4 primary key,
    entity_count          int8 not null,
    block_number          int4,
    block_hash            bytea,
    firehose_cursor       text
);

create table subgraphs.deployment (
    id                        int4 primary key,

    subgraph                text unique not null,

    earliest_block_number     int4 default 0 not null,

    health                    text not null,
    failed                    boolean not null,
    fatal_error               text,
    non_fatal_errors          text[] default '{}'::text[],

    graft_base                text,
    graft_block_hash          bytea,
    graft_block_number        int4,

    reorg_count               int4 default 0 not null,
    current_reorg_depth       int4 default 0 not null,
    max_reorg_depth           int4 default 0 not null,

    last_healthy_block_hash   bytea,
    last_healthy_block_number int4,

    debug_fork                text,

    synced_at                 timestamptz,
    synced_at_block_number    int4,

    constraint deployment_health_new_check
      check ((health = any (array['failed', 'healthy', 'unhealthy']))),
    constraint deployment_id
      foreign key (id) references subgraphs.head(id) on delete cascade
);

insert into subgraphs.head
  (id, block_hash, block_number, entity_count, firehose_cursor)
select id, latest_ethereum_block_hash,
       latest_ethereum_block_number, entity_count, firehose_cursor
  from subgraphs.subgraph_deployment;

insert into subgraphs.deployment
  (id, subgraph, failed, graft_base, graft_block_hash, graft_block_number,
   fatal_error, non_fatal_errors, reorg_count, current_reorg_depth,
   max_reorg_depth,
   last_healthy_block_hash, last_healthy_block_number,
   debug_fork, earliest_block_number,
   health,
   synced_at, synced_at_block_number)
select
   id, deployment, failed, graft_base, graft_block_hash, graft_block_number,
   fatal_error, non_fatal_errors, reorg_count, current_reorg_depth,
   max_reorg_depth,
   last_healthy_ethereum_block_hash, last_healthy_ethereum_block_number,
   debug_fork, earliest_block_number,
   health,
   synced_at, synced_at_block_number
from subgraphs.subgraph_deployment;

-- Support joining with subgraph_error
create index deployment_fatal_error
    on subgraphs.deployment(fatal_error);

alter table subgraphs.copy_state
 drop constraint copy_state_dst_fkey,
  add constraint copy_state_dst_fkey
      foreign key (dst) references subgraphs.deployment(id) on delete cascade;

alter table subgraphs.subgraph_error
 drop constraint subgraph_error_subgraph_id_fkey,
  add constraint subgraph_error_subgraph_id_fkey
      foreign key (subgraph_id) references
      subgraphs.deployment(subgraph) on delete cascade;

alter table subgraphs.subgraph_manifest
 drop constraint subgraph_manifest_new_id_fkey,
  add constraint subgraph_manifest_id_fkey
      foreign key (id) references subgraphs.deployment(id) on delete cascade;

alter table subgraphs.table_stats
 drop constraint table_stats_deployment_fkey,
  add constraint table_stats_deployment_fkey
      foreign key (deployment) references subgraphs.deployment(id)
      on delete cascade;

drop view info.subgraph_info;

drop table subgraphs.subgraph_deployment;

create view info.subgraph_info as
select ds.id as schema_id,
       ds.name as schema_name,
       ds.subgraph,
       ds.version,
       s.name,
        CASE
            WHEN s.pending_version = v.id THEN 'pending'
            WHEN s.current_version = v.id THEN 'current'
            ELSE 'unused'
        END AS status,
       d.failed,
       d.synced_at
  from deployment_schemas ds,
       subgraphs.deployment d,
       subgraphs.subgraph_version v,
       subgraphs.subgraph s
 where d.id = ds.id
   and v.deployment = d.subgraph
   and v.subgraph = s.id;
