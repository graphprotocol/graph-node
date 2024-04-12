alter table
    subgraphs.subgraph_deployment
add
    column if not exists health_new text not null default 'failed'
        check (health_new in ('failed', 'healthy', 'unhealthy'));

update
    subgraphs.subgraph_deployment
set
    health_new = health;

alter table
    subgraphs.subgraph_deployment
alter column
    health_new drop default;

alter table
    subgraphs.subgraph_deployment
drop column
    health;

alter table
    subgraphs.subgraph_deployment
rename column
    health_new to health;

-- Drop imported subgraph_deployment tables from other shards so that we
-- can drop our local 'health' type
-- graph-node startup will recreate the foreign table import
do $$
  declare r record;
  begin
    for r in select foreign_table_schema as nsp
               from information_schema.foreign_tables
              where foreign_table_schema like 'shard_%'
                and foreign_table_name = 'subgraph_deployment'
    loop
      execute 'drop foreign table ' || r.nsp || '.subgraph_deployment';
    end loop;
  end
$$;

drop type subgraphs."health";
