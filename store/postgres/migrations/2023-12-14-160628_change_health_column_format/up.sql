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

drop type subgraphs."health";
