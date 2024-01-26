
create type subgraphs."health"
    as enum ('failed', 'healthy', 'unhealthy');

alter table
    subgraphs.subgraph_deployment
add
    column health_new subgraphs.health;

update
    subgraphs.subgraph_deployment
set
    health_new = health::subgraphs.health;

alter table
    subgraphs.subgraph_deployment
drop column
    health;

alter table
    subgraphs.subgraph_deployment
rename column
    health_new to health;

alter table
    subgraphs.subgraph_deployment
alter column
    health
set
    not null;
