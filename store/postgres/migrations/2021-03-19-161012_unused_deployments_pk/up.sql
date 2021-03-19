alter table unused_deployments
      add column new_id int
          references subgraphs.subgraph_deployment(id) on delete cascade;

create temp sequence unused_pk;

update unused_deployments
   set new_id = nextval('unused_pk');

alter table unused_deployments
      rename column id to deployment;

alter table unused_deployments
      rename column new_id to id;

alter table unused_deployments
      drop constraint unused_deployments_pkey;

alter table unused_deployments
      add primary key(id);
