alter table subgraphs.subgraph_deployment add column has_causality_region text[] not null default array[]::text[];
