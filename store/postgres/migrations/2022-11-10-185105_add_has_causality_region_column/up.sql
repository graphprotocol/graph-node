alter table subgraphs.subgraph_manifest add column entities_with_causality_region text[] not null default array[]::text[];
