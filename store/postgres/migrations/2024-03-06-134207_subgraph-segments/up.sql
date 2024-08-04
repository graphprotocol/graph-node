create table subgraphs.subgraph_segments(
  id            serial primary key,
  deployment    int4 references subgraphs.subgraph_deployment(id) on delete cascade,
  start_block   int4 not null,
  stop_block     int4 not null,
  current_block int4 null
);
