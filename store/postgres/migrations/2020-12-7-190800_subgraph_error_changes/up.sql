alter table
    subgraphs.subgraph_error
drop
    column block_number;

alter table
    subgraphs.subgraph_error
add
    column created_at timestamp NOT NULL DEFAULT (now() at time zone 'utc');
