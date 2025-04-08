create table subgraphs.prune_state(
  -- diesel can't deal with composite primary keys
  vid            int primary key
                     generated always as identity,

  -- id of the deployment
  id             int not null,
  -- how many times the deployment has been pruned
  run            int not null,

  -- from PruneRequest
  first_block    int not null,
  final_block    int not null,
  latest_block   int not null,
  history_blocks int not null,

  started_at     timestamptz not null,
  finished_at    timestamptz,

  constraint prune_state_id_run_uq unique(id, run)
);

create table subgraphs.prune_table_state(
  -- diesel can't deal with composite primary keys
  vid            int primary key
                     generated always as identity,

  id             int not null,
  run            int not null,
  table_name     text not null,
  -- 'r' (rebuild) or 'd' (delete)
  strategy       char not null,
  phase          text not null,

  start_vid      int8,
  final_vid      int8,
  nonfinal_vid   int8,
  rows           int8,

  next_vid       int8,
  batch_size     int8,

  started_at     timestamptz,
  finished_at    timestamptz,

  constraint prune_table_state_id_run_table_name_uq
    unique(id, run, table_name),

  constraint prune_table_state_strategy_ck
    check(strategy in ('r', 'd')),

  constraint prune_table_state_phase_ck
    check(phase in ('queued', 'started', 'copy_final',
                    'copy_nonfinal', 'delete', 'done')),

  constraint prune_table_state_id_run_fk
    foreign key(id, run)
    references subgraphs.prune_state(id, run)
    on delete cascade
);
