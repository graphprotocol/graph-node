alter table subgraphs.prune_state
  add column errored_at timestamptz,
  add column error text,
  add constraint error_ck check ((errored_at is null) = (error is null));
