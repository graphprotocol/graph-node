create table public.graphman_command_executions
(
    id            bigserial primary key,
    kind          varchar                  not null check (kind in ('restart_deployment')),
    status        varchar                  not null check (status in ('initializing', 'running', 'failed', 'succeeded')),
    error_message varchar                  default null,
    created_at    timestamp with time zone not null,
    updated_at    timestamp with time zone default null,
    completed_at  timestamp with time zone default null
);
