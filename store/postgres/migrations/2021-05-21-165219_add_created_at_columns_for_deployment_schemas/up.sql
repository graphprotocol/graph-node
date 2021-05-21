alter table public.deployment_schemas
    add column created_at timestamptz not null default now();

alter table public.unused_deployments
    add column created_at timestamptz not null;
