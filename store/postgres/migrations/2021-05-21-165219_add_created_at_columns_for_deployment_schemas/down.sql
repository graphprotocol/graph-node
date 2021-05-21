alter table public.deployment_schemas
    drop column created_at;

alter table public.unused_deployments
    drop column deployment_created_at;
