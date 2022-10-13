do $$
declare 
   deployments cursor for select * from deployment_schemas where version = 1;
begin
   for deployment in deployments loop
      execute 'alter table ' || deployment.name || '.data_sources$ add done_at int';
      execute 'update deployment_schemas set version = 2 where id = ' || deployment.id;
   end loop;
end;
$$;
