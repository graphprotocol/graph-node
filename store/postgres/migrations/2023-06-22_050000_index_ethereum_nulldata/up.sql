create index if not exists
    ethereum_blocks_nulldata_idx
on
	ethereum_blocks
using
    BTREE ((data->'block'->'data'))
where
    data->'block'->'data' = 'null'::jsonb;

do $$
declare
    tables cursor for select namespace
                        from ethereum_networks
                       where namespace != 'public';
begin
	for table_record in tables loop
		execute
			'create index if not exists blocks_nulldata_idx on '
			|| table_record.namespace
			|| '.'
			|| 'blocks '
            || 'using BTREE ((data->''block''->''data'')) '
            || 'where data->''block''->''data'' = ''null''::jsonb';
	end loop;
end;
$$;
