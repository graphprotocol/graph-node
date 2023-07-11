drop index ethereum_blocks_nulldata_idx ;

do $$
declare
    tables cursor for select namespace
                        from ethereum_networks
                       where namespace != 'public';
begin
	for table_record in tables loop
		execute
			'drop index '
			|| table_record.namespace
			|| '.'
			|| 'blocks_nulldata_idx';
	end loop;
end;
$$;
