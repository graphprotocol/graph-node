with xlat as (
  select * from unnest($1::text[], $2::text[]) as xlat(id, new_id))
insert into subgraphs.dynamic_ethereum_contract_data_source(id, name,
              address, abi, start_block, ethereum_block_hash,
              ethereum_block_number, deployment, context, block_range)
select x.new_id, e.name,
       e.address, e.abi, e.start_block,
       e.ethereum_block_hash, e.ethereum_block_number, $3 as deployment,
       e.context, e.block_range
  from xlat x, subgraphs.dynamic_ethereum_contract_data_source e
 where x.id = e.id
