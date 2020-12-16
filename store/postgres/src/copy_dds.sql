with xlat as (
  select * from unnest($1::text[], $2::text[]) as xlat(id, new_id)),
 md0 as (
    insert into subgraphs.ethereum_block_handler_entity(id, handler, filter, block_range)
    select (x.new_id || right(e.id, -40)) as id, handler, (x.new_id || right(e.filter, -40)) as filter, block_range
      from subgraphs.ethereum_block_handler_entity e, xlat x
     where left(e.id, 40) = x.id),
 md1 as (
    insert into subgraphs.ethereum_block_handler_filter_entity(id, kind, block_range)
    select (x.new_id || right(e.id, -40)) as id, kind, block_range
      from subgraphs.ethereum_block_handler_filter_entity e, xlat x
     where left(e.id, 40) = x.id),
 md2 as (
    insert into subgraphs.ethereum_call_handler_entity(id, function, handler, block_range)
    select (x.new_id || right(e.id, -40)) as id, function, handler, block_range
      from subgraphs.ethereum_call_handler_entity e, xlat x
     where left(e.id, 40) = x.id),
 md3 as (
    insert into subgraphs.ethereum_contract_abi(id, name, file, block_range)
    select (x.new_id || right(e.id, -40)) as id, name, file, block_range
      from subgraphs.ethereum_contract_abi e, xlat x
     where left(e.id, 40) = x.id),
 md4 as (
    insert into subgraphs.ethereum_contract_data_source(id, kind, name, network, source, mapping, block_range)
    select (x.new_id || right(e.id, -40)) as id, kind, name, network, (x.new_id || right(e.source, -40)) as source, (x.new_id || right(e.mapping, -40)) as mapping, block_range
      from subgraphs.ethereum_contract_data_source e, xlat x
     where left(e.id, 40) = x.id),
 md5 as (
    insert into subgraphs.ethereum_contract_data_source_template(id, kind, name, network, source, mapping, block_range)
    select (x.new_id || right(e.id, -40)) as id, kind, name, network, (x.new_id || right(e.source, -40)) as source, (x.new_id || right(e.mapping, -40)) as mapping, block_range
      from subgraphs.ethereum_contract_data_source_template e, xlat x
     where left(e.id, 40) = x.id),
 md6 as (
    insert into subgraphs.ethereum_contract_data_source_template_source(id, abi, block_range)
    select (x.new_id || right(e.id, -40)) as id, abi, block_range
      from subgraphs.ethereum_contract_data_source_template_source e, xlat x
     where left(e.id, 40) = x.id),
 md7 as (
    insert into subgraphs.ethereum_contract_event_handler(id, event, topic_0, handler, block_range)
    select (x.new_id || right(e.id, -40)) as id, event, topic_0, handler, block_range
      from subgraphs.ethereum_contract_event_handler e, xlat x
     where left(e.id, 40) = x.id),
 md8 as (
    insert into subgraphs.ethereum_contract_mapping(id, kind, api_version, language, file, entities, abis, block_handlers, call_handlers, event_handlers, block_range)
    select (x.new_id || right(e.id, -40)) as id, kind, api_version, language, file, entities, (select array_agg(x.new_id || right(a.elt, -40)) from unnest(e.abis) a(elt)) as abis, (select array_agg(x.new_id || right(a.elt, -40)) from unnest(e.block_handlers) a(elt)) as block_handlers, (select array_agg(x.new_id || right(a.elt, -40)) from unnest(e.call_handlers) a(elt)) as call_handlers, (select array_agg(x.new_id || right(a.elt, -40)) from unnest(e.event_handlers) a(elt)) as event_handlers, block_range
      from subgraphs.ethereum_contract_mapping e, xlat x
     where left(e.id, 40) = x.id),
 md9 as (
    insert into subgraphs.ethereum_contract_source(id, address, abi, start_block, block_range)
    select (x.new_id || right(e.id, -40)) as id, address, abi, start_block, block_range
      from subgraphs.ethereum_contract_source e, xlat x
     where left(e.id, 40) = x.id)
insert into subgraphs.dynamic_ethereum_contract_data_source(id, kind, name,
              network, source, mapping, ethereum_block_hash,
              ethereum_block_number, deployment, block_range)
select x.new_id, e.kind, e.name, e.network, (x.new_id || right(e.source, -40)) as source, (x.new_id || right(e.mapping, -40)) as mapping,
       e.ethereum_block_hash, e.ethereum_block_number, $3 as deployment,
       e.block_range
  from xlat x, subgraphs.dynamic_ethereum_contract_data_source e
 where x.id = e.id
