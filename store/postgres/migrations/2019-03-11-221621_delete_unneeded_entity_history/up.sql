-- Remove mention of data_after from the log_entity_event function
CREATE OR REPLACE FUNCTION log_entity_event()
    RETURNS trigger AS
$$
DECLARE
    event_id INTEGER;
    new_event_id INTEGER;
    is_reversion BOOLEAN := FALSE;
    operation_type INTEGER := 10;
    event_source  VARCHAR;
    subgraph VARCHAR;
    entity VARCHAR;
    entity_id VARCHAR;
    data_before JSONB;
BEGIN
    -- Get operation type and source
    IF (TG_OP = 'INSERT') THEN
        operation_type := 0;
        event_source := NEW.event_source;
        subgraph := NEW.subgraph;
        entity := NEW.entity;
        entity_id := NEW.id;
        data_before := NULL;
    ELSIF (TG_OP = 'UPDATE') THEN
        operation_type := 1;
        event_source := NEW.event_source;
        subgraph := OLD.subgraph;
        entity := OLD.entity;
        entity_id := OLD.id;
        data_before := OLD.data;
    ELSIF (TG_OP = 'DELETE') THEN
        operation_type := 2;
        event_source := current_setting('vars.current_event_source', TRUE);
        subgraph := OLD.subgraph;
        entity := OLD.entity;
        entity_id := OLD.id;
        data_before := OLD.data;
    ELSE
        RAISE EXCEPTION 'unexpected entity row operation type, %', TG_OP;
    END IF;

    IF event_source = 'REVERSION' THEN
        is_reversion := TRUE;
    END IF;

    SELECT id INTO event_id
    FROM event_meta_data
    WHERE db_transaction_id = txid_current();

    new_event_id := null;

    IF event_id IS NULL THEN
        -- Log information on the postgres transaction for later use in revert operations
        INSERT INTO event_meta_data
            (db_transaction_id, db_transaction_time, source)
        VALUES
            (txid_current(), statement_timestamp(), event_source)
        RETURNING event_meta_data.id INTO new_event_id;
    END IF;

    -- Log row metadata and changes, specify whether event was an original ethereum event or a reversion
    INSERT INTO entity_history
        (event_id, entity_id, subgraph, entity, data_before, reversion, op_id)
    VALUES
        (COALESCE(new_event_id, event_id), entity_id, subgraph, entity, data_before, is_reversion, operation_type);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Remove unneeded entity_history data, and event_meta_data that becomes
-- unnecessary because of that, too. Since we need to get rid of most of
-- the data, it is faster to copy the 'good' data into a new table and swap
-- tables around

-- Create a new table that clones the entity_history table. For now,
-- we only clone the schema. We create indexes and constraints after
-- we are done copying, for speed.
create table eh(like entity_history
                     including defaults
                     including comments
                     including storage);
-- eh and entity_history both use entity_history_id_seq now. Remove its
-- use from entity_history, so we don't get a conflict later when we
-- drop that table
alter table entity_history
      alter column id drop default;
alter sequence entity_history_id_seq
      owned by eh.id;
alter table eh drop column data_after;

-- Lock any writes to entity_history and event_meta_data. We can not
-- afford to have any changes happen while we copy, as we would lose
-- them because we are copying
lock table entity_history in exclusive mode;
lock table event_meta_data in exclusive mode;

-- Copy the good data. Do not log to avoid stress on the WAL. Note that
-- either data_before or data_after can be null, and we need to make sure
-- we include rows with a null in either column, but that 'anything !=
-- null' will always be false. We lift SQL NULL to JSON null, since JSON
-- uses normal, not three-state logic for comparisons with null
alter table eh set unlogged;
insert into eh
  select id, event_id, entity_id, subgraph, entity, data_before,
         reversion, op_id from entity_history
  where subgraph != 'subgraphs'
    and coalesce(data_before, 'null'::jsonb)
        != coalesce(data_after, 'null'::jsonb);
alter table eh set logged;

-- Swap old and new tables
alter table entity_history rename to eh_old;
alter table eh rename to entity_history;

-- Create indexes
alter index entity_history_pkey rename to eh_old_pkey;
alter table entity_history
  add constraint entity_history_pkey primary key(id);

alter index entity_history_event_id_btree_idx
  rename to eh_old_event_id_btree_idx;
create index entity_history_event_id_btree_idx
  on entity_history(event_id);

--
-- Deal with event_meta_data
--

-- Remove uneeded event_meta_data entries following the same pattern
create table emd(like event_meta_data
                     including defaults
                     including comments
                     including storage);
alter table event_meta_data
      alter column id drop default;
alter sequence event_meta_data_id_seq
      owned by emd.id;

-- Copy the good event_meta_data
alter table emd set unlogged;
insert into emd
  select * from event_meta_data d
  where exists (select 1 from entity_history h where d.id = h.event_id);
alter table emd set logged;

alter table event_meta_data rename to emd_old;
alter table emd rename to event_meta_data;

-- Create indexes
alter index event_meta_data_pkey rename to emd_old_pkey;
alter table event_meta_data
  add constraint event_meta_data_pkey primary key(id);

alter index event_meta_data_db_transaction_id_key
  rename to emd_old_db_transaction_id_key;
alter table event_meta_data
  add constraint event_meta_data_db_transaction_id_key
                 unique(db_transaction_id);

-- Make sure the fk constraint on entity_history points to the new
-- event_meta_data table
alter table eh_old
  drop constraint entity_history_event_id_fkey;

alter table entity_history
  add constraint entity_history_event_id_fkey
  foreign key (event_id) references event_meta_data(id)
  match full on update cascade on delete cascade;

-- Recreate the entity_history_with_source view so that
-- it points to the right version of these tables
drop view entity_history_with_source;
create view entity_history_with_source as
  select
    h.subgraph,
    h.entity,
    h.entity_id,
    h.data_before,
    h.op_id,
    m.source
  from entity_history h, event_meta_data m
  where h.event_id = m.id
  order by h.event_id desc;

-- Clean up
drop table eh_old;
drop table emd_old;

analyze entity_history;
analyze event_meta_data;
