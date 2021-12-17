alter table public.ethereum_networks
    add column backfill_block_cursor text default null,
    add column backfill_completed boolean default false,
    add column backfill_completed_date timestamp default null,
    add column backfill_target_block_number bigint default null;