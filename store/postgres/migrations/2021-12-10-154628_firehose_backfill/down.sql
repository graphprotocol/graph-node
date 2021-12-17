alter table public.ethereum_networks
    drop column backfill_block_cursor,
    drop column backfill_completed,
    drop column backfill_completed_date,
    drop column backfill_target_block_number;
