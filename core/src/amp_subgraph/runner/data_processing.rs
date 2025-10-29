use std::sync::Arc;

use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::anyhow;
use arrow::array::RecordBatch;
use chrono::{DateTime, Utc};
use graph::{
    amp::{
        codec::{utils::auto_block_timestamp_decoder, DecodeOutput, DecodedEntity, Decoder},
        stream_aggregator::{RecordBatchGroup, RecordBatchGroups, StreamRecordBatch},
    },
    blockchain::block_stream::FirehoseCursor,
    cheap_clone::CheapClone,
    components::store::{EntityCache, ModificationsAndCache},
};
use slog::{debug, trace};

use super::{data_stream::TablePtr, Compat, Context, Error};

pub(super) async fn process_record_batch_groups<AC>(
    cx: &mut Context<AC>,
    mut entity_cache: EntityCache,
    record_batch_groups: RecordBatchGroups,
    stream_table_ptr: Arc<[TablePtr]>,
    latest_block: BlockNumber,
) -> Result<EntityCache, Error> {
    let from_block = record_batch_groups
        .first_key_value()
        .map(|((block, _), _)| *block);

    let to_block = record_batch_groups
        .last_key_value()
        .map(|((block, _), _)| *block);

    debug!(cx.logger, "Processing record batch groups";
        "from_block" => ?from_block,
        "to_block" => ?to_block
    );

    for ((block_number, block_hash), record_batch_group) in record_batch_groups {
        trace!(cx.logger, "Processing record batch group";
            "block" => block_number,
            "record_batches_count" => record_batch_group.record_batches.len()
        );

        entity_cache = process_record_batch_group(
            cx,
            entity_cache,
            block_number,
            block_hash,
            record_batch_group,
            &stream_table_ptr,
            latest_block,
        )
        .await
        .map_err(|e| {
            e.context(format!(
                "failed to process record batch group at block '{block_number}'"
            ))
        })?;

        trace!(cx.logger, "Completed processing record batch group";
            "block" => block_number
        );
    }

    debug!(cx.logger, "Completed processing record batch groups";
        "from_block" => ?from_block,
        "to_block" => ?to_block
    );

    Ok(entity_cache)
}

async fn process_record_batch_group<AC>(
    cx: &mut Context<AC>,
    mut entity_cache: EntityCache,
    block_number: BlockNumber,
    block_hash: BlockHash,
    record_batch_group: RecordBatchGroup,
    stream_table_ptr: &[TablePtr],
    latest_block: BlockNumber,
) -> Result<EntityCache, Error> {
    let RecordBatchGroup { record_batches } = record_batch_group;

    if record_batches.is_empty() {
        debug!(cx.logger, "Record batch group is empty");
        return Ok(entity_cache);
    }

    let block_timestamp = decode_block_timestamp(&record_batches)
        .map_err(|e| e.context("failed to decode block timestamp"))?;

    for record_batch in record_batches {
        let StreamRecordBatch {
            stream_index,
            record_batch,
        } = record_batch;

        process_record_batch(
            cx,
            &mut entity_cache,
            block_number,
            record_batch,
            stream_table_ptr[stream_index],
        )
        .await
        .map_err(|e| {
            e.context(format!(
                "failed to process record batch for stream '{stream_index}'"
            ))
        })?;
    }

    let ModificationsAndCache {
        modifications,
        entity_lfu_cache,
        evict_stats: _,
    } = entity_cache
        .as_modifications(block_number.compat())
        .await
        .map_err(Error::from)
        .map_err(|e| e.context("failed to extract entity modifications from the state"))?;

    let is_close_to_chain_head = latest_block.saturating_sub(block_number) <= 100;

    cx.store
        .transact_block_operations(
            (block_number, block_hash).compat(),
            block_timestamp.compat(),
            FirehoseCursor::None,
            modifications,
            &cx.metrics.stopwatch,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            is_close_to_chain_head,
        )
        .await
        .map_err(Error::from)
        .map_err(|e| e.context("failed to transact block operations"))?;

    Ok(EntityCache::with_current(
        cx.store.cheap_clone(),
        entity_lfu_cache,
    ))
}

async fn process_record_batch<AC>(
    cx: &mut Context<AC>,
    entity_cache: &mut EntityCache,
    block_number: BlockNumber,
    record_batch: RecordBatch,
    (i, j): TablePtr,
) -> Result<(), Error> {
    let table = &cx.manifest.data_sources[i].transformer.tables[j];
    let entity_name = &table.name;

    let DecodeOutput {
        entity_type,
        id_type,
        decoded_entities,
    } = cx
        .codec
        .decode(record_batch, entity_name.as_str())
        .map_err(|e| {
            Error::Deterministic(
                e.context(format!("failed to decode entities of type '{entity_name}'")),
            )
        })?;

    for decoded_entity in decoded_entities {
        let DecodedEntity {
            key,
            mut entity_data,
        } = decoded_entity;

        let key = match key {
            Some(key) => key,
            None => {
                let entity_id = entity_cache
                    .generate_id(id_type, block_number.compat())
                    .map_err(|e| {
                        Error::Deterministic(e.context(format!(
                            "failed to generate a new id for an entity of type '{entity_name}'"
                        )))
                    })?;

                entity_data.push(("id".into(), entity_id.clone().into()));
                entity_type.key(entity_id)
            }
        };

        let entity_id = key.entity_id.clone();
        let entity = cx.manifest.schema.make_entity(entity_data).map_err(|e| {
            Error::Deterministic(anyhow!(e).context(format!(
                "failed to create a new entity of type '{entity_name}' with id '{entity_id}'"
            )))
        })?;

        entity_cache
            .set(key, entity, block_number.compat(), None)
            .await
            .map_err(|e| {
                Error::Deterministic(e.context(format!(
                    "failed to store a new entity of type '{entity_name}' with id '{entity_id}'"
                )))
            })?;
    }

    Ok(())
}

fn decode_block_timestamp(record_batches: &[StreamRecordBatch]) -> Result<DateTime<Utc>, Error> {
    let mut last_error: Option<Error> = None;

    for record_batch in record_batches {
        match auto_block_timestamp_decoder(&record_batch.record_batch) {
            Ok(decoder) => {
                return decoder
                    .decode(0)
                    .map_err(|e| Error::Deterministic(e))?
                    .ok_or_else(|| Error::Deterministic(anyhow!("block timestamp is empty")));
            }
            Err(e) => {
                last_error = Some(Error::Deterministic(e));
            }
        }
    }

    Err(last_error.unwrap())
}
