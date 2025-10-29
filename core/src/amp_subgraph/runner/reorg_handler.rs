use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::anyhow;
use futures::{future::try_join_all, StreamExt, TryFutureExt};
use graph::{
    amp::{
        client::{LatestBlockBeforeReorg, RequestMetadata, ResponseBatch, ResumeStreamingQuery},
        common::Ident,
        Client,
    },
    blockchain::block_stream::FirehoseCursor,
};
use itertools::Itertools;
use slog::debug;

use super::{Compat, Context, Error, LatestBlocks};

pub(super) async fn check_and_handle_reorg<AC>(
    cx: &Context<AC>,
    latest_blocks: &LatestBlocks,
) -> Result<(), Error>
where
    AC: Client,
{
    let logger = cx
        .logger
        .new(slog::o!("process" => "check_and_handle_reorg"));

    let Some((latest_synced_block_number, latest_synced_block_hash)) = cx.latest_synced_block_ptr()
    else {
        debug!(logger, "There are no synced blocks; Skipping reorg check");
        return Ok(());
    };

    debug!(logger, "Running reorg check");

    let Some(latest_block_before_reorg) = detect_deepest_reorg(
        cx,
        latest_blocks,
        latest_synced_block_number,
        latest_synced_block_hash,
    )
    .await?
    else {
        debug!(logger, "Successfully checked for reorg: No reorg detected";
            "latest_synced_block" => latest_synced_block_number
        );
        return Ok(());
    };

    debug!(logger, "Handling reorg";
        "latest_synced_block" => latest_synced_block_number,
        "latest_block_before_reorg" => ?latest_block_before_reorg.block_number
    );

    let (block_number, block_hash) = match (
        latest_block_before_reorg.block_number,
        latest_block_before_reorg.block_hash,
    ) {
        (Some(block_number), Some(block_hash)) => (block_number, block_hash),
        (_, _) => {
            // TODO: Handle reorgs to the genesis block
            return Err(Error::Deterministic(anyhow!(
                "invalid reorg: rewind to the genesis block not supported"
            )));
        }
    };

    if block_number > latest_synced_block_number {
        return Err(Error::Deterministic(anyhow!(
            "invalid reorg: latest block before reorg cannot be higher than the invalidated block"
        )));
    } else if block_number == latest_synced_block_number && block_hash == latest_synced_block_hash {
        return Err(Error::Deterministic(anyhow!(
            "invalid reorg: latest block before reorg cannot be equal to the invalidated block"
        )));
    }

    cx.store
        .revert_block_operations((block_number, block_hash).compat(), FirehoseCursor::None)
        .await
        .map_err(Error::from)?;

    Ok(())
}

async fn detect_deepest_reorg<AC>(
    cx: &Context<AC>,
    latest_blocks: &LatestBlocks,
    latest_synced_block_number: BlockNumber,
    latest_synced_block_hash: BlockHash,
) -> Result<Option<LatestBlockBeforeReorg>, Error>
where
    AC: Client,
{
    let detect_reorg_futs = latest_blocks
        .iter()
        .filter(|(_, latest_block)| *latest_block >= latest_synced_block_number)
        .map(|((i, j), _)| {
            let data_source = &cx.manifest.data_sources[*i];
            let network = &data_source.network;
            let dataset = &data_source.source.dataset;
            let table = &data_source.source.tables[*j];

            detect_reorg(
                &cx,
                network,
                dataset,
                table,
                latest_synced_block_number,
                latest_synced_block_hash,
            )
            .map_err(move |e| e.context(format!("failed to detect reorg in '{dataset}.{table}'")))
        });

    let deepest_reorg = try_join_all(detect_reorg_futs)
        .await?
        .into_iter()
        .flatten()
        .min_by_key(|latest_block_before_reorg| latest_block_before_reorg.block_number);

    Ok(deepest_reorg)
}

async fn detect_reorg<AC>(
    cx: &Context<AC>,
    network: &str,
    dataset: &Ident,
    table: &Ident,
    latest_synced_block_number: BlockNumber,
    latest_synced_block_hash: BlockHash,
) -> Result<Option<LatestBlockBeforeReorg>, Error>
where
    AC: Client,
{
    let query = format!("SELECT _block_num FROM {dataset}.{table} SETTINGS stream = true");
    let mut stream = cx.client.query(
        &cx.logger,
        query,
        Some(RequestMetadata {
            resume_streaming_query: Some(vec![ResumeStreamingQuery {
                network: network.to_string(),
                block_number: latest_synced_block_number,
                block_hash: latest_synced_block_hash,
            }]),
        }),
    );

    let response = stream
        .next()
        .await
        .ok_or_else(|| Error::NonDeterministic(anyhow!("stream is empty")))?
        .map_err(Error::from)?;

    match response {
        ResponseBatch::Batch { .. } => Ok(None),
        ResponseBatch::Reorg(reorg) => reorg
            .into_iter()
            .exactly_one()
            .map_err(|_e| Error::Deterministic(anyhow!("multi-chain datasets are not supported")))
            .map(Some),
    }
}
