mod compat;
mod context;
mod data_processing;
mod data_stream;
mod error;
mod latest_blocks;
mod reorg_handler;

use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;
use graph::{
    amp::Client, cheap_clone::CheapClone, components::store::EntityCache,
    data::subgraph::schema::SubgraphError,
};
use slog::{debug, error, warn};
use tokio_util::sync::CancellationToken;

use self::{
    compat::Compat, data_processing::process_record_batch_groups, data_stream::new_data_stream,
    error::Error, latest_blocks::LatestBlocks, reorg_handler::check_and_handle_reorg,
};

pub(super) use self::context::Context;

pub(super) async fn new_runner<AC>(
    mut cx: Context<AC>,
    cancel_token: CancellationToken,
) -> Result<()>
where
    AC: Client + Send + Sync + 'static,
{
    let indexing_duration_handle = tokio::spawn({
        let mut instant = Instant::now();
        let indexing_duration = cx.metrics.indexing_duration.clone();

        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let prev_instant = std::mem::replace(&mut instant, Instant::now());
                indexing_duration.record(prev_instant.elapsed());
            }
        }
    });

    let result = cancel_token
        .run_until_cancelled(run_indexing_with_retries(&mut cx))
        .await;

    indexing_duration_handle.abort();

    match result {
        Some(result) => result?,
        None => {
            debug!(cx.logger, "Processed cancel signal");
        }
    }

    cx.metrics.deployment_status.stopped();

    debug!(cx.logger, "Waiting for the store to finish processing");
    cx.store.flush().await?;
    Ok(())
}

async fn run_indexing<AC>(cx: &mut Context<AC>) -> Result<(), Error>
where
    AC: Client + Send + Sync + 'static,
{
    cx.metrics.deployment_status.starting();

    if let Some(latest_synced_block) = cx.latest_synced_block() {
        cx.metrics.deployment_head.update(latest_synced_block);
    }

    cx.metrics
        .deployment_synced
        .record(cx.store.is_deployment_synced());

    loop {
        cx.metrics.deployment_status.running();

        debug!(cx.logger, "Running indexing";
            "latest_synced_block_ptr" => ?cx.latest_synced_block_ptr()
        );

        let mut latest_blocks = LatestBlocks::load(cx).await?;
        check_and_handle_reorg(cx, &latest_blocks).await?;

        if cx.indexing_completed() {
            cx.metrics.deployment_synced.record(true);

            debug!(cx.logger, "Indexing completed");
            return Ok(());
        }

        latest_blocks = latest_blocks.filter_completed(cx);
        let latest_block = latest_blocks.min();

        cx.metrics
            .deployment_target
            .update(latest_block.min(cx.end_block()));

        let mut deployment_is_failed = cx.store.health().await?.is_failed();
        let mut entity_cache = EntityCache::new(cx.store.cheap_clone());
        let mut stream = new_data_stream(cx, latest_block);

        while let Some(result) = stream.next().await {
            let (record_batch_groups, stream_table_ptr) = result?;

            entity_cache = process_record_batch_groups(
                cx,
                entity_cache,
                record_batch_groups,
                stream_table_ptr,
                latest_block,
            )
            .await?;

            if deployment_is_failed {
                if let Some(block_ptr) = cx.store.block_ptr() {
                    cx.store.unfail_non_deterministic_error(&block_ptr).await?;
                    deployment_is_failed = false;
                }
            }
        }

        // Check if the Amp Flight server has data covering through every data
        // source's endBlock. This handles the case where endBlock has no entity
        // data — the persisted block pointer never advances to endBlock, but the
        // server's latest block confirms all queries have been served.
        if latest_block >= cx.end_block() {
            cx.metrics.deployment_synced.record(true);

            debug!(cx.logger, "Indexing completed; endBlock reached via server latest block";
                "latest_block" => latest_block,
                "end_block" => cx.end_block()
            );
            return Ok(());
        }

        debug!(cx.logger, "Completed indexing iteration";
            "latest_synced_block_ptr" => ?cx.latest_synced_block_ptr()
        );

        latest_blocks.changed(cx).await?;
        cx.backoff.reset();
    }
}

async fn run_indexing_with_retries<AC>(cx: &mut Context<AC>) -> Result<()>
where
    AC: Client + Send + Sync + 'static,
{
    loop {
        match run_indexing(cx).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                cx.metrics.deployment_status.failed();

                let deterministic = e.is_deterministic();

                cx.store
                    .fail_subgraph(SubgraphError {
                        subgraph_id: cx.deployment.cheap_clone(),
                        message: format!("{e:#}"),
                        block_ptr: None, // TODO: Find a way to propagate the block ptr here
                        handler: None,
                        deterministic,
                    })
                    .await?;

                if deterministic {
                    error!(cx.logger, "Subgraph failed with a deterministic error";
                        "e" => ?e
                    );
                    return Err(e.into());
                }

                warn!(cx.logger, "Subgraph failed with a non-deterministic error";
                    "e" => ?e,
                    "retry_delay_seconds" => cx.backoff.delay().as_secs()
                );

                cx.backoff.sleep_async().await;
                debug!(cx.logger, "Restarting indexing");
            }
        }
    }
}
