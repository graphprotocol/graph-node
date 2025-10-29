use alloy::primitives::BlockNumber;
use anyhow::anyhow;
use arrow::array::RecordBatch;
use futures::{future::try_join_all, stream::BoxStream, StreamExt, TryFutureExt};
use graph::{
    amp::{
        client::ResponseBatch,
        codec::{utils::block_number_decoder, Decoder},
        common::Ident,
        error::IsDeterministic,
        manifest::DataSource,
        Client,
    },
    cheap_clone::CheapClone,
};
use itertools::Itertools;
use slog::debug;

use super::{Context, Error};

pub(super) type TablePtr = (usize, usize);

pub(super) struct LatestBlocks(Vec<(TablePtr, BlockNumber)>);

impl LatestBlocks {
    pub(super) async fn load<AC>(cx: &Context<AC>) -> Result<Self, Error>
    where
        AC: Client,
    {
        debug!(cx.logger, "Loading latest blocks");

        let latest_block_futs = cx
            .manifest
            .data_sources
            .iter()
            .enumerate()
            .map(|(i, data_source)| {
                data_source
                    .source
                    .tables
                    .iter()
                    .enumerate()
                    .map(move |(j, table)| ((i, j), &data_source.source.dataset, table))
            })
            .flatten()
            .unique_by(|(_, dataset, table)| (dataset.cheap_clone(), table.cheap_clone()))
            .map(|(table_ptr, dataset, table)| {
                latest_block(&cx, dataset, table)
                    .map_ok(move |latest_block| (table_ptr, latest_block))
                    .map_err(move |e| {
                        e.context(format!(
                            "failed to load latest block for '{dataset}.{table}'"
                        ))
                    })
            });

        try_join_all(latest_block_futs).await.map(Self)
    }

    pub(super) fn filter_completed<AC>(self, cx: &Context<AC>) -> Self
    where
        AC: Client,
    {
        let latest_synced_block = cx.latest_synced_block();

        Self(
            self.0
                .into_iter()
                .filter(|((i, _), _)| {
                    !indexing_completed(&cx.manifest.data_sources[*i], &latest_synced_block)
                })
                .collect(),
        )
    }

    pub(super) fn min(&self) -> BlockNumber {
        self.0
            .iter()
            .min_by_key(|(_, latest_block)| *latest_block)
            .map(|(_, latest_block)| *latest_block)
            .unwrap()
    }

    pub(super) async fn changed<AC>(self, cx: &Context<AC>) -> Result<(), Error>
    where
        AC: Client,
    {
        debug!(cx.logger, "Waiting for new blocks");

        let min_latest_block = self.min();
        let latest_synced_block = cx.latest_synced_block();

        let latest_block_changed_futs = self
            .0
            .into_iter()
            .filter(|(_, latest_block)| *latest_block == min_latest_block)
            .filter(|((i, _), _)| {
                !indexing_completed(&cx.manifest.data_sources[*i], &latest_synced_block)
            })
            .map(|((i, j), latest_block)| {
                let source = &cx.manifest.data_sources[i].source;
                let dataset = &source.dataset;
                let table = &source.tables[j];

                latest_block_changed(&cx, dataset, table, latest_block).map_err(move |e| {
                    e.context(format!(
                        "failed to check if the latest block changed in '{dataset}.{table}'"
                    ))
                })
            });

        let _response = try_join_all(latest_block_changed_futs).await?;

        Ok(())
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &(TablePtr, BlockNumber)> {
        self.0.iter()
    }
}

fn indexing_completed(data_source: &DataSource, latest_synced_block: &Option<BlockNumber>) -> bool {
    latest_synced_block
        .as_ref()
        .is_some_and(|latest_synced_block| *latest_synced_block >= data_source.source.end_block)
}

async fn latest_block<AC>(
    cx: &Context<AC>,
    dataset: &Ident,
    table: &Ident,
) -> Result<BlockNumber, Error>
where
    AC: Client,
{
    let query = format!("SELECT MAX(_block_num) FROM {dataset}.{table}");
    let stream = cx.client.query(&cx.logger, query, None);
    let record_batch = read_once(stream).await?;

    let latest_block = block_number_decoder(&record_batch, 0)
        .map_err(|e| Error::Deterministic(e))?
        .decode(0)
        .map_err(|e| Error::Deterministic(e))?
        .ok_or_else(|| Error::NonDeterministic(anyhow!("table is empty")))?;

    Ok(latest_block)
}

async fn latest_block_changed<AC>(
    cx: &Context<AC>,
    dataset: &Ident,
    table: &Ident,
    latest_block: BlockNumber,
) -> Result<(), Error>
where
    AC: Client,
{
    let query = format!("SELECT _block_num FROM {dataset}.{table} WHERE _block_num > {latest_block} SETTINGS stream = true");
    let stream = cx.client.query(&cx.logger, query, None);
    let _record_batch = read_once(stream).await?;

    Ok(())
}

async fn read_once<E>(
    mut stream: BoxStream<'static, Result<ResponseBatch, E>>,
) -> Result<RecordBatch, Error>
where
    E: std::error::Error + IsDeterministic + Send + Sync + 'static,
{
    let response = stream
        .next()
        .await
        .ok_or_else(|| Error::NonDeterministic(anyhow!("stream is empty")))?
        .map_err(Error::from)?;

    match response {
        ResponseBatch::Batch { data } => Ok(data),
        _ => Err(Error::NonDeterministic(anyhow!("response is empty"))),
    }
}
