use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use alloy::primitives::BlockNumber;
use anyhow::anyhow;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use graph::{
    amp::{
        client::ResponseBatch,
        error::IsDeterministic,
        manifest::DataSource,
        stream_aggregator::{RecordBatchGroups, StreamAggregator},
        Client,
    },
    cheap_clone::CheapClone,
    prelude::StopwatchMetrics,
};
use slog::{debug, warn};

use super::{Context, Error};

pub(super) type TablePtr = (usize, usize);

pub(super) fn new_data_stream<AC>(
    cx: &Context<AC>,
    latest_block: BlockNumber,
) -> BoxStream<'static, Result<(RecordBatchGroups, Arc<[TablePtr]>), Error>>
where
    AC: Client + Send + Sync + 'static,
{
    let logger = cx.logger.new(slog::o!("process" => "new_data_stream"));
    let client = cx.client.cheap_clone();
    let manifest = cx.manifest.clone();
    let max_buffer_size = cx.max_buffer_size;
    let max_block_range = cx.max_block_range;
    let stopwatch = cx.metrics.stopwatch.cheap_clone();

    debug!(logger, "Creating data stream";
        "from_block" => cx.latest_synced_block().unwrap_or(BlockNumber::MIN),
        "to_block" => latest_block,
        "start_block" => cx.min_start_block(),
        "max_block_range" => max_block_range,
    );

    // State: (latest_queried_block, max_end_block, is_first)
    let initial_state = (cx.latest_synced_block(), BlockNumber::MIN, true);

    stream::unfold(
        initial_state,
        move |(latest_queried_block, mut end_block, is_first)| {
            let block_ranges = next_block_ranges(
                &manifest.data_sources,
                max_block_range,
                latest_queried_block,
                latest_block,
            );

            if block_ranges.is_empty() {
                if is_first {
                    warn!(logger, "There are no unprocessed block ranges");
                }
                return futures::future::ready(None);
            }

            let start_block = block_ranges.values().map(|r| *r.start()).min().unwrap();
            end_block = end_block.max(block_ranges.values().map(|r| *r.end()).max().unwrap());

            let (query_streams, table_ptrs) =
                build_query_streams(&*client, &logger, &manifest.data_sources, &block_ranges);

            let data_stream = build_data_stream(
                &logger,
                query_streams,
                table_ptrs,
                max_buffer_size,
                &stopwatch,
                start_block,
            );

            debug!(logger, "Created a new data stream";
                "latest_queried_block" => latest_queried_block,
                "start_block" => start_block,
                "end_block" => end_block,
            );
            futures::future::ready(Some((data_stream, (Some(end_block), end_block, false))))
        },
    )
    .flatten()
    .boxed()
}

fn build_query_streams<AC: Client>(
    client: &AC,
    logger: &slog::Logger,
    data_sources: &[DataSource],
    block_ranges: &HashMap<usize, RangeInclusive<BlockNumber>>,
) -> (
    Vec<(String, BoxStream<'static, Result<ResponseBatch, AC::Error>>)>,
    Arc<[TablePtr]>,
) {
    let total_queries: usize = data_sources
        .iter()
        .map(|ds| ds.transformer.tables.len())
        .sum();

    let mut query_streams = Vec::with_capacity(total_queries);
    let mut table_ptrs = Vec::with_capacity(total_queries);

    for (i, data_source) in data_sources.iter().enumerate() {
        let Some(block_range) = block_ranges.get(&i) else {
            continue;
        };

        for (j, table) in data_source.transformer.tables.iter().enumerate() {
            let query = table.query.build_with_block_range(block_range);
            let stream = client.query(logger, query, None);
            let stream_name = format!("{}.{}", data_source.name, table.name);

            query_streams.push((stream_name, stream));
            table_ptrs.push((i, j));
        }
    }

    (query_streams, table_ptrs.into())
}

fn build_data_stream<E>(
    logger: &slog::Logger,
    query_streams: Vec<(String, BoxStream<'static, Result<ResponseBatch, E>>)>,
    table_ptrs: Arc<[TablePtr]>,
    max_buffer_size: usize,
    stopwatch: &StopwatchMetrics,
    min_start_block: BlockNumber,
) -> BoxStream<'static, Result<(RecordBatchGroups, Arc<[TablePtr]>), Error>>
where
    E: std::error::Error + IsDeterministic + Send + Sync + 'static,
{
    let mut min_start_block_checked = false;
    let mut load_first_record_batch_group_section =
        Some(stopwatch.start_section("load_first_record_batch_group"));

    StreamAggregator::new(logger, query_streams, max_buffer_size)
        .map_ok(move |response| (response, table_ptrs.cheap_clone()))
        .map_err(Error::from)
        .map(move |result| {
            if load_first_record_batch_group_section.is_some() {
                let _section = load_first_record_batch_group_section.take();
            }

            match result {
                Ok(response) => {
                    if !min_start_block_checked {
                        if let Some(((first_block, _), _)) = response.0.first_key_value() {
                            if *first_block < min_start_block {
                                return Err(Error::NonDeterministic(anyhow!("chain reorg")));
                            }
                        }

                        min_start_block_checked = true;
                    }

                    Ok(response)
                }
                Err(e) => Err(e),
            }
        })
        .boxed()
}

fn next_block_ranges(
    data_sources: &[DataSource],
    max_block_range: usize,
    latest_queried_block: Option<BlockNumber>,
    latest_block: BlockNumber,
) -> HashMap<usize, RangeInclusive<BlockNumber>> {
    let block_ranges = data_sources
        .iter()
        .enumerate()
        .filter_map(|(i, data_source)| {
            next_block_range(
                max_block_range,
                data_source,
                latest_queried_block,
                latest_block,
            )
            .map(|block_range| (i, block_range))
        })
        .collect::<HashMap<_, _>>();

    let Some(min_block_range) = block_ranges
        .iter()
        .min_by_key(|(_, block_range)| *block_range.start())
        .map(|(_, min_block_range)| min_block_range.clone())
    else {
        return HashMap::new();
    };

    block_ranges
        .into_iter()
        .filter(|(_, block_range)| block_range.start() <= min_block_range.end())
        .collect()
}

fn next_block_range(
    max_block_range: usize,
    data_source: &DataSource,
    latest_queried_block: Option<BlockNumber>,
    latest_block: BlockNumber,
) -> Option<RangeInclusive<BlockNumber>> {
    let start_block = match latest_queried_block {
        Some(latest_queried_block) => {
            if latest_queried_block >= data_source.source.end_block {
                return None;
            }

            latest_queried_block + 1
        }
        None => data_source.source.start_block,
    };

    let end_block = [
        start_block.saturating_add(max_block_range as BlockNumber),
        data_source.source.end_block,
        latest_block,
    ]
    .into_iter()
    .min()
    .unwrap();

    if start_block > end_block {
        return None;
    }

    Some(start_block..=end_block)
}
