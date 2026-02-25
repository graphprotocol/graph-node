use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use alloy::primitives::BlockNumber;
use anyhow::anyhow;
use futures::{
    stream::{empty, BoxStream},
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
    AC: Client,
{
    let logger = cx.logger.new(slog::o!("process" => "new_data_stream"));

    let mut total_queries_to_execute = 0;
    let mut data_streams = Vec::new();
    let mut latest_queried_block = cx.latest_synced_block();
    let mut max_end_block = BlockNumber::MIN;

    debug!(logger, "Creating data stream";
        "from_block" => latest_queried_block.unwrap_or(BlockNumber::MIN),
        "to_block" => latest_block,
        "min_start_block" => cx.min_start_block(),
        "max_block_range" => cx.max_block_range,
    );

    loop {
        let block_ranges = next_block_ranges(
            &cx.manifest.data_sources,
            cx.max_block_range,
            latest_queried_block,
            latest_block,
        );

        if block_ranges.is_empty() {
            if data_streams.is_empty() {
                warn!(logger, "There are no unprocessed block ranges");
            }
            break;
        }

        let min_start_block = block_ranges.values().map(|r| *r.start()).min().unwrap();
        max_end_block =
            max_end_block.max(block_ranges.values().map(|r| *r.end()).max().unwrap());

        let (query_streams, table_ptrs) =
            build_query_streams(&*cx.client, &logger, &cx.manifest.data_sources, &block_ranges);
        total_queries_to_execute += query_streams.len();

        data_streams.push(build_data_stream(
            &logger,
            query_streams,
            table_ptrs,
            cx.max_buffer_size,
            &cx.metrics.stopwatch,
            min_start_block,
        ));

        if max_end_block >= latest_block {
            break;
        }

        latest_queried_block = Some(max_end_block);
    }

    debug!(logger, "Created aggregated data streams";
        "total_data_streams" => data_streams.len(),
        "total_queries_to_execute" => total_queries_to_execute
    );

    let mut iter = data_streams.into_iter();
    let mut merged_data_stream = iter.next().unwrap_or_else(|| empty().boxed());

    for data_stream in iter {
        merged_data_stream = merged_data_stream.chain(data_stream).boxed();
    }

    merged_data_stream
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
