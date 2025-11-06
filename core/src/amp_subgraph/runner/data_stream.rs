use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use alloy::primitives::BlockNumber;
use anyhow::anyhow;
use futures::{
    stream::{empty, BoxStream},
    StreamExt, TryStreamExt,
};
use graph::{
    amp::{
        manifest::DataSource,
        stream_aggregator::{RecordBatchGroups, StreamAggregator},
        Client,
    },
    cheap_clone::CheapClone,
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

    let total_queries = cx.total_queries();
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
        let next_block_ranges = next_block_ranges(&cx, latest_queried_block, latest_block);

        if next_block_ranges.is_empty() {
            if data_streams.is_empty() {
                warn!(logger, "There are no unprocessed block ranges");
            }
            break;
        }

        let mut query_streams = Vec::with_capacity(total_queries);
        let mut query_streams_table_ptr = Vec::with_capacity(total_queries);
        let mut min_start_block = BlockNumber::MAX;

        for (i, data_source) in cx.manifest.data_sources.iter().enumerate() {
            let Some(block_range) = next_block_ranges.get(&i) else {
                continue;
            };

            if *block_range.start() < min_start_block {
                min_start_block = *block_range.start();
            }

            if *block_range.end() > max_end_block {
                max_end_block = *block_range.end();
            }

            for (j, table) in data_source.transformer.tables.iter().enumerate() {
                let query = table.query.build_with_block_range(block_range);
                let stream_name = format!("{}.{}", data_source.name, table.name);

                query_streams.push((stream_name, cx.client.query(&cx.logger, query, None)));
                query_streams_table_ptr.push((i, j));
            }
        }

        let query_streams_table_ptr: Arc<[TablePtr]> = query_streams_table_ptr.into();
        total_queries_to_execute += query_streams.len();

        data_streams.push(
            StreamAggregator::new(&cx.logger, query_streams, cx.max_buffer_size)
                .map_ok(move |response| (response, query_streams_table_ptr.cheap_clone()))
                .map_err(Error::from)
                .and_then(move |response| async move {
                    if let Some(((first_block, _), _)) = response.0.first_key_value() {
                        if *first_block < min_start_block {
                            return Err(Error::NonDeterministic(anyhow!("chain reorg")));
                        }
                    }

                    Ok(response)
                })
                .boxed(),
        );

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

fn next_block_ranges<AC>(
    cx: &Context<AC>,
    latest_queried_block: Option<BlockNumber>,
    latest_block: BlockNumber,
) -> HashMap<usize, RangeInclusive<BlockNumber>> {
    let block_ranges = cx
        .manifest
        .data_sources
        .iter()
        .enumerate()
        .filter_map(|(i, data_source)| {
            next_block_range(cx, data_source, latest_queried_block, latest_block)
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

fn next_block_range<AC>(
    cx: &Context<AC>,
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
        start_block.saturating_add(cx.max_block_range as BlockNumber),
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
