use super::block_stream::SubstreamsMapper;
use crate::blockchain::block_stream::{BlockStream, BlockStreamEvent};
use crate::blockchain::Blockchain;
use crate::firehose::FirehoseEndpoint;
use crate::prelude::*;
use crate::substreams::response::Message;
use crate::substreams::ForkStep::{StepNew, StepUndo};
use crate::substreams::{Modules, Request, Response};
use crate::util::backoff::ExponentialBackoff;
use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tonic::Status;

struct SubstreamsBlockStreamMetrics {
    deployment: DeploymentHash,
    provider: String,
    restarts: CounterVec,
    connect_duration: GaugeVec,
    time_between_responses: HistogramVec,
    responses: CounterVec,
}

impl SubstreamsBlockStreamMetrics {
    pub fn new(
        registry: Arc<dyn MetricsRegistry>,
        deployment: DeploymentHash,
        provider: String,
    ) -> Self {
        Self {
            deployment,
            provider,

            restarts: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_restarts",
                    "Counts the number of times a Substreams block stream is (re)started",
                    vec!["deployment", "provider", "success"].as_slice(),
                )
                .unwrap(),

            connect_duration: registry
                .global_gauge_vec(
                    "deployment_substreams_blockstream_connect_duration",
                    "Measures the time it takes to connect a Substreams block stream",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            time_between_responses: registry
                .global_histogram_vec(
                    "deployment_substreams_blockstream_time_between_responses",
                    "Measures the time between receiving and processing Substreams stream responses",
                    vec!["deployment", "provider"].as_slice(),
                )
                .unwrap(),

            responses: registry
                .global_counter_vec(
                    "deployment_substreams_blockstream_responses",
                    "Counts the number of responses received from a Substreams block stream",
                    vec!["deployment", "provider", "kind"].as_slice(),
                )
                .unwrap(),
        }
    }

    fn observe_successful_connection(&self, time: &mut Instant) {
        self.restarts
            .with_label_values(&[&self.deployment, &self.provider, "true"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &self.provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_failed_connection(&self, time: &mut Instant) {
        self.restarts
            .with_label_values(&[&self.deployment, &self.provider, "false"])
            .inc();
        self.connect_duration
            .with_label_values(&[&self.deployment, &self.provider])
            .set(time.elapsed().as_secs_f64());

        // Reset last connection timestamp
        *time = Instant::now();
    }

    fn observe_response(&self, kind: &str, time: &mut Instant) {
        self.time_between_responses
            .with_label_values(&[&self.deployment, &self.provider])
            .observe(time.elapsed().as_secs_f64());
        self.responses
            .with_label_values(&[&self.deployment, &self.provider, kind])
            .inc();

        // Reset last response timestamp
        *time = Instant::now();
    }
}

pub struct SubstreamsBlockStream<C: Blockchain> {
    //fixme: not sure if this is ok to be set as public, maybe
    // we do not want to expose the stream to the caller
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>> + Send>>,
}

impl<C> SubstreamsBlockStream<C>
where
    C: Blockchain,
{
    pub fn new<F>(
        deployment: DeploymentHash,
        endpoint: Arc<FirehoseEndpoint>,
        subgraph_current_block: Option<BlockPtr>,
        cursor: Option<String>,
        mapper: Arc<F>,
        modules: Option<Modules>,
        module_name: String,
        start_blocks: Vec<BlockNumber>,
        end_blocks: Vec<BlockNumber>,
        logger: Logger,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self
    where
        F: SubstreamsMapper<C> + 'static,
    {
        let manifest_start_block_num = start_blocks.into_iter().min().unwrap_or(0);

        let manifest_end_block_num = end_blocks.into_iter().min().unwrap_or(0);

        let metrics =
            SubstreamsBlockStreamMetrics::new(registry, deployment, endpoint.provider.clone());

        SubstreamsBlockStream {
            stream: Box::pin(stream_blocks(
                endpoint,
                cursor,
                mapper,
                modules,
                module_name,
                manifest_start_block_num,
                manifest_end_block_num,
                subgraph_current_block,
                logger,
                metrics,
            )),
        }
    }
}

fn stream_blocks<C: Blockchain, F: SubstreamsMapper<C>>(
    endpoint: Arc<FirehoseEndpoint>,
    cursor: Option<String>,
    mapper: Arc<F>,
    modules: Option<Modules>,
    module_name: String,
    manifest_start_block_num: BlockNumber,
    manifest_end_block_num: BlockNumber,
    _subgraph_current_block: Option<BlockPtr>,
    logger: Logger,
    metrics: SubstreamsBlockStreamMetrics,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>> {
    let mut latest_cursor = cursor.unwrap_or_else(|| "".to_string());

    let start_block_num = manifest_start_block_num as i64;
    let stop_block_num = manifest_end_block_num as u64;

    let request = Request {
        start_block_num,
        start_cursor: latest_cursor.clone(),
        stop_block_num,
        fork_steps: vec![StepNew as i32, StepUndo as i32],
        irreversibility_condition: "".to_string(),
        modules,
        output_modules: vec![module_name],
        ..Default::default()
    };

    // Back off exponentially whenever we encounter a connection error or a stream with bad data
    let mut backoff = ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));

    // This attribute is needed because `try_stream!` seems to break detection of `skip_backoff` assignments
    #[allow(unused_assignments)]
    let mut skip_backoff = false;

    try_stream! {
        loop {
            info!(
                &logger,
                "Blockstreams disconnected, connecting";
                "endpoint_uri" => format_args!("{}", endpoint),
                "start_block" => start_block_num,
                "cursor" => &latest_cursor,
            );

            // We just reconnected, assume that we want to back off on errors
            skip_backoff = false;

            let mut connect_start = Instant::now();
            let result = endpoint.clone().substreams(request.clone()).await;

            match result {
                Ok(stream) => {
                    info!(&logger, "Blockstreams connected");

                    // Track the time it takes to set up the block stream
                    metrics.observe_successful_connection(&mut connect_start);

                    let mut last_response_time = Instant::now();
                    let mut expected_stream_end = false;

                    for await response in stream{
                        match process_substreams_response(
                            response,
                            mapper.as_ref(),
                            &logger,
                        ).await {
                            Ok(block_response) => {
                                match block_response {
                                    None => {}
                                    Some(BlockResponse::Proceed(event, cursor)) => {
                                        // Reset backoff because we got a good value from the stream
                                        backoff.reset();

                                        metrics.observe_response("proceed", &mut last_response_time);

                                        yield event;

                                        latest_cursor = cursor;
                                    }
                                }
                            },
                            Err(err) => {
                                info!(&logger, "received err");
                                // We have an open connection but there was an error processing the Firehose
                                // response. We will reconnect the stream after this; this is the case where
                                // we actually _want_ to back off in case we keep running into the same error.
                                // An example of this situation is if we get invalid block or transaction data
                                // that cannot be decoded properly.

                                metrics.observe_response("error", &mut last_response_time);

                                error!(logger, "{:#}", err);
                                expected_stream_end = true;
                                break;
                            }
                        }
                    }

                    if !expected_stream_end {
                        error!(logger, "Stream blocks complete unexpectedly, expecting stream to always stream blocks");
                    }
                },
                Err(e) => {
                    // We failed to connect and will try again; this is another
                    // case where we actually _want_ to back off in case we keep
                    // having connection errors.

                    metrics.observe_failed_connection(&mut connect_start);

                    error!(logger, "Unable to connect to endpoint: {:?}", e);
                }
            }

            // If we reach this point, we must wait a bit before retrying, unless `skip_backoff` is true
            if !skip_backoff {
                backoff.sleep_async().await;
            }
        }
    }
}

enum BlockResponse<C: Blockchain> {
    Proceed(BlockStreamEvent<C>, String),
}

async fn process_substreams_response<C: Blockchain, F: SubstreamsMapper<C>>(
    result: Result<Response, Status>,
    mapper: &F,
    logger: &Logger,
) -> Result<Option<BlockResponse<C>>, Error> {
    let response = match result {
        Ok(v) => v,
        Err(e) => return Err(anyhow!("An error occurred while streaming blocks: {:?}", e)),
    };

    match response.message {
        Some(Message::Data(block_scoped_data)) => {
            match mapper
                .to_block_stream_event(logger, &block_scoped_data)
                .await
                .context("Mapping block to BlockStreamEvent failed")?
            {
                Some(event) => Ok(Some(BlockResponse::Proceed(
                    event,
                    block_scoped_data.cursor.to_string(),
                ))),
                None => Ok(None),
            }
        }
        None => {
            warn!(&logger, "Got None on substream message");
            Ok(None)
        }
        _ => Ok(None),
    }
}

impl<C: Blockchain> Stream for SubstreamsBlockStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<C: Blockchain> BlockStream<C> for SubstreamsBlockStream<C> {}
