pub struct TracingServer;

use std::pin::Pin;

use graph::{
    data::query::Trace,
    futures03::{stream::unfold, Stream, StreamExt as _},
    grpc::pb::graph::tracing::v1::{
        stream_server::{Stream as StreamProto, StreamServer},
        Request, Response as ResponseProto, Trace as TraceProto,
    },
};
use graph_store_postgres::TRACING_CONTROL;
use tonic::{async_trait, Status};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ResponseProto, Status>> + Send>>;

#[async_trait]
impl StreamProto for TracingServer {
    type QueryTraceStream = ResponseStream;

    async fn query_trace(
        &self,
        request: tonic::Request<Request>,
    ) -> std::result::Result<tonic::Response<Self::QueryTraceStream>, tonic::Status> {
        let Request { deployment_id } = request.into_inner();

        let rx = TRACING_CONTROL
            .subscribe(graph::components::store::DeploymentId(deployment_id))
            .await;

        fn query_traces(deployment_id: i32, trace: Trace, out: &mut Vec<TraceProto>) {
            match trace {
                Trace::Root {
                    query,
                    variables: _,
                    query_id: _,
                    setup: _,
                    elapsed,
                    query_parsing: _,
                    blocks: _,
                } => out.push(TraceProto {
                    deployment_id,
                    query: query.to_string(),
                    duration_millis: elapsed.as_millis() as u64,
                    children: 0,
                    conn_wait_millis: None,
                    permit_wait_millis: None,
                    entity_count: None,
                }),
                Trace::Query {
                    query,
                    elapsed,
                    conn_wait,
                    permit_wait,
                    entity_count,
                    children,
                } => {
                    out.push(TraceProto {
                        deployment_id,
                        query: query.to_string(),
                        duration_millis: elapsed.as_millis() as u64,
                        children: children.len() as u32,
                        conn_wait_millis: Some(conn_wait.as_millis() as u64),
                        permit_wait_millis: Some(permit_wait.as_millis() as u64),
                        entity_count: Some(entity_count as u64),
                    });

                    for (_key, child) in children {
                        query_traces(deployment_id, child, out);
                    }
                }
                _ => return,
            }
        }

        let stream: Pin<Box<dyn Stream<Item = Result<ResponseProto, tonic::Status>> + Send>> =
            unfold(rx, move |mut rx| async move {
                rx.recv().await.and_then(|trace| {
                    let mut traces = vec![];
                    query_traces(deployment_id, trace, &mut traces);

                    if traces.is_empty() {
                        None
                    } else {
                        Some((ResponseProto { traces }, rx))
                    }
                })
            })
            .map(Ok)
            .boxed();

        Ok(tonic::Response::new(stream as Self::QueryTraceStream))
    }
}

pub async fn start(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("[::]:{}", port).parse()?;
    println!("gRPC server listening on {}", addr);
    tonic::transport::Server::builder()
        .add_service(StreamServer::new(TracingServer))
        .serve(addr)
        .await?;

    Ok(())
}
