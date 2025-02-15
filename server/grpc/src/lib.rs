pub struct TracingServer;

use std::pin::Pin;

use graph::{
    futures03::Stream,
    grpc::pb::graph::tracing::v1::{
        stream_server::{Stream as StreamProto, StreamServer},
        Request, Trace as TraceProto,
    },
    tokio_stream::wrappers::ReceiverStream,
};
use graph_store_postgres::TRACING_CONTROL;
use tonic::{async_trait, Status};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TraceProto, Status>> + Send>>;

#[async_trait]
impl StreamProto for TracingServer {
    type QueryTraceStream = ResponseStream;

    async fn query_trace(
        &self,
        request: tonic::Request<Request>,
    ) -> std::result::Result<tonic::Response<Self::QueryTraceStream>, tonic::Status> {
        let Request { deployment_id } = request.into_inner();

        let mut rx = TRACING_CONTROL
            .subscribe(graph::components::store::DeploymentId(deployment_id))
            .await;

        // let stream: Pin<Box<dyn Stream<Item = _>>> = unfold(rx, |mut rx| async move {
        //     rx.recv().await.map(|trace| {
        //         let trace = match trace {
        //             graph::data::query::Trace::None => vec![],
        //             graph::data::query::Trace::Root {
        //                 query,
        //                 variables,
        //                 query_id,
        //                 setup,
        //                 elapsed,
        //                 query_parsing,
        //                 blocks,
        //             } => vec![],
        //             graph::data::query::Trace::Block {
        //                 block,
        //                 elapsed,
        //                 permit_wait,
        //                 children,
        //             } => vec![],
        //             graph::data::query::Trace::Query {
        //                 query,
        //                 elapsed,
        //                 conn_wait,
        //                 permit_wait,
        //                 entity_count,
        //                 children,
        //             } => vec![TraceProto {
        //                 deployment_id,
        //                 query,
        //                 duration_millis: elapsed.as_millis() as u64,
        //             }],
        //         };

        //         (trace, rx)
        //     })
        // })
        // .boxed();
        // .flatten();
        //
        let (tx, rx2) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(result) = rx.recv().await {
                let out = match result {
                    graph::data::query::Trace::None => continue,
                    graph::data::query::Trace::Root {
                        query: _,
                        variables: _,
                        query_id: _,
                        setup: _,
                        elapsed: _,
                        query_parsing: _,
                        blocks: _,
                    } => continue,
                    graph::data::query::Trace::Block {
                        block: _,
                        elapsed: _,
                        permit_wait: _,
                        children: _,
                    } => continue,
                    graph::data::query::Trace::Query {
                        query,
                        elapsed,
                        conn_wait: _,
                        permit_wait: _,
                        entity_count: _,
                        children: _,
                    } => TraceProto {
                        deployment_id,
                        query,
                        duration_millis: elapsed.as_millis() as u64,
                    },
                };

                tx.send(Ok(out)).await.unwrap();
            }
            println!("\tstream ended");
        });

        let out_stream = ReceiverStream::new(rx2);

        Ok(tonic::Response::new(
            Box::pin(out_stream) as Self::QueryTraceStream
        ))
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
