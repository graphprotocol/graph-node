/// For historical segments, forks are not passed
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    /// Controls where the stream of blocks will start.
    ///
    /// The stream will start **inclusively** at the requested block num.
    ///
    /// When not provided, starts at first streamable block of the chain. Not all
    /// chain starts at the same block number, so you might get an higher block than
    /// requested when using default value of 0.
    ///
    /// Can be negative, will be resolved relative to the chain head block, assuming
    /// a chain at head block #100, then using `-50` as the value will start at block
    /// #50. If it resolves before first streamable block of chain, we assume start
    /// of chain.
    ///
    /// If `start_cursor` is passed, this value is ignored and the stream instead starts
    /// immediately after the Block pointed by the opaque `start_cursor` value.
    #[prost(int64, tag = "1")]
    pub start_block_num: i64,
    /// Controls where the stream of blocks will start which will be immediately after
    /// the Block pointed by this opaque cursor.
    ///
    /// Obtain this value from a previously received from `Response.cursor`.
    ///
    /// This value takes precedence over `start_block_num`.
    #[prost(string, tag = "13")]
    pub start_cursor: ::prost::alloc::string::String,
    /// When non-zero, controls where the stream of blocks will stop.
    ///
    /// The stream will close **after** that block has passed so the boundary is
    /// **inclusive**.
    #[prost(uint64, tag = "5")]
    pub stop_block_num: u64,
    /// Filter the steps you want to see. If not specified, defaults to all steps.
    ///
    /// Most common steps will be [STEP_IRREVERSIBLE], or [STEP_NEW, STEP_UNDO, STEP_IRREVERSIBLE].
    #[prost(enumeration = "ForkStep", repeated, tag = "8")]
    pub fork_steps: ::prost::alloc::vec::Vec<i32>,
    /// The CEL filter expression used to include transactions, specific to the target protocol,
    /// works in combination with `exclude_filter_expr` value.
    #[prost(string, tag = "10")]
    pub include_filter_expr: ::prost::alloc::string::String,
    /// The CEL filter expression used to exclude transactions, specific to the target protocol, works
    /// in combination with `include_filter_expr` value.
    #[prost(string, tag = "11")]
    pub exclude_filter_expr: ::prost::alloc::string::String,
    ///- EOS "handoffs:3"
    ///- EOS "lib"
    ///- EOS "confirms:3"
    ///- ETH "confirms:200"
    ///- ETH "confirms:7"
    ///- SOL "commmitement:finalized"
    ///- SOL "confirms:200"
    #[prost(string, tag = "17")]
    pub irreversibility_condition: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "18")]
    pub transforms: ::prost::alloc::vec::Vec<::prost_types::Any>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    /// Chain specific block payload, one of:
    /// - sf.eosio.codec.v1.Block
    /// - sf.ethereum.codec.v1.Block
    /// - sf.near.codec.v1.Block
    /// - sf.solana.codec.v1.Block
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<::prost_types::Any>,
    #[prost(enumeration = "ForkStep", tag = "6")]
    pub step: i32,
    #[prost(string, tag = "10")]
    pub cursor: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ForkStep {
    StepUnknown = 0,
    /// Block is new head block of the chain, that is linear with the previous block
    StepNew = 1,
    /// Block is now forked and should be undone, it's not the head block of the chain anymore
    StepUndo = 2,
    /// Block is now irreversible and can be committed to (finality is chain specific, see chain documentation for more details)
    StepIrreversible = 4,
}
/// TODO: move to ethereum specific transforms
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockDetails {
    Full = 0,
    Light = 1,
}
#[doc = r" Generated client implementations."]
pub mod stream_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct StreamClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl StreamClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> StreamClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> StreamClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            StreamClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn blocks(
            &mut self,
            request: impl tonic::IntoRequest<super::Request>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::Response>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/sf.firehose.v1.Stream/Blocks");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod stream_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with StreamServer."]
    #[async_trait]
    pub trait Stream: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Blocks method."]
        type BlocksStream: futures_core::Stream<Item = Result<super::Response, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn blocks(
            &self,
            request: tonic::Request<super::Request>,
        ) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct StreamServer<T: Stream> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Stream> StreamServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for StreamServer<T>
    where
        T: Stream,
        B: Body + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/sf.firehose.v1.Stream/Blocks" => {
                    #[allow(non_camel_case_types)]
                    struct BlocksSvc<T: Stream>(pub Arc<T>);
                    impl<T: Stream> tonic::server::ServerStreamingService<super::Request> for BlocksSvc<T> {
                        type Response = super::Response;
                        type ResponseStream = T::BlocksStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Request>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).blocks(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BlocksSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Stream> Clone for StreamServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Stream> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Stream> tonic::transport::NamedService for StreamServer<T> {
        const NAME: &'static str = "sf.firehose.v1.Stream";
    }
}
