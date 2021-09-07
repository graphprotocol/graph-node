// Version 1 request

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockRequest {
    /// Number of blocks we want to get in burst upon connection, on a best effort basis.
    #[prost(int64, tag = "1")]
    pub burst: i64,
    /// Type of blocks we're after here, is it 'ethereum' data, 'eos', etc.. The server can fail early
    /// if he doesn't match the data he serves (services mismatch, etc..)
    #[prost(string, tag = "2")]
    pub content_type: ::prost::alloc::string::String,
    #[prost(enumeration = "block_request::Order", tag = "3")]
    pub order: i32,
    #[prost(string, tag = "4")]
    pub requester: ::prost::alloc::string::String,
}
/// Nested message and enum types in `BlockRequest`.
pub mod block_request {
    /// Whether we can assume the data will come ordered, unless there is a chain reorganization.
    /// mindreaders output ordered data, whereas relayers can output unordered data.
    /// The server can fail early if the assumption of the caller cannot be fulfilled.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Order {
        Unspecified = 0,
        Ordered = 1,
        Unordered = 2,
    }
}
// Version 2 request

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IrreversibleBlocksRequestV2 {
    #[prost(int64, tag = "1")]
    pub start_block_num: i64,
}
/// For historical segments, forks are not passed
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlocksRequestV2 {
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
    /// Obtain this value from a previously received BlockResponseV2.cursor.
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
    /// **Warning** Experimental API, controls how blocks are trimmed for extraneous information before
    /// being sent back. The actual trimming is chain dependent.
    #[prost(enumeration = "BlockDetails", tag = "15")]
    pub details: i32,
    /// controls how many confirmations will consider a given block as final (STEP_IRREVERSIBLE). Warning, if any reorg goes beyond that number of confirmations, the request will stall forever
    #[prost(uint64, tag = "16")]
    pub confirmations: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockResponseV2 {
    /// Chain specific block payload, one of:
    /// - dfuse.eosio.codec.v1.Block
    /// - dfuse.ethereum.codec.v1.Block
    /// - sf.near.codec.v1.Block
    /// - sf.solana.codec.v1.Block
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<::prost_types::Any>,
    #[prost(enumeration = "ForkStep", tag = "6")]
    pub step: i32,
    #[prost(string, tag = "10")]
    pub cursor: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Cursor {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<BlockRef>,
    #[prost(message, optional, tag = "2")]
    pub head_block: ::core::option::Option<BlockRef>,
    #[prost(message, optional, tag = "3")]
    pub lib: ::core::option::Option<BlockRef>,
    #[prost(enumeration = "ForkStep", tag = "4")]
    pub step: i32,
}
// General response and structs

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(uint64, tag = "1")]
    pub number: u64,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub previous_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(uint64, tag = "5")]
    pub lib_num: u64,
    #[prost(enumeration = "Protocol", tag = "6")]
    pub payload_kind: i32,
    #[prost(int32, tag = "7")]
    pub payload_version: i32,
    #[prost(bytes = "vec", tag = "8")]
    pub payload_buffer: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockRef {
    #[prost(uint64, tag = "1")]
    pub num: u64,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockDetails {
    Full = 0,
    Light = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Protocol {
    Unknown = 0,
    Eos = 1,
    Eth = 2,
    Solana = 3,
    Near = 4,
}
#[doc = r" Generated client implementations."]
pub mod block_stream_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct BlockStreamClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl BlockStreamClient<tonic::transport::Channel> {
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
    impl<T> BlockStreamClient<T>
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
        ) -> BlockStreamClient<InterceptedService<T, F>>
        where
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            BlockStreamClient::new(InterceptedService::new(inner, interceptor))
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
            request: impl tonic::IntoRequest<super::BlockRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::Block>>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/dfuse.bstream.v1.BlockStream/Blocks");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod block_stream_v2_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct BlockStreamV2Client<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl BlockStreamV2Client<tonic::transport::Channel> {
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
    impl<T> BlockStreamV2Client<T>
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
        ) -> BlockStreamV2Client<InterceptedService<T, F>>
        where
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            BlockStreamV2Client::new(InterceptedService::new(inner, interceptor))
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
            request: impl tonic::IntoRequest<super::BlocksRequestV2>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::BlockResponseV2>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/dfuse.bstream.v1.BlockStreamV2/Blocks");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod block_stream_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with BlockStreamServer."]
    #[async_trait]
    pub trait BlockStream: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Blocks method."]
        type BlocksStream: futures_core::Stream<Item = Result<super::Block, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn blocks(
            &self,
            request: tonic::Request<super::BlockRequest>,
        ) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct BlockStreamServer<T: BlockStream> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: BlockStream> BlockStreamServer<T> {
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
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for BlockStreamServer<T>
    where
        T: BlockStream,
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
                "/dfuse.bstream.v1.BlockStream/Blocks" => {
                    #[allow(non_camel_case_types)]
                    struct BlocksSvc<T: BlockStream>(pub Arc<T>);
                    impl<T: BlockStream> tonic::server::ServerStreamingService<super::BlockRequest> for BlocksSvc<T> {
                        type Response = super::Block;
                        type ResponseStream = T::BlocksStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BlockRequest>,
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
    impl<T: BlockStream> Clone for BlockStreamServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: BlockStream> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: BlockStream> tonic::transport::NamedService for BlockStreamServer<T> {
        const NAME: &'static str = "dfuse.bstream.v1.BlockStream";
    }
}
#[doc = r" Generated server implementations."]
pub mod block_stream_v2_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with BlockStreamV2Server."]
    #[async_trait]
    pub trait BlockStreamV2: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Blocks method."]
        type BlocksStream: futures_core::Stream<Item = Result<super::BlockResponseV2, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn blocks(
            &self,
            request: tonic::Request<super::BlocksRequestV2>,
        ) -> Result<tonic::Response<Self::BlocksStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct BlockStreamV2Server<T: BlockStreamV2> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: BlockStreamV2> BlockStreamV2Server<T> {
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
            F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for BlockStreamV2Server<T>
    where
        T: BlockStreamV2,
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
                "/dfuse.bstream.v1.BlockStreamV2/Blocks" => {
                    #[allow(non_camel_case_types)]
                    struct BlocksSvc<T: BlockStreamV2>(pub Arc<T>);
                    impl<T: BlockStreamV2>
                        tonic::server::ServerStreamingService<super::BlocksRequestV2>
                        for BlocksSvc<T>
                    {
                        type Response = super::BlockResponseV2;
                        type ResponseStream = T::BlocksStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BlocksRequestV2>,
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
    impl<T: BlockStreamV2> Clone for BlockStreamV2Server<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: BlockStreamV2> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: BlockStreamV2> tonic::transport::NamedService for BlockStreamV2Server<T> {
        const NAME: &'static str = "dfuse.bstream.v1.BlockStreamV2";
    }
}
