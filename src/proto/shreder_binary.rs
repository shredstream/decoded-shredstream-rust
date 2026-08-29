#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeBinaryTransactionsRequest {
    #[prost(map = "string, message", tag = "3")]
    pub transactions: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        SubscribeRequestFilterBinaryTransactions,
    >,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeBinaryTransactionsResponse {
    #[prost(string, repeated, tag = "1")]
    pub filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub transaction: ::core::option::Option<SubscribeUpdateBinaryTransaction>,
    #[prost(message, optional, tag = "11")]
    pub created_at: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeUpdateBinaryTransaction {
    #[prost(message, optional, tag = "1")]
    pub transaction: ::core::option::Option<BinaryTransaction>,
    #[prost(uint64, tag = "2")]
    pub slot: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeRequestFilterBinaryTransactions {
    #[prost(string, repeated, tag = "3")]
    pub account_include: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub account_exclude: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "6")]
    pub account_required: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryTransaction {
    #[prost(bytes = "bytes", repeated, tag = "1")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::bytes::Bytes>,
    #[prost(bytes = "bytes", tag = "3")]
    pub binary_transaction: ::prost::bytes::Bytes,
}
pub mod shreder_binary_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ShrederBinaryServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShrederBinaryServiceClient<tonic::transport::Channel> {
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShrederBinaryServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShrederBinaryServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            ShrederBinaryServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn subscribe_binary_transactions(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::SubscribeBinaryTransactionsRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<
                tonic::codec::Streaming<super::SubscribeBinaryTransactionsResponse>,
            >,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/shreder_binary.ShrederBinaryService/SubscribeBinaryTransactions",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "shreder_binary.ShrederBinaryService",
                        "SubscribeBinaryTransactions",
                    ),
                );
            self.inner.streaming(req, path, codec).await
        }
    }
}
pub mod shreder_binary_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    #[async_trait]
    pub trait ShrederBinaryService: std::marker::Send + std::marker::Sync + 'static {
        type SubscribeBinaryTransactionsStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<
                    super::SubscribeBinaryTransactionsResponse,
                    tonic::Status,
                >,
            >
            + std::marker::Send
            + 'static;
        async fn subscribe_binary_transactions(
            &self,
            request: tonic::Request<
                tonic::Streaming<super::SubscribeBinaryTransactionsRequest>,
            >,
        ) -> std::result::Result<
            tonic::Response<Self::SubscribeBinaryTransactionsStream>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ShrederBinaryServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> ShrederBinaryServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for ShrederBinaryServiceServer<T>
    where
        T: ShrederBinaryService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/shreder_binary.ShrederBinaryService/SubscribeBinaryTransactions" => {
                    #[allow(non_camel_case_types)]
                    struct SubscribeBinaryTransactionsSvc<T: ShrederBinaryService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: ShrederBinaryService,
                    > tonic::server::StreamingService<
                        super::SubscribeBinaryTransactionsRequest,
                    > for SubscribeBinaryTransactionsSvc<T> {
                        type Response = super::SubscribeBinaryTransactionsResponse;
                        type ResponseStream = T::SubscribeBinaryTransactionsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::SubscribeBinaryTransactionsRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ShrederBinaryService>::subscribe_binary_transactions(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SubscribeBinaryTransactionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for ShrederBinaryServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    pub const SERVICE_NAME: &str = "shreder_binary.ShrederBinaryService";
    impl<T> tonic::server::NamedService for ShrederBinaryServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
