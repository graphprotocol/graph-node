use futures::future::IntoFuture;
use futures::stream::SplitStream;
use futures::sync::mpsc;
use graphql_parser::parse_query;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

use graph::prelude::*;
use graph::serde_json;

#[derive(Debug, Deserialize, Serialize)]
struct StartPayload {
    query: String,
    variables: Option<serde_json::Value>,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
}

/// GraphQL/WebSocket message received from a client.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum IncomingMessage {
    #[serde(rename = "connection_init")]
    ConnectionInit { payload: Option<serde_json::Value> },
    #[serde(rename = "connection_terminate")]
    ConnectionTerminate,
    #[serde(rename = "start")]
    Start { id: String, payload: StartPayload },
    #[serde(rename = "stop")]
    Stop { id: String },
}

impl IncomingMessage {
    pub fn from_ws_message(msg: WsMessage) -> Result<Self, WsError> {
        let text = msg.into_text()?;
        serde_json::from_str(text.as_str()).map_err(|e| {
            WsError::Protocol(
                format!("Invalid GraphQL over WebSocket message: {}: {}", text, e).into(),
            )
        })
    }
}

/// GraphQL/WebSocket message to be sent to the client.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum OutgoingMessage {
    #[serde(rename = "connection_ack")]
    ConnectionAck,
    #[serde(rename = "error")]
    Error { id: String, payload: String },
    #[serde(rename = "data")]
    Data { id: String, payload: QueryResult },
    #[serde(rename = "complete")]
    Complete { id: String },
}

impl OutgoingMessage {
    pub fn from_query_result(id: String, result: QueryResult) -> Self {
        OutgoingMessage::Data {
            id: id,
            payload: result,
        }
    }

    pub fn from_error_string(id: String, s: String) -> Self {
        OutgoingMessage::Error { id, payload: s }
    }
}

impl From<OutgoingMessage> for WsMessage {
    fn from(msg: OutgoingMessage) -> Self {
        WsMessage::text(serde_json::to_string(&msg).expect("invalid GraphQL/WebSocket message"))
    }
}

/// A WebSocket connection implementing the GraphQL over WebSocket protocol.
pub struct GraphQlConnection<Q, S> {
    id: String,
    logger: Logger,
    graphql_runner: Arc<Q>,
    stream: WebSocketStream<S>,
    subgraphs: Arc<RwLock<SubgraphRegistry<Schema>>>,
    subgraph: String,
}

impl<Q, S> GraphQlConnection<Q, S>
where
    Q: GraphQLRunner + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    /// Creates a new GraphQL subscription service.
    pub fn new(
        logger: &Logger,
        subgraphs: Arc<RwLock<SubgraphRegistry<Schema>>>,
        subgraph: String,
        stream: WebSocketStream<S>,
        graphql_runner: Arc<Q>,
    ) -> Self {
        GraphQlConnection {
            id: Uuid::new_v4().to_string(),
            logger: logger.new(o!("component" => "GraphQlConnection")),
            graphql_runner,
            stream,
            subgraphs,
            subgraph,
        }
    }

    fn handle_incoming_messages(
        ws_stream: SplitStream<WebSocketStream<S>>,
        mut msg_sink: mpsc::UnboundedSender<WsMessage>,
        logger: Logger,
        id: String,
        subgraphs: Arc<RwLock<SubgraphRegistry<Schema>>>,
        subgraph: String,
        graphql_runner: Arc<Q>,
    ) -> impl Future<Item = (), Error = WsError> {
        // Set up a mapping of subscription IDs to GraphQL subscriptions
        let operations: Arc<Mutex<HashMap<String, Subscription>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Helper function to send outgoing messages
        let send_message = |sink: &mpsc::UnboundedSender<WsMessage>, msg: OutgoingMessage| {
            sink.unbounded_send(msg.into())
                .map_err(|_| WsError::Http(500))
        };

        // Helper function to send error messages
        let send_error_string = |sink: &mpsc::UnboundedSender<WsMessage>, id, s| {
            sink.unbounded_send(OutgoingMessage::from_error_string(id, s).into())
                .map_err(|_| WsError::Http(500))
        };

        // Process incoming messages as long as the WebSocket is open
        ws_stream.for_each(move |ws_msg| {
            use self::IncomingMessage::*;
            use self::OutgoingMessage::*;

            debug!(logger, "Received message";
                   "connection" => &id,
                   "msg" => format!("{}", ws_msg).as_str());

            let msg = IncomingMessage::from_ws_message(ws_msg.clone())?;

            debug!(logger, "GraphQL/WebSocket message";
                   "connection" => &id,
                   "msg" => format!("{:?}", msg).as_str());

            match msg {
                // Always accept connection init requests
                ConnectionInit { payload: _ } => send_message(&msg_sink, ConnectionAck),

                // When receiving a connection termination request
                ConnectionTerminate => {
                    // Close the message sink
                    msg_sink.close().unwrap();

                    // Return an error here to terminate the connection
                    Err(WsError::ConnectionClosed(None))
                }

                // When receiving a stop request
                Stop { id } => {
                    // Remove the operation with this ID from the known operations
                    if let None = operations.lock().unwrap().remove(&id) {
                        return send_error_string(
                            &msg_sink,
                            id.clone(),
                            format!("Unknown operation ID: {}", id),
                        );
                    }

                    // TODO: Close any streams we need to close, without terminating the
                    // connection itself.
                    //
                    // How can we achieve this? Should we use a close channel and then
                    // `select` between the query result stream and the close oneshot
                    // to terminate the subscription?

                    // Send a GQL_COMPLETE to indicate the operation is been completed
                    send_message(&msg_sink, Complete { id: id.clone() })
                }

                // When receiving a start request
                Start { id, payload } => {
                    // Respond with a GQL_ERROR if we already have an operation with this ID
                    if operations.lock().unwrap().contains_key(&id) {
                        return send_error_string(
                            &msg_sink,
                            id.clone(),
                            format!("Operation with ID already started: {}", id),
                        );
                    }

                    // Respond with a GQL_ERROR if the subgraph name or ID is unknown
                    let subgraphs = subgraphs.read().unwrap();
                    let schema = if let Some(schema) = subgraphs.resolve(&subgraph) {
                        schema
                    } else {
                        return send_error_string(
                            &msg_sink,
                            id.clone(),
                            format!("Unknown subgraph name or ID: {}", subgraph),
                        );
                    };

                    // Parse the GraphQL query document; respond with a GQL_ERROR if
                    // the query is invalid
                    let query = match parse_query(&payload.query) {
                        Ok(query) => query,
                        Err(e) => {
                            return send_error_string(
                                &msg_sink,
                                id.clone(),
                                format!("Invalid query: {}: {}", payload.query, e),
                            );
                        }
                    };

                    // TODO Parse query variables and operation name

                    // Construct a subscription
                    let subscription = Subscription {
                        query: Query {
                            schema: schema.clone(),
                            document: query,
                            variables: None,
                        },
                    };

                    // Remember this subscription
                    operations
                        .lock()
                        .unwrap()
                        .insert(id.clone(), subscription.clone());

                    // Execute the GraphQL subscription
                    let graphql_runner = graphql_runner.clone();
                    let error_sink = msg_sink.clone();
                    let result_sink = msg_sink.clone();
                    let result_id = id.clone();
                    let err_id = id.clone();
                    tokio::spawn(
                        graphql_runner
                            .run_subscription(subscription)
                            .map_err(move |e| {
                                // Send errors back to the client as GQL_DATA
                                match e {
                                    SubscriptionError::GraphQLError(e) => {
                                        let result = QueryResult::from(e);
                                        let msg = OutgoingMessage::from_query_result(
                                            err_id.clone(),
                                            result,
                                        );
                                        error_sink.unbounded_send(msg.into()).unwrap();
                                    }
                                };
                            })
                            .and_then(move |result| {
                                // Send results back to the client as GQL_DATA
                                result
                                    .stream
                                    .map(move |result| {
                                        OutgoingMessage::from_query_result(
                                            result_id.clone(),
                                            result,
                                        )
                                    })
                                    .map(WsMessage::from)
                                    .forward(result_sink.sink_map_err(|_| ()))
                            })
                            .and_then(|_| Ok(())),
                    );

                    Ok(())
                }
            }
        })
    }
}

impl<Q, S> IntoFuture for GraphQlConnection<Q, S>
where
    Q: GraphQLRunner + 'static,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    type Future = Box<Future<Item = Self::Item, Error = Self::Error> + Send>;
    type Item = ();
    type Error = ();

    fn into_future(self) -> Self::Future {
        debug!(self.logger, "GraphQL over WebSocket connection opened"; "id" => &self.id);

        // Obtain sink/stream pair to send and receive WebSocket messages
        let (ws_sink, ws_stream) = self.stream.split();

        // Allocate a channel for writing
        let (msg_sink, msg_stream) = mpsc::unbounded();

        // Handle incoming messages asynchronously
        let ws_reader = Self::handle_incoming_messages(
            ws_stream,
            msg_sink,
            self.logger.clone(),
            self.id.clone(),
            self.subgraphs.clone(),
            self.subgraph.clone(),
            self.graphql_runner.clone(),
        );

        // Send outgoing messages asynchronously
        let logger = self.logger.clone();
        let id = self.id.clone();
        let ws_writer = msg_stream
            .inspect(move |msg| {
                debug!(logger, "Sending message";
                       "connection" => &id,
                       "msg" => format!("{}", msg).as_str());
            })
            .forward(ws_sink.sink_map_err(|_| ()));

        // Silently swallow internal send results and errors
        let ws_writer = ws_writer.map(|_| ()).map_err(|_| ());
        let ws_reader = ws_reader.map(|_| ()).map_err(|_| ());

        // Return a future that is fulfilled when either the reader or the writer are closed
        let logger = self.logger.clone();
        let id = self.id.clone();
        Box::new(ws_reader.select(ws_writer).then(move |_| {
            debug!(logger, "GraphQL over WebSocket connection closed"; "connection" => id);
            Ok(())
        }))
    }
}
