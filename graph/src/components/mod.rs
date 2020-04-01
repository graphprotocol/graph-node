//! The Graph network nodes are internally structured as a layers of reusable
//! components with non-blocking communication, with each component having a
//! corresponding trait defining it's interface.
//!
//! As examples of components, at the top layer component there is the GraphQL
//! server which interacts with clients, and at the lowest layer we have data
//! sources that interact with storage backends.
//!
//! The layers are not well defined, but it's expected that a higher-level
//! component will make requests for a lower-level component to respond, and
//! that a lower-level component will send events to interested higher-level
//! components when it's state changes.
//!
//! A request/response interaction between C1 and C2 is made by C1 requiring an
//! `Arc<C2>` in it's constructor and then calling the functions defined on C2.
//!
//! Event-based interactions propagate changes in the underlining data upwards
//! in the component graph, with low level components generating event streams
//! based on changes in external systems, mid level components transforming
//! these streams and high level components finally consuming the received
//! events.
//!
//! These events are communicated through sinks and streams (typically senders
//! and receivers of channels), which are managed by long-running Tokio tasks.
//! Each component may have an internal task for handling input events and
//! sending out output events, and the "dumb pipes" that plug together components
//! are tasks that send out events in the order that they are received.
//!
//! A component declares it's inputs and outputs by having `EventConsumer<U>` and
//! `EventProducer<T>` traits as supertraits.
//!
//! Components should use the helper functions in this module (e.g. `forward`)
//! that define common operations on event streams, facilitating the
//! configuration of component graphs.

use futures::prelude::*;

/// Components dealing with subgraphs.
pub mod subgraph;

/// Components dealing with Ethereum.
pub mod ethereum;

pub mod arweave;

pub mod three_box;

/// Components dealing with processing GraphQL.
pub mod graphql;

/// Components powering GraphQL, JSON-RPC, WebSocket APIs, Metrics.
pub mod server;

/// Components dealing with storing entities.
pub mod store;

pub mod link_resolver;

/// Components dealing with collecting metrics
pub mod metrics;

/// Plug the outputs of `output` of type `E` to the matching inputs in `input`.
/// This is a lazy operation, nothing will be sent until you spawn the returned
/// future. Returns `Some` in the first call and `None` on any further calls.
///
/// See `Stream::forward` for details.
pub fn forward<E: Send, O: EventProducer<E>, I: EventConsumer<E>>(
    output: &mut O,
    input: &I,
) -> Option<impl Future<Item = (), Error = ()> + Send> {
    output
        .take_event_stream()
        .map(|stream| stream.forward(input.event_sink()).map(|_| ()))
}

/// Like `forward`, but forwards outputs to two components by cloning the
/// events. If you need more, create more versions of this or manipulate
/// `event_sink()` and `take_event_stream()` directly.
pub fn forward2<
    E: Clone + Send,
    O: EventProducer<E>,
    I1: EventConsumer<E>,
    I2: EventConsumer<E>,
>(
    output: &mut O,
    input1: &I1,
    input2: &I2,
) -> Option<impl Future<Item = (), Error = ()> + Send> {
    output.take_event_stream().map(|stream| {
        stream
            .forward(input1.event_sink().fanout(input2.event_sink()))
            .map(|_| ())
    })
}

/// A component that receives events of type `T`.
pub trait EventConsumer<E> {
    /// Get the event sink.
    ///
    /// Avoid calling directly, prefer helpers such as `forward`.
    fn event_sink(&self) -> Box<dyn Sink<SinkItem = E, SinkError = ()> + Send>;
}

/// A component that outputs events of type `T`.
pub trait EventProducer<E> {
    /// Get the event stream. Because we use single-consumer semantics, the
    /// first caller will take the output stream and any further calls will
    /// return `None`.
    ///
    /// Avoid calling directly, prefer helpers such as `forward`.
    fn take_event_stream(&mut self) -> Option<Box<dyn Stream<Item = E, Error = ()> + Send>>;
}
