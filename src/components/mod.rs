use futures::prelude::*;

pub mod store;

pub trait EventConsumer<E> {
    fn event_sink(&self) -> Box<dyn Sink<SinkItem = E, SinkError = ()> + Send>;
}

pub trait EventProducer<E> {
    fn take_event_stream(&mut self) -> Option<Box<dyn Stream<Item = E, Error = ()> + Send>>;
}
