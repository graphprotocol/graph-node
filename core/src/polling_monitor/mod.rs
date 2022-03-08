use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::Arc;

use futures::stream;
use futures::stream::StreamExt;
use graph::cheap_clone::CheapClone;
use graph::parking_lot::Mutex;
use graph::prelude::tokio;
use graph::slog::{debug, Logger};
use tokio::sync::{mpsc, watch};
use tower::{Service, ServiceExt};

/// Spawn a monitor that actively polls a service. Whenever the service has capacity, the monitor
/// pulls object ids from the queue and polls the service. If the object is not present or in case
/// of error, the object id is pushed to the back of the queue to be polled again.
///
/// The service returns the request ID along with errors or responses. The response is an
/// `Option`, to represent the object not being found. `S::poll_ready` should never error.
pub fn spawn_monitor<ID, S, E, Response: Send + 'static>(
    service: S,
    response_sender: mpsc::Sender<(ID, Response)>,
    logger: Logger,
) -> PollingMonitor<ID>
where
    ID: Display + Send + 'static,
    S: Service<ID, Response = (ID, Option<Response>), Error = (ID, E)> + Send + 'static,
    E: Display + Send + 'static,
    S::Future: Send,
{
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let (wake_up_queue, queue_woken) = watch::channel(());

    let queue_to_stream = {
        let queue = queue.cheap_clone();
        stream::unfold((), move |()| {
            let queue = queue.cheap_clone();
            let mut queue_woken = queue_woken.clone();
            async move {
                loop {
                    let id = queue.lock().pop_front();
                    match id {
                        Some(id) => break Some((id, ())),
                        None => match queue_woken.changed().await {
                            // Queue woken, check it.
                            Ok(()) => {}

                            // The `PollingMonitor` has been dropped, cancel this task.
                            Err(_) => break None,
                        },
                    };
                }
            }
        })
    };

    {
        let queue = queue.cheap_clone();
        graph::spawn(async move {
            let mut responses = service.call_all(queue_to_stream).unordered().boxed();
            while let Some(response) = responses.next().await {
                match response {
                    Ok((id, Some(response))) => {
                        let send_result = response_sender.send((id, response)).await;
                        if send_result.is_err() {
                            // The receiver has been dropped, cancel this task.
                            break;
                        }
                    }

                    // Object not found, push the id to the back of the queue.
                    Ok((id, None)) => queue.lock().push_back(id),

                    // Error polling, log it and push the id to the back of the queue.
                    Err((id, e)) => {
                        debug!(logger, "error polling";
                                    "error" => format!("{:#}", e),
                                    "object_id" => id.to_string());
                        queue.lock().push_back(id)
                    }
                }
            }
        });
    }

    PollingMonitor {
        queue,
        wake_up_queue,
    }
}

/// Handle for adding objects to be monitored.
pub struct PollingMonitor<ID> {
    queue: Arc<Mutex<VecDeque<ID>>>,

    // This serves two purposes, to wake up the monitor when an item arrives on an empty queue, and
    // to stop the montior task when this handle is dropped.
    wake_up_queue: watch::Sender<()>,
}

impl<ID> PollingMonitor<ID> {
    /// Add an object id to the polling queue. New requests have priority and are pushed to the
    /// front of the queue.
    pub fn monitor(&self, id: ID) {
        let mut queue = self.queue.lock();
        if queue.is_empty() {
            // If the send fails, the response receiver has been dropped, so this handle is useless.
            let _ = self.wake_up_queue.send(());
        }
        queue.push_front(id);
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use futures::{Future, FutureExt, TryFutureExt};
    use graph::log;
    use std::{pin::Pin, task::Poll};
    use tower_test::mock;

    use super::*;

    struct MockService(mock::Mock<&'static str, Option<&'static str>>);

    impl Service<&'static str> for MockService {
        type Response = (&'static str, Option<&'static str>);

        type Error = (&'static str, anyhow::Error);

        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.poll_ready(cx).map_err(|_| unreachable!())
        }

        fn call(&mut self, req: &'static str) -> Self::Future {
            self.0
                .call(req)
                .map_ok(move |x| (req, x))
                .map_err(move |e| (req, anyhow!(e.to_string())))
                .boxed()
        }
    }

    async fn send_response<T, U>(handle: &mut mock::Handle<T, U>, res: U) {
        handle.next_request().await.unwrap().1.send_response(res)
    }

    #[tokio::test]
    async fn polling_monitor_simple() {
        let (svc, mut handle) = mock::pair();
        let (tx, mut rx) = mpsc::channel(10);
        let monitor = spawn_monitor(MockService(svc), tx, log::discard());

        // Basic test, single file is immediately available.
        monitor.monitor("req-0");
        send_response(&mut handle, Some("res-0")).await;
        assert_eq!(rx.recv().await, Some(("req-0", "res-0")));
    }

    #[tokio::test]
    async fn polling_monitor_unordered() {
        let (svc, mut handle) = mock::pair();
        let (tx, mut rx) = mpsc::channel(10);
        let monitor = spawn_monitor(MockService(svc), tx, log::discard());

        // Test unorderedness of the response stream, and the LIFO semantics of `monitor`.
        //
        // `req-1` has priority since it is the last request, but `req-0` is responded first.
        monitor.monitor("req-0");
        monitor.monitor("req-1");
        let req_1 = handle.next_request().await.unwrap().1;
        let req_0 = handle.next_request().await.unwrap().1;
        req_0.send_response(Some("res-0"));
        assert_eq!(rx.recv().await, Some(("req-0", "res-0")));
        req_1.send_response(Some("res-1"));
        assert_eq!(rx.recv().await, Some(("req-1", "res-1")));
    }

    #[tokio::test]
    async fn polling_monitor_failed_push_to_back() {
        let (svc, mut handle) = mock::pair();
        let (tx, mut rx) = mpsc::channel(10);

        // Limit service to one request at a time.
        let svc = tower::limit::ConcurrencyLimit::new(MockService(svc), 1);
        let monitor = spawn_monitor(svc, tx, log::discard());

        // Test that objects not found go on the back of the queue.
        monitor.monitor("req-0");
        monitor.monitor("req-1");
        send_response(&mut handle, None).await;
        send_response(&mut handle, Some("res-0")).await;
        assert_eq!(rx.recv().await, Some(("req-0", "res-0")));
        send_response(&mut handle, Some("res-1")).await;
        assert_eq!(rx.recv().await, Some(("req-1", "res-1")));

        // Test that failed requests go on the back of the queue.
        monitor.monitor("req-0");
        monitor.monitor("req-1");
        let req = handle.next_request().await.unwrap().1;
        req.send_error(anyhow!("e"));
        send_response(&mut handle, Some("res-0")).await;
        assert_eq!(rx.recv().await, Some(("req-0", "res-0")));
        send_response(&mut handle, Some("res-1")).await;
        assert_eq!(rx.recv().await, Some(("req-1", "res-1")));
    }

    #[tokio::test]
    async fn polling_monitor_cancelation() {
        let (svc, _handle) = mock::pair();
        let (tx, mut rx) = mpsc::channel(10);
        let monitor = spawn_monitor(MockService(svc), tx, log::discard());

        // Cancelation on monitor drop.
        drop(monitor);
        assert_eq!(rx.recv().await, None);

        let (svc, mut handle) = mock::pair();
        let (tx, rx) = mpsc::channel(10);
        let monitor = spawn_monitor(MockService(svc), tx, log::discard());

        // Cancelation on receiver drop.
        monitor.monitor("req-0");
        drop(rx);
        send_response(&mut handle, Some("res-0")).await;
        assert!(handle.next_request().await.is_none());
    }
}
