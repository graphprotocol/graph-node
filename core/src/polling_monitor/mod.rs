pub mod ipfs_service;
mod metrics;

use std::fmt::Display;
use std::sync::Arc;

use futures::stream::StreamExt;
use futures::{stream, FutureExt};
use graph::cheap_clone::CheapClone;
use graph::parking_lot::Mutex;
use graph::prelude::tokio;
use graph::prometheus::{Counter, Gauge};
use graph::slog::{debug, Logger};
use graph::util::monitored::MonitoredVecDeque as VecDeque;
use tokio::sync::{mpsc, watch};
use tower::{Service, ServiceExt};

pub use self::metrics::PollingMonitorMetrics;

// A queue that notifies `waker` whenever an element is pushed.
struct Queue<T> {
    queue: Mutex<VecDeque<T>>,
    waker: watch::Sender<()>,
}

impl<T> Queue<T> {
    fn new(depth: Gauge, popped: Counter) -> (Arc<Self>, watch::Receiver<()>) {
        let queue = Mutex::new(VecDeque::new(depth, popped));
        let (waker, woken) = watch::channel(());
        let this = Queue { queue, waker };
        (Arc::new(this), woken)
    }

    fn push_back(&self, e: T) {
        self.queue.lock().push_back(e);
        let _ = self.waker.send(());
    }

    fn push_front(&self, e: T) {
        self.queue.lock().push_front(e);
        let _ = self.waker.send(());
    }

    fn pop_front(&self) -> Option<T> {
        self.queue.lock().pop_front()
    }
}

/// Spawn a monitor that actively polls a service. Whenever the service has capacity, the monitor
/// pulls object ids from the queue and polls the service. If the object is not present or in case
/// of error, the object id is pushed to the back of the queue to be polled again.
///
/// The service returns the request ID along with errors or responses. The response is an
/// `Option`, to represent the object not being found.
pub fn spawn_monitor<ID, S, E, Response: Send + 'static>(
    service: S,
    response_sender: mpsc::Sender<(ID, Response)>,
    logger: Logger,
    metrics: PollingMonitorMetrics,
) -> PollingMonitor<ID>
where
    ID: Display + Send + 'static,
    S: Service<ID, Response = (ID, Option<Response>), Error = (ID, E)> + Send + 'static,
    E: Display + Send + 'static,
    S::Future: Send,
{
    let (queue, queue_woken) = Queue::new(metrics.queue_depth.clone(), metrics.requests.clone());

    let cancel_check = response_sender.clone();
    let queue_to_stream = {
        let queue = queue.cheap_clone();
        stream::unfold((), move |()| {
            let queue = queue.cheap_clone();
            let mut queue_woken = queue_woken.clone();
            let cancel_check = cancel_check.clone();
            async move {
                loop {
                    if cancel_check.is_closed() {
                        break None;
                    }

                    let id = queue.pop_front();
                    match id {
                        Some(id) => break Some((id, ())),

                        // Nothing on the queue, wait for a queue wake up or cancellation.
                        None => {
                            futures::future::select(
                                // Unwrap: `queue` holds a sender.
                                queue_woken.changed().map(|r| r.unwrap()).boxed(),
                                cancel_check.closed().boxed(),
                            )
                            .await;
                        }
                    }
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
                    Ok((id, None)) => {
                        metrics.not_found.inc();
                        queue.push_back(id);
                    }

                    // Error polling, log it and push the id to the back of the queue.
                    Err((id, e)) => {
                        debug!(logger, "error polling";
                                    "error" => format!("{:#}", e),
                                    "object_id" => id.to_string());
                        metrics.errors.inc();
                        queue.push_back(id);
                    }
                }
            }
        });
    }

    PollingMonitor { queue }
}

/// Handle for adding objects to be monitored.
pub struct PollingMonitor<ID> {
    queue: Arc<Queue<ID>>,
}

impl<ID> PollingMonitor<ID> {
    /// Add an object id to the polling queue. New requests have priority and are pushed to the
    /// front of the queue.
    pub fn monitor(&self, id: ID) {
        self.queue.push_front(id);
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

    fn setup() -> (
        mock::Handle<&'static str, Option<&'static str>>,
        PollingMonitor<&'static str>,
        mpsc::Receiver<(&'static str, &'static str)>,
    ) {
        let (svc, handle) = mock::pair();
        let (tx, rx) = mpsc::channel(10);
        let monitor = spawn_monitor(
            MockService(svc),
            tx,
            log::discard(),
            PollingMonitorMetrics::mock(),
        );
        (handle, monitor, rx)
    }

    #[tokio::test]
    async fn polling_monitor_simple() {
        let (mut handle, monitor, mut rx) = setup();

        // Basic test, single file is immediately available.
        monitor.monitor("req-0");
        send_response(&mut handle, Some("res-0")).await;
        assert_eq!(rx.recv().await, Some(("req-0", "res-0")));
    }

    #[tokio::test]
    async fn polling_monitor_unordered() {
        let (mut handle, monitor, mut rx) = setup();

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
        let (mut handle, monitor, mut rx) = setup();

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
        // Cancelation on receiver drop, no pending request.
        let (mut handle, _monitor, rx) = setup();
        drop(rx);
        assert!(handle.next_request().await.is_none());

        // Cancelation on receiver drop, with pending request.
        let (mut handle, monitor, rx) = setup();
        monitor.monitor("req-0");
        drop(rx);
        assert!(handle.next_request().await.is_none());

        // Cancelation on receiver drop, while queue is waiting.
        let (mut handle, _monitor, rx) = setup();
        let handle = tokio::spawn(async move { handle.next_request().await });
        tokio::task::yield_now().await;
        drop(rx);
        assert!(handle.await.unwrap().is_none());
    }
}
