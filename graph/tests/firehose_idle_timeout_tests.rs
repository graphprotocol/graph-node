// Regression tests for graphprotocol/graph-node#6689.
//
// The firehose block-stream receive loop (graph/src/blockchain/firehose_block_stream.rs)
// had no per-message idle/read timeout: if the upstream holds the HTTP/2 stream open but
// stops sending frames (no message, no error, no EOF), the subgraph would hang indexing
// indefinitely and silently until a manual restart/reassignment.
//
// The fix wraps each wait for the next stream message in a configurable idle timeout
// (GRAPH_FIREHOSE_STREAM_IDLE_TIMEOUT_SECS, disabled by default). These tests exercise the
// two new building blocks: the timeout application and the env-var parsing.

use graph::blockchain::firehose_block_stream::{idle_timeout_from_env, next_with_idle_timeout};
use std::env::VarError;
use std::time::Duration;

#[test]
fn idle_timeout_from_env_parsing() {
    // Missing / unparseable / zero values disable the timeout.
    assert_eq!(idle_timeout_from_env(Err(VarError::NotPresent)), None);
    assert_eq!(
        idle_timeout_from_env(Err(VarError::NotUnicode("x".into()))),
        None
    );
    assert_eq!(idle_timeout_from_env(Ok("not-a-number".to_string())), None);
    assert_eq!(idle_timeout_from_env(Ok("0".to_string())), None);
    assert_eq!(idle_timeout_from_env(Ok(" 0 ".to_string())), None);

    // Positive values enable the timeout.
    assert_eq!(
        idle_timeout_from_env(Ok("30".to_string())),
        Some(Duration::from_secs(30))
    );
    assert_eq!(
        idle_timeout_from_env(Ok(" 5 ".to_string())),
        Some(Duration::from_secs(5))
    );
}

#[graph::test]
async fn stream_idle_timeout_returns_item_within_deadline() {
    let (tx, rx) = tokio::sync::mpsc::channel::<i32>(1);
    let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

    tx.send(1).await.unwrap();
    assert_eq!(
        next_with_idle_timeout(&mut rx, Some(Duration::from_millis(100))).await,
        Ok(Some(1))
    );
}

#[graph::test]
async fn stream_idle_timeout_breaks_stalled_stream() {
    let (_tx, rx) = tokio::sync::mpsc::channel::<i32>(1);
    let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

    // No message arrives within the idle timeout: the receive loop must not hang
    // indefinitely on a stalled upstream (issue #6689).
    assert_eq!(
        next_with_idle_timeout(&mut rx, Some(Duration::from_millis(50))).await,
        Err(())
    );
}

#[graph::test]
async fn stream_idle_timeout_returns_none_when_stream_ends() {
    let (tx, rx) = tokio::sync::mpsc::channel::<i32>(1);
    let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

    drop(tx);
    assert_eq!(
        next_with_idle_timeout(&mut rx, Some(Duration::from_millis(100))).await,
        Ok(None)
    );
}

#[graph::test]
async fn stream_idle_timeout_disabled_waits_for_item() {
    let (tx, rx) = tokio::sync::mpsc::channel::<i32>(1);
    let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

    // With no idle timeout the helper must wait unconditionally for the next item;
    // this is the default, backward-compatible behavior.
    tx.send(7).await.unwrap();
    assert_eq!(
        next_with_idle_timeout(&mut rx, None).await,
        Ok(Some(7))
    );
}
