// Copyright 2026 Tabsdata Inc.

//! Generic poll-with-backoff helper used by the secret and configmap
//! resolvers.
//!
//! The helper retries a fallible async operation as long as the error is
//! classified as retryable (e.g. "configmap not yet defined"), sleeping
//! between attempts on an exponential schedule that caps at 122 seconds:
//!
//! `0.5s, 1s, 2s, 4s, 8s, 16s, 32s, 64s, 122s, 122s, 122s, ...`
//!
//! The wait is bounded by an optional timeout (the pod's
//! `activeDeadlineSeconds`) and a cancellation signal (typically a
//! `watch::Receiver<()>` that fires on pod deletion or process shutdown).
//! When the timeout expires the last retryable error is returned wrapped
//! in [`PollError::Timeout`]; when cancellation fires
//! [`PollError::Cancelled`] is returned.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

/// Exponential backoff schedule. Nine ticks doubling from 500ms; the last
/// tick is capped at 122s (would otherwise be 128s). After the schedule
/// is exhausted the last value (122s) is used indefinitely until the
/// caller's timeout or cancel signal fires.
pub const BACKOFF_SCHEDULE: [Duration; 9] = [
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(32),
    Duration::from_secs(64),
    Duration::from_secs(122),
];

/// Return the backoff for the `attempt`-th retry (zero-indexed). Attempts
/// beyond the schedule clamp to the last value.
pub fn backoff_for_attempt(attempt: usize) -> Duration {
    BACKOFF_SCHEDULE[attempt.min(BACKOFF_SCHEDULE.len() - 1)]
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Outcome when [`poll_with_backoff`] does not produce a value.
#[derive(Debug, thiserror::Error)]
pub enum PollError<E: std::fmt::Display + std::fmt::Debug> {
    /// The total elapsed time exceeded the caller-supplied timeout. The
    /// last retryable error is returned for diagnostics.
    #[error("timed out after {elapsed:?}: {last_err}")]
    Timeout { last_err: E, elapsed: Duration },
    /// The cancel signal fired before a value was produced.
    #[error("cancelled after {elapsed:?}")]
    Cancelled { elapsed: Duration },
    /// The operation produced a non-retryable error. Returned without
    /// further retries.
    #[error("{0}")]
    Fatal(E),
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Run `op` repeatedly until it succeeds, the timeout expires, the cancel
/// signal fires, or `op` returns a non-retryable error.
///
/// `op_name` is used as the `op` field in the per-backoff log lines.
/// `is_retryable(&err)` is consulted on every error: when true the next
/// retry is scheduled; when false the error is returned immediately as
/// [`PollError::Fatal`].
///
/// Logging:
/// - At `info` on every backoff with the attempt number and sleep duration.
/// - At `warn` when the timeout fires.
pub async fn poll_with_backoff<T, E, F, Fut>(
    op_name: &str,
    timeout: Option<Duration>,
    cancel: &mut watch::Receiver<()>,
    is_retryable: impl Fn(&E) -> bool,
    mut op: F,
) -> Result<T, PollError<E>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display + std::fmt::Debug,
{
    let start = Instant::now();
    let mut attempt: usize = 0;
    // Holds the most recent retryable error, used for diagnostics on
    // Timeout. The initial `None` is never read — every read path goes
    // through the `Err(e) =>` arm below which assigns first.
    #[allow(unused_assignments)]
    let mut last_err: Option<E> = None;

    loop {
        // Race the attempt against cancellation. Timeout is enforced after
        // the attempt resolves so an in-flight call can still complete
        // even if it crosses the deadline; the next loop iteration will
        // observe the overrun and bail.
        let result = tokio::select! {
            biased;
            _ = cancel.changed() => {
                let elapsed = start.elapsed();
                debug!(op = op_name, ?elapsed, "Poll cancelled");
                return Err(PollError::Cancelled { elapsed });
            }
            r = op() => r,
        };

        match result {
            Ok(v) => return Ok(v),
            Err(e) if !is_retryable(&e) => return Err(PollError::Fatal(e)),
            Err(e) => {
                last_err = Some(e);

                let elapsed = start.elapsed();
                if let Some(t) = timeout
                    && elapsed >= t
                {
                    warn!(
                        op = op_name,
                        ?elapsed,
                        timeout_secs = t.as_secs_f64(),
                        last_error = %last_err.as_ref().unwrap(),
                        "Poll timed out",
                    );
                    return Err(PollError::Timeout {
                        last_err: last_err.unwrap(),
                        elapsed,
                    });
                }

                let base_delay = backoff_for_attempt(attempt);
                let sleep_dur = match timeout {
                    Some(t) => base_delay.min(t - elapsed),
                    None => base_delay,
                };

                warn!(
                    op = op_name,
                    attempt = attempt + 1,
                    next_retry_secs = sleep_dur.as_secs_f64(),
                    last_error = %last_err.as_ref().unwrap(),
                    "Resource not ready; backing off",
                );

                tokio::select! {
                    biased;
                    _ = cancel.changed() => {
                        let elapsed = start.elapsed();
                        debug!(op = op_name, ?elapsed, "Poll cancelled during backoff");
                        return Err(PollError::Cancelled { elapsed });
                    }
                    _ = tokio::time::sleep(sleep_dur) => {}
                }
                attempt += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Toy error type for tests. `retryable=true` simulates "resource not
    /// found yet"; `retryable=false` simulates a non-retryable failure
    /// (e.g. YAML parse error).
    #[derive(Debug)]
    struct ToyErr {
        retryable: bool,
        msg: String,
    }

    impl std::fmt::Display for ToyErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.msg)
        }
    }

    #[test]
    fn backoff_schedule_has_nine_entries() {
        assert_eq!(BACKOFF_SCHEDULE.len(), 9);
    }

    #[test]
    fn backoff_schedule_starts_at_half_second() {
        assert_eq!(BACKOFF_SCHEDULE[0], Duration::from_millis(500));
    }

    #[test]
    fn backoff_schedule_caps_at_122_seconds() {
        assert_eq!(*BACKOFF_SCHEDULE.last().unwrap(), Duration::from_secs(122));
    }

    #[test]
    fn backoff_schedule_doubles_until_cap() {
        assert_eq!(BACKOFF_SCHEDULE[1], Duration::from_secs(1));
        assert_eq!(BACKOFF_SCHEDULE[2], Duration::from_secs(2));
        assert_eq!(BACKOFF_SCHEDULE[3], Duration::from_secs(4));
        assert_eq!(BACKOFF_SCHEDULE[4], Duration::from_secs(8));
        assert_eq!(BACKOFF_SCHEDULE[5], Duration::from_secs(16));
        assert_eq!(BACKOFF_SCHEDULE[6], Duration::from_secs(32));
        assert_eq!(BACKOFF_SCHEDULE[7], Duration::from_secs(64));
    }

    #[test]
    fn backoff_for_attempt_clamps_beyond_schedule() {
        assert_eq!(backoff_for_attempt(0), Duration::from_millis(500));
        assert_eq!(backoff_for_attempt(8), Duration::from_secs(122));
        // Any attempt past the schedule re-uses the last (cap) value.
        assert_eq!(backoff_for_attempt(9), Duration::from_secs(122));
        assert_eq!(backoff_for_attempt(100), Duration::from_secs(122));
    }

    #[tokio::test(start_paused = true)]
    async fn returns_value_on_first_success() {
        let (_tx, mut rx) = watch::channel(());
        let result: Result<i32, _> = poll_with_backoff(
            "test",
            Some(Duration::from_secs(60)),
            &mut rx,
            |_| true,
            || async { Ok::<i32, ToyErr>(42) },
        )
        .await;
        assert!(matches!(result, Ok(42)));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        let (_tx, mut rx) = watch::channel(());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);

        let result: Result<i32, _> = poll_with_backoff(
            "test",
            Some(Duration::from_secs(3600)),
            &mut rx,
            |e: &ToyErr| e.retryable,
            move || {
                let calls = Arc::clone(&calls_inner);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < 3 {
                        Err(ToyErr {
                            retryable: true,
                            msg: format!("not ready (call {})", n),
                        })
                    } else {
                        Ok(7)
                    }
                }
            },
        )
        .await;

        assert!(matches!(result, Ok(7)));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_error_returns_immediately_without_retry() {
        let (_tx, mut rx) = watch::channel(());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);

        let result: Result<i32, _> = poll_with_backoff(
            "test",
            Some(Duration::from_secs(60)),
            &mut rx,
            |e: &ToyErr| e.retryable,
            move || {
                let calls = Arc::clone(&calls_inner);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(ToyErr {
                        retryable: false,
                        msg: "parse failure".to_string(),
                    })
                }
            },
        )
        .await;

        assert!(matches!(result, Err(PollError::Fatal(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_with_last_error() {
        let (_tx, mut rx) = watch::channel(());

        let result: Result<i32, _> = poll_with_backoff(
            "test",
            Some(Duration::from_secs(3)),
            &mut rx,
            |_| true,
            || async {
                Err(ToyErr {
                    retryable: true,
                    msg: "never ready".to_string(),
                })
            },
        )
        .await;

        match result {
            Err(PollError::Timeout { last_err, elapsed }) => {
                assert_eq!(last_err.msg, "never ready");
                assert!(elapsed >= Duration::from_secs(3));
            }
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_during_backoff_returns_cancelled() {
        let (tx, mut rx) = watch::channel(());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);

        let cancel_after = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = tx.send(());
        });

        let result: Result<i32, _> = poll_with_backoff(
            "test",
            None, // no timeout — only cancel can stop it
            &mut rx,
            |_| true,
            move || {
                let calls = Arc::clone(&calls_inner);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(ToyErr {
                        retryable: true,
                        msg: "still not ready".to_string(),
                    })
                }
            },
        )
        .await;

        assert!(matches!(result, Err(PollError::Cancelled { .. })));
        cancel_after.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn no_timeout_polls_indefinitely_until_success() {
        let (_tx, mut rx) = watch::channel(());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);

        // 12 attempts — exceeds the 9-entry schedule, ensuring the cap
        // value is reused.
        let result: Result<i32, _> = poll_with_backoff(
            "test",
            None,
            &mut rx,
            |e: &ToyErr| e.retryable,
            move || {
                let calls = Arc::clone(&calls_inner);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < 11 {
                        Err(ToyErr {
                            retryable: true,
                            msg: format!("attempt {}", n),
                        })
                    } else {
                        Ok(99)
                    }
                }
            },
        )
        .await;

        assert!(matches!(result, Ok(99)));
        assert_eq!(calls.load(Ordering::SeqCst), 12);
    }
}
