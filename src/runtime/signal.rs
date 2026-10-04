//! Termination signals, routed into the one orderly stop the supervisor has.
//!
//! There is no graph to tear down: this process started nothing. An orderly
//! stop is closing the control plane, the bus session, and the embedded router,
//! in that order, and exiting 0. Nothing here records a failure, because being
//! asked to stop is not one.
//!
//! Handling the signal at all still matters. Under the default disposition of
//! SIGTERM the process would die where it stands, taking the router down
//! without closing it: every participant would lose its links to a socket that
//! vanished mid-session rather than to a router that finished with them, and
//! the lock and socket the next run needs would be left behind.
//!
//! The handler comes from `ctrlc` with its `termination` feature, which covers
//! SIGINT, SIGTERM, and SIGHUP in one registration: SIGTERM is what a service
//! manager and a `kill` send, SIGINT is the interactive form of the same
//! request, and SIGHUP arrives when the terminal that launched a foreground
//! supervisor goes away - all three leave the graph with no owner, so all
//! three mean stop rather than continue headless.

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

/// Register the termination handler now, and cancel `shutdown` on the first
/// signal that arrives.
///
/// Registration is deliberately fallible and eager: a supervisor that cannot
/// install the handler has no orderly stop at all, which is a startup
/// precondition rather than a warning, and installing it before anything is
/// launched means a startup-time signal is caught rather than lethal.
///
/// A second signal while the stop is already in flight does nothing extra: the
/// token is already cancelled, and what is left is bounded transport close, so
/// there is nothing an impatient repeat could usefully shorten. SIGKILL remains
/// the operator's escape hatch, as always.
pub(crate) fn cancel_on_termination(shutdown: CancellationToken) -> Result<()> {
    ctrlc::set_handler(move || {
        tracing::info!("termination signal received; stopping execution");
        shutdown.cancel();
    })
    .context("failed to install the termination signal handler")
}

/// SIGHUP stands in for the whole set: `ctrlc`'s `termination` feature
/// registers the three signals in one call, and it is the one signal no test
/// harness raises on its own, so the test binary cannot mistake a stray SIGTERM
/// for this one. The handler is installed before the signal is raised, so this
/// process catches it instead of dying under the default disposition.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_termination_signal_cancels_the_orderly_shutdown_token() {
        let shutdown = CancellationToken::new();
        cancel_on_termination(shutdown.clone()).expect("the handler installs");
        assert!(!shutdown.is_cancelled());

        // SAFETY: `raise` only enqueues a signal for this process and touches
        // no memory; the handler for it is installed above, so the default
        // terminating disposition does not apply.
        assert_eq!(
            unsafe { libc::raise(libc::SIGHUP) },
            0,
            "raising SIGHUP at this process must succeed"
        );

        tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
            .await
            .expect("a termination signal must cancel the token");
    }
}
