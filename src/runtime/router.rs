//! The embedded Zenoh router.
//!
//! The comms fabric is infrastructure the supervisor firsthand owns, so it runs
//! in this process rather than as a supervised child. What
//! that deletes is the point: there is no router binary to stage or resolve, no
//! spawn, no readiness probe polling a socket, and no full-graph recovery epoch
//! driven by a child exit. [`crate::transport::Router::open`] returning means the
//! endpoint is bound - `phoxal::runtime::connection` pins the Zenoh listen settings that make
//! that true - so the router is simply ready or the run failed to start.
//!
//! The router owns no keys and no subscriptions; participants and the
//! supervisor's own observer session reach it as ordinary clients over the
//! endpoint it listens on.

use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;

/// What to do when the fabric disappears under a running session. It receives
/// the rendered reason rather than a shared state handle, so the caller decides
/// what losing the router means to it.
pub(crate) type RouterLost = Arc<dyn Fn(String) + Send + Sync>;

/// The running embedded router. Holding it keeps the fabric up; dropping or
/// [`EmbeddedRouter::close`]ing it takes every link down with it.
#[derive(Debug)]
pub(crate) struct EmbeddedRouter {
    router: crate::transport::Router,
    /// Watches this router from the outside. Closed before the router is, so
    /// an ordinary shutdown is never reported as a loss.
    watch: crate::transport::RouterWatch,
}

impl EmbeddedRouter {
    /// Close the router. Called on the way out of a session, after the graph
    /// has been torn down, so participants lose their links to a router that is
    /// already finished with them rather than mid-shutdown.
    ///
    /// The watch goes first, so a deliberate stop is not reported as the fabric
    /// failing.
    pub(crate) async fn close(self) -> Result<()> {
        if let Err(error) = self.watch.close().await {
            tracing::debug!("failed to close the router watch: {error}");
        }
        self.router
            .close()
            .await
            .context("failed to close the embedded router")
    }
}

/// Open the embedded router on `endpoint`.
///
/// The router's configuration is entirely framework-owned: `phoxal::runtime::connection` pins
/// the transport policy and the listen settings, so the fabric cannot be put at
/// odds with the participants that dial it.
///
/// `endpoint` must be a plain endpoint string: a per-endpoint config fragment
/// (`tcp/…#exit_on_failure=false`) would override the pinned listen settings
/// that make a successful open mean "bound".
///
/// `on_lost` is called if the fabric disappears under a running session. It
/// receives the rendered reason rather than a shared state handle so the
/// caller decides what losing the router means to it: for the supervisor it is a
/// typed `RouterLost` failure that terminates the execution.
pub(crate) async fn start_embedded_router(
    execution: phoxal::identity::ExecutionId,
    endpoint: String,
    on_lost: RouterLost,
) -> Result<EmbeddedRouter> {
    validate_endpoint(&endpoint)?;
    prepare_endpoint_parent(&endpoint)?;
    // One execution equals one router lifetime: the router's ZID IS the
    // execution id, so a client that reads the router id has learned the
    // execution without asking anyone.
    let router = crate::transport::Router::open(execution, std::slice::from_ref(&endpoint))
        .await
        .with_context(|| format!("failed to open the embedded router on {endpoint}"))?;

    // The router runs in this process, so nothing else would notice it going
    // away: participants would simply go stale a Liveliness lease later, and
    // the supervisor would keep reporting a graph it can no longer reach. The
    // watch dials the router from the outside and answers the one question the
    // router's own session cannot answer about itself.
    let lost_endpoint = endpoint.clone();
    let watch = crate::transport::RouterWatch::open(&endpoint, move || {
        tracing::error!("the router at {lost_endpoint} is gone; the robot graph is unreachable");
        on_lost(format!(
            "the embedded router at {lost_endpoint} went away while the session was running"
        ));
    })
    .await
    .with_context(|| format!("failed to watch the embedded router on {endpoint}"))?;

    Ok(EmbeddedRouter { router, watch })
}

fn validate_endpoint(endpoint: &str) -> Result<()> {
    anyhow::ensure!(
        !endpoint.contains('#'),
        "router endpoint {endpoint} carries a per-endpoint config fragment; that would override \
         the listen settings which make a successful open mean the endpoint is bound"
    );
    Ok(())
}

/// The filesystem path behind a `unixsock-stream/` endpoint, if it is one.
/// Zenoh binds the socket itself but will not create missing parent
/// directories, so the caller creates them first.
fn unixsock_stream_path(endpoint: &str) -> Option<&Path> {
    endpoint.strip_prefix("unixsock-stream/").map(Path::new)
}

/// Create the parent needed by a Unix-domain endpoint without opening a socket
/// or starting Zenoh. Non-filesystem endpoints need no preparation.
fn prepare_endpoint_parent(endpoint: &str) -> Result<()> {
    let Some(parent) = unixsock_stream_path(endpoint).and_then(Path::parent) else {
        return Ok(());
    };
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create the router socket directory {}",
            parent.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_unix_socket_endpoint_yields_its_path() {
        assert_eq!(
            unixsock_stream_path("unixsock-stream//tmp/phoxal/router.sock"),
            Some(Path::new("/tmp/phoxal/router.sock"))
        );
        assert_eq!(unixsock_stream_path("tcp/127.0.0.1:7447"), None);
    }

    #[test]
    fn endpoint_parent_creation_is_a_pure_filesystem_operation() {
        let root = tempfile::tempdir().expect("temp directory");
        let socket = root.path().join("run/phoxal/router.sock");
        let endpoint = format!("unixsock-stream/{}", socket.display());

        prepare_endpoint_parent(&endpoint).expect("create endpoint parent");

        assert!(socket.parent().expect("socket parent").is_dir());
        assert!(
            !socket.exists(),
            "preparation must not bind or create the socket"
        );
    }

    #[test]
    fn an_endpoint_config_fragment_is_rejected_before_transport_open() {
        // A fragment can set `exit_on_failure=false`, which would send binding
        // to a background retry task and make a successful open mean nothing.
        let error = validate_endpoint("tcp/127.0.0.1:7447#exit_on_failure=false")
            .expect_err("a per-endpoint config fragment must be rejected");
        assert!(error.to_string().contains("config fragment"), "{error:#}");
        validate_endpoint("tcp/127.0.0.1:7447").expect("a plain endpoint is accepted");
    }
}
