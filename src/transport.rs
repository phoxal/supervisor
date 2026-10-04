//! Embedded router and owned link-loss monitoring for the supervisor process.

use phoxal::identity::ExecutionId;

use phoxal::runtime::connection::{ConnectionError, Result};
use phoxal::runtime::connection::{
    apply_phoxal_transport_policy, client_config, execution_from_zid, zenoh_id_for,
};

/// A running Zenoh router session.
///
/// The router owns no keys, publishes nothing, and subscribes to nothing. It
/// routes, and it stays alive until dropped or [`Router::close`]d. Participants,
/// including the supervisor's own [`phoxal::runtime::connection::Connection`], reach it as
/// ordinary clients over the endpoints it listens on.
#[derive(Debug)]
pub(crate) struct Router {
    session: zenoh::Session,
}

impl Router {
    /// Open a router for `execution`, listening on `listen_endpoints`.
    ///
    /// The router's session id *is* the execution, so the fabric a trace names
    /// and the key root that trace carries are the same string. The whole
    /// configuration is framework-owned: there is no authored Zenoh file, so a
    /// robot project cannot put the router at odds with the clients dialing it.
    ///
    /// Returning `Ok` means the router is listening: Zenoh has bound every
    /// endpoint. There is no readiness probe to run afterwards and no window in
    /// which the endpoint exists but does not accept. `router_config` pins
    /// the settings that guarantee this.
    pub(crate) async fn open(execution: ExecutionId, listen_endpoints: &[String]) -> Result<Self> {
        validate_listen_endpoints(listen_endpoints)?;
        let session = zenoh::open(router_config(execution, listen_endpoints)?)
            .await
            .map_err(|error| ConnectionError::Transport(error.to_string()))?;
        let observed = match execution_from_zid(session.zid()) {
            Ok(observed) => observed,
            Err(error) => {
                let _ = session.close().await;
                return Err(error);
            }
        };
        if observed != execution {
            let _ = session.close().await;
            return Err(ConnectionError::ExecutionIdentityMismatch {
                expected: execution.to_string(),
                observed: observed.to_string(),
            });
        }
        Ok(Router { session })
    }

    /// Close the router, dropping every link to it.
    pub(crate) async fn close(self) -> Result<()> {
        self.session
            .close()
            .await
            .map_err(|error| ConnectionError::Transport(error.to_string()))
    }
}

fn validate_listen_endpoints(listen_endpoints: &[String]) -> Result<()> {
    if listen_endpoints.is_empty() {
        return Err(ConnectionError::Transport(
            "a router needs at least one listen endpoint".to_string(),
        ));
    }
    Ok(())
}

/// Watches the supervisor's own link to the router it is running.
///
/// The router runs inside the supervisor process, so "is the router still
/// there" is not answerable from the router's own session - if that session
/// dies it takes its own listener with it. The answer comes from the other end:
/// a client session dialing the router sees exactly one link, and that link
/// going away *is* the router going away.
///
/// This is deliberately not built on participant links. Participants report
/// themselves through Liveliness, which names them; a transport link over the
/// local unix socket identifies its peer only by a generated UUID, so watching
/// those would duplicate Liveliness with strictly worse information.
#[derive(Debug)]
pub(crate) struct RouterWatch {
    session: zenoh::Session,
    _listener: zenoh::session::LinkEventsListener<()>,
}

impl RouterWatch {
    /// Dial `endpoint` and call `on_lost` if the link to the router goes away.
    ///
    /// Returning `Ok` means the link is up. `on_lost` fires at most once per
    /// loss; a Zenoh client reconnects transparently, so a later recovery is
    /// not reported here - the caller has already decided what a lost fabric
    /// means by then.
    pub(crate) async fn open(
        endpoint: &str,
        on_lost: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self> {
        let session = zenoh::open(client_config(endpoint)?)
            .await
            .map_err(|error| ConnectionError::Transport(error.to_string()))?;

        let lost = std::sync::atomic::AtomicBool::new(false);
        let listener = session
            .info()
            .link_events_listener()
            .history(true)
            .callback(move |event| {
                if event.kind() == zenoh::sample::SampleKind::Delete
                    && !lost.swap(true, std::sync::atomic::Ordering::Relaxed)
                {
                    on_lost();
                }
            })
            .await
            .map_err(|error| ConnectionError::Transport(error.to_string()))?;
        Ok(RouterWatch {
            session,
            _listener: listener,
        })
    }

    /// Stop watching. Call this before closing the router being watched, so an
    /// ordinary shutdown is not reported as a loss.
    pub(crate) async fn close(self) -> Result<()> {
        self.session
            .close()
            .await
            .map_err(|error| ConnectionError::Transport(error.to_string()))
    }
}

fn router_config(execution: ExecutionId, listen_endpoints: &[String]) -> Result<zenoh::Config> {
    let mut config = zenoh::Config::default();
    apply_phoxal_transport_policy(&mut config)?;
    let endpoints = serde_json::to_string(listen_endpoints)
        .map_err(|error| ConnectionError::Transport(error.to_string()))?;
    // Rendered through the session-id conversion rather than from the
    // execution's own text, so this is the value Zenoh will report back, not a
    // string that merely looks like it.
    let id = serde_json::to_string(&zenoh_id_for(execution)?.to_string())
        .map_err(|error| ConnectionError::Transport(error.to_string()))?;
    // Every key below is pinned. The three `listen/*` keys are what make
    // `open`'s success mean "bound", which is the guarantee that lets the
    // supervisor delete its readiness probe: with a nonzero `timeout_ms` or
    // `exit_on_failure: false`, Zenoh moves binding to a background retry task
    // and `open` returns `Ok` with nothing listening. `scouting/delay` is a
    // flat sleep at the end of router startup, paid even though Phoxal keeps
    // multicast scouting off, so it is pure startup latency here.
    for (key, value) in [
        ("id", id.as_str()),
        ("mode", "\"router\""),
        ("listen/endpoints", endpoints.as_str()),
        ("listen/timeout_ms", "0"),
        ("listen/exit_on_failure", "true"),
        ("scouting/delay", "0"),
    ] {
        config
            .insert_json5(key, value)
            .map_err(|error| ConnectionError::Transport(error.to_string()))?;
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENDPOINT: &str = "tcp/127.0.0.1:7447";

    fn endpoints() -> Vec<String> {
        vec![ENDPOINT.to_string()]
    }

    #[test]
    fn router_config_pins_mode_and_listen_endpoints() {
        let config = router_config(ExecutionId::mint(), &endpoints()).expect("router config");
        assert_eq!(config.get_json("mode").expect("mode is set"), "\"router\"");
        assert_eq!(
            config
                .get_json("listen/endpoints")
                .expect("listen endpoints are set"),
            "[\"tcp/127.0.0.1:7447\"]"
        );
    }

    /// The router's session id is the execution it routes.
    #[test]
    fn the_router_session_id_is_the_execution() {
        let execution = ExecutionId::mint();
        let config = router_config(execution, &endpoints()).expect("router config");
        assert_eq!(
            config.get_json("id").expect("the session id is pinned"),
            format!("\"{execution}\""),
        );
    }

    #[test]
    fn router_config_carries_the_same_transport_policy_as_a_client() {
        // The whole reason this lives with the bus: both ends of a link must
        // agree, so assert it rather than trusting the call order.
        let config = router_config(ExecutionId::mint(), &endpoints()).expect("router config");
        assert_eq!(
            config
                .get_json("transport/link/tx/lease")
                .expect("lease is set"),
            "3000"
        );
        assert_eq!(
            config
                .get_json("transport/link/tx/keep_alive")
                .expect("keepalive is set"),
            "4"
        );
        assert_eq!(
            config
                .get_json("scouting/multicast/enabled")
                .expect("multicast is set"),
            "false"
        );
    }

    #[test]
    fn the_bound_on_open_guarantee_is_pinned() {
        // `Router::open` returning `Ok` means "listening", and the supervisor
        // deletes its readiness probe on that promise. Zenoh only keeps it when
        // binding is synchronous and fatal: a nonzero `listen/timeout_ms` or
        // `exit_on_failure: false` moves binding to a background retry task and
        // `open` succeeds with nothing bound.
        let config = router_config(ExecutionId::mint(), &endpoints()).expect("router config");
        assert_eq!(
            config
                .get_json("listen/timeout_ms")
                .expect("listen timeout is pinned"),
            "0",
            "a background-retry bind would make `open` succeed with nothing listening"
        );
        assert_eq!(
            config
                .get_json("listen/exit_on_failure")
                .expect("listen exit_on_failure is pinned"),
            "true",
            "a bind failure must fail `open`, not be swallowed"
        );
    }

    #[test]
    fn opening_without_a_listen_endpoint_is_rejected_before_transport_open() {
        let error =
            validate_listen_endpoints(&[]).expect_err("a router with nowhere to listen must fail");
        assert!(error.to_string().contains("listen endpoint"));
    }
}
