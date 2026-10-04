//! Loopback router fixture for supervisor integration tests.
//!
//! Test-only — never reachable from a production build of the supervisor
//! binary. The supervisor owns its own protocol-conformance router fixture
//! the same way it owns its session table and adapter; the SDK has no
//! fixture in its public API.

#[allow(clippy::expect_used, reason = "test fixture setup")]
pub async fn open(endpoints: &[String]) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    for (key, value) in [
        ("mode", "\"router\"".to_owned()),
        (
            "listen/endpoints",
            serde_json::to_string(endpoints).expect("test endpoints"),
        ),
        ("listen/timeout_ms", "0".to_owned()),
        ("listen/exit_on_failure", "true".to_owned()),
        ("scouting/multicast/enabled", "false".to_owned()),
        ("scouting/gossip/enabled", "false".to_owned()),
        ("scouting/delay", "0".to_owned()),
    ] {
        config
            .insert_json5(key, &value)
            .expect("test router config");
    }
    zenoh::open(config).await.expect("test router")
}
