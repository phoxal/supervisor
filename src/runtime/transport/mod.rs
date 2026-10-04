//! Supervisor-owned public-session transport.
//!
//! The client transport still lives in the SDK (`phoxal::communication_transport`).
//! This module owns the server half: queryables, route/session/grant admission,
//! dispatch into the configured service or simulation backend, and the
//! simulation authority state machine.
pub(crate) mod server;
pub(crate) mod simulation;
