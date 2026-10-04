//! Opt-in bounded metadata diagnostics for accepted execution cuts.
//!
//! Enable with `RUST_LOG=phoxal::boundary=debug`. Payload bytes and private
//! runtime state are never included. Trace records do not change admission.

use phoxal::communication::execution as wire;
use phoxal::communication::simulation::{Actuation, ProductMembership, TransitionKey};
use serde_json::json;

pub(super) fn invocation(accepted: &wire::InvocationAccepted) {
    receipt("invocation", accepted);
}

pub(super) fn initialized(accepted: &wire::InvocationAccepted) {
    receipt("initialized", accepted);
}

fn receipt(event: &str, accepted: &wire::InvocationAccepted) {
    if !tracing::enabled!(target: "phoxal::boundary", tracing::Level::DEBUG) {
        return;
    }
    let inputs = accepted
        .required_inputs
        .iter()
        .map(|input| {
            json!({
                "input": input.input, "source": input.source, "port": input.port,
                "sequence": input.sequence, "items": input.items, "bytes": input.bytes,
            })
        })
        .collect::<Vec<_>>();
    let products = accepted
        .required_products
        .iter()
        .map(|product| {
            json!({
                "port": product.port, "sequence": product.sequence,
                "items": product.items, "bytes": product.bytes,
            })
        })
        .collect::<Vec<_>>();
    tracing::debug!(target: "phoxal::boundary", record = %json!({
        "event": event, "execution": accepted.execution_id,
        "timeline": accepted.timeline_id, "boundary": accepted.boundary,
        "runtime": accepted.runtime_instance, "inputs": inputs, "products": products,
    }));
}

pub(super) fn admission<'a>(
    key: &TransitionKey,
    boundary: u64,
    observations: &[ProductMembership],
    actuation: impl Iterator<Item = &'a Actuation>,
) {
    if !tracing::enabled!(target: "phoxal::boundary", tracing::Level::DEBUG) {
        return;
    }
    let observations = observations.iter().map(membership).collect::<Vec<_>>();
    let actuation = actuation
        .filter_map(|actuation| {
            actuation.membership.as_ref().map(|member| {
        json!({"membership": membership(member), "valid_until_ns": actuation.valid_until_ns})
    })
        })
        .collect::<Vec<_>>();
    tracing::debug!(target: "phoxal::boundary", record = %json!({
        "event": "admission", "execution": key.execution_id,
        "timeline": key.timeline_id, "boundary": boundary,
        "observations": observations, "actuation": actuation,
    }));
}

fn membership(member: &ProductMembership) -> serde_json::Value {
    json!({"source": member.producer, "port": member.port,
        "sequence": member.sequence, "capture_boundary": member.capture_boundary,
        "capture_time_ns": member.capture_time_ns, "disposition": member.disposition as i32,
        "items": member.item_count, "bytes": member.encoded_bytes})
}
