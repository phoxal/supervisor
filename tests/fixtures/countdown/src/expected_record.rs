//! The countdown fixture's expected compiled runtime record.
//!
//! This module is shared by the fixture binary's unit test and the process
//! acceptance test's bundle construction: the unit test compares it against
//! the macro-retained artifact record in the compiled binary, and the
//! process test serializes it into the launch manifest. Supervisor
//! admission does not itself compare manifest endpoint records with the
//! linked binary record; this shared fixture is what keeps the process
//! bundle consistent with the binary, and its scope is exactly that
//! fixture consistency.

use phoxal::artifact::{
    InputDelivery, InputRecord, MethodShape, MethodSignature, OutputRecord, RuntimeRecord,
};

fn call_signature(endpoint: &str, service: &str, request: &str, response: &str) -> MethodSignature {
    MethodSignature {
        endpoint: endpoint.to_owned(),
        service: service.to_owned(),
        method: endpoint.to_owned(),
        shape: MethodShape::Call,
        request: request.to_owned(),
        response: response.to_owned(),
        retained_latest: false,
        lease_valid_for_ms: None,
    }
}

fn observation_signature(endpoint: &str, response: &str, retained_latest: bool) -> MethodSignature {
    MethodSignature {
        endpoint: endpoint.to_owned(),
        service: response.to_owned(),
        method: endpoint.to_owned(),
        shape: MethodShape::Observation,
        request: "google.protobuf.Empty".to_owned(),
        response: response.to_owned(),
        retained_latest,
        lease_valid_for_ms: None,
    }
}

/// The complete runtime record the countdown fixture's binary retains.
pub fn expected_runtime_record() -> RuntimeRecord {
    RuntimeRecord::V0 {
        record: "runtime".to_owned(),
        conversions: Vec::new(),
        period_ms: 20,
        timeout_ms: 100,
        init_timeout_ms: 1_000,
        config_schema: serde_json::json!({"type": "null"}),
        inputs: vec![
            InputRecord {
                name: "start".to_owned(),
                delivery: InputDelivery::CallIngress,
                max_age_ms: None,
                max_items: Some(16),
                max_bytes: Some(16_384),
                port: Some("start".to_owned()),
                signature: Some(call_signature(
                    "start",
                    "phoxal.tests.authoring.countdown.v1.Start",
                    "phoxal.tests.authoring.countdown.v1.StartRequest",
                    "phoxal.tests.authoring.countdown.v1.StartResponse",
                )),
                request_fqn: Some("phoxal.tests.authoring.countdown.v1.StartRequest".to_owned()),
                response_fqn: Some("phoxal.tests.authoring.countdown.v1.StartResponse".to_owned()),
                response_max_bytes: Some(16_384),
                response_max_items: Some(16),
            },
            InputRecord {
                name: "cancel".to_owned(),
                delivery: InputDelivery::CallIngress,
                max_age_ms: None,
                max_items: Some(16),
                max_bytes: Some(16_384),
                port: Some("cancel".to_owned()),
                signature: Some(call_signature(
                    "cancel",
                    "phoxal.tests.authoring.countdown.v1.Cancel",
                    "phoxal.tests.authoring.countdown.v1.CancelRequest",
                    "phoxal.tests.authoring.countdown.v1.CancelResponse",
                )),
                request_fqn: Some("phoxal.tests.authoring.countdown.v1.CancelRequest".to_owned()),
                response_fqn: Some("phoxal.tests.authoring.countdown.v1.CancelResponse".to_owned()),
                response_max_bytes: Some(16_384),
                response_max_items: Some(16),
            },
        ],
        outputs: [
            vec![OutputRecord {
                family: None,
                family_template: None,
                name: "finished".to_owned(),
                port: Some("finished".to_owned()),
                signature: Some(observation_signature(
                    "finished",
                    "phoxal.tests.authoring.countdown.v1.FinishedEvent",
                    false,
                )),
                max_items: Some(16),
                max_bytes: Some(4_096),
                max_request_bytes: None,
                every_steps: None,
                bootstrap: false,
                timeout_ms: None,
            }],
            vec![OutputRecord {
                family: None,
                family_template: None,
                name: "status".to_owned(),
                port: Some("status".to_owned()),
                signature: Some(observation_signature(
                    "status",
                    "phoxal.tests.authoring.countdown.v1.CountdownState",
                    true,
                )),
                max_items: None,
                max_bytes: Some(16_384),
                max_request_bytes: None,
                every_steps: None,
                bootstrap: true,
                timeout_ms: None,
            }],
        ]
        .concat(),
    }
}
