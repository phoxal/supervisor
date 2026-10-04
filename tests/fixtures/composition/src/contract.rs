//! The brain's authored contract vocabulary.

use phoxal::contracts::{Empty, RequestReply};

use crate::api::consumer::ConsumerStatus;

#[phoxal::message(package = "example.probe.v1")]
pub struct ProbeReport {
    /// Count of completed inspect calls.
    #[phoxal(tag = 1)]
    pub inspect_completions: u64,
    /// Count of failed inspect calls.
    #[phoxal(tag = 2)]
    pub inspect_failures: u64,
    /// Phase reported by the last completed inspect.
    #[phoxal(tag = 3)]
    pub last_phase: String,
}

/// The brain's endpoint contract.
#[phoxal::endpoints]
pub struct BrainApi {
    #[phoxal::call(
        contract = "example.contract_evaluation.v1.InspectConsumer",
        response = "example.contract_evaluation.v1.ConsumerStatus",
        max_items = 8,
        max_bytes = 4096
    )]
    inspect: RequestReply<Empty, ConsumerStatus>,

    #[phoxal::operation(
        contract = "example.probe.v1.ProbeReport",
        max_items = 8,
        max_bytes = 4096
    )]
    report: RequestReply<Empty, ProbeReport>,
}
