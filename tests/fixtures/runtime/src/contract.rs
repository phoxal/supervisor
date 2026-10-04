//! Fixture-owned inspection API alongside the standard encoder capability.

use phoxal::contracts::{Empty, Latest, RequestReply};

#[phoxal::messages(package = "example.inspection.v1")]
mod v1 {
    use super::{Empty, Latest, RequestReply};

    pub struct InspectionState {
        #[phoxal(tag = 1)]
        pub count: u64,
        #[phoxal(tag = 2)]
        pub active: bool,
    }

    pub struct InspectionReadRequest {
        #[phoxal(tag = 1)]
        pub key: String,
    }

    pub struct InspectionReadResponse {
        #[phoxal(tag = 1)]
        pub state: Option<InspectionState>,
    }

    /// The reference runtime's endpoint contract.
    ///
    /// This package also declares an encoder capability in `component.yaml`;
    /// the build helper's derived standard endpoint splices in beside these
    /// authored fields, so the compiled Runtime serves the standard component
    /// observation and the component-specific operations in one contract.
    #[phoxal::endpoints]
    pub struct InspectionApi {
        #[phoxal::output(projection = state, bootstrap, on_change, max_bytes = 4096)]
        status: Latest<InspectionState>,

        #[phoxal::operation(max_items = 8, max_bytes = 4096)]
        read: RequestReply<InspectionReadRequest, InspectionReadResponse>,

        #[phoxal::operation(max_items = 4, max_bytes = 1024)]
        calibrate: RequestReply<Empty, Empty>,
    }
}

pub use v1::*;
