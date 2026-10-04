//! The countdown provider's authored contract: two request/reply
//! operations, one retained state publication, and one queued event output.

#[phoxal::messages(package = "phoxal.tests.authoring.countdown.v1")]
mod v1 {
    use phoxal::contracts::{Queue, RequestReply, State};

    pub struct StartRequest {
        #[phoxal(tag = 1)]
        pub job_id: u64,
        #[phoxal(tag = 2)]
        pub duration_ms: u64,
    }

    pub enum StartResponse {
        #[phoxal(tag = 1)]
        Accepted,
        #[phoxal(tag = 2)]
        Busy,
        #[phoxal(tag = 3)]
        Invalid,
    }

    pub struct CancelRequest {
        #[phoxal(tag = 1)]
        pub job_id: u64,
    }

    pub enum CancelResponse {
        #[phoxal(tag = 1)]
        Cancelled,
        #[phoxal(tag = 2)]
        UnknownJob,
    }

    pub struct CountdownState {
        #[phoxal(tag = 1)]
        pub active_job_id: Option<u64>,
        #[phoxal(tag = 2)]
        pub last_job_id: Option<u64>,
        #[phoxal(tag = 3)]
        pub last_outcome: Outcome,
    }

    pub enum Outcome {
        Unspecified = 0,
        Completed = 1,
        Cancelled = 2,
    }

    pub struct FinishedEvent {
        #[phoxal(tag = 1)]
        pub job_id: u64,
        #[phoxal(tag = 2)]
        pub outcome: Outcome,
    }

    /// The countdown service's endpoint contract.
    #[phoxal::endpoints]
    pub struct CountdownApi {
        #[phoxal::operation]
        start: RequestReply<StartRequest, StartResponse>,

        #[phoxal::operation]
        cancel: RequestReply<CancelRequest, CancelResponse>,

        #[phoxal::output]
        status: State<CountdownState>,

        #[phoxal::output(max_items = 16, max_bytes = 4096)]
        finished: Queue<FinishedEvent>,
    }
}

pub use v1::*;
