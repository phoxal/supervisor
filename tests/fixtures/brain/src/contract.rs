//! The brain consumer's authored contract: one bounded queue input for the
//! provider's finished events and one retained state publication exposing
//! the events it has actually handled.

#[phoxal::messages(package = "phoxal.tests.authoring.consumer.v1")]
mod v1 {
    use crate::provider::{CountdownState, FinishedEvent};
    use phoxal::contracts::{Latest, Queue, State};

    pub struct ConsumedEventState {
        #[phoxal(tag = 1)]
        pub last_job_id: Option<u64>,
        #[phoxal(tag = 2)]
        pub last_outcome: Option<crate::provider::Outcome>,
        #[phoxal(tag = 3)]
        pub handled_count: u64,
    }

    /// The mission state the brain publishes from its sequence.
    pub struct MissionState {
        #[phoxal(tag = 1)]
        pub phase: MissionPhase,
    }

    pub enum MissionPhase {
        Unspecified = 0,
        Running = 1,
        Succeeded = 2,
        Refused = 3,
        Failed = 4,
        Cancelled = 5,
        TimedOut = 6,
    }

    /// The brain's endpoint contract: one typed call requirement beside
    /// its queued event input, retained-state exports, and the leased
    /// actuator projection a controlled simulation delivers to its native
    /// actuators.
    #[phoxal::endpoints]
    pub struct BrainApi {
        #[phoxal::input(lease_ms = 100, max_bytes = 4096)]
        target: Latest<ConsumedEventState>,

        #[phoxal::call]
        start_countdown: crate::Start,

        #[phoxal::input(max_items = 16, max_bytes = 4096)]
        countdown_finished: Queue<FinishedEvent>,

        #[phoxal::input(max_age_ms = 100, max_bytes = 4096)]
        countdown_status: Latest<CountdownState>,

        #[phoxal::output]
        handled: State<ConsumedEventState>,

        #[phoxal::output]
        mission: State<MissionState>,

        #[phoxal::output(projection = state, lease_ms = 100, max_bytes = 1024)]
        command: Latest<::phoxal::contracts::component::actuator::ActuatorCommand>,
    }
}

pub use v1::*;
