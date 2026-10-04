//! The authored brain consumer for the countdown acceptance graph.
//!
//! The brain occupies the bundle's brain slot and receives the countdown
//! provider's finished events through the robot graph's
//! `brain.countdown_finished: countdown.finished` connection; its retained
//! state exposes exactly the events its handler has handled.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

phoxal::api!();

mod contract;
mod provider;

use crate::api::operations::phoxal::tests::authoring::countdown::v1::Start;
use crate::api::types::phoxal::tests::authoring::countdown::v1::{StartRequest, StartResponse};
use crate::contract::ConsumedEventState;
use crate::provider::{FinishedEvent, Outcome};
use phoxal::Result;
use phoxal::runtime::Context;

struct Brain {
    mission: phoxal::runtime::behavior::Tree<Brain>,
    last_job_id: Option<u64>,
    last_outcome: Option<Outcome>,
    handled_count: u64,
    direct_replies: u64,
}

impl Brain {
    /// The overview's sequence against the mission's own dedicated
    /// countdown instance: wait for idle evidence, start one job, require
    /// its accepted response, wait for completion, then hold a bounded
    /// window before declaring success. The dedicated instance makes the
    /// sequence deterministic — no startup grace or retry policy is
    /// coupled to other traffic on the shared provider.
    fn fresh_mission() -> Result<phoxal::runtime::behavior::Tree<Brain>> {
        let mission_job = 42;
        phoxal::runtime::behavior::Sequence::<Self>::new()
            .wait_until(|ctx| {
                ctx.countdown_status()
                    .fresh()
                    .is_some_and(|status| status.active_job_id.is_none())
            })
            .call(crate::contract::BrainApi::start_countdown(StartRequest {
                job_id: mission_job,
                duration_ms: 500,
            }))
            .expect_response(|response: &StartResponse| matches!(response, StartResponse::Accepted))
            .wait_until(move |ctx| {
                ctx.countdown_status().fresh().is_some_and(|status| {
                    status.last_job_id == Some(mission_job)
                        && matches!(status.last_outcome, Outcome::Completed)
                })
            })
            .delay(std::time::Duration::from_millis(100))
            .within(std::time::Duration::from_secs(10))
            .build()
    }
}

#[phoxal::runtime(contract = contract::BrainApi, period_ms = 20)]
impl Brain {
    #[init]
    fn new(_config: ()) -> Result<Self> {
        Ok(Self {
            mission: Self::fresh_mission()?,
            last_job_id: None,
            last_outcome: None,
            handled_count: 0,
            direct_replies: 0,
        })
    }

    #[handle(countdown_finished)]
    fn on_finished(&mut self, _ctx: &mut Context<'_, Self>, event: FinishedEvent) -> Result<()> {
        self.last_job_id = Some(event.job_id);
        self.last_outcome = Some(event.outcome);
        self.handled_count = self.handled_count.saturating_add(1);
        Ok(())
    }

    #[complete(start_countdown)]
    fn start_completed(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        completion: phoxal::runtime::input::CallCompletion<StartResponse>,
    ) -> Result<()> {
        // Only tickets the tree does not own arrive here: concurrent
        // direct calls to the same endpoint, each exactly once.
        self.direct_replies = self.direct_replies.saturating_add(1);
        let _ = completion.ticket();
        Ok(())
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> Result<()> {
        self.mission.tick(ctx)
    }

    #[publish(handled)]
    fn handled(&self) -> ConsumedEventState {
        ConsumedEventState {
            last_job_id: self.last_job_id,
            last_outcome: self.last_outcome,
            handled_count: self.handled_count,
        }
    }

    #[publish(command)]
    fn command(&self) -> Option<phoxal::contracts::component::actuator::ActuatorSetpoint> {
        // The leased actuator projection: one zero-velocity command for the
        // simulated mission motor, re-derived from the runtime state.
        Some(phoxal::contracts::component::actuator::ActuatorSetpoint {
            targets: vec![phoxal::contracts::component::actuator::ActuatorTarget {
                actuator_id: "mission_motor".to_owned(),
                control: Some(phoxal::contracts::component::actuator::Control::VelocityRadps(0.0)),
            }],
        })
    }

    #[publish(mission)]
    fn mission(&self) -> contract::MissionState {
        use phoxal::runtime::behavior::TreeStatus as Status;
        let phase = match self.mission.status() {
            Status::Running => contract::MissionPhase::Running,
            Status::Succeeded => contract::MissionPhase::Succeeded,
            Status::Refused => contract::MissionPhase::Refused,
            Status::Failed => contract::MissionPhase::Failed,
            Status::Cancelled => contract::MissionPhase::Cancelled,
            Status::TimedOut => contract::MissionPhase::TimedOut,
        };
        contract::MissionState { phase }
    }
}

fn main() -> Result<()> {
    phoxal::runtime::run::<Brain>()
}

#[cfg(test)]
mod expected_record;

#[cfg(test)]
mod tests {
    use super::expected_record::expected_runtime_record;
    use super::{
        Brain,
        contract::BrainApi,
        provider::{FinishedEvent, Outcome},
    };
    use phoxal::artifact::RuntimeRecord;
    use phoxal::runtime::input::InputSet;
    use phoxal::runtime::{Harness, LaunchedRuntime, RuntimeContract};

    #[test]
    fn the_retained_artifact_record_matches_the_shared_expected_record() {
        let record = Brain::artifact_metadata().as_bytes();
        let decoded: RuntimeRecord =
            serde_json::from_slice(&record[12..]).expect("retained contract decodes");
        assert_eq!(decoded, expected_runtime_record());
    }

    #[test]
    fn the_authored_brain_launches_and_counts_handled_events() -> phoxal::Result<()> {
        let launch: fn() -> phoxal::Result<()> = <Brain as LaunchedRuntime>::launch;
        let _ = launch;
        let field = <<BrainApi as RuntimeContract>::Inputs as InputSet>::FIELDS
            .iter()
            .find(|field| field.name == "countdown_finished")
            .expect("queue input");
        assert_eq!(field.max_items, Some(16));
        assert_eq!(field.max_bytes, Some(4_096));
        assert!(
            BrainApi::BINDINGS
                .iter()
                .find(|field| field.name == "handled")
                .expect("state binding")
                .bootstrap
        );
        let mut host = Harness::<Brain>::new(())?;
        host.enqueue_countdown_finished(FinishedEvent {
            job_id: 4,
            outcome: Outcome::Completed,
        })?;
        host.advance_to(std::time::Duration::ZERO)?;
        let handled = host.handled().expect("accepted state");
        assert_eq!(handled.last_job_id, Some(4));
        assert!(matches!(handled.last_outcome, Some(Outcome::Completed)));
        assert_eq!(handled.handled_count, 1);
        Ok(())
    }
}
