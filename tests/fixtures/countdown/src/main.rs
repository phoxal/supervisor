//! The standalone authored countdown provider.
//!
//! The binary owns its contract and runtime privately and launches through
//! the accepted type-only spelling; configuration comes only from the
//! immutable bundle named by the supervisor's launch contract.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

mod contract;

use crate::contract::{
    CancelRequest, CancelResponse, CountdownState, FinishedEvent, Outcome, StartRequest,
    StartResponse,
};
use phoxal::Result;
use phoxal::runtime::Context;

struct Job {
    id: u64,
    deadline_ns: u64,
}

struct Countdown {
    active: Option<Job>,
    last: Option<(u64, Outcome)>,
}

#[phoxal::runtime(contract = contract::CountdownApi, period_ms = 20)]
impl Countdown {
    #[init]
    fn new(_config: ()) -> Result<Self> {
        Ok(Self {
            active: None,
            last: None,
        })
    }

    #[handle(start)]
    fn start(
        &mut self,
        ctx: &mut Context<'_, Self>,
        request: StartRequest,
    ) -> Result<StartResponse> {
        if request.job_id == 0 || !(1..=60_000).contains(&request.duration_ms) {
            return Ok(StartResponse::Invalid);
        }
        if self.active.is_some() {
            return Ok(StartResponse::Busy);
        }
        if self
            .last
            .as_ref()
            .is_some_and(|(id, _)| *id == request.job_id)
        {
            return Ok(StartResponse::Invalid);
        }
        let Some(deadline_ns) = ctx
            .now()
            .as_nanos()
            .checked_add(request.duration_ms * 1_000_000)
        else {
            return Ok(StartResponse::Invalid);
        };
        self.active = Some(Job {
            id: request.job_id,
            deadline_ns,
        });
        Ok(StartResponse::Accepted)
    }

    #[handle(cancel)]
    fn cancel(
        &mut self,
        ctx: &mut Context<'_, Self>,
        request: CancelRequest,
    ) -> Result<CancelResponse> {
        if self
            .active
            .as_ref()
            .is_some_and(|job| job.id == request.job_id)
        {
            self.finish(ctx, Outcome::Cancelled)?;
            Ok(CancelResponse::Cancelled)
        } else {
            Ok(CancelResponse::UnknownJob)
        }
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> Result<()> {
        if self
            .active
            .as_ref()
            .is_some_and(|job| ctx.now().as_nanos() >= job.deadline_ns)
        {
            self.finish(ctx, Outcome::Completed)?;
        }
        Ok(())
    }

    #[publish(status)]
    fn status(&self) -> CountdownState {
        CountdownState {
            active_job_id: self.active.as_ref().map(|job| job.id),
            last_job_id: self.last.as_ref().map(|(id, _)| *id),
            last_outcome: self
                .last
                .as_ref()
                .map_or(Outcome::Unspecified, |(_, outcome)| *outcome),
        }
    }

    fn finish(&mut self, ctx: &mut Context<'_, Self>, outcome: Outcome) -> Result<()> {
        if let Some(job) = self.active.take() {
            ctx.emit_finished(FinishedEvent {
                job_id: job.id,
                outcome,
            })?;
            self.last = Some((job.id, outcome));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    phoxal::runtime::run::<Countdown>()
}

#[cfg(test)]
mod expected_record;

#[cfg(test)]
mod tests {
    use super::expected_record::expected_runtime_record;
    use super::{
        Countdown,
        contract::{CancelRequest, CountdownApi, Outcome, StartRequest, StartResponse},
    };
    use phoxal::artifact::RuntimeRecord;
    use phoxal::runtime::input::InputSet;
    use phoxal::runtime::outputs::OutputSet;
    use phoxal::runtime::{ExecutionDuration, Harness, LaunchedRuntime, RuntimeContract};
    use std::time::Duration;

    #[test]
    fn the_retained_artifact_record_matches_the_shared_expected_record() {
        let record = Countdown::artifact_metadata().as_bytes();
        let decoded: RuntimeRecord =
            serde_json::from_slice(&record[12..]).expect("retained record decodes");
        assert_eq!(decoded, expected_runtime_record());
    }

    #[test]
    fn the_overview_execution_time_test_runs_on_the_real_implementation() -> phoxal::Result<()> {
        let mut host = Harness::<Countdown>::new(())?;
        let call = host.enqueue_start(StartRequest {
            job_id: 1,
            duration_ms: 3_000,
        })?;
        host.advance_to(Duration::ZERO)?;
        assert!(matches!(host.reply(call)?, StartResponse::Accepted));
        host.advance_to(Duration::from_millis(2_999))?;
        assert!(host.finished().is_empty());
        host.advance_to(Duration::from_secs(3))?;
        let finished = host.finished();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].job_id, 1);
        assert!(matches!(finished[0].outcome, Outcome::Completed));
        Ok(())
    }

    #[test]
    fn harness_reset_discards_old_work_and_republishes_initial_state() -> phoxal::Result<()> {
        let mut host = Harness::<Countdown>::new(())?;
        let call = host.enqueue_start(StartRequest {
            job_id: 5,
            duration_ms: 40,
        })?;
        host.advance_to(Duration::ZERO)?;
        assert!(matches!(host.reply(call)?, StartResponse::Accepted));
        host.advance_to(Duration::from_millis(60))?;
        assert_eq!(host.finished().len(), 1);
        let stale = host.enqueue_start(StartRequest {
            job_id: 2,
            duration_ms: 100,
        })?;
        host.reset(())?;
        assert!(host.finished().is_empty());
        let status = host.status().expect("bootstrap state");
        assert_eq!(status.active_job_id, None);
        assert_eq!(status.last_job_id, None);
        assert_eq!(status.last_outcome, Outcome::Unspecified);
        assert!(matches!(
            host.reply(stale),
            Err(phoxal::runtime::HarnessError::ReplyConsumed)
        ));
        host.advance_to(Duration::from_millis(60))?;
        let recall = host.enqueue_start(StartRequest {
            job_id: 5,
            duration_ms: 40,
        })?;
        host.advance_to(Duration::from_millis(80))?;
        assert!(matches!(host.reply(recall)?, StartResponse::Accepted));
        Ok(())
    }

    #[test]
    fn the_deadline_fires_exactly_once_at_the_execution_time_boundary() -> phoxal::Result<()> {
        let mut host = Harness::<Countdown>::new(())?;
        let call = host.enqueue_start(StartRequest {
            job_id: 1,
            duration_ms: 3_000,
        })?;
        host.advance_to(Duration::ZERO)?;
        assert!(matches!(host.reply(call)?, StartResponse::Accepted));
        for millis in (20..=2_980).step_by(20) {
            host.advance_to(Duration::from_millis(millis))?;
            assert!(
                host.finished().is_empty(),
                "no event before deadline at {millis} ms"
            );
        }
        host.advance_to(Duration::from_millis(3_000))?;
        let finished = host.finished();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].job_id, 1);
        assert!(matches!(finished[0].outcome, Outcome::Completed));
        host.advance_to(Duration::from_millis(3_100))?;
        assert!(
            host.finished().is_empty(),
            "a terminal job never completes again"
        );
        Ok(())
    }

    #[test]
    fn cancellation_before_the_deadline_never_completes_the_job() -> phoxal::Result<()> {
        let mut host = Harness::<Countdown>::new(())?;
        let start = host.enqueue_start(StartRequest {
            job_id: 5,
            duration_ms: 5_000,
        })?;
        host.advance_to(Duration::from_millis(980))?;
        assert!(matches!(host.reply(start)?, StartResponse::Accepted));
        host.enqueue_cancel(CancelRequest { job_id: 5 })?;
        host.advance_to(Duration::from_millis(1_000))?;
        let finished = host.finished();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].job_id, 5);
        assert!(matches!(finished[0].outcome, Outcome::Cancelled));
        host.advance_to(Duration::from_millis(5_100))?;
        assert!(host.finished().is_empty());
        Ok(())
    }

    #[test]
    fn invalid_durations_and_deadline_overflow_are_refused_without_events() -> phoxal::Result<()> {
        let mut host = Harness::<Countdown>::new(())?;
        for (attempt, duration_ms) in [0, 60_001].into_iter().enumerate() {
            let call = host.enqueue_start(StartRequest {
                job_id: 9,
                duration_ms,
            })?;
            host.advance_to(Duration::from_millis(attempt as u64 * 20))?;
            assert!(matches!(host.reply(call)?, StartResponse::Invalid));
            assert!(host.finished().is_empty());
        }
        let far_future = Duration::from_nanos(u64::MAX - 1_000_000_000);
        let mut host = Harness::<Countdown>::new_at((), far_future)?;
        let call = host.enqueue_start(StartRequest {
            job_id: 11,
            duration_ms: 60_000,
        })?;
        host.advance_to(far_future)?;
        assert!(matches!(host.reply(call)?, StartResponse::Invalid));
        assert!(host.finished().is_empty());
        Ok(())
    }

    #[test]
    fn the_authored_runtime_launches_and_records_its_contract() {
        let launch: fn() -> phoxal::Result<()> = <Countdown as LaunchedRuntime>::launch;
        let _ = launch;
        let spec = Countdown::SPEC;
        assert_eq!(spec.period, ExecutionDuration::from_millis(20));
        assert_eq!(spec.timeout, ExecutionDuration::from_millis(100));
        assert_eq!(spec.init_timeout, ExecutionDuration::from_millis(1_000));
        for field in <<CountdownApi as RuntimeContract>::Inputs as InputSet>::FIELDS {
            let signature = field.port_signature.expect("operation signature");
            match field.name {
                "start" => {
                    assert_eq!(
                        signature.service,
                        "phoxal.tests.authoring.countdown.v1.Start"
                    );
                    assert_eq!(
                        signature.request,
                        "phoxal.tests.authoring.countdown.v1.StartRequest"
                    );
                    assert_eq!(
                        signature.response,
                        "phoxal.tests.authoring.countdown.v1.StartResponse"
                    );
                    assert_eq!(field.max_items, Some(16));
                    assert_eq!(field.max_bytes, Some(16_384));
                }
                "cancel" => {
                    assert_eq!(
                        signature.service,
                        "phoxal.tests.authoring.countdown.v1.Cancel"
                    );
                    assert_eq!(
                        signature.request,
                        "phoxal.tests.authoring.countdown.v1.CancelRequest"
                    );
                    assert_eq!(
                        signature.response,
                        "phoxal.tests.authoring.countdown.v1.CancelResponse"
                    );
                }
                other => panic!("unexpected input {other}"),
            }
        }
        for field in <<CountdownApi as RuntimeContract>::Outputs as OutputSet>::FIELDS {
            if field.name == "finished" {
                assert_eq!(field.max_items, Some(16));
                assert_eq!(field.max_bytes, Some(4_096));
            }
        }
        assert!(
            CountdownApi::BINDINGS
                .iter()
                .find(|field| field.name == "status")
                .expect("status binding")
                .bootstrap
        );
    }
}
