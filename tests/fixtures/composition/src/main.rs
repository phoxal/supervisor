//! Robot brain for the contract-evaluation composition: the endpoint surface
//! (cross-service `inspect` requirement and the served `report` operation)
//! is declared in Rust here; external participant bindings still come from
//! the prepared composition products through `api`.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

mod contract;
phoxal::api!();

use phoxal::runtime::{CallCompletion, Context};

use crate::contract::ProbeReport;
use phoxal::contracts::Empty;

use crate::api::consumer::ConsumerStatus;

#[derive(Default)]
struct Brain {
    inspect_in_flight: bool,
    completions: u64,
    failures: u64,
    last_phase: String,
    step: u64,
}

/// Probe cadence: one generated operation every 25 periods (500 ms).  A
/// runtime caller paces itself against the receiver's declared ingress
/// bound; per-step calling can outrun a slower receiver under controlled
/// scheduling.
const PROBE_EVERY_STEPS: u64 = 25;
const PROBE_WARMUP_STEPS: u64 = 50;

fn report(state: &Brain) -> ProbeReport {
    ProbeReport {
        inspect_completions: state.completions,
        inspect_failures: state.failures,
        last_phase: state.last_phase.clone(),
    }
}

#[phoxal::runtime(contract = crate::contract::BrainApi, period_ms = 20)]
impl Brain {
    #[init]
    fn new(_config: ()) -> phoxal::Result<Self> {
        Ok(Self::default())
    }

    /// Reports the probe tallies from this invocation's observed state.
    #[handle(report)]
    fn report_status(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        _request: Empty,
    ) -> phoxal::Result<ProbeReport> {
        Ok(report(self))
    }

    /// Completes one generated cross-service inspect; failures are
    /// counted, never fabricated into completions.
    #[complete(inspect)]
    fn inspect_completed(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        completion: CallCompletion<ConsumerStatus>,
    ) -> phoxal::Result<()> {
        self.inspect_in_flight = false;
        match completion.into_result() {
            Ok(status) => {
                self.completions = self.completions.saturating_add(1);
                self.last_phase = status.phase.clone();
            }
            Err(_) => {
                self.failures = self.failures.saturating_add(1);
            }
        }
        Ok(())
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> phoxal::Result<()> {
        self.step = self.step.saturating_add(1);
        // One paced robot-brain-initiated operation through the shared
        // generated robot API.
        if !self.inspect_in_flight
            && self.step >= PROBE_WARMUP_STEPS
            && (self.step - PROBE_WARMUP_STEPS).is_multiple_of(PROBE_EVERY_STEPS)
        {
            ctx.inspect(Empty {})?;
            self.inspect_in_flight = true;
        }
        Ok(())
    }
}

fn main() -> phoxal::Result<()> {
    phoxal::runtime::run::<Brain>()
}
