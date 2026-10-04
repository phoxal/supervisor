//! Rust-authored consumer fixture: endpoints and payloads are declared once
//! in contract.rs; behavior owns only configuration, state, and stepping.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

use phoxal::contracts::Empty;
use phoxal::contracts::component::encoder::EncoderSample;
use phoxal::runtime::{CallCompletion, Context};

mod contract;
use contract::{ConsumerApi, ConsumerStatus};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize, phoxal::Config)]
struct ConsumerConfig {
    /// Phase reported after the first fresh encoder observation arrives.
    #[serde(default = "default_ready_phase")]
    ready_phase: String,
}

fn default_ready_phase() -> String {
    "running".to_owned()
}

#[derive(Debug)]
struct Consumer {
    ready_phase: String,
    phase: String,
    steps: u64,
    observed: u64,
    ticks: u64,
    readings: u64,
    backups: u64,
    failed_reads: u64,
    last_position: Option<f64>,
    backup_position: Option<f64>,
    read_in_flight: bool,
    backup_in_flight: bool,
    applied_invocation: u64,
}

fn status(state: &Consumer) -> ConsumerStatus {
    ConsumerStatus {
        phase: if state.phase.is_empty() {
            "starting".to_owned()
        } else {
            state.phase.clone()
        },
        observed: state.observed,
        readings: state.readings,
        ticks: state.ticks,
        backups: state.backups,
        position_rad: state.last_position,
        backup_position_rad: state.backup_position,
    }
}

/// Observes the encoder input and the queued tick batch exactly once per
/// invocation, so an inspect dispatched before the periodic step reports
/// the same invocation state the unified step observed.
fn observe(state: &mut Consumer, ctx: &Context<'_, Consumer>) -> phoxal::Result<()> {
    if state.applied_invocation == ctx.invocation_index() {
        return Ok(());
    }
    state.applied_invocation = ctx.invocation_index();
    // Freshness gates the ready phase through the declared 100 ms bound;
    // hardware-local clocks share the wall-clock epoch, so a foreign stamp
    // evaluates against this runtime's now within the bounded-skew policy.
    // Absence and staleness are both deliberate observable states: the
    // previous position is kept and the phase reports waiting rather than
    // treating stale data as current.
    if !ctx.encoder().is_fresh() {
        state.phase = "waiting".to_owned();
    } else if let Some(sample) = ctx.encoder().sample()
        && sample.payload().validate().is_ok()
    {
        state.observed = state.observed.saturating_add(1);
        state.last_position = sample.payload().position_rad;
        state.phase = if state.ready_phase.is_empty() {
            "running".to_owned()
        } else {
            state.ready_phase.clone()
        };
    } else {
        state.phase = "waiting".to_owned();
    }

    // Queued data is required: an overflow that dropped records is a
    // visible rejection, never a silent prefix.
    if ctx.ticks().has_gap() {
        return Err(phoxal::anyhow!(
            "queued ticks overflowed the declared 4-item bound"
        ));
    }
    for tick in ctx.ticks().items() {
        tick.payload()
            .validate()
            .map_err(|error| phoxal::anyhow!(error))?;
        state.ticks = state.ticks.saturating_add(1);
    }
    Ok(())
}

#[phoxal::runtime(contract = ConsumerApi, period_ms = 20)]
impl Consumer {
    #[init]
    fn new(config: ConsumerConfig) -> phoxal::Result<Self> {
        Ok(Self {
            ready_phase: config.ready_phase,
            phase: String::new(),
            steps: 0,
            observed: 0,
            ticks: 0,
            readings: 0,
            backups: 0,
            failed_reads: 0,
            last_position: None,
            backup_position: None,
            read_in_flight: false,
            backup_in_flight: false,
            applied_invocation: u64::MAX,
        })
    }

    /// Reports the consumer's status from this invocation's observed state.
    #[handle(inspect)]
    fn inspect(
        &mut self,
        ctx: &mut Context<'_, Self>,
        _request: Empty,
    ) -> phoxal::Result<ConsumerStatus> {
        observe(self, ctx)?;
        Ok(status(self))
    }

    /// Completes one staged primary reading; failures are counted, never
    /// fabricated into successful readings.
    #[complete(read_encoder)]
    fn read_completed(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        completion: CallCompletion<EncoderSample>,
    ) -> phoxal::Result<()> {
        self.read_in_flight = false;
        self.complete_reading(completion, false)
    }

    /// Completes one staged backup reading on its own completion field.
    #[complete(read_backup)]
    fn backup_completed(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        completion: CallCompletion<EncoderSample>,
    ) -> phoxal::Result<()> {
        self.backup_in_flight = false;
        self.complete_reading(completion, true)
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> phoxal::Result<()> {
        observe(self, ctx)?;
        ctx.publish_status(status(self))?;

        // Stage the next composition-bound readings; the provider instances
        // are resolved from this service's own robot connections at
        // execution.  A runtime caller paces itself well below the
        // provider's declared ingress bound: boundary counters drift
        // between independent runtimes, so per-step calling can overflow
        // the receiver.
        self.steps = self.steps.saturating_add(1);
        const READ_WARMUP_STEPS: u64 = 10;
        const READ_EVERY_STEPS: u64 = 25;
        if self.steps >= READ_WARMUP_STEPS
            && (self.steps - READ_WARMUP_STEPS).is_multiple_of(READ_EVERY_STEPS)
        {
            if !self.read_in_flight {
                ctx.read_encoder(Empty {})?;
                self.read_in_flight = true;
            }
            if !self.backup_in_flight {
                ctx.read_backup(Empty {})?;
                self.backup_in_flight = true;
            }
        }
        Ok(())
    }

    fn complete_reading(
        &mut self,
        completion: CallCompletion<EncoderSample>,
        backup: bool,
    ) -> phoxal::Result<()> {
        match completion.into_result() {
            Ok(sample) => {
                sample.validate().map_err(|error| phoxal::anyhow!(error))?;
                if backup {
                    self.backups = self.backups.saturating_add(1);
                    self.backup_position = sample.position_rad;
                } else {
                    self.readings = self.readings.saturating_add(1);
                    self.last_position = sample.position_rad;
                }
            }
            Err(_) => {
                self.failed_reads = self.failed_reads.saturating_add(1);
            }
        }
        Ok(())
    }
}

fn main() -> phoxal::Result<()> {
    phoxal::runtime::run::<Consumer>()
}

/// Acceptance through the authored endpoint surface and the canonical runtime owner.
#[cfg(test)]
mod tests {
    use super::*;
    use phoxal::runtime::{ExecutionTime, Harness, ObservationStamp, Sample};
    use std::time::Duration;

    fn encoder(at_ms: u64) -> Sample<EncoderSample> {
        Sample::new(
            EncoderSample {
                position_rad: Some(1.5),
                ..Default::default()
            },
            ObservationStamp::new(
                "encoder_source",
                ExecutionTime::from_nanos(at_ms * 1_000_000),
                None,
            ),
        )
    }

    #[test]
    fn canonical_runtime_uses_typed_initialization_and_step() -> phoxal::Result<()> {
        let mut host = Harness::<Consumer>::new(ConsumerConfig {
            ready_phase: "cruising".to_owned(),
        })?;
        host.advance_to(Duration::ZERO)?;
        assert_eq!(host.status().expect("accepted status").phase, "waiting");
        host.inject_encoder(encoder(20))?;
        host.advance_to(Duration::from_millis(20))?;
        assert_eq!(host.status().expect("configured phase").phase, "cruising");
        Ok(())
    }

    #[test]
    fn an_inspect_merged_with_its_reply_reports_the_pre_completion_state() -> phoxal::Result<()> {
        let mut host = Harness::<Consumer>::new(ConsumerConfig::default())?;
        host.advance_to(Duration::from_millis(200))?;
        let reading = host
            .take_request::<Empty, EncoderSample>("read_encoder")?
            .expect("accepted primary survey");
        host.complete_request(
            &reading,
            Ok(EncoderSample {
                position_rad: Some(4.0),
                velocity_radps: Some(0.0),
            }),
        )?;
        let inspect = host.enqueue_inspect(Empty {})?;
        host.advance_to(Duration::from_millis(220))?;
        let reported = host.reply(inspect)?;
        assert_eq!(
            reported.readings, 0,
            "inspect executes before the completion handler"
        );
        assert_eq!(reported.position_rad, None);
        let committed = host.status().expect("accepted post-completion status");
        assert_eq!(committed.readings, 1);
        assert_eq!(committed.position_rad, Some(4.0));
        Ok(())
    }

    #[test]
    fn owner_serializes_acceptance_from_the_authored_endpoint_surface() -> phoxal::Result<()> {
        let mut host =
            Harness::<Consumer>::new_at(ConsumerConfig::default(), Duration::from_millis(20))?;
        assert_eq!(host.advance_to(Duration::from_millis(20))?, 1);
        let waiting = host.status().expect("accepted status");
        assert_eq!(waiting.phase, "waiting");
        assert_eq!(waiting.observed, 0);
        host.inject_encoder(encoder(40))?;
        assert_eq!(host.advance_to(Duration::from_millis(40))?, 1);
        let running = host.status().expect("accepted observation");
        assert_eq!(running.phase, "running");
        assert_eq!(running.observed, 1);
        assert_eq!(running.position_rad, Some(1.5));
        assert_eq!(
            host.advance_to(Duration::from_millis(40))?,
            0,
            "no accepted release is replayed"
        );
        Ok(())
    }

    #[test]
    fn output_capacity_is_reserved_before_invocation_acceptance() -> phoxal::Result<()> {
        let mut host = Harness::<Consumer>::new(ConsumerConfig::default())?;
        let mut retained = Vec::new();
        for _ in 0..8 {
            retained.push(host.enqueue_inspect(Empty {})?);
        }
        host.advance_to(Duration::ZERO)?;
        let before = host.status().expect("accepted status");
        let rejected = host.enqueue_inspect(Empty {})?;
        host.inject_encoder(encoder(20))?;
        let error = host
            .advance_to(Duration::from_millis(20))
            .expect_err("undrained replies reject the complete candidate");
        assert!(error.to_string().contains("retained"));
        assert_eq!(host.status().expect("unchanged accepted status"), before);
        assert!(host.reply(rejected).is_err());
        for call in retained {
            assert_eq!(host.reply(call)?.observed, 0);
        }
        assert!(
            host.advance_to(Duration::from_millis(40)).is_err(),
            "rejected owner is terminal"
        );
        Ok(())
    }
}
