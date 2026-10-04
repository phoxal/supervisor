//! Producer A: the standard encoder contract from its own independent
//! package identity, with observably different values from producer B.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

use phoxal::contracts::component::encoder::EncoderSample;
use phoxal::contracts::{Empty, Latest, Queue, RequestReply};
use phoxal::runtime::Context;

/// The standard encoder contract from this independent package identity.
#[phoxal::endpoints]
pub struct EncoderApi {
    #[phoxal::output(max_bytes = 1024)]
    encoder: Latest<EncoderSample>,

    #[phoxal::output(max_items = 4, max_bytes = 4096)]
    ticks: Queue<EncoderSample>,

    #[phoxal::operation(
        contract = "example.contract_evaluation.v1.ReadEncoder",
        max_items = 8,
        max_bytes = 1024
    )]
    read_encoder: RequestReply<Empty, EncoderSample>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, phoxal::Config)]
struct EncoderConfig {
    /// Offset distinguishing this producer's measurements.
    #[serde(default = "default_base_position")]
    base_position_rad: f64,
}

fn default_base_position() -> f64 {
    0.0
}

#[derive(Debug)]
struct EncoderA {
    base_position_rad: f64,
    step: u64,
    sample: EncoderSample,
    applied_invocation: u64,
}

fn sample(base: f64, step: u64) -> EncoderSample {
    EncoderSample {
        position_rad: Some(base + f64::from(u32::try_from(step).unwrap_or(u32::MAX)) * 0.001),
        velocity_radps: Some(1.0),
    }
}

#[phoxal::runtime(contract = EncoderApi, period_ms = 20)]
impl EncoderA {
    #[init]
    fn new(config: EncoderConfig) -> phoxal::Result<Self> {
        Ok(Self {
            base_position_rad: config.base_position_rad,
            step: 0,
            sample: sample(config.base_position_rad, 0),
            applied_invocation: u64::MAX,
        })
    }

    /// A side-effect-free read of the invocation's current measurement.
    #[handle(read_encoder)]
    fn read(
        &mut self,
        ctx: &mut Context<'_, Self>,
        _request: Empty,
    ) -> phoxal::Result<EncoderSample> {
        self.advance_once(ctx);
        Ok(self.sample)
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> phoxal::Result<()> {
        self.advance_once(ctx);
        ctx.publish_encoder(self.sample)?;
        ctx.emit_ticks(self.sample)?;
        Ok(())
    }

    /// Advances this invocation's measurement exactly once, so a read
    /// dispatched before the periodic step observes the measurement the
    /// unified step published.
    fn advance_once(&mut self, ctx: &Context<'_, Self>) {
        if self.applied_invocation == ctx.invocation_index() {
            return;
        }
        self.applied_invocation = ctx.invocation_index();
        self.step = self.step.saturating_add(1);
        self.sample = sample(self.base_position_rad, self.step);
    }
}

fn main() -> phoxal::Result<()> {
    phoxal::runtime::run::<EncoderA>()
}
