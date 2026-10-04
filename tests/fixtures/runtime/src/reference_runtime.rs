//! The reference runtime used by the supervisor process-boundary tests.

use phoxal::contracts::Empty;
use phoxal::contracts::component::encoder::EncoderSample;
use phoxal::runtime::Context;

use crate::contract::{InspectionReadResponse, InspectionState};

/// The marker path parsed from the process launch: the authored runtime
/// reaches it through this process-global because its configuration comes
/// only from the immutable launch contract.
pub static MARKER: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);

/// A compiled, input-free runtime owning one generated call, one retained
/// inspection observation, and a standard encoder output.
pub struct ReferenceRuntime {
    marker: std::path::PathBuf,
    count: u64,
    applied_invocation: u64,
}

#[phoxal::runtime(contract = crate::contract::InspectionApi, period_ms = 20)]
impl ReferenceRuntime {
    #[init]
    fn new(_config: ()) -> phoxal::Result<Self> {
        let marker = MARKER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_default();
        std::fs::write(&marker, b"initialized")?;
        Ok(Self {
            marker,
            count: 0,
            applied_invocation: u64::MAX,
        })
    }

    /// Reads the inspection counter at its merged dispatch position.
    #[handle(read)]
    fn read(
        &mut self,
        ctx: &mut Context<'_, Self>,
        request: crate::contract::InspectionReadRequest,
    ) -> phoxal::Result<InspectionReadResponse> {
        self.advance_once(ctx);
        Ok(InspectionReadResponse {
            state: Some(InspectionState {
                count: self.count,
                active: request.key == "status",
            }),
        })
    }

    /// Calibration is a null operation on this reference runtime.
    #[handle(calibrate)]
    fn calibrate(
        &mut self,
        _ctx: &mut Context<'_, Self>,
        _request: Empty,
    ) -> phoxal::Result<Empty> {
        Ok(Empty {})
    }

    #[step]
    fn advance(&mut self, ctx: &mut Context<'_, Self>) -> phoxal::Result<()> {
        let previous = self.count;
        self.advance_once(ctx);
        // The derived standard encoder endpoint: one live sample per step,
        // derived from the step counter rather than fabricated constants.
        ctx.emit_encoder(EncoderSample {
            position_rad: Some(previous as f64 * 0.001),
            velocity_radps: Some(0.05),
        })?;
        Ok(())
    }

    /// Projects the retained inspection counter.
    #[publish(status)]
    fn status(&self) -> InspectionState {
        InspectionState {
            count: self.count,
            active: true,
        }
    }

    /// Advances this invocation's counter exactly once, so a read
    /// dispatched before the periodic step observes the counter the
    /// unified step reported.
    fn advance_once(&mut self, ctx: &Context<'_, Self>) {
        if self.applied_invocation == ctx.invocation_index() {
            return;
        }
        self.applied_invocation = ctx.invocation_index();
        if self.count == 0 {
            std::fs::write(&self.marker, b"stepped").ok();
        }
        self.count = self.count.saturating_add(1);
    }
}
