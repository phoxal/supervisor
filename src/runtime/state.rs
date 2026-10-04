//! Supervisor-owned execution state.
//!
//! The public session adapter owns the wire-facing execution summary. This
//! private state owner keeps only the execution facts needed by the host and
//! the controlled Runtime boundary: the current timeline, its scheduling
//! mode, and the last committed boundary.

use std::sync::{Arc, Mutex, MutexGuard};

use phoxal::identity::TimelineId;

/// Scheduling mode selected for the current execution timeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimeMode {
    /// Hardware execution follows host-monotonic scheduling.
    Monotonic,
    /// Controlled execution advances at explicit logical boundaries.
    Simulated,
}

/// Private timeline authority shared by the supervisor host and execution
/// coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimeDomain {
    pub(crate) revision: u64,
    pub(crate) timeline: TimelineId,
    pub(crate) mode: TimeMode,
}

#[derive(Debug)]
struct Data {
    time_domain: TimeDomain,
    runtime_boundary: u64,
    ready: bool,
}

/// Shared handle to one supervisor execution's private state.
#[derive(Clone, Debug)]
pub(crate) struct ExecutionState {
    inner: Arc<Mutex<Data>>,
}

impl ExecutionState {
    /// Start a fresh execution on a newly minted monotonic timeline.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Data {
                time_domain: TimeDomain {
                    revision: 0,
                    timeline: TimelineId::mint(),
                    mode: TimeMode::Monotonic,
                },
                runtime_boundary: 0,
                ready: false,
            })),
        }
    }

    /// The supervisor's current execution time authority.
    pub(crate) fn time_domain(&self) -> TimeDomain {
        self.lock().time_domain
    }

    /// The last complete controlled Runtime boundary.
    pub(crate) fn runtime_boundary(&self) -> u64 {
        self.lock().runtime_boundary
    }

    /// Commit exactly one complete controlled Runtime boundary.
    pub(crate) fn complete_runtime_boundary(&self, boundary: u64) -> Result<(), String> {
        let mut data = self.lock();
        let expected = data
            .runtime_boundary
            .checked_add(1)
            .ok_or_else(|| "runtime boundary is exhausted".to_owned())?;
        if boundary != expected {
            return Err(format!(
                "runtime boundary {boundary} is out of order; expected {expected}"
            ));
        }
        data.runtime_boundary = boundary;
        Ok(())
    }

    /// Reset the boundary prefix when a fresh timeline is installed.
    pub(crate) fn reset_runtime_boundary(&self) {
        self.lock().runtime_boundary = 0;
    }

    /// Whether the host has admitted the complete Runtime graph.
    pub(crate) fn is_ready(&self) -> bool {
        self.lock().ready
    }

    /// Mark the complete Runtime graph ready after private admission succeeds.
    pub(crate) fn mark_ready(&self) {
        self.lock().ready = true;
    }

    /// Replace the current timeline with a freshly minted identity.
    #[allow(
        dead_code,
        reason = "used by host lifecycle callers that select a fresh timeline"
    )]
    pub(crate) fn replace_time_domain(&self, mode: TimeMode) -> Result<TimeDomain, String> {
        self.replace_time_domain_with(mode, TimelineId::mint())
    }

    /// Install an agreed timeline identity after the execution protocol has
    /// acknowledged the reset.
    pub(crate) fn replace_time_domain_with(
        &self,
        mode: TimeMode,
        timeline: TimelineId,
    ) -> Result<TimeDomain, String> {
        let mut data = self.lock();
        let revision = data
            .time_domain
            .revision
            .checked_add(1)
            .ok_or_else(|| "execution time-domain revision is exhausted".to_owned())?;
        let domain = TimeDomain {
            revision,
            timeline,
            mode,
        };
        data.time_domain = domain;
        Ok(domain)
    }

    fn lock(&self) -> MutexGuard<'_, Data> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_state_starts_at_boundary_zero_on_monotonic_timeline() {
        let state = ExecutionState::new();
        let domain = state.time_domain();
        assert_eq!(domain.revision, 0);
        assert_eq!(domain.mode, TimeMode::Monotonic);
        assert_eq!(state.runtime_boundary(), 0);
        assert!(!state.is_ready());
    }

    #[test]
    fn boundaries_are_committed_as_a_strict_prefix() {
        let state = ExecutionState::new();
        assert!(state.complete_runtime_boundary(1).is_ok());
        assert!(state.complete_runtime_boundary(3).is_err());
        assert_eq!(state.runtime_boundary(), 1);
        state.reset_runtime_boundary();
        assert_eq!(state.runtime_boundary(), 0);
    }

    #[test]
    fn replacing_a_timeline_changes_identity_and_mode() {
        let state = ExecutionState::new();
        let initial = state.time_domain();
        let replacement = state
            .replace_time_domain(TimeMode::Simulated)
            .expect("timeline replacement");
        assert_eq!(replacement.revision, initial.revision + 1);
        assert_ne!(replacement.timeline, initial.timeline);
        assert_eq!(replacement.mode, TimeMode::Simulated);
    }
}
