//! Public simulation authority and three-phase admission, independent of native physics.
mod authority;
mod phases;
mod products;

use phoxal::communication_transport::{
    DEFAULT_PUBLIC_DEADLINE, PublicTransportError, bounded_error_detail,
    cancel_session_subscriptions,
};

use super::server::{PublicBackendError, PublicSimulationBackend, PublicSimulationContext};
use crate::runtime::adapter::{SupervisorAdapter, SupervisorAdapterError};
use phoxal::communication::simulation::{
    AcquireAuthorityRequest, AcquireAuthorityResponse, AdmitInitialObservationsRequest,
    AdmitInitialObservationsResponse, AdmitObservationsRequest, AdmitObservationsResponse,
    PrepareBoundaryRequest, PrepareBoundaryResponse, ProgressRequest, ProgressResponse,
    ReleaseAuthorityRequest, ReleaseAuthorityResponse, ResetRequest, ResetResponse, TransitionKey,
};
use phoxal::communication::{PublicOperation, PublicRoute};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub(crate) use authority::{
    acquire_simulation_authority, progress_simulation, release_backend_authority,
    release_simulation, reset_simulation, revoke_simulation_for_session,
};
pub(crate) use phases::{admit_initial_observations, admit_observations, prepare_boundary};

/// One exclusive, lease-bound simulation authority held by this supervisor.
///
/// The grant is opaque and never reused.  Every simulation request rechecks
/// the protected principal, grant, execution, timeline, and lease before it
/// mutates the boundary, so an old grant cannot become valid after release,
/// reset, or a later acquisition.
#[derive(Clone, Debug)]
pub(crate) struct SimulationAuthority {
    principal: String,
    session_id: Vec<u8>,
    grant: Vec<u8>,
    execution_id: String,
    timeline_id: String,
    model_identity: String,
    quantum_ns: u64,
    boundary: u64,
    lease_deadline: Instant,
    /// The currently executing transition, if a phase is awaiting a backend
    /// result.  A second transition cannot overtake it.
    in_flight: Option<TransitionKey>,
    /// Highest completed transition sequence.
    accepted_sequence_watermark: u64,
    max_product_bytes: usize,
    max_cut_bytes: usize,
    active_phase: Option<PublicOperation>,
    failure: Option<String>,
    resetting: bool,
}

const SIMULATION_AUTHORITY_LEASE: Duration = Duration::from_secs(30);
const SIMULATION_GRANT_BYTES: usize = 32;
const MAX_SIMULATION_CORRELATION_BYTES: usize = 64;
const MAX_SIMULATION_PRODUCT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_SIMULATION_CUT_BYTES: usize = 8 * 1024 * 1024;
#[derive(Clone, Debug)]
struct SimulationPhaseAdmission {
    adapter: Arc<Mutex<SupervisorAdapter>>,
    route: PublicRoute,
    authorized_at_ms: u64,
    started: Instant,
    context: PublicSimulationContext,
    transition_key: TransitionKey,
    definition: crate::runtime::adapter::SimulationDefinition,
    operation: PublicOperation,
}

struct PhaseAdmissionContext<'a> {
    adapter: &'a Arc<Mutex<SupervisorAdapter>>,
    authority: &'a Arc<Mutex<Option<SimulationAuthority>>>,
    backend: &'a Arc<dyn PublicSimulationBackend>,
}

#[cfg(test)]
mod tests;
