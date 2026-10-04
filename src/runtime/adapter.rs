//! Transport-independent admission for the public supervisor session.
//!
//! [`SupervisorAdapter`] is the small policy owner between a transport and a
//! running supervisor. It does not open Zenoh, schedule a participant, or
//! execute a service operation. A transport adapter supplies the exact routed
//! key and the decoded Protobuf request, then uses this owner to validate the
//! protected principal, session lease, execution identity, timeline, and
//! generated port metadata before admitting work.
//!
//! Keeping this state machine independent of transport makes the dangerous
//! decisions testable without a router and keeps the protocol feature free of
//! the runner and simulator dependency closures.

use std::collections::{BTreeMap, BTreeSet};

use crate::runtime::session_table::{SessionId, SessionTable, SessionTableError};
use phoxal::communication::route::{PublicOperation, PublicRoute, PublicRouteKind};
use phoxal::communication::session::{
    BindMethodRequest, BindMethodResponse, ExecutionState, ExecutionSummary, ListExecutionsRequest,
    ListExecutionsResponse, ListMethodsRequest, ListMethodsResponse, MethodMetadata, MethodShape,
    OpenSessionRequest, OpenSessionResponse, RenewSessionRequest, RenewSessionResponse,
    SupervisorInfoRequest, SupervisorInfoResponse, SupervisorState, SupervisorStatusRequest,
    SupervisorStatusResponse,
};
use phoxal::communication::validation::{DeploymentTarget, valid_identifier};

/// Default maximum number of execution records retained by one adapter.
pub const DEFAULT_MAX_EXECUTIONS: usize = 256;
/// Default maximum number of public method records retained by one execution.
pub const DEFAULT_MAX_METHODS: usize = 1_024;
/// Default page size used when a list request leaves `page_size` at zero.
pub const DEFAULT_PAGE_SIZE: usize = 64;
/// Default maximum diagnostic detail length in bytes.
pub const DEFAULT_MAX_DETAIL_BYTES: usize = 4 * 1024;
/// Default maximum active sessions for an adapter, named to make the adapter
/// bound visible without requiring callers to import the session table.
pub const DEFAULT_MAX_SESSIONS_PER_ADAPTER: usize =
    crate::runtime::session_table::DEFAULT_MAX_SESSIONS;
/// The fixed byte length of an opaque binding identifier.
pub const BINDING_ID_BYTES: usize = 32;
/// Page tokens are one bounded big-endian `u64` offset.
pub const MAX_PAGE_TOKEN_BYTES: usize = std::mem::size_of::<u64>();
const MAX_VERSION_BYTES: usize = 128;
const MAX_IDENTIFIER_BYTES: usize = 64;
const MAX_FQN_BYTES: usize = 512;

/// Bounded resources owned by one [`SupervisorAdapter`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdapterLimits {
    /// Maximum active logical sessions.
    pub max_sessions: usize,
    /// Maximum active port bindings across all sessions.
    pub max_bindings: usize,
    /// Maximum execution records retained by the adapter.
    pub max_executions: usize,
    /// Maximum public methods retained in one execution definition.
    pub max_methods: usize,
    /// Maximum accepted list page size.
    pub max_page_size: usize,
    /// Maximum bytes in a diagnostic status detail.
    pub max_detail_bytes: usize,
}

impl Default for AdapterLimits {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_SESSIONS_PER_ADAPTER,
            max_bindings: 1_024,
            max_executions: DEFAULT_MAX_EXECUTIONS,
            max_methods: DEFAULT_MAX_METHODS,
            max_page_size: DEFAULT_PAGE_SIZE,
            max_detail_bytes: DEFAULT_MAX_DETAIL_BYTES,
        }
    }
}

impl AdapterLimits {
    /// Construct caller-selected finite bounds.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorAdapterError::InvalidLimits`] when any bound is
    /// zero or cannot be represented by the wire's `u32` page/count fields.
    pub const fn new(
        max_sessions: usize,
        max_bindings: usize,
        max_executions: usize,
        max_methods: usize,
        max_page_size: usize,
        max_detail_bytes: usize,
    ) -> Result<Self, SupervisorAdapterError> {
        if max_sessions == 0
            || max_bindings == 0
            || max_executions == 0
            || max_methods == 0
            || max_page_size == 0
            || max_detail_bytes == 0
            || max_page_size > u32::MAX as usize
            || max_detail_bytes > u32::MAX as usize
        {
            return Err(SupervisorAdapterError::InvalidLimits);
        }
        Ok(Self {
            max_sessions,
            max_bindings,
            max_executions,
            max_methods,
            max_page_size,
            max_detail_bytes,
        })
    }

    fn validate(self) -> Result<(), SupervisorAdapterError> {
        if self.max_sessions == 0
            || self.max_bindings == 0
            || self.max_executions == 0
            || self.max_methods == 0
            || self.max_page_size == 0
            || self.max_detail_bytes == 0
            || self.max_page_size > u32::MAX as usize
            || self.max_detail_bytes > u32::MAX as usize
        {
            return Err(SupervisorAdapterError::InvalidLimits);
        }
        Ok(())
    }
}

/// Public methods implemented by one service instance in one execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMethods {
    instance: String,
    methods: Vec<MethodMetadata>,
}

impl ServiceMethods {
    /// Validate and retain one bounded service instance's generated metadata.
    ///
    /// # Errors
    ///
    /// Returns a typed adapter error for an invalid instance, duplicate port,
    /// invalid kind/FQN, or non-finite port bound.
    pub fn new(
        instance: impl Into<String>,
        methods: Vec<MethodMetadata>,
    ) -> Result<Self, SupervisorAdapterError> {
        let instance = instance.into();
        validate_identifier(&instance, "service instance")?;
        let mut names = BTreeSet::new();
        let mut methods = methods;
        methods.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
        for port in &methods {
            validate_port(port)?;
            if !names.insert(port.endpoint.as_str()) {
                return Err(SupervisorAdapterError::DuplicateMethod {
                    service_instance: instance,
                    endpoint: port.endpoint.clone(),
                });
            }
        }
        Ok(Self { instance, methods })
    }

    /// The deployed service instance identity.
    #[must_use]
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// The generated public method metadata in deterministic owner order.
    #[must_use]
    pub fn methods(&self) -> &[MethodMetadata] {
        &self.methods
    }
}

/// One execution and its generated public service metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationProviderDefinition {
    rate_microhertz: u64,
    service_instance: String,
    port: String,
    shape: MethodShape,
    input_fqn: String,
    payload_fqn: String,
}

impl SimulationProviderDefinition {
    /// Construct one immutable provider requirement from generated metadata.
    pub fn new(
        service_instance: impl Into<String>,
        port: impl Into<String>,
        shape: MethodShape,
        input_fqn: impl Into<String>,
        payload_fqn: impl Into<String>,
        rate_microhertz: u64,
    ) -> Result<Self, SupervisorAdapterError> {
        let definition = Self {
            rate_microhertz,
            service_instance: service_instance.into(),
            port: port.into(),
            shape,
            input_fqn: input_fqn.into(),
            payload_fqn: payload_fqn.into(),
        };
        validate_identifier(&definition.service_instance, "simulation service instance")?;
        validate_identifier(&definition.port, "simulation provider port")?;
        if definition.shape != MethodShape::Observation
            || !valid_fqn(&definition.input_fqn)
            || !valid_fqn(&definition.payload_fqn)
            || definition.payload_fqn.is_empty()
            || definition.rate_microhertz == 0
        {
            return Err(SupervisorAdapterError::InvalidSimulationDefinition);
        }
        Ok(definition)
    }

    /// Immutable publication frequency in millionths of one hertz.
    #[must_use]
    pub const fn rate_microhertz(&self) -> u64 {
        self.rate_microhertz
    }

    /// Whether the source must close a capture at this logical boundary.
    /// The containing definition validates the frequency against its quantum.
    #[must_use]
    pub fn due(&self, boundary: u64, quantum_ns: u64) -> bool {
        let ticks = u128::from(self.rate_microhertz) * u128::from(quantum_ns);
        boundary == 0
            || u128::from(boundary) * ticks / 1_000_000_000_000_000
                > u128::from(boundary - 1) * ticks / 1_000_000_000_000_000
    }

    /// Service instance containing the provider.
    #[must_use]
    pub fn service_instance(&self) -> &str {
        &self.service_instance
    }

    /// Generated provider port name.
    #[must_use]
    pub fn port(&self) -> &str {
        &self.port
    }

    /// Generated public method shape.
    #[must_use]
    pub const fn shape(&self) -> MethodShape {
        self.shape
    }

    /// Input-side payload signature, when the generated port defines one.
    #[must_use]
    pub fn input_fqn(&self) -> &str {
        &self.input_fqn
    }

    /// Provider observation payload signature.
    #[must_use]
    pub fn payload_fqn(&self) -> &str {
        &self.payload_fqn
    }
}

/// Immutable simulation contract compiled into one admitted execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationDefinition {
    model_identity: String,
    quantum_ns: u64,
    providers: Vec<SimulationProviderDefinition>,
}

impl SimulationDefinition {
    /// Construct a complete simulation contract owned by the supervisor.
    pub fn new(
        model_identity: impl Into<String>,
        quantum_ns: u64,
        mut providers: Vec<SimulationProviderDefinition>,
    ) -> Result<Self, SupervisorAdapterError> {
        let model_identity = model_identity.into();
        if model_identity.is_empty()
            || model_identity.len() > MAX_FQN_BYTES
            || !model_identity.is_ascii()
            || model_identity
                .bytes()
                .any(|byte| byte.is_ascii_whitespace())
            || quantum_ns == 0
            || providers.is_empty()
            || providers.iter().any(|provider| {
                u128::from(provider.rate_microhertz) * u128::from(quantum_ns)
                    > 1_000_000_000_000_000
            })
        {
            return Err(SupervisorAdapterError::InvalidSimulationDefinition);
        }
        providers.sort_by(|left, right| {
            left.service_instance
                .cmp(&right.service_instance)
                .then_with(|| left.port.cmp(&right.port))
        });
        if providers.windows(2).any(|pair| {
            pair[0].service_instance == pair[1].service_instance && pair[0].port == pair[1].port
        }) {
            return Err(SupervisorAdapterError::InvalidSimulationDefinition);
        }
        Ok(Self {
            model_identity,
            quantum_ns,
            providers,
        })
    }

    /// Immutable model identity selected by the supervisor bundle.
    #[must_use]
    pub fn model_identity(&self) -> &str {
        &self.model_identity
    }

    /// Immutable common simulation quantum in nanoseconds.
    #[must_use]
    pub const fn quantum_ns(&self) -> u64 {
        self.quantum_ns
    }

    /// Required generated observation providers in canonical order.
    #[must_use]
    pub fn providers(&self) -> &[SimulationProviderDefinition] {
        &self.providers
    }
}

/// One execution and its generated public service metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDefinition {
    summary: ExecutionSummary,
    services: Vec<ServiceMethods>,
    simulation: Option<SimulationDefinition>,
}

impl ExecutionDefinition {
    /// Validate one execution definition before it enters the adapter.
    ///
    /// Service and port order is canonicalized so list responses do not depend
    /// on map insertion order supplied by a host implementation.
    pub fn new(
        summary: ExecutionSummary,
        mut services: Vec<ServiceMethods>,
    ) -> Result<Self, SupervisorAdapterError> {
        validate_identifier(&summary.execution_id, "execution id")?;
        validate_identifier(&summary.timeline_id, "timeline id")?;
        validate_execution_state(summary.state)?;
        services.sort_by(|left, right| left.instance.cmp(&right.instance));
        for pair in services.windows(2) {
            if pair[0].instance == pair[1].instance {
                return Err(SupervisorAdapterError::DuplicateService {
                    service_instance: pair[0].instance.clone(),
                });
            }
        }
        Ok(Self {
            summary,
            services,
            simulation: None,
        })
    }

    /// Attach the complete immutable simulation definition to this execution.
    ///
    /// The definition is checked against the generated service metadata now,
    /// so an authority request cannot invent a model, quantum, provider kind,
    /// or payload signature later.
    pub fn with_simulation(
        mut self,
        definition: SimulationDefinition,
    ) -> Result<Self, SupervisorAdapterError> {
        for provider in definition.providers() {
            let service = self.service(provider.service_instance()).ok_or_else(|| {
                SupervisorAdapterError::ServiceNotFound {
                    execution_id: self.summary.execution_id.clone(),
                    service_instance: provider.service_instance().to_owned(),
                }
            })?;
            let metadata = service
                .methods
                .iter()
                .find(|port| port.endpoint == provider.port())
                .ok_or_else(|| SupervisorAdapterError::MethodNotFound {
                    execution_id: self.summary.execution_id.clone(),
                    service_instance: provider.service_instance().to_owned(),
                    endpoint: provider.port().to_owned(),
                })?;
            let metadata_shape = metadata.shape;
            if metadata_shape != provider.shape()
                || metadata.input_fqn != provider.input_fqn()
                || metadata.output_fqn != provider.payload_fqn()
            {
                return Err(SupervisorAdapterError::SimulationProviderMismatch {
                    service_instance: provider.service_instance().to_owned(),
                    port: provider.port().to_owned(),
                });
            }
        }
        self.simulation = Some(definition);
        Ok(self)
    }

    /// Whether this execution may acquire public simulation authority.
    #[must_use]
    pub const fn is_simulation(&self) -> bool {
        self.simulation.is_some()
    }

    /// The immutable simulation contract, when this execution is configured
    /// for public simulation authority.
    #[must_use]
    pub fn simulation_definition(&self) -> Option<&SimulationDefinition> {
        self.simulation.as_ref()
    }

    /// The execution's exact identity and current timeline.
    #[must_use]
    pub fn summary(&self) -> &ExecutionSummary {
        &self.summary
    }

    /// The services selected in this execution.
    #[must_use]
    pub fn services(&self) -> &[ServiceMethods] {
        &self.services
    }

    fn service(&self, instance: &str) -> Option<&ServiceMethods> {
        self.services
            .binary_search_by(|service| service.instance.as_str().cmp(instance))
            .ok()
            .map(|index| &self.services[index])
    }

    fn total_ports(&self) -> usize {
        self.services
            .iter()
            .map(|service| service.methods.len())
            .sum()
    }
}

/// A binding identity returned after an exact public-port admission.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BindingId([u8; BINDING_ID_BYTES]);

impl BindingId {
    /// Parse the fixed-size wire representation.
    pub fn from_bytes(value: &[u8]) -> Result<Self, SupervisorAdapterError> {
        let bytes = value
            .try_into()
            .map_err(|_| SupervisorAdapterError::InvalidBindingId)?;
        Ok(Self(bytes))
    }

    /// Return the opaque wire bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; BINDING_ID_BYTES] {
        &self.0
    }
}

impl std::fmt::Debug for BindingId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BindingId(<redacted>)")
    }
}

/// The fully validated identity supplied to a public service backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingContext {
    /// Opaque logical-session identifier.
    pub session_id: Vec<u8>,
    /// Opaque binding identifier.
    pub binding_id: Vec<u8>,
    /// Exact execution identity.
    pub execution_id: String,
    /// Exact timeline identity.
    pub timeline_id: String,
    /// Deployed service instance.
    pub service_instance: String,
    /// Admitted generated descriptor.
    pub metadata: MethodMetadata,
}

#[derive(Clone, Debug)]
struct Binding {
    session: SessionId,
    execution_id: String,
    timeline_id: String,
    service_instance: String,
    metadata: MethodMetadata,
}

/// Why previously admitted bindings no longer exist.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Invalidation {
    /// No prior execution existed, so no binding was retired.
    Installed,
    /// A new execution definition retired old bindings for this identity.
    ExecutionReplaced {
        execution_id: String,
        old_timeline_id: String,
        new_timeline_id: String,
        bindings: usize,
    },
    /// A timeline reset retired old bindings while keeping the execution.
    TimelineReset {
        execution_id: String,
        old_timeline_id: String,
        new_timeline_id: String,
        bindings: usize,
    },
    /// An execution was removed and its bindings were retired.
    ExecutionRemoved {
        execution_id: String,
        timeline_id: String,
        bindings: usize,
    },
}

/// Transport-independent supervisor-side public session adapter.
#[derive(Debug)]
pub struct SupervisorAdapter {
    target: DeploymentTarget,
    info: SupervisorInfoResponse,
    status: SupervisorStatusResponse,
    limits: AdapterLimits,
    sessions: SessionTable,
    executions: BTreeMap<String, ExecutionDefinition>,
    bindings: BTreeMap<BindingId, Binding>,
}

impl SupervisorAdapter {
    /// Create an empty adapter with the frozen session protocol and no
    /// execution selected.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, version strings, or a generated
    /// session table that cannot be constructed.
    pub fn new(
        target: DeploymentTarget,
        supervisor_version: impl Into<String>,
        framework_version: impl Into<String>,
        limits: AdapterLimits,
    ) -> Result<Self, SupervisorAdapterError> {
        limits.validate()?;
        let supervisor_version = supervisor_version.into();
        let framework_version = framework_version.into();
        validate_version(&supervisor_version, "supervisor version")?;
        validate_version(&framework_version, "framework version")?;
        let sessions = SessionTable::with_limits(
            crate::runtime::session_table::DEFAULT_LEASE_MS,
            limits.max_sessions,
        )?;
        Ok(Self {
            target,
            info: SupervisorInfoResponse {
                supervisor_version,
                framework_version,
            },
            status: SupervisorStatusResponse {
                state: SupervisorState::Idle,
                detail: None,
            },
            limits,
            sessions,
            executions: BTreeMap::new(),
            bindings: BTreeMap::new(),
        })
    }

    /// Create an adapter with default resource bounds.
    pub fn with_defaults(
        target: DeploymentTarget,
        supervisor_version: impl Into<String>,
        framework_version: impl Into<String>,
    ) -> Result<Self, SupervisorAdapterError> {
        Self::new(
            target,
            supervisor_version,
            framework_version,
            AdapterLimits::default(),
        )
    }

    /// The exact deployment target whose keys this adapter serves.
    #[must_use]
    pub fn target(&self) -> &DeploymentTarget {
        &self.target
    }

    /// The adapter's immutable resource bounds.
    #[must_use]
    pub const fn limits(&self) -> AdapterLimits {
        self.limits
    }

    /// Number of active logical sessions before a lease sweep.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Number of active port bindings.
    #[must_use]
    pub fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    /// Replace the supervisor status projection used by `status`.
    pub fn set_status(
        &mut self,
        state: SupervisorState,
        detail: Option<String>,
    ) -> Result<(), SupervisorAdapterError> {
        if state == SupervisorState::Unspecified {
            return Err(SupervisorAdapterError::InvalidEnum("supervisor state"));
        }
        validate_detail(detail.as_deref(), self.limits.max_detail_bytes)?;
        self.status = SupervisorStatusResponse { state, detail };
        Ok(())
    }

    /// Replace one execution's lifecycle projection without changing its
    /// identity, timeline, or admitted public service metadata.
    #[allow(
        dead_code,
        reason = "the public session client profile validates the adapter without hosting execution state"
    )]
    pub(crate) fn set_execution_state(
        &mut self,
        execution_id: &str,
        state: ExecutionState,
    ) -> Result<(), SupervisorAdapterError> {
        validate_execution_state(state)?;
        let execution = self.executions.get_mut(execution_id).ok_or_else(|| {
            SupervisorAdapterError::ExecutionNotFound {
                execution_id: execution_id.to_owned(),
            }
        })?;
        execution.summary.state = state;
        Ok(())
    }

    /// Install or replace one execution's generated public metadata.
    ///
    /// Replacing an existing execution always retires its bindings, even when
    /// the timeline happens to be unchanged. A host implementation must not
    /// let a stale binding survive a service graph replacement.
    pub fn install_execution(
        &mut self,
        execution: ExecutionDefinition,
    ) -> Result<Invalidation, SupervisorAdapterError> {
        if execution.total_ports() > self.limits.max_methods {
            return Err(SupervisorAdapterError::PortCapacityExceeded);
        }
        if self.executions.len() >= self.limits.max_executions
            && !self
                .executions
                .contains_key(&execution.summary.execution_id)
        {
            return Err(SupervisorAdapterError::ExecutionCapacityExceeded);
        }
        let id = execution.summary.execution_id.clone();
        let old = self.executions.insert(id.clone(), execution.clone());
        let Some(old) = old else {
            return Ok(Invalidation::Installed);
        };
        let bindings = self.remove_bindings_for_execution(&id);
        Ok(Invalidation::ExecutionReplaced {
            execution_id: id,
            old_timeline_id: old.summary.timeline_id,
            new_timeline_id: execution.summary.timeline_id,
            bindings,
        })
    }

    /// Reset one execution to a fresh timeline and retire all old bindings.
    pub fn reset_timeline(
        &mut self,
        execution_id: &str,
        new_timeline_id: impl Into<String>,
    ) -> Result<Invalidation, SupervisorAdapterError> {
        validate_identifier(execution_id, "execution id")?;
        let new_timeline_id = new_timeline_id.into();
        validate_identifier(&new_timeline_id, "timeline id")?;
        let old_timeline_id = {
            let execution = self.executions.get_mut(execution_id).ok_or_else(|| {
                SupervisorAdapterError::ExecutionNotFound {
                    execution_id: execution_id.to_owned(),
                }
            })?;
            if execution.summary.timeline_id == new_timeline_id {
                return Err(SupervisorAdapterError::TimelineUnchanged);
            }
            std::mem::replace(&mut execution.summary.timeline_id, new_timeline_id.clone())
        };
        let bindings = self.remove_bindings_for_execution(execution_id);
        Ok(Invalidation::TimelineReset {
            execution_id: execution_id.to_owned(),
            old_timeline_id,
            new_timeline_id,
            bindings,
        })
    }

    /// Remove an execution and explicitly retire its bindings.
    pub fn remove_execution(
        &mut self,
        execution_id: &str,
    ) -> Result<Invalidation, SupervisorAdapterError> {
        let execution = self.executions.remove(execution_id).ok_or_else(|| {
            SupervisorAdapterError::ExecutionNotFound {
                execution_id: execution_id.to_owned(),
            }
        })?;
        let bindings = self.remove_bindings_for_execution(execution_id);
        Ok(Invalidation::ExecutionRemoved {
            execution_id: execution_id.to_owned(),
            timeline_id: execution.summary.timeline_id,
            bindings,
        })
    }

    /// Admit an exact session open on the protected control lane.
    pub fn open(
        &mut self,
        route: &PublicRoute,
        request: &OpenSessionRequest,
        now_ms: u64,
    ) -> Result<OpenSessionResponse, SupervisorAdapterError> {
        self.require_operation(route, PublicOperation::Open)?;
        self.sweep(now_ms);
        let session = self
            .sessions
            .open(&request.protocol, route.principal(), now_ms)?;
        Ok(OpenSessionResponse {
            session_id: session.id().as_bytes().to_vec(),
            protocol: session.protocol().to_owned(),
            lease_ms: self.sessions.lease_ms(),
        })
    }

    /// Renew an exact principal-bound session.
    pub fn renew(
        &mut self,
        route: &PublicRoute,
        request: &RenewSessionRequest,
        now_ms: u64,
    ) -> Result<RenewSessionResponse, SupervisorAdapterError> {
        self.require_operation(route, PublicOperation::Renew)?;
        let id = SessionId::from_bytes(&request.session_id)?;
        self.sessions.renew(id, route.principal(), now_ms)?;
        Ok(RenewSessionResponse {
            lease_ms: self.sessions.lease_ms(),
        })
    }

    /// Close a session and all bindings owned by it.
    pub fn close(
        &mut self,
        route: &PublicRoute,
        request: &phoxal::communication::session::CloseSessionRequest,
        now_ms: u64,
    ) -> Result<phoxal::communication::session::CloseSessionResponse, SupervisorAdapterError> {
        self.require_operation(route, PublicOperation::Close)?;
        let id = SessionId::from_bytes(&request.session_id)?;
        self.sessions.close(id, route.principal(), now_ms)?;
        self.remove_bindings_for_session(id);
        Ok(phoxal::communication::session::CloseSessionResponse {})
    }

    /// Serve static supervisor package/framework version information.
    pub fn info(
        &mut self,
        route: &PublicRoute,
        session_id: &[u8],
        _request: &SupervisorInfoRequest,
        now_ms: u64,
    ) -> Result<SupervisorInfoResponse, SupervisorAdapterError> {
        self.authorize_session(route, PublicOperation::Info, session_id, now_ms)?;
        Ok(self.info.clone())
    }

    /// Serve the current status projection, independently of execution
    /// readiness.
    pub fn status(
        &mut self,
        route: &PublicRoute,
        session_id: &[u8],
        _request: &SupervisorStatusRequest,
        now_ms: u64,
    ) -> Result<SupervisorStatusResponse, SupervisorAdapterError> {
        self.authorize_session(route, PublicOperation::Status, session_id, now_ms)?;
        Ok(self.status.clone())
    }

    /// List bounded execution summaries with deterministic offset tokens.
    pub fn list_executions(
        &mut self,
        route: &PublicRoute,
        session_id: &[u8],
        request: &ListExecutionsRequest,
        now_ms: u64,
    ) -> Result<ListExecutionsResponse, SupervisorAdapterError> {
        self.authorize_session(route, PublicOperation::ListExecutions, session_id, now_ms)?;
        let page = self.page(
            request.page_size,
            &request.page_token,
            self.executions.len(),
        )?;
        let executions = self
            .executions
            .values()
            .skip(page.offset)
            .take(page.limit)
            .map(|execution| execution.summary.clone())
            .collect();
        Ok(ListExecutionsResponse {
            executions,
            next_page_token: page.next_token(self.executions.len()),
        })
    }

    /// List one exact service instance's generated public method metadata.
    pub fn list_ports(
        &mut self,
        route: &PublicRoute,
        session_id: &[u8],
        request: &ListMethodsRequest,
        now_ms: u64,
    ) -> Result<ListMethodsResponse, SupervisorAdapterError> {
        self.authorize_session(route, PublicOperation::ListMethods, session_id, now_ms)?;
        let execution = self.execution(&request.execution_id)?;
        let service = execution
            .service(&request.service_instance)
            .ok_or_else(|| SupervisorAdapterError::ServiceNotFound {
                execution_id: request.execution_id.clone(),
                service_instance: request.service_instance.clone(),
            })?;
        let page = self.page(
            request.page_size,
            &request.page_token,
            service.methods.len(),
        )?;
        let methods = service
            .methods
            .iter()
            .skip(page.offset)
            .take(page.limit)
            .cloned()
            .collect();
        Ok(ListMethodsResponse {
            methods,
            next_page_token: page.next_token(service.methods.len()),
        })
    }

    /// Return one execution only when it is eligible for simulation authority.
    pub fn simulation_execution(
        &self,
        execution_id: &str,
    ) -> Result<ExecutionSummary, SupervisorAdapterError> {
        let execution = self.execution(execution_id)?;
        if execution.simulation.is_none() {
            return Err(SupervisorAdapterError::SimulationUnavailable {
                execution_id: execution_id.to_owned(),
            });
        }
        if execution.summary.state == ExecutionState::Stopped
            || execution.summary.state == ExecutionState::Failed
        {
            return Err(SupervisorAdapterError::ExecutionUnavailable {
                execution_id: execution_id.to_owned(),
            });
        }
        Ok(execution.summary.clone())
    }

    /// Return the immutable simulation definition compiled into one
    /// simulation-configured execution.
    pub fn simulation_definition(
        &self,
        execution_id: &str,
    ) -> Result<SimulationDefinition, SupervisorAdapterError> {
        let execution = self.execution(execution_id)?;
        execution
            .simulation
            .clone()
            .ok_or_else(|| SupervisorAdapterError::SimulationUnavailable {
                execution_id: execution_id.to_owned(),
            })
    }

    /// Validate one complete simulation provider declaration against generated
    /// service metadata before authority is granted.
    pub fn validate_simulation_provider(
        &self,
        execution_id: &str,
        service_instance: &str,
        port_name: &str,
        shape: i32,
        input_fqn: &str,
        payload_fqn: &str,
    ) -> Result<(), SupervisorAdapterError> {
        let execution = self.execution(execution_id)?;
        let definition = execution.simulation.as_ref().ok_or_else(|| {
            SupervisorAdapterError::SimulationUnavailable {
                execution_id: execution_id.to_owned(),
            }
        })?;
        let service = execution.service(service_instance).ok_or_else(|| {
            SupervisorAdapterError::ServiceNotFound {
                execution_id: execution_id.to_owned(),
                service_instance: service_instance.to_owned(),
            }
        })?;
        let port = service
            .methods
            .iter()
            .find(|port| port.endpoint == port_name)
            .ok_or_else(|| SupervisorAdapterError::MethodNotFound {
                execution_id: execution_id.to_owned(),
                service_instance: service_instance.to_owned(),
                endpoint: port_name.to_owned(),
            })?;
        let metadata_shape = port.shape;
        if metadata_shape != MethodShape::Observation
            || shape != metadata_shape as i32
            || port.input_fqn != input_fqn
            || port.output_fqn != payload_fqn
        {
            return Err(SupervisorAdapterError::SimulationProviderMismatch {
                service_instance: service_instance.to_owned(),
                port: port_name.to_owned(),
            });
        }
        let requested_shape = MethodShape::try_from(shape)
            .map_err(|_| SupervisorAdapterError::InvalidEnum("simulation provider shape"))?;
        if !definition.providers().iter().any(|provider| {
            provider.service_instance() == service_instance
                && provider.port() == port_name
                && provider.shape() == requested_shape
                && provider.input_fqn() == input_fqn
                && provider.payload_fqn() == payload_fqn
        }) {
            return Err(SupervisorAdapterError::SimulationProviderMismatch {
                service_instance: service_instance.to_owned(),
                port: port_name.to_owned(),
            });
        }
        Ok(())
    }

    /// Validate one runtime observation against the immutable provider set
    /// and return its generated metadata for byte-bound checks.
    pub fn validate_simulation_observation(
        &self,
        execution_id: &str,
        service_instance: &str,
        port_name: &str,
    ) -> Result<MethodMetadata, SupervisorAdapterError> {
        let execution = self.execution(execution_id)?;
        let definition = execution.simulation.as_ref().ok_or_else(|| {
            SupervisorAdapterError::SimulationUnavailable {
                execution_id: execution_id.to_owned(),
            }
        })?;
        if !definition.providers().iter().any(|provider| {
            provider.service_instance() == service_instance && provider.port() == port_name
        }) {
            return Err(SupervisorAdapterError::SimulationProviderMismatch {
                service_instance: service_instance.to_owned(),
                port: port_name.to_owned(),
            });
        }
        let service = execution.service(service_instance).ok_or_else(|| {
            SupervisorAdapterError::ServiceNotFound {
                execution_id: execution_id.to_owned(),
                service_instance: service_instance.to_owned(),
            }
        })?;
        service
            .methods
            .iter()
            .find(|port| port.endpoint == port_name)
            .cloned()
            .ok_or_else(|| SupervisorAdapterError::MethodNotFound {
                execution_id: execution_id.to_owned(),
                service_instance: service_instance.to_owned(),
                endpoint: port_name.to_owned(),
            })
    }

    /// Admit a binding only when the current generated metadata exactly equals
    /// the caller's expected descriptor.
    pub fn bind(
        &mut self,
        route: &PublicRoute,
        request: &BindMethodRequest,
        now_ms: u64,
    ) -> Result<BindMethodResponse, SupervisorAdapterError> {
        self.require_operation(route, PublicOperation::BindMethod)?;
        let session = SessionId::from_bytes(&request.session_id)?;
        self.authorize_session(
            route,
            PublicOperation::BindMethod,
            &request.session_id,
            now_ms,
        )?;
        let expected = request
            .expected
            .as_ref()
            .ok_or(SupervisorAdapterError::MissingMethodMetadata)?;
        validate_port(expected)?;
        let execution = self.execution(&request.execution_id)?;
        if execution.summary.state == ExecutionState::Stopped
            || execution.summary.state == ExecutionState::Failed
        {
            return Err(SupervisorAdapterError::ExecutionUnavailable {
                execution_id: request.execution_id.clone(),
            });
        }
        if execution.summary.timeline_id.is_empty() {
            return Err(SupervisorAdapterError::InvalidExecution("empty timeline"));
        }
        let service = execution
            .service(&request.service_instance)
            .ok_or_else(|| SupervisorAdapterError::ServiceNotFound {
                execution_id: request.execution_id.clone(),
                service_instance: request.service_instance.clone(),
            })?;
        let admitted = service
            .methods
            .iter()
            .find(|port| *port == expected)
            .ok_or_else(|| SupervisorAdapterError::MethodNotFound {
                execution_id: request.execution_id.clone(),
                service_instance: request.service_instance.clone(),
                endpoint: expected.endpoint.clone(),
            })?
            .clone();
        if self.bindings.len() >= self.limits.max_bindings {
            return Err(SupervisorAdapterError::BindingCapacityExceeded);
        }
        let id = new_binding_id(&self.bindings)?;
        self.bindings.insert(
            id,
            Binding {
                session,
                execution_id: request.execution_id.clone(),
                timeline_id: execution.summary.timeline_id.clone(),
                service_instance: request.service_instance.clone(),
                metadata: admitted.clone(),
            },
        );
        Ok(BindMethodResponse {
            binding_id: id.as_bytes().to_vec(),
            admitted: Some(admitted),
        })
    }

    /// Authorize a simulation request against the active logical session.
    ///
    /// Simulation routes have a dedicated lane rather than a session operation
    /// suffix, so this check is explicit and is performed for every authority
    /// request before grant or boundary state is touched.
    pub fn authorize_simulation_session(
        &mut self,
        route: &PublicRoute,
        session_id: &[u8],
        now_ms: u64,
    ) -> Result<SessionId, SupervisorAdapterError> {
        self.require_route(route, PublicRouteKind::Simulation)?;
        let id = SessionId::from_bytes(session_id)?;
        self.sessions.authorize(id, route.principal(), now_ms)?;
        Ok(id)
    }

    /// Check whether an authority owner's logical session is still active.
    ///
    /// This is used while reclaiming an authority whose client disappeared
    /// without sending release. It intentionally returns only a boolean so an
    /// expired session's diagnostic cannot be confused with a new admission.
    pub fn simulation_session_active(
        &mut self,
        principal: &str,
        session_id: &[u8],
        now_ms: u64,
    ) -> bool {
        let Ok(id) = SessionId::from_bytes(session_id) else {
            return false;
        };
        self.sessions.authorize(id, principal, now_ms).is_ok()
    }

    /// Validate a previously admitted binding before a read or command.
    ///
    /// This is the adapter's transport-independent stale-resource check. The
    /// later operation implementation can call it immediately before domain
    /// admission and then select its own typed payload path.
    pub fn validate_binding(
        &mut self,
        route: &PublicRoute,
        request: &phoxal::communication::session::OperationRequest,
        now_ms: u64,
    ) -> Result<MethodMetadata, SupervisorAdapterError> {
        Ok(self
            .validate_binding_context(route, request, now_ms)?
            .metadata)
    }

    /// Validate one bound operation and return all identity required by the
    /// service-owned public backend.
    pub fn validate_operation_binding(
        &mut self,
        route: &PublicRoute,
        request: &phoxal::communication::session::OperationRequest,
        operation: PublicOperation,
        now_ms: u64,
    ) -> Result<BindingContext, SupervisorAdapterError> {
        let context = self.validate_binding_context(route, request, now_ms)?;
        let shape = context.metadata.shape;
        let valid = match operation {
            PublicOperation::Call => shape == MethodShape::Call,
            PublicOperation::Observe => shape == MethodShape::Observation,
            _ => false,
        };
        if !valid {
            return Err(SupervisorAdapterError::BindingShapeMismatch { operation, shape });
        }
        Ok(context)
    }

    fn validate_binding_context(
        &mut self,
        route: &PublicRoute,
        request: &phoxal::communication::session::OperationRequest,
        now_ms: u64,
    ) -> Result<BindingContext, SupervisorAdapterError> {
        let session = SessionId::from_bytes(&request.session_id)?;
        let binding_id = BindingId::from_bytes(&request.binding_id)?;
        self.authorize_session(route, route.operation(), &request.session_id, now_ms)?;
        let binding = self
            .bindings
            .get(&binding_id)
            .ok_or(SupervisorAdapterError::UnknownBinding)?;
        if binding.session != session {
            return Err(SupervisorAdapterError::BindingSessionMismatch);
        }
        let Some(execution) = self.executions.get(&binding.execution_id) else {
            return Err(SupervisorAdapterError::ExecutionInvalidated {
                execution_id: binding.execution_id.clone(),
            });
        };
        if execution.summary.timeline_id != binding.timeline_id {
            return Err(SupervisorAdapterError::TimelineInvalidated {
                execution_id: binding.execution_id.clone(),
                expected: binding.timeline_id.clone(),
                current: execution.summary.timeline_id.clone(),
            });
        }
        let Some(service) = execution.service(&binding.service_instance) else {
            return Err(SupervisorAdapterError::BindingInvalidated {
                execution_id: binding.execution_id.clone(),
                service_instance: binding.service_instance.clone(),
            });
        };
        if !service.methods.iter().any(|port| port == &binding.metadata) {
            return Err(SupervisorAdapterError::BindingInvalidated {
                execution_id: binding.execution_id.clone(),
                service_instance: binding.service_instance.clone(),
            });
        }
        if request.execution_id != binding.execution_id {
            return Err(SupervisorAdapterError::ExecutionMismatch);
        }
        if request.timeline_id != binding.timeline_id {
            return Err(SupervisorAdapterError::TimelineMismatch);
        }
        Ok(BindingContext {
            session_id: request.session_id.clone(),
            binding_id: request.binding_id.clone(),
            execution_id: binding.execution_id.clone(),
            timeline_id: binding.timeline_id.clone(),
            service_instance: binding.service_instance.clone(),
            metadata: binding.metadata.clone(),
        })
    }

    /// Expire leases and clean all resources owned by expired sessions.
    pub fn sweep(&mut self, now_ms: u64) {
        let candidates = self
            .bindings
            .iter()
            .map(|(id, binding)| (*id, binding.session))
            .collect::<Vec<_>>();
        let expired = candidates
            .into_iter()
            .filter_map(|(id, session)| {
                let is_expired = match self.sessions.principal(session) {
                    Some(principal) => self
                        .sessions
                        .authorize(session, &principal, now_ms)
                        .is_err(),
                    None => true,
                };
                is_expired.then_some(id)
            })
            .collect::<Vec<_>>();
        self.sessions.expire(now_ms);
        for id in expired {
            self.bindings.remove(&id);
        }
    }

    fn execution(
        &self,
        execution_id: &str,
    ) -> Result<&ExecutionDefinition, SupervisorAdapterError> {
        self.executions
            .get(execution_id)
            .ok_or_else(|| SupervisorAdapterError::ExecutionNotFound {
                execution_id: execution_id.to_owned(),
            })
    }

    fn require_route(
        &self,
        route: &PublicRoute,
        expected: PublicRouteKind,
    ) -> Result<(), SupervisorAdapterError> {
        if route.target() != &self.target {
            return Err(SupervisorAdapterError::WrongRoute);
        }
        if route.kind() != expected {
            return Err(SupervisorAdapterError::RouteKindMismatch {
                expected,
                actual: route.kind(),
            });
        }
        Ok(())
    }

    fn authorize_session(
        &mut self,
        route: &PublicRoute,
        expected_operation: PublicOperation,
        session_id: &[u8],
        now_ms: u64,
    ) -> Result<SessionId, SupervisorAdapterError> {
        self.require_operation(route, expected_operation)?;
        let id = SessionId::from_bytes(session_id)?;
        self.sessions.authorize(id, route.principal(), now_ms)?;
        Ok(id)
    }

    fn require_operation(
        &self,
        route: &PublicRoute,
        expected: PublicOperation,
    ) -> Result<(), SupervisorAdapterError> {
        self.require_route(route, expected.kind())?;
        if route.operation() != expected {
            return Err(SupervisorAdapterError::OperationMismatch {
                expected,
                actual: route.operation(),
            });
        }
        Ok(())
    }

    fn remove_bindings_for_execution(&mut self, execution_id: &str) -> usize {
        let before = self.bindings.len();
        self.bindings
            .retain(|_, binding| binding.execution_id != execution_id);
        before - self.bindings.len()
    }

    fn remove_bindings_for_session(&mut self, session: SessionId) -> usize {
        let before = self.bindings.len();
        self.bindings
            .retain(|_, binding| binding.session != session);
        before - self.bindings.len()
    }

    fn page(
        &self,
        requested_size: u32,
        token: &[u8],
        total: usize,
    ) -> Result<Page, SupervisorAdapterError> {
        let limit = if requested_size == 0 {
            self.limits.max_page_size
        } else {
            let requested = usize::try_from(requested_size)
                .map_err(|_| SupervisorAdapterError::InvalidPageSize)?;
            if requested > self.limits.max_page_size {
                return Err(SupervisorAdapterError::PageSizeExceeded);
            }
            requested
        };
        if token.len() > MAX_PAGE_TOKEN_BYTES {
            return Err(SupervisorAdapterError::InvalidPageToken);
        }
        let offset = if token.is_empty() {
            0
        } else if token.len() == MAX_PAGE_TOKEN_BYTES {
            let bytes: [u8; MAX_PAGE_TOKEN_BYTES] = token
                .try_into()
                .map_err(|_| SupervisorAdapterError::InvalidPageToken)?;
            usize::try_from(u64::from_be_bytes(bytes))
                .map_err(|_| SupervisorAdapterError::InvalidPageToken)?
        } else {
            return Err(SupervisorAdapterError::InvalidPageToken);
        };
        if offset > total {
            return Err(SupervisorAdapterError::InvalidPageToken);
        }
        Ok(Page { offset, limit })
    }
}

#[derive(Clone, Copy, Debug)]
struct Page {
    offset: usize,
    limit: usize,
}

impl Page {
    fn next_token(self, total: usize) -> Vec<u8> {
        let next = self.offset.saturating_add(self.limit);
        if next >= total {
            Vec::new()
        } else {
            (next as u64).to_be_bytes().to_vec()
        }
    }
}

fn validate_identifier(value: &str, field: &'static str) -> Result<(), SupervisorAdapterError> {
    if !valid_identifier(value) || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(SupervisorAdapterError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_version(value: &str, field: &'static str) -> Result<(), SupervisorAdapterError> {
    if value.is_empty()
        || value.len() > MAX_VERSION_BYTES
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(SupervisorAdapterError::InvalidVersion { field });
    }
    Ok(())
}

fn validate_detail(value: Option<&str>, max_bytes: usize) -> Result<(), SupervisorAdapterError> {
    if let Some(value) = value
        && (value.len() > max_bytes || !value.is_ascii())
    {
        return Err(SupervisorAdapterError::InvalidDetail);
    }
    Ok(())
}

fn validate_execution_state(state: ExecutionState) -> Result<(), SupervisorAdapterError> {
    if state == ExecutionState::Unspecified {
        return Err(SupervisorAdapterError::InvalidEnum("execution state"));
    }
    Ok(())
}

fn validate_port(port: &MethodMetadata) -> Result<(), SupervisorAdapterError> {
    validate_identifier(&port.endpoint, "port name")?;
    let shape = port.shape;
    if shape == MethodShape::Unspecified {
        return Err(SupervisorAdapterError::InvalidEnum("method shape"));
    }
    if (port.input_fqn.is_empty() && port.output_fqn.is_empty())
        || !valid_fqn(&port.input_fqn)
        || !valid_fqn(&port.output_fqn)
    {
        return Err(SupervisorAdapterError::InvalidMethodMetadata);
    }
    if port.max_message_bytes == 0
        || port.max_buffered_items == 0
        || (shape == MethodShape::Call && port.retained_latest)
        || port.lease_valid_for_ms == Some(0)
    {
        return Err(SupervisorAdapterError::InvalidMethodMetadata);
    }
    Ok(())
}

fn valid_fqn(value: &str) -> bool {
    value.is_empty()
        || (value.len() <= MAX_FQN_BYTES
            && value.split('.').all(|segment| {
                !segment.is_empty()
                    && segment
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                    && segment
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            }))
}

fn new_binding_id(
    bindings: &BTreeMap<BindingId, Binding>,
) -> Result<BindingId, SupervisorAdapterError> {
    let mut bytes = [0_u8; BINDING_ID_BYTES];
    getrandom::fill(&mut bytes).map_err(|_| SupervisorAdapterError::EntropyUnavailable)?;
    let id = BindingId(bytes);
    if bindings.contains_key(&id) {
        return Err(SupervisorAdapterError::BindingIdCollision);
    }
    Ok(id)
}

/// Public session adapter failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupervisorAdapterError {
    /// One configured resource bound was zero or not representable on wire.
    #[error("public supervisor adapter limits must be finite and nonzero")]
    InvalidLimits,
    /// The route principal is not a protected identifier segment.
    #[error("public route principal is invalid")]
    InvalidPrincipal,
    /// The route does not belong to this adapter's target.
    #[error("public route addresses another deployment target")]
    WrongRoute,
    /// The route key has an invalid number or spelling of segments.
    #[error("public route is malformed")]
    MalformedRoute,
    /// The request was sent over the wrong ACL lane.
    #[error("public route lane mismatch: expected {expected:?}, got {actual:?}")]
    RouteKindMismatch {
        /// The lane required by the operation.
        expected: PublicRouteKind,
        /// The lane encoded by the request route.
        actual: PublicRouteKind,
    },
    /// The request reached the right ACL lane but the wrong exact operation.
    #[error("public route operation mismatch: expected {expected:?}, got {actual:?}")]
    OperationMismatch {
        /// The exact operation required by the adapter method.
        expected: PublicOperation,
        /// The operation encoded by the incoming route.
        actual: PublicOperation,
    },
    /// A generated binding identifier was not exactly 32 bytes.
    #[error("binding identifier is invalid")]
    InvalidBindingId,
    /// A session table operation failed.
    #[error("session admission failed: {0}")]
    Session(#[from] SessionTableError),
    /// A diagnostic version was empty, non-ASCII, or oversized.
    #[error("{field} is invalid")]
    InvalidVersion { field: &'static str },
    /// A status detail was non-ASCII or exceeded its bound.
    #[error("status detail is invalid")]
    InvalidDetail,
    /// A mandatory enum was unspecified or unknown.
    #[error("{0} is unspecified or unknown")]
    InvalidEnum(&'static str),
    /// One execution or timeline identifier violated the route grammar.
    #[error("{field} is invalid")]
    InvalidIdentifier { field: &'static str },
    /// An execution's state or identity was internally invalid.
    #[error("execution definition is invalid: {0}")]
    InvalidExecution(&'static str),
    /// A generated public method descriptor is not admissible.
    #[error("public method metadata is invalid")]
    InvalidMethodMetadata,
    /// One service instance name was repeated.
    #[error("service instance is repeated: {service_instance}")]
    DuplicateService { service_instance: String },
    /// One public method name was repeated within a service instance.
    #[error("method {endpoint} is repeated in service {service_instance}")]
    DuplicateMethod {
        /// The service instance with the duplicate.
        service_instance: String,
        /// The repeated public method name.
        endpoint: String,
    },
    /// The adapter has no room for another execution.
    #[error("execution capacity is exhausted")]
    ExecutionCapacityExceeded,
    /// The adapter has no room for another public method.
    #[error("public method capacity is exhausted")]
    PortCapacityExceeded,
    /// The adapter has no room for another binding.
    #[error("binding capacity is exhausted")]
    BindingCapacityExceeded,
    /// The selected execution is not known.
    #[error("execution is not found: {execution_id}")]
    ExecutionNotFound { execution_id: String },
    /// The selected execution is known but cannot serve a binding.
    #[error("execution is unavailable: {execution_id}")]
    ExecutionUnavailable { execution_id: String },
    /// The selected execution is a hardware or ordinary runtime execution.
    #[error("execution is not configured for public simulation authority: {execution_id}")]
    SimulationUnavailable { execution_id: String },
    /// The immutable simulation definition is incomplete or internally
    /// inconsistent with generated service metadata.
    #[error("simulation definition is invalid")]
    InvalidSimulationDefinition,
    /// The selected service instance is not known.
    #[error("service {service_instance} is not found in execution {execution_id}")]
    ServiceNotFound {
        /// The execution that was queried.
        execution_id: String,
        /// The service instance that was queried.
        service_instance: String,
    },
    /// The selected public method is not known or no longer matches.
    #[error(
        "method {endpoint} is not found in service {service_instance} of execution {execution_id}"
    )]
    MethodNotFound {
        /// The execution that was queried.
        execution_id: String,
        /// The service instance that was queried.
        service_instance: String,
        /// The public method name that was queried.
        endpoint: String,
    },
    /// The requested simulation provider does not match an admitted generated
    /// publication port and payload contract.
    #[error("simulation provider does not match generated port {service_instance}/{port}")]
    SimulationProviderMismatch {
        /// Service instance containing the provider port.
        service_instance: String,
        /// Provider port name.
        port: String,
    },
    /// A bind request omitted its expected generated descriptor.
    #[error("bind request is missing expected port metadata")]
    MissingMethodMetadata,
    /// A page size was not representable or otherwise invalid.
    #[error("page size is invalid")]
    InvalidPageSize,
    /// A requested page exceeds the configured finite limit.
    #[error("page size exceeds the configured limit")]
    PageSizeExceeded,
    /// A page token is malformed or points outside the current result.
    #[error("page token is invalid")]
    InvalidPageToken,
    /// A binding references a session different from its owner.
    #[error("binding belongs to another session")]
    BindingSessionMismatch,
    /// No active binding has the requested identifier.
    #[error("binding is unknown or was invalidated")]
    UnknownBinding,
    /// The operation does not match the admitted public method shape.
    #[error("public method shape {shape:?} cannot be used with {operation:?}")]
    BindingShapeMismatch {
        /// Requested operation.
        operation: PublicOperation,
        /// Admitted method shape.
        shape: MethodShape,
    },
    /// A stale request uses a different execution identity.
    #[error("operation execution identity does not match its binding")]
    ExecutionMismatch,
    /// A stale request uses a different timeline identity.
    #[error("operation timeline identity does not match its binding")]
    TimelineMismatch,
    /// The old execution was replaced or removed after a binding was issued.
    #[error("execution binding was invalidated: {execution_id}")]
    ExecutionInvalidated { execution_id: String },
    /// The bound service or public descriptor was replaced in place.
    #[error("public binding was invalidated for {service_instance} in execution {execution_id}")]
    BindingInvalidated {
        /// The execution whose service graph changed.
        execution_id: String,
        /// The service instance whose descriptor changed.
        service_instance: String,
    },
    /// The old timeline was reset after a binding was issued.
    #[error("execution timeline was invalidated for {execution_id}: {expected} -> {current}")]
    TimelineInvalidated {
        /// The execution whose timeline changed.
        execution_id: String,
        /// The timeline captured by the old binding.
        expected: String,
        /// The currently active timeline.
        current: String,
    },
    /// A timeline reset was requested without changing the timeline identity.
    #[error("timeline identity did not change")]
    TimelineUnchanged,
    /// Secure random bytes were unavailable.
    #[error("cannot obtain entropy for binding identifier")]
    EntropyUnavailable,
    /// A generated binding identifier collided with a live binding.
    #[error("generated binding identifier collides with a live binding")]
    BindingIdCollision,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum TestRole {
        State,
        Read,
        Commands,
    }

    impl TestRole {
        const fn shape(self) -> MethodShape {
            match self {
                Self::State => MethodShape::Observation,
                Self::Read | Self::Commands => MethodShape::Call,
            }
        }

        const fn retained_latest(self) -> bool {
            matches!(self, Self::State)
        }
    }

    fn target() -> DeploymentTarget {
        DeploymentTarget::new("workshop", "rover-01").expect("target")
    }

    fn port(endpoint: &str, role: TestRole) -> MethodMetadata {
        MethodMetadata {
            endpoint: endpoint.to_owned(),
            shape: role.shape(),
            input_fqn: "example.Request".to_owned(),
            output_fqn: "example.Response".to_owned(),
            max_message_bytes: 1024,
            max_buffered_items: 32,
            retained_latest: role.retained_latest(),
            lease_valid_for_ms: None,
        }
    }

    fn adapter() -> SupervisorAdapter {
        SupervisorAdapter::with_defaults(target(), "0.1.0", "0.1.0").expect("adapter")
    }

    fn execution(timeline: &str) -> ExecutionDefinition {
        ExecutionDefinition::new(
            ExecutionSummary {
                execution_id: "execution-1".to_owned(),
                timeline_id: timeline.to_owned(),
                state: ExecutionState::Ready,
            },
            vec![
                ServiceMethods::new(
                    "navigation",
                    vec![
                        port("status", TestRole::State),
                        port("commands", TestRole::Commands),
                    ],
                )
                .expect("service"),
            ],
        )
        .expect("execution")
    }

    fn open(adapter: &mut SupervisorAdapter, principal: &str) -> (PublicRoute, Vec<u8>) {
        let route = target()
            .public_route(principal, PublicRouteKind::Control)
            .expect("route");
        let response = adapter
            .open(
                &route,
                &OpenSessionRequest {
                    protocol: phoxal::communication::SESSION_PROTOCOL.to_owned(),
                },
                100,
            )
            .expect("open");
        (route, response.session_id)
    }

    #[test]
    fn simulation_admission_requires_an_explicit_simulation_execution() {
        let mut adapter = adapter();
        adapter
            .install_execution(execution("timeline-1"))
            .expect("install hardware execution");
        assert!(matches!(
            adapter.simulation_execution("execution-1"),
            Err(SupervisorAdapterError::SimulationUnavailable { .. })
        ));
        let simulation = SimulationDefinition::new(
            "example-model",
            1_000_000,
            vec![
                SimulationProviderDefinition::new(
                    "navigation",
                    "status",
                    MethodShape::Observation,
                    "example.Request",
                    "example.Response",
                    100_000_000,
                )
                .expect("provider"),
            ],
        )
        .expect("simulation definition");
        adapter
            .install_execution(
                execution("timeline-2")
                    .with_simulation(simulation)
                    .expect("simulation execution"),
            )
            .expect("replace with simulation execution");
        assert_eq!(
            adapter
                .simulation_execution("execution-1")
                .expect("simulation execution")
                .timeline_id,
            "timeline-2"
        );
    }

    #[test]
    fn open_renew_info_status_and_close_are_principal_bound() {
        let mut adapter = adapter();
        let (_, session) = open(&mut adapter, "operator-a");
        let inspect = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::Info))
            .expect("route");
        let info = adapter
            .info(
                &inspect,
                &session,
                &SupervisorInfoRequest {
                    session_id: session.clone(),
                },
                101,
            )
            .expect("info");
        assert_eq!(info.framework_version, "0.1.0");
        adapter
            .set_status(SupervisorState::Preparing, Some("loading".to_owned()))
            .expect("status");
        let status_route = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::Status))
            .expect("route");
        assert_eq!(
            adapter
                .status(
                    &status_route,
                    &session,
                    &SupervisorStatusRequest {
                        session_id: session.clone(),
                    },
                    102,
                )
                .expect("status")
                .state,
            SupervisorState::Preparing
        );
        adapter
            .renew(
                &target()
                    .public_route("operator-a", PublicRouteKind::Control)
                    .and_then(|route| route.with_operation(PublicOperation::Renew))
                    .expect("route"),
                &RenewSessionRequest {
                    session_id: session.clone(),
                },
                103,
            )
            .expect("renew");
        let wrong_principal = target()
            .public_route("operator-b", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::Info))
            .expect("route");
        assert_eq!(
            adapter.info(
                &wrong_principal,
                &session,
                &SupervisorInfoRequest {
                    session_id: session.clone(),
                },
                104,
            ),
            Err(SupervisorAdapterError::Session(
                SessionTableError::PrincipalMismatch
            ))
        );
        adapter
            .close(
                &target()
                    .public_route("operator-a", PublicRouteKind::Control)
                    .and_then(|route| route.with_operation(PublicOperation::Close))
                    .expect("route"),
                &phoxal::communication::session::CloseSessionRequest {
                    session_id: session.clone(),
                },
                105,
            )
            .expect("close");
        assert_eq!(adapter.session_count(), 0);
        assert_eq!(
            adapter.info(
                &inspect,
                &session,
                &SupervisorInfoRequest {
                    session_id: session.clone(),
                },
                106,
            ),
            Err(SupervisorAdapterError::Session(
                SessionTableError::UnknownSession
            ))
        );
    }

    #[test]
    fn list_and_bind_require_exact_execution_service_and_port_metadata() {
        let mut adapter = adapter();
        let (_, session) = open(&mut adapter, "operator-a");
        let inspect = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::ListExecutions))
            .expect("route");
        let ports_route = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::ListMethods))
            .expect("route");
        let bind_route = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::BindMethod))
            .expect("route");
        adapter
            .install_execution(execution("timeline-1"))
            .expect("install");
        let listed = adapter
            .list_executions(
                &inspect,
                &session,
                &ListExecutionsRequest {
                    page_size: 1,
                    page_token: Vec::new(),
                    session_id: session.clone(),
                },
                101,
            )
            .expect("list");
        assert_eq!(listed.executions.len(), 1);
        let first_ports = adapter
            .list_ports(
                &ports_route,
                &session,
                &ListMethodsRequest {
                    execution_id: "execution-1".to_owned(),
                    service_instance: "navigation".to_owned(),
                    page_size: 1,
                    page_token: Vec::new(),
                    session_id: session.clone(),
                },
                101,
            )
            .expect("list methods");
        assert_eq!(first_ports.methods.len(), 1);
        assert_eq!(first_ports.methods[0].endpoint, "commands");
        assert_eq!(first_ports.next_page_token.len(), MAX_PAGE_TOKEN_BYTES);
        let expected = port("commands", TestRole::Commands);
        let response = adapter
            .bind(
                &bind_route,
                &BindMethodRequest {
                    session_id: session.clone(),
                    execution_id: "execution-1".to_owned(),
                    service_instance: "navigation".to_owned(),
                    expected: Some(expected.clone()),
                },
                102,
            )
            .expect("bind");
        assert_eq!(response.admitted, Some(expected));
        assert_eq!(adapter.binding_count(), 1);
        let wrong = adapter.bind(
            &bind_route,
            &BindMethodRequest {
                session_id: session,
                execution_id: "execution-1".to_owned(),
                service_instance: "navigation".to_owned(),
                expected: Some(port("commands", TestRole::State)),
            },
            103,
        );
        assert!(matches!(
            wrong,
            Err(SupervisorAdapterError::MethodNotFound { .. })
        ));
    }

    #[test]
    fn timeline_reset_and_execution_replacement_invalidate_bindings() {
        let mut adapter = adapter();
        let (_, session) = open(&mut adapter, "operator-a");
        let inspect = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::BindMethod))
            .expect("route");
        let mutation = target()
            .public_route("operator-a", PublicRouteKind::Mutation)
            .and_then(|route| route.with_operation(PublicOperation::Call))
            .expect("route");
        adapter
            .install_execution(execution("timeline-1"))
            .expect("install");
        let binding = adapter
            .bind(
                &inspect,
                &BindMethodRequest {
                    session_id: session.clone(),
                    execution_id: "execution-1".to_owned(),
                    service_instance: "navigation".to_owned(),
                    expected: Some(port("commands", TestRole::Commands)),
                },
                101,
            )
            .expect("bind");
        let request = phoxal::communication::session::OperationRequest {
            session_id: session.clone(),
            binding_id: binding.binding_id.clone(),
            correlation_id: vec![1],
            execution_id: "execution-1".to_owned(),
            timeline_id: "timeline-1".to_owned(),
            payload: Vec::new(),
            timeout_ms: 10,
        };
        assert!(adapter.validate_binding(&mutation, &request, 102).is_ok());
        assert_eq!(
            adapter
                .reset_timeline("execution-1", "timeline-2")
                .expect("reset"),
            Invalidation::TimelineReset {
                execution_id: "execution-1".to_owned(),
                old_timeline_id: "timeline-1".to_owned(),
                new_timeline_id: "timeline-2".to_owned(),
                bindings: 1,
            }
        );
        assert_eq!(
            adapter.validate_binding(&mutation, &request, 103),
            Err(SupervisorAdapterError::UnknownBinding)
        );
        adapter
            .install_execution(execution("timeline-3"))
            .expect("replace");
        assert_eq!(adapter.binding_count(), 0);
    }

    #[test]
    fn lease_expiry_sweeps_sessions_and_their_bindings() {
        let limits = AdapterLimits::new(1, 1, 1, 2, 1, 32).expect("limits");
        let mut adapter =
            SupervisorAdapter::new(target(), "0.1.0", "0.1.0", limits).expect("adapter");
        let (_, session) = open(&mut adapter, "operator-a");
        adapter
            .install_execution(execution("timeline-1"))
            .expect("install");
        adapter
            .bind(
                &target()
                    .public_route("operator-a", PublicRouteKind::Inspection)
                    .and_then(|route| route.with_operation(PublicOperation::BindMethod))
                    .expect("route"),
                &BindMethodRequest {
                    session_id: session,
                    execution_id: "execution-1".to_owned(),
                    service_instance: "navigation".to_owned(),
                    expected: Some(port("status", TestRole::State)),
                },
                100,
            )
            .expect("bind");
        adapter.sweep(30_100);
        assert_eq!(adapter.session_count(), 0);
        assert_eq!(adapter.binding_count(), 0);
    }

    #[test]
    fn a_fresh_adapter_after_restart_rejects_old_session_state() {
        let mut first = adapter();
        let (inspect, session) = {
            let (_, session) = open(&mut first, "operator-a");
            (
                target()
                    .public_route("operator-a", PublicRouteKind::Inspection)
                    .and_then(|route| route.with_operation(PublicOperation::Info))
                    .expect("route"),
                session,
            )
        };
        let mut restarted = adapter();
        assert_eq!(
            restarted.info(
                &inspect,
                &session,
                &SupervisorInfoRequest {
                    session_id: session.clone(),
                },
                101,
            ),
            Err(SupervisorAdapterError::Session(
                SessionTableError::UnknownSession
            ))
        );
    }

    #[test]
    fn page_tokens_and_route_lanes_are_bounded() {
        let limits = AdapterLimits::new(2, 2, 2, 2, 1, 16).expect("limits");
        let mut adapter =
            SupervisorAdapter::new(target(), "0.1.0", "0.1.0", limits).expect("adapter");
        let (control, session) = open(&mut adapter, "operator-a");
        adapter
            .install_execution(execution("timeline-1"))
            .expect("install");
        let inspect = target()
            .public_route("operator-a", PublicRouteKind::Inspection)
            .and_then(|route| route.with_operation(PublicOperation::ListExecutions))
            .expect("route");
        assert_eq!(
            adapter.open(
                &inspect,
                &OpenSessionRequest {
                    protocol: phoxal::communication::SESSION_PROTOCOL.to_owned(),
                },
                101,
            ),
            Err(SupervisorAdapterError::RouteKindMismatch {
                expected: PublicRouteKind::Control,
                actual: PublicRouteKind::Inspection,
            })
        );
        let first = adapter
            .list_executions(
                &inspect,
                &session,
                &ListExecutionsRequest {
                    page_size: 1,
                    page_token: Vec::new(),
                    session_id: session.clone(),
                },
                102,
            )
            .expect("page");
        assert!(first.next_page_token.is_empty());
        assert_eq!(
            adapter.list_executions(
                &inspect,
                &session,
                &ListExecutionsRequest {
                    page_size: 2,
                    page_token: Vec::new(),
                    session_id: session.clone(),
                },
                103,
            ),
            Err(SupervisorAdapterError::PageSizeExceeded)
        );
        assert_eq!(
            adapter.list_executions(
                &inspect,
                &session,
                &ListExecutionsRequest {
                    page_size: 1,
                    page_token: vec![1],
                    session_id: session.clone(),
                },
                104,
            ),
            Err(SupervisorAdapterError::InvalidPageToken)
        );
        assert_eq!(control.kind(), PublicRouteKind::Control);
    }

    #[test]
    fn invalid_bounds_states_versions_and_metadata_are_refused() {
        assert_eq!(
            AdapterLimits::new(0, 1, 1, 1, 1, 1),
            Err(SupervisorAdapterError::InvalidLimits)
        );
        assert!(matches!(
            SupervisorAdapter::with_defaults(target(), "", "0.1.0"),
            Err(SupervisorAdapterError::InvalidVersion { .. })
        ));
        assert!(matches!(
            ServiceMethods::new(
                "navigation",
                vec![MethodMetadata {
                    endpoint: "status".to_owned(),
                    shape: MethodShape::Observation,
                    input_fqn: "not valid".to_owned(),
                    output_fqn: String::new(),
                    max_message_bytes: 1,
                    max_buffered_items: 1,
                    retained_latest: true,
                    lease_valid_for_ms: None,
                }]
            ),
            Err(SupervisorAdapterError::InvalidMethodMetadata)
        ));
        let mut adapter = adapter();
        assert_eq!(
            adapter.set_status(SupervisorState::Unspecified, None),
            Err(SupervisorAdapterError::InvalidEnum("supervisor state"))
        );
    }
}
