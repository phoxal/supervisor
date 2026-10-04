//! Server-side public-session transport.
//!
//! Public types: `PublicSessionServer`, `PrincipalPolicy`, `PublicSessionBackend`,
//! `PublicSimulationBackend`, `PublicSimulationContext`, `PublicBindingContext`,
//! `PublicBackendOutcome`, `PublicBackendError`, `PublicBackendSubscription`,
//! and the unavailable-backend stubs that are no-ops when no real backend is
//! registered.
//!
#![allow(unexpected_cfgs, unused_imports, dead_code)]
//! The server half owns the public queryables, performs route/session/grant
//! admission, dispatches to the configured service and simulation backends,
//! and supervises bounded subscription cleanup. Client transport types
//! (`PublicTransportLimits`, `PublicTransportError`) and shared helpers
//! (`encode_message`, `decode_message`, `bounded_error_detail`, etc.) come from
//! the parent `phoxal::communication_transport` module.
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zenoh::bytes::Encoding;
use zenoh::handlers::FifoChannel;
use zenoh::query::{ConsolidationMode, Query, Queryable};

use crate::runtime::adapter::{SupervisorAdapter, SupervisorAdapterError};
use phoxal::communication::bootstrap::SessionOffers;
use phoxal::communication::route::{PublicOperation, PublicRoute, PublicRouteKind};
use phoxal::communication::session::{
    BindMethodRequest, BindMethodResponse, CloseSessionRequest, CloseSessionResponse,
    ExecutionState, ListExecutionsRequest, ListExecutionsResponse, ListMethodsRequest,
    ListMethodsResponse, MethodMetadata, OpenSessionRequest, OpenSessionResponse, OperationOutcome,
    OperationRequest, OperationResponse, RecordKind, RenewSessionRequest, RenewSessionResponse,
    SubscriptionAdmission, SubscriptionRecord, SubscriptionRequest, SupervisorInfoRequest,
    SupervisorInfoResponse, SupervisorState, SupervisorStatusRequest, SupervisorStatusResponse,
};
use phoxal::communication::simulation::{
    AcquireAuthorityRequest, AdmitInitialObservationsRequest, AdmitInitialObservationsResponse,
    AdmitObservationsRequest, AdmitObservationsResponse, PrepareBoundaryRequest,
    PrepareBoundaryResponse, ProgressRequest, ProgressResponse, ReleaseAuthorityRequest,
    ResetRequest,
};
use phoxal::communication::validation::{DeploymentTarget, SESSION_PROTOCOL};

use super::simulation::{
    MAX_SIMULATION_CUT_BYTES, SimulationAuthority, acquire_simulation_authority,
    admit_initial_observations, admit_observations, prepare_boundary, progress_simulation,
    release_backend_authority, release_simulation, reset_simulation, revoke_simulation_for_session,
};
use phoxal::communication_transport::{
    MAX_PUBLIC_ERROR_BYTES, MAX_PUBLIC_SUBSCRIPTION_ID_BYTES, PUBLIC_PROTOBUF_ENCODING,
    PublicTransportError, PublicTransportLimits, bounded_error_detail,
    cancel_session_subscriptions, decode_message, decode_request, encode_message, hex_bytes,
    malformed_client, operation_key_expression, subscription_key, validate_state_initial_record,
    validate_subscription_record, validate_subscription_request,
};

const PUBLIC_OPERATION_QUERYABLES: [PublicOperation; 17] = [
    PublicOperation::Open,
    PublicOperation::Renew,
    PublicOperation::Close,
    PublicOperation::Info,
    PublicOperation::Status,
    PublicOperation::ListExecutions,
    PublicOperation::ListMethods,
    PublicOperation::BindMethod,
    PublicOperation::Call,
    PublicOperation::Observe,
    PublicOperation::AcquireAuthority,
    PublicOperation::AdmitInitialObservations,
    PublicOperation::PrepareBoundary,
    PublicOperation::AdmitObservations,
    PublicOperation::Reset,
    PublicOperation::ReleaseAuthority,
    PublicOperation::Progress,
];

type PublicQueryable = Queryable<zenoh::handlers::FifoChannelHandler<Query>>;

#[derive(Clone)]
struct OperationServerContext {
    target: DeploymentTarget,
    session: zenoh::Session,
    adapter: Arc<Mutex<SupervisorAdapter>>,
    backend: Arc<dyn PublicSessionBackend>,
    subscriptions: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    subscription_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    simulation_authority: Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: Arc<dyn PublicSimulationBackend>,
    principal_policy: PrincipalPolicy,
    limits: PublicTransportLimits,
    started: Instant,
    shutdown: CancellationToken,
}

#[derive(Clone)]
pub enum PrincipalPolicy {
    /// Trust the configured router's protected principal namespace.
    Any,
    /// Admit only the listed protected route principals.
    Only(BTreeSet<String>),
}

impl PrincipalPolicy {
    /// Allow exactly the supplied principal identifiers.
    #[must_use]
    pub fn only<I, S>(principals: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Only(principals.into_iter().map(Into::into).collect())
    }

    fn allows(&self, principal: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Only(principals) => principals.contains(principal),
        }
    }
}

/// The exact binding context passed to a production public-service backend.
///
/// The context is copied from the supervisor adapter only after session,
/// binding, execution, and timeline validation has succeeded.  A backend must
/// treat it as admission evidence, not as a substitute for its own domain
/// authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicBindingContext {
    /// Opaque logical-session identifier.
    pub session_id: Vec<u8>,
    /// Opaque binding identifier.
    pub binding_id: Vec<u8>,
    /// Exact execution identity selected by the client.
    pub execution_id: String,
    /// Exact timeline identity selected by the client.
    pub timeline_id: String,
    /// Deployed service instance receiving the operation.
    pub service_instance: String,
    /// Generated descriptor admitted by the adapter.
    pub metadata: MethodMetadata,
}

impl From<crate::runtime::adapter::BindingContext> for PublicBindingContext {
    fn from(binding: crate::runtime::adapter::BindingContext) -> Self {
        Self {
            session_id: binding.session_id,
            binding_id: binding.binding_id,
            execution_id: binding.execution_id,
            timeline_id: binding.timeline_id,
            service_instance: binding.service_instance,
            metadata: binding.metadata,
        }
    }
}

/// A result from a backend operation after the public adapter has admitted it.
///
/// `OutcomeUnknown` is deliberately available to a backend because a transport
/// or service may have accepted a command before its reply path failed.  The
/// server never changes that result into a retryable ordinary error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicBackendOutcome {
    /// The operation completed and carries the generated response body.
    Received(Vec<u8>),
    /// Local admission proved that the operation was never transmitted.
    NotSent(String),
    /// The target refused the operation before queue admission.
    RejectedBeforeAdmission(String),
    /// The operation may have reached the target but no definitive result was
    /// observed.
    OutcomeUnknown(String),
}

/// A bounded backend failure that occurred before an operation outcome could
/// be established.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PublicBackendError {
    /// The backend could not admit the operation locally.
    #[error("backend rejected the operation before admission: {0}")]
    RejectedBeforeAdmission(String),
    /// The backend could not allocate its bounded observer resources.
    #[error("backend observer capacity is exhausted")]
    Capacity,
    /// The backend's service transport failed before admission evidence.
    #[error("backend transport failed: {0}")]
    Transport(String),
}

/// One bounded observation source returned by a public-service backend.
pub struct PublicBackendSubscription {
    initial: Option<SubscriptionRecord>,
    records: mpsc::Receiver<Result<SubscriptionRecord, PublicBackendError>>,
}

impl PublicBackendSubscription {
    /// Construct a source with an optional initial record and bounded updates.
    #[must_use]
    pub fn new(
        initial: Option<SubscriptionRecord>,
        records: mpsc::Receiver<Result<SubscriptionRecord, PublicBackendError>>,
    ) -> Self {
        Self { initial, records }
    }

    fn take_initial(&mut self) -> Option<SubscriptionRecord> {
        self.initial.take()
    }
}

/// Identity and boundary evidence passed to a simulation backend after public
/// route and authority checks. A backend must use the grant and session
/// identity as part of its own admission key and must not infer authority from
/// the Zenoh source alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicSimulationContext {
    /// Protected principal from the exact simulation route.
    pub principal: String,
    /// Logical public session that acquired the grant.
    pub session_id: Vec<u8>,
    /// Opaque grant currently admitted by the supervisor.
    pub authority_grant: Vec<u8>,
    /// Caller-owned operation correlation retained for delayed-result
    /// reconciliation.
    pub correlation_id: Vec<u8>,
    /// Exact execution identity.
    pub execution_id: String,
    /// Exact execution timeline.
    pub timeline_id: String,
    /// Current completed boundary before this operation.
    pub completed_boundary: u64,
    /// Model identity negotiated during acquisition.
    pub model_identity: String,
    /// Fixed simulation quantum.
    pub quantum_ns: u64,
}

/// Runtime-facing hook for the public simulation authority lane.
///
/// The public server owns route/session/grant fencing and bounded wire
/// exchange. A production implementation owns required robot-boundary
/// admission, exactly-once execution, observation receipts, actuation cut,
/// reset, and authoritative progress. The default implementation refuses all
/// simulation work, so a control-only supervisor cannot claim a fake advance.
pub trait PublicSimulationBackend: Send + Sync {
    /// Validate and reserve one complete model/provider agreement.
    fn acquire(
        &self,
        context: PublicSimulationContext,
        request: AcquireAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>>;

    /// Admit the complete boundary-zero observation cut without invoking
    /// services.
    fn admit_initial_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitInitialObservationsRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AdmitInitialObservationsResponse, PublicBackendError>>
                + Send,
        >,
    >;

    /// Prepare one admitted boundary and return the immutable actuator cut.
    fn prepare_boundary(
        &self,
        context: PublicSimulationContext,
        request: PrepareBoundaryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PrepareBoundaryResponse, PublicBackendError>> + Send>>;

    /// Admit the complete observation cut captured after native integration.
    fn admit_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitObservationsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AdmitObservationsResponse, PublicBackendError>> + Send>>;

    /// Reset the selected execution from its authoritative completed boundary.
    fn reset(
        &self,
        context: PublicSimulationContext,
        request: ResetRequest,
        next_timeline_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>>;

    /// Release the reserved model/provider authority.
    fn release(
        &self,
        context: PublicSimulationContext,
        request: ReleaseAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>>;

    /// Return authoritative progress after a possibly uncertain exchange.
    fn progress(
        &self,
        context: PublicSimulationContext,
        request: ProgressRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProgressResponse, PublicBackendError>> + Send>>;
}

#[derive(Debug, Default)]
struct UnavailableSimulationBackend;

impl PublicSimulationBackend for UnavailableSimulationBackend {
    fn acquire(
        &self,
        _context: PublicSimulationContext,
        _request: AcquireAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }

    fn admit_initial_observations(
        &self,
        _context: PublicSimulationContext,
        _request: AdmitInitialObservationsRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AdmitInitialObservationsResponse, PublicBackendError>>
                + Send,
        >,
    > {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }

    fn prepare_boundary(
        &self,
        _context: PublicSimulationContext,
        _request: PrepareBoundaryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PrepareBoundaryResponse, PublicBackendError>> + Send>>
    {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }

    fn admit_observations(
        &self,
        _context: PublicSimulationContext,
        _request: AdmitObservationsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AdmitObservationsResponse, PublicBackendError>> + Send>>
    {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }

    fn reset(
        &self,
        _context: PublicSimulationContext,
        _request: ResetRequest,
        _next_timeline_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }

    fn release(
        &self,
        _context: PublicSimulationContext,
        _request: ReleaseAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn progress(
        &self,
        _context: PublicSimulationContext,
        _request: ProgressRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProgressResponse, PublicBackendError>> + Send>> {
        Box::pin(async {
            Err(PublicBackendError::RejectedBeforeAdmission(
                "the selected supervisor has no simulation boundary backend".to_owned(),
            ))
        })
    }
}

/// Backend hook used by the production supervisor and independent fixtures.
///
/// The public transport owns routing, Protobuf framing, admission, correlation,
/// and bounded subscription cleanup.  A backend owns the service-specific
/// exchange after that boundary.  Returning a boxed future keeps the public
/// crate independent of an async-trait compatibility layer.
pub trait PublicSessionBackend: Send + Sync {
    /// Execute one admitted Read or Commands operation.
    fn call(
        &self,
        operation: PublicOperation,
        binding: PublicBindingContext,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<PublicBackendOutcome, PublicBackendError>> + Send>>;

    /// Establish one admitted State, Sample, Event, or Stream observation.
    fn subscribe(
        &self,
        operation: PublicOperation,
        binding: PublicBindingContext,
        request: SubscriptionRequest,
        capacity: usize,
    ) -> Result<PublicBackendSubscription, PublicBackendError>;
}

#[derive(Debug, Default)]
struct UnavailableBackend;

impl PublicSessionBackend for UnavailableBackend {
    fn call(
        &self,
        _operation: PublicOperation,
        _binding: PublicBindingContext,
        _payload: Vec<u8>,
        _timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<PublicBackendOutcome, PublicBackendError>> + Send>>
    {
        Box::pin(async {
            Ok(PublicBackendOutcome::NotSent(
                "the selected service has no public data backend".to_owned(),
            ))
        })
    }

    fn subscribe(
        &self,
        _operation: PublicOperation,
        _binding: PublicBindingContext,
        _request: SubscriptionRequest,
        _capacity: usize,
    ) -> Result<PublicBackendSubscription, PublicBackendError> {
        Err(PublicBackendError::RejectedBeforeAdmission(
            "the selected service has no public observation backend".to_owned(),
        ))
    }
}
pub struct PublicSessionServer {
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    _adapter: Arc<Mutex<SupervisorAdapter>>,
    subscriptions: Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    subscription_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    simulation_authority: Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: Arc<dyn PublicSimulationBackend>,
    _presence: zenoh::liveliness::LivelinessToken,
    _session: zenoh::Session,
}

impl std::fmt::Debug for PublicSessionServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublicSessionServer")
            .field("tasks", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

impl PublicSessionServer {
    /// Declare the exact bootstrap and public-session operation queryables.
    ///
    /// The supplied session may be a clone of the supervisor's owner session.
    /// The `Any` policy requires an authenticated router to protect the
    /// principal segment; use [`PrincipalPolicy::only`] when this process has
    /// an additional local allow-list.
    pub async fn start(
        session: zenoh::Session,
        adapter: SupervisorAdapter,
        principal_policy: PrincipalPolicy,
        limits: PublicTransportLimits,
    ) -> Result<Self, PublicTransportError> {
        Self::start_with_backend(
            session,
            adapter,
            Arc::new(UnavailableBackend),
            principal_policy,
            limits,
        )
        .await
    }

    /// Declare the public surface with a service-owned data backend.
    ///
    /// The backend is called only after the route, principal, lease, exact
    /// binding, execution, and timeline have been checked by the adapter.
    /// This is the production entry point for a supervisor that has a live
    /// runtime graph, while [`Self::start`] remains useful for control-only
    /// supervisors and protocol fixtures.
    pub async fn start_with_backend(
        session: zenoh::Session,
        adapter: SupervisorAdapter,
        backend: Arc<dyn PublicSessionBackend>,
        principal_policy: PrincipalPolicy,
        limits: PublicTransportLimits,
    ) -> Result<Self, PublicTransportError> {
        Self::start_with_backends(
            session,
            adapter,
            backend,
            Arc::new(UnavailableSimulationBackend),
            principal_policy,
            limits,
        )
        .await
    }

    /// Declare the public surface with service and simulation backends.
    ///
    /// The simulation backend is the only component permitted to perform a
    /// robot boundary. Without one, authority acquisition and advance are
    /// refused before a grant is installed, making the absence of runtime
    /// integration explicit instead of exposing a counter-only fixture.
    pub async fn start_with_backends(
        session: zenoh::Session,
        adapter: SupervisorAdapter,
        backend: Arc<dyn PublicSessionBackend>,
        simulation_backend: Arc<dyn PublicSimulationBackend>,
        principal_policy: PrincipalPolicy,
        limits: PublicTransportLimits,
    ) -> Result<Self, PublicTransportError> {
        limits.validate()?;
        let target = adapter.target().clone();
        let presence = session
            .liveliness()
            .declare_token(format!("{}/presence", target.prefix()))
            .await
            .map_err(|error| PublicTransportError::Transport(error.to_string()))?;
        let bootstrap = session
            .declare_queryable(target.bootstrap_key())
            .complete(true)
            .with(FifoChannel::new(limits.query_capacity()))
            .await
            .map_err(|error| PublicTransportError::Transport(error.to_string()))?;
        let mut operation_queryables = Vec::with_capacity(PUBLIC_OPERATION_QUERYABLES.len());
        for operation in PUBLIC_OPERATION_QUERYABLES {
            let key = operation_key_expression(&target, operation);
            let queryable = session
                .declare_queryable(key)
                .complete(true)
                .with(FifoChannel::new(limits.query_capacity()))
                .await
                .map_err(|error| PublicTransportError::Transport(error.to_string()))?;
            operation_queryables.push((operation, queryable));
        }

        let shutdown = CancellationToken::new();
        let started = Instant::now();
        let adapter = Arc::new(Mutex::new(adapter));
        let subscriptions = Arc::new(Mutex::new(BTreeMap::new()));
        let subscription_tasks = Arc::new(Mutex::new(Vec::new()));
        let simulation_authority = Arc::new(Mutex::new(None));
        let mut tasks = Vec::with_capacity(1 + operation_queryables.len());
        tasks.push(tokio::spawn(serve_bootstrap(
            bootstrap,
            target.clone(),
            limits.clone(),
            started,
            shutdown.clone(),
        )));
        for (operation, queryable) in operation_queryables {
            tasks.push(tokio::spawn(serve_operation(
                queryable,
                operation,
                OperationServerContext {
                    target: target.clone(),
                    session: session.clone(),
                    adapter: adapter.clone(),
                    backend: backend.clone(),
                    subscriptions: subscriptions.clone(),
                    subscription_tasks: subscription_tasks.clone(),
                    simulation_authority: simulation_authority.clone(),
                    simulation_backend: simulation_backend.clone(),
                    principal_policy: principal_policy.clone(),
                    limits: limits.clone(),
                    started,
                    shutdown: shutdown.clone(),
                },
            )));
        }
        Ok(Self {
            shutdown,
            tasks,
            _adapter: adapter,
            subscriptions,
            subscription_tasks,
            simulation_authority,
            simulation_backend,
            _presence: presence,
            _session: session,
        })
    }

    /// Update the supervisor status while retaining the public surface.
    #[allow(
        dead_code,
        reason = "the session profile owns the public client while the supervisor profile owns server status publication"
    )]
    pub async fn set_status(
        &self,
        state: SupervisorState,
        detail: Option<String>,
    ) -> Result<(), PublicTransportError> {
        self._adapter
            .lock()
            .await
            .set_status(state, detail)
            .map_err(|error| PublicTransportError::Adapter {
                operation: "status".to_owned(),
                detail: error.to_string(),
            })
    }

    /// Update the execution lifecycle after a process graph transition.
    #[allow(
        dead_code,
        reason = "the session profile owns the public client while the supervisor profile owns execution-state publication"
    )]
    pub async fn set_execution_state(
        &self,
        execution_id: &str,
        state: ExecutionState,
    ) -> Result<(), PublicTransportError> {
        self._adapter
            .lock()
            .await
            .set_execution_state(execution_id, state)
            .map_err(|error| PublicTransportError::Adapter {
                operation: "execution-state".to_owned(),
                detail: error.to_string(),
            })
    }

    /// Stop every public queryable and wait for its bounded receive loop.
    pub async fn close(mut self) -> Result<(), PublicTransportError> {
        self.shutdown.cancel();
        for token in self.subscriptions.lock().await.values() {
            token.cancel();
        }
        if let Some(authority) = self.simulation_authority.lock().await.take() {
            release_backend_authority(authority, &self.simulation_backend).await;
        }
        let mut failure = None;
        for task in self.tasks.drain(..) {
            if let Err(error) = task.await {
                failure.get_or_insert_with(|| {
                    PublicTransportError::Transport(format!(
                        "public session server task failed: {error}"
                    ))
                });
            }
        }
        for task in self.subscription_tasks.lock().await.drain(..) {
            if let Err(error) = task.await {
                failure.get_or_insert_with(|| {
                    PublicTransportError::Transport(format!(
                        "public subscription task failed: {error}"
                    ))
                });
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for PublicSessionServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn serve_bootstrap(
    queryable: PublicQueryable,
    target: DeploymentTarget,
    limits: PublicTransportLimits,
    started: Instant,
    shutdown: CancellationToken,
) {
    loop {
        let query = tokio::select! {
            () = shutdown.cancelled() => return,
            received = queryable.recv_async() => match received {
                Ok(query) => query,
                Err(_) => return,
            },
        };
        let key = query.key_expr().to_string();
        let operation = "bootstrap";
        if key != target.bootstrap_key() {
            send_error(
                &query,
                &PublicTransportError::WrongReplyKey {
                    expected: target.bootstrap_key(),
                    actual: key,
                    operation: operation.to_owned(),
                },
                &limits,
            )
            .await;
            continue;
        }
        if query.payload().is_some_and(|payload| !payload.is_empty()) {
            send_error(
                &query,
                &PublicTransportError::Malformed {
                    operation: operation.to_owned(),
                    detail: "bootstrap request body must be empty".to_owned(),
                },
                &limits,
            )
            .await;
            continue;
        }
        if query.encoding().is_some() {
            send_error(
                &query,
                &PublicTransportError::Malformed {
                    operation: operation.to_owned(),
                    detail: "bootstrap request must not carry an encoding".to_owned(),
                },
                &limits,
            )
            .await;
            continue;
        }
        let offers = target.session_offers();
        let response = match encode_message(&offers, limits.max_response_bytes(), operation) {
            Ok(response) => response,
            Err(error) => {
                send_error(&query, &error, &limits).await;
                continue;
            }
        };
        if let Err(error) = query
            .reply(query.key_expr(), response)
            .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
            .await
        {
            tracing::debug!(error = %error, "public bootstrap reply failed");
        }
        let _ = started;
    }
}

async fn serve_operation(
    queryable: PublicQueryable,
    operation: PublicOperation,
    context: OperationServerContext,
) {
    loop {
        let query = tokio::select! {
            () = context.shutdown.cancelled() => return,
            received = queryable.recv_async() => match received {
                Ok(query) => query,
                Err(_) => return,
            },
        };
        serve_one_operation(
            &query,
            operation,
            &context.target,
            &context.session,
            &context.adapter,
            &context.backend,
            &context.subscriptions,
            &context.subscription_tasks,
            &context.simulation_authority,
            &context.simulation_backend,
            &context.principal_policy,
            &context.limits,
            &context.shutdown,
            context.started,
        )
        .await;
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the operation loop receives one immutable bounded server context"
)]
async fn serve_one_operation(
    query: &Query,
    operation: PublicOperation,
    target: &DeploymentTarget,
    session: &zenoh::Session,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    backend: &Arc<dyn PublicSessionBackend>,
    subscriptions: &Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    subscription_tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    simulation_authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    principal_policy: &PrincipalPolicy,
    limits: &PublicTransportLimits,
    shutdown: &CancellationToken,
    started: Instant,
) {
    let operation_name = operation.segment();
    let key = query.key_expr().to_string();
    let route = match PublicRoute::parse(target, &key) {
        Ok(route) if route.operation() == operation => route,
        Ok(route) => {
            send_error(
                query,
                &PublicTransportError::Unauthorized {
                    operation: operation_name.to_owned(),
                    detail: format!(
                        "route operation is {:?}, expected {:?}",
                        route.operation(),
                        operation
                    ),
                },
                limits,
            )
            .await;
            return;
        }
        Err(error) => {
            send_error(
                query,
                &PublicTransportError::Unauthorized {
                    operation: operation_name.to_owned(),
                    detail: error.to_string(),
                },
                limits,
            )
            .await;
            return;
        }
    };
    if !principal_policy.allows(route.principal()) {
        send_error(
            query,
            &PublicTransportError::Unauthorized {
                operation: operation_name.to_owned(),
                detail: "principal is not admitted by the trusted ingress policy".to_owned(),
            },
            limits,
        )
        .await;
        return;
    }
    let request_bytes = match query.payload() {
        Some(payload) if payload.len() <= limits.request_limit(operation_name) => {
            payload.to_bytes()
        }
        Some(payload) => {
            send_error(
                query,
                &PublicTransportError::BodyTooLarge {
                    operation: operation_name.to_owned(),
                    bytes: payload.len(),
                    maximum: limits.request_limit(operation_name),
                },
                limits,
            )
            .await;
            return;
        }
        None => {
            send_error(
                query,
                &PublicTransportError::Malformed {
                    operation: operation_name.to_owned(),
                    detail: "operation request body is missing".to_owned(),
                },
                limits,
            )
            .await;
            return;
        }
    };
    if query.encoding() != Some(&Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned())) {
        send_error(
            query,
            &PublicTransportError::Malformed {
                operation: operation_name.to_owned(),
                detail: "operation request must use standard Protobuf encoding".to_owned(),
            },
            limits,
        )
        .await;
        return;
    }

    let now_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    match operation {
        PublicOperation::Open => {
            let request = match decode_request::<OpenSessionRequest>(
                &request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = adapter.lock().await.open(&route, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::Renew => {
            let request =
                match decode_request::<RenewSessionRequest>(&request_bytes, operation_name, limits)
                {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let result = adapter.lock().await.renew(&route, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::Close => {
            let request =
                match decode_request::<CloseSessionRequest>(&request_bytes, operation_name, limits)
                {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let result = adapter.lock().await.close(&route, &request, now_ms);
            if result.is_ok() {
                cancel_session_subscriptions(&route, &request.session_id, subscriptions).await;
                revoke_simulation_for_session(
                    &route,
                    &request.session_id,
                    adapter,
                    simulation_authority,
                    simulation_backend,
                )
                .await;
            }
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::Info => {
            let request = match decode_request::<SupervisorInfoRequest>(
                &request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let session_id = request.session_id.clone();
            let result = adapter
                .lock()
                .await
                .info(&route, &session_id, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::Status => {
            let request = match decode_request::<SupervisorStatusRequest>(
                &request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let session_id = request.session_id.clone();
            let result = adapter
                .lock()
                .await
                .status(&route, &session_id, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::ListExecutions => {
            let request = match decode_request::<ListExecutionsRequest>(
                &request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let session_id = request.session_id.clone();
            let result =
                adapter
                    .lock()
                    .await
                    .list_executions(&route, &session_id, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::ListMethods => {
            let request = match decode_request::<ListMethodsRequest>(
                &request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let session_id = request.session_id.clone();
            let result = adapter
                .lock()
                .await
                .list_ports(&route, &session_id, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::BindMethod => {
            let request =
                match decode_request::<BindMethodRequest>(&request_bytes, operation_name, limits) {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let result = adapter.lock().await.bind(&route, &request, now_ms);
            reply_adapter(query, result, operation_name, limits).await;
        }
        PublicOperation::Call => {
            let request =
                match decode_request::<OperationRequest>(&request_bytes, operation_name, limits) {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let binding = match adapter
                .lock()
                .await
                .validate_operation_binding(&route, &request, operation, now_ms)
            {
                Ok(binding) => binding,
                Err(error) => {
                    send_error(
                        query,
                        &PublicTransportError::Adapter {
                            operation: operation_name.to_owned(),
                            detail: error.to_string(),
                        },
                        limits,
                    )
                    .await;
                    return;
                }
            };
            if request.correlation_id.is_empty() {
                send_error(
                    query,
                    &PublicTransportError::Malformed {
                        operation: operation_name.to_owned(),
                        detail: "operation correlation_id must not be empty".to_owned(),
                    },
                    limits,
                )
                .await;
                return;
            }
            let timeout = bounded_timeout(request.timeout_ms, limits.deadline());
            let outcome = match tokio::time::timeout(
                timeout,
                backend.call(operation, binding.into(), request.payload.clone(), timeout),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => Ok(PublicBackendOutcome::OutcomeUnknown(
                    "public service backend deadline elapsed".to_owned(),
                )),
            };
            let response = match outcome {
                Ok(PublicBackendOutcome::Received(payload))
                    if payload.len() <= limits.max_response_bytes() =>
                {
                    OperationResponse {
                        session_id: request.session_id.clone(),
                        binding_id: request.binding_id.clone(),
                        correlation_id: request.correlation_id.clone(),
                        execution_id: request.execution_id.clone(),
                        timeline_id: request.timeline_id.clone(),
                        outcome: OperationOutcome::Received,
                        payload,
                        detail: None,
                    }
                }
                Ok(PublicBackendOutcome::Received(_payload)) => OperationResponse {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    outcome: OperationOutcome::Unknown,
                    payload: Vec::new(),
                    detail: Some(
                        "public service returned a response larger than the wire bound".to_owned(),
                    ),
                },
                Ok(PublicBackendOutcome::NotSent(detail)) => OperationResponse {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    outcome: OperationOutcome::NotSent,
                    payload: Vec::new(),
                    detail: Some(bounded_error_detail(&detail)),
                },
                Ok(PublicBackendOutcome::RejectedBeforeAdmission(detail)) => OperationResponse {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    outcome: OperationOutcome::RejectedBeforeAdmission,
                    payload: Vec::new(),
                    detail: Some(bounded_error_detail(&detail)),
                },
                Ok(PublicBackendOutcome::OutcomeUnknown(detail))
                | Err(PublicBackendError::Transport(detail)) => OperationResponse {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    outcome: OperationOutcome::Unknown,
                    payload: Vec::new(),
                    detail: Some(bounded_error_detail(&detail)),
                },
                Err(error) => OperationResponse {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    correlation_id: request.correlation_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    outcome: OperationOutcome::RejectedBeforeAdmission,
                    payload: Vec::new(),
                    detail: Some(bounded_error_detail(&error.to_string())),
                },
            };
            reply_message(query, &response, operation_name, limits).await;
        }
        PublicOperation::Observe => {
            let request =
                match decode_request::<SubscriptionRequest>(&request_bytes, operation_name, limits)
                {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            if let Err(error) = validate_subscription_request(&request, limits) {
                send_error(query, &error, limits).await;
                return;
            }
            let operation_request = OperationRequest {
                session_id: request.session_id.clone(),
                binding_id: request.binding_id.clone(),
                correlation_id: request.subscription_id.clone(),
                execution_id: request.execution_id.clone(),
                timeline_id: request.timeline_id.clone(),
                payload: Vec::new(),
                timeout_ms: 0,
            };
            let binding = match adapter.lock().await.validate_operation_binding(
                &route,
                &operation_request,
                operation,
                now_ms,
            ) {
                Ok(binding) => binding,
                Err(error) => {
                    send_error(
                        query,
                        &PublicTransportError::Adapter {
                            operation: operation_name.to_owned(),
                            detail: error.to_string(),
                        },
                        limits,
                    )
                    .await;
                    return;
                }
            };
            let subscription_id = subscription_map_key(
                route.principal(),
                &request.session_id,
                &request.subscription_id,
            );
            let cancellation = CancellationToken::new();
            let duplicate = {
                let mut active = subscriptions.lock().await;
                if active.contains_key(&subscription_id) {
                    true
                } else {
                    active.insert(subscription_id.clone(), cancellation.clone());
                    false
                }
            };
            if duplicate {
                send_error(
                    query,
                    &PublicTransportError::Rejected {
                        operation: operation_name.to_owned(),
                        detail: "subscription identifier is already active for this session"
                            .to_owned(),
                    },
                    limits,
                )
                .await;
                return;
            }
            let capacity = match validate_subscription_request(&request, limits) {
                Ok(capacity) => capacity,
                Err(error) => {
                    subscriptions.lock().await.remove(&subscription_id);
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let mut source =
                match backend.subscribe(operation, binding.into(), request.clone(), capacity) {
                    Ok(source) => source,
                    Err(error) => {
                        subscriptions.lock().await.remove(&subscription_id);
                        send_error(
                            query,
                            &PublicTransportError::Rejected {
                                operation: operation_name.to_owned(),
                                detail: bounded_error_detail(&error.to_string()),
                            },
                            limits,
                        )
                        .await;
                        return;
                    }
                };
            let source_initial = source.take_initial();
            if operation == PublicOperation::Observe && source_initial.is_some() {
                subscriptions.lock().await.remove(&subscription_id);
                send_error(
                    query,
                    &PublicTransportError::Rejected {
                        operation: operation_name.to_owned(),
                        detail: "non-State subscriptions cannot replay an initial value".to_owned(),
                    },
                    limits,
                )
                .await;
                return;
            }
            let initial = if operation == PublicOperation::Observe {
                Some(source_initial.unwrap_or_else(|| SubscriptionRecord {
                    session_id: request.session_id.clone(),
                    binding_id: request.binding_id.clone(),
                    subscription_id: request.subscription_id.clone(),
                    execution_id: request.execution_id.clone(),
                    timeline_id: request.timeline_id.clone(),
                    revision: 0,
                    // State watches use an explicit control marker when the
                    // admitted state has no value.  This marker is carried
                    // by the admission response, never by the data stream.
                    kind: RecordKind::InitialAbsent,
                    payload: Vec::new(),
                    dropped: 0,
                    detail: None,
                }))
            } else {
                // Event, Sample, and Stream subscriptions begin at the
                // admitted boundary with no initial record.
                None
            };
            if let Some(initial) = initial.as_ref()
                && let Err(error) = validate_state_initial_record(initial, &request, limits)
            {
                subscriptions.lock().await.remove(&subscription_id);
                send_error(query, &error, limits).await;
                return;
            }
            let key = subscription_key(target, route.principal(), &request.subscription_id);
            let key_for_task = key.clone();
            let session = session.clone();
            let limits_for_task = limits.clone();
            let cancellation_for_task = cancellation.clone();
            let request_for_task = request.clone();
            let shutdown_for_task = shutdown.clone();
            let subscriptions_for_task = subscriptions.clone();
            let subscription_id_for_task = subscription_id.clone();
            let adapter_for_task = adapter.clone();
            let route_for_task = route.clone();
            let operation_request_for_task = operation_request.clone();
            let started_for_task = started;
            let task = tokio::spawn(async move {
                let mut health_check = tokio::time::interval(Duration::from_secs(1));
                health_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    let next = tokio::select! {
                        () = cancellation_for_task.cancelled() => break,
                        () = shutdown_for_task.cancelled() => break,
                        _ = health_check.tick() => {
                            let now_ms = started_for_task.elapsed().as_millis().min(u64::MAX as u128) as u64;
                            let valid = adapter_for_task
                                .lock()
                                .await
                                .validate_operation_binding(
                                    &route_for_task,
                                    &operation_request_for_task,
                                    operation,
                                    now_ms,
                                )
                                .is_ok();
                            if !valid { break; }
                            continue;
                        }
                        record = source.records.recv() => match record {
                            Some(record) => record,
                            None => break,
                        },
                    };
                    match next {
                        Ok(record) => {
                            if validate_subscription_record(
                                &record,
                                &request_for_task,
                                &limits_for_task,
                                false,
                            )
                            .is_err()
                            {
                                continue;
                            }
                            let Ok(payload) = encode_message(
                                &record,
                                limits_for_task.max_response_bytes(),
                                "subscription",
                            ) else {
                                continue;
                            };
                            let _ = session
                                .put(key_for_task.clone(), payload)
                                .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
                                .await;
                        }
                        Err(error) => {
                            let record = SubscriptionRecord {
                                session_id: request_for_task.session_id.clone(),
                                binding_id: request_for_task.binding_id.clone(),
                                subscription_id: request_for_task.subscription_id.clone(),
                                execution_id: request_for_task.execution_id.clone(),
                                timeline_id: request_for_task.timeline_id.clone(),
                                revision: 0,
                                kind: RecordKind::Failed,
                                payload: Vec::new(),
                                dropped: 0,
                                detail: Some(bounded_error_detail(&error.to_string())),
                            };
                            if let Ok(payload) = encode_message(
                                &record,
                                limits_for_task.max_response_bytes(),
                                "subscription",
                            ) {
                                let _ = session
                                    .put(key_for_task.clone(), payload)
                                    .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
                                    .await;
                            }
                            break;
                        }
                    }
                }
                subscriptions_for_task
                    .lock()
                    .await
                    .remove(&subscription_id_for_task);
            });
            subscription_tasks.lock().await.push(task);
            reply_message(
                query,
                &SubscriptionAdmission {
                    session_id: request.session_id,
                    binding_id: request.binding_id,
                    subscription_id: request.subscription_id,
                    execution_id: request.execution_id,
                    timeline_id: request.timeline_id,
                    initial,
                },
                operation_name,
                limits,
            )
            .await;
        }
        PublicOperation::AcquireAuthority
        | PublicOperation::AdmitInitialObservations
        | PublicOperation::PrepareBoundary
        | PublicOperation::AdmitObservations
        | PublicOperation::Reset
        | PublicOperation::ReleaseAuthority
        | PublicOperation::Progress => {
            serve_simulation_operation(
                query,
                operation,
                &route,
                &request_bytes,
                adapter,
                subscriptions,
                simulation_authority,
                simulation_backend,
                now_ms,
                limits,
            )
            .await;
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the operation loop receives one immutable bounded server context"
)]
async fn serve_simulation_operation(
    query: &Query,
    operation: PublicOperation,
    route: &PublicRoute,
    request_bytes: &[u8],
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    subscriptions: &Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    simulation_authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
    limits: &PublicTransportLimits,
) {
    let operation_name = operation.segment();
    match operation {
        PublicOperation::AcquireAuthority => {
            let request = match decode_request::<AcquireAuthorityRequest>(
                request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = acquire_simulation_authority(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::AdmitInitialObservations => {
            let request = match decode_request::<AdmitInitialObservationsRequest>(
                request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = admit_initial_observations(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::PrepareBoundary => {
            let request = match decode_request::<PrepareBoundaryRequest>(
                request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = prepare_boundary(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::AdmitObservations => {
            let request = match decode_request::<AdmitObservationsRequest>(
                request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = admit_observations(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::Reset => {
            let request =
                match decode_request::<ResetRequest>(request_bytes, operation_name, limits) {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let result = reset_simulation(
                route,
                request,
                adapter,
                subscriptions,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::ReleaseAuthority => {
            let request = match decode_request::<ReleaseAuthorityRequest>(
                request_bytes,
                operation_name,
                limits,
            ) {
                Ok(request) => request,
                Err(error) => {
                    send_error(query, &error, limits).await;
                    return;
                }
            };
            let result = release_simulation(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        PublicOperation::Progress => {
            let request =
                match decode_request::<ProgressRequest>(request_bytes, operation_name, limits) {
                    Ok(request) => request,
                    Err(error) => {
                        send_error(query, &error, limits).await;
                        return;
                    }
                };
            let result = progress_simulation(
                route,
                request,
                adapter,
                simulation_authority,
                simulation_backend,
                now_ms,
            )
            .await;
            reply_simulation_result(query, result, operation_name, limits).await;
        }
        _ => unreachable!("serve_simulation_operation received a non-simulation operation"),
    }
}

async fn reply_simulation_result<M: Message>(
    query: &Query,
    result: Result<M, PublicTransportError>,
    operation: &str,
    limits: &PublicTransportLimits,
) {
    match result {
        Ok(response) => {
            match encode_message(&response, limits.response_limit(operation), operation) {
                Ok(payload) => {
                    if let Err(error) = query
                        .reply(query.key_expr(), payload)
                        .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
                        .await
                    {
                        tracing::debug!(error = %error, "simulation response failed");
                    }
                }
                Err(error) => send_error(query, &error, limits).await,
            }
        }
        Err(error) => send_error(query, &error, limits).await,
    }
}

async fn reply_adapter<M: Message>(
    query: &Query,
    result: Result<M, SupervisorAdapterError>,
    operation: &str,
    limits: &PublicTransportLimits,
) {
    match result {
        Ok(response) => match encode_message(&response, limits.max_response_bytes(), operation) {
            Ok(payload) => {
                if let Err(error) = query
                    .reply(query.key_expr(), payload)
                    .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
                    .await
                {
                    tracing::debug!(operation, error = %error, "public session reply failed");
                }
            }
            Err(error) => send_error(query, &error, limits).await,
        },
        Err(error) => {
            send_error(
                query,
                &PublicTransportError::Adapter {
                    operation: operation.to_owned(),
                    detail: error.to_string(),
                },
                limits,
            )
            .await;
        }
    }
}

async fn reply_message<M: Message>(
    query: &Query,
    message: &M,
    operation: &str,
    limits: &PublicTransportLimits,
) {
    match encode_message(message, limits.max_response_bytes(), operation) {
        Ok(payload) => {
            if let Err(error) = query
                .reply(query.key_expr(), payload)
                .encoding(Encoding::from(PUBLIC_PROTOBUF_ENCODING.to_owned()))
                .await
            {
                tracing::debug!(operation, error = %error, "public session reply failed");
            }
        }
        Err(error) => send_error(query, &error, limits).await,
    }
}
fn bounded_timeout(timeout_ms: u32, default: Duration) -> Duration {
    if timeout_ms == 0 {
        default
    } else {
        Duration::from_millis(u64::from(timeout_ms)).min(default)
    }
}

fn subscription_map_key(principal: &str, session_id: &[u8], subscription_id: &[u8]) -> String {
    format!(
        "{principal}/{}/{}",
        hex_bytes(session_id),
        hex_bytes(subscription_id)
    )
}

async fn send_error(query: &Query, error: &PublicTransportError, limits: &PublicTransportLimits) {
    let mut payload = error.to_string().into_bytes();
    if payload.len() > MAX_PUBLIC_ERROR_BYTES.min(limits.max_response_bytes()) {
        payload.truncate(MAX_PUBLIC_ERROR_BYTES.min(limits.max_response_bytes()));
    }
    if let Err(send_error) = query.reply_err(payload).await {
        tracing::debug!(error = %send_error, "public session error reply failed");
    }
}

#[cfg(test)]
mod tests {
    #[path = "support/router.rs"]
    mod support;

    use super::*;
    use phoxal::communication_transport::{
        PublicSessionConfig, PublicSessionConnection, PublicSessionTransport,
    };

    /// Exercise two independently routed supervisor namespaces and two
    /// principal-bound logical sessions over one local Zenoh router.
    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn routed_supervisors_keep_bootstrap_and_sessions_isolated() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("test port");
        let endpoint = format!("tcp/{}", listener.local_addr().expect("test address"));
        drop(listener);

        let router = support::open(std::slice::from_ref(&endpoint)).await;
        let target_a = DeploymentTarget::new("workshop", "rover-a").expect("target a");
        let target_b = DeploymentTarget::new("workshop", "rover-b").expect("target b");
        let (owner_a, bus_a) = phoxal::runtime::connection::ConnectionOwner::open(
            phoxal::runtime::connection::ConnectionConfig::for_external(
                phoxal::identity::ExecutionId::mint(),
                None,
                vec![endpoint.clone()],
            ),
        )
        .await
        .expect("supervisor a bus");
        let (owner_b, bus_b) = phoxal::runtime::connection::ConnectionOwner::open(
            phoxal::runtime::connection::ConnectionConfig::for_external(
                phoxal::identity::ExecutionId::mint(),
                None,
                vec![endpoint.clone()],
            ),
        )
        .await
        .expect("supervisor b bus");
        let adapter_a =
            SupervisorAdapter::with_defaults(target_a.clone(), "supervisor-a", "framework-a")
                .expect("adapter a");
        let adapter_b =
            SupervisorAdapter::with_defaults(target_b.clone(), "supervisor-b", "framework-b")
                .expect("adapter b");
        let server_a = PublicSessionServer::start(
            bus_a.session().expect("a session").clone(),
            adapter_a,
            PrincipalPolicy::only(["operator-a"]),
            PublicTransportLimits::default(),
        )
        .await
        .expect("server a");
        let server_b = PublicSessionServer::start(
            bus_b.session().expect("b session").clone(),
            adapter_b,
            PrincipalPolicy::only(["operator-a"]),
            PublicTransportLimits::default(),
        )
        .await
        .expect("server b");

        let transport_a = PublicSessionTransport::connect(&endpoint, "operator-a")
            .await
            .expect("transport a");
        // Queryable declarations propagate asynchronously through the router.
        // Wait for both declarations under a fixed test deadline instead of
        // assuming the first inventory request observes their propagation.
        let discovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let inventory = transport_a
                    .discover("workshop")
                    .await
                    .expect("bounded supervisor inventory");
                if inventory.len() == 2 {
                    break inventory;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("both supervisor declarations must become discoverable");
        assert_eq!(
            discovered
                .iter()
                .map(DeploymentTarget::supervisor)
                .collect::<Vec<_>>(),
            ["rover-a", "rover-b"]
        );
        assert!(matches!(
            transport_a.discover_bounded("workshop", 1).await,
            Err(PublicTransportError::InventoryOverflow { maximum: 1 })
        ));
        let connection_a = transport_a.open(target_a).await.expect("session a");
        let connection_b = transport_a.open(target_b).await.expect("session b");
        assert_eq!(connection_a.info().supervisor_version, "supervisor-a");
        assert_eq!(connection_b.info().supervisor_version, "supervisor-b");
        assert_eq!(
            connection_a.status().await.expect("status a").state,
            phoxal::communication::session::SupervisorState::Idle
        );
        assert_eq!(
            connection_b.status().await.expect("status b").state,
            phoxal::communication::session::SupervisorState::Idle
        );

        let spoofed = PublicSessionConnection::connect(
            PublicSessionConfig::new(
                &endpoint,
                DeploymentTarget::new("workshop", "rover-a").expect("target"),
                "operator-b",
            )
            .expect("spoofed client"),
        )
        .await
        .expect_err("router policy must refuse a principal spoof");
        assert!(matches!(spoofed, PublicTransportError::Rejected { .. }));

        let connection_b = connection_b;
        connection_a.close().await.expect("close a");
        assert_eq!(
            connection_b
                .status()
                .await
                .expect("session b survives")
                .state,
            phoxal::communication::session::SupervisorState::Idle
        );
        connection_b.close().await.expect("close b");
        transport_a.close().await.expect("transport a close");
        server_a.close().await.expect("server a close");
        server_b.close().await.expect("server b close");
        let _ = owner_a.close().await;
        let _ = owner_b.close().await;
        router.close().await.expect("router close");
    }
}
