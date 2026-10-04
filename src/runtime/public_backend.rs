//! Supervisor-owned bridges for the typed public Runtime surface.
//!
//! The public session protocol deliberately knows nothing about a service's
//! Rust types.  This module therefore forwards the already-encoded generated
//! Protobuf body to the exact Runtime port selected by the admitted bundle
//! graph, and maps Runtime wire metadata back into the public observation
//! record.  The bundle remains the only source of public method identity and
//! bounds.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::Deserialize;
use tokio::sync::mpsc;
use zenoh::bytes::Encoding;
use zenoh::key_expr::OwnedKeyExpr;

use phoxal::communication::PublicOperation;
use phoxal::communication::session::{
    MethodMetadata, MethodShape, RecordKind, SubscriptionRecord, SubscriptionRequest,
};
use phoxal::communication::simulation::{
    AcquireAuthorityRequest, AdmitInitialObservationsRequest, AdmitInitialObservationsResponse,
    AdmitObservationsRequest, AdmitObservationsResponse, Observation, PrepareBoundaryRequest,
    PrepareBoundaryResponse, ProgressRequest, ProgressResponse, ReleaseAuthorityRequest,
    ResetRequest,
};

use crate::runtime::adapter::{
    ExecutionDefinition, ServiceMethods, SimulationDefinition, SimulationProviderDefinition,
};
use phoxal::communication_transport::PublicTransportLimits;

use crate::runtime::transport::server::{
    PublicBackendError, PublicBackendOutcome, PublicBackendSubscription, PublicBindingContext,
    PublicSessionBackend, PublicSimulationBackend, PublicSimulationContext,
};
use phoxal::communication::execution::RuntimeWireMetadata;
use phoxal::runtime::connection::Connection;
use phoxal::runtime::transport::{PROTOBUF_ENCODING, WireControl, WireSample, port_key};
use phoxal::runtime::{ExecutionTime, ObservationStamp};

use super::bundle::RuntimeBundle;
use super::state::ExecutionState;
use phoxal::artifact::bundle::BundleSimulation;

const PUBLIC_INGRESS_INSTANCE: &str = "supervisor";
const PUBLIC_INGRESS_FIELD: &str = "public";
const MAX_RUNTIME_SUBSCRIBER_ITEMS: usize = 4_096;
const MAX_RUNTIME_METADATA_BYTES: usize = 1_024;

/// Private execution role retained by the supervisor while public contracts
/// expose only Protobuf call and observation shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeMethodRole {
    State,
    Sample,
    Event,
    Stream,
    Setpoint,
    Read,
    Commands,
}

impl RuntimeMethodRole {
    const fn shape(self) -> MethodShape {
        match self {
            Self::State | Self::Sample | Self::Event | Self::Stream => MethodShape::Observation,
            Self::Setpoint | Self::Read | Self::Commands => MethodShape::Call,
        }
    }
}

/// The exact runtime-facing facts for one admitted public method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeMethodContract {
    /// Public metadata returned by `ListMethods` and `Bind`.
    pub(crate) metadata: MethodMetadata,
    /// Private runtime execution role, never serialized as public contract metadata.
    role: RuntimeMethodRole,
    /// Maximum encoded request body accepted by the generated runtime.
    request_max_bytes: u64,
    /// Maximum encoded response/publication body accepted by the generated runtime.
    response_max_bytes: u64,
}

/// The ordering facts assigned by the supervisor/coordinator to one external
/// request.  The sequence is distinct from the request correlation so a
/// runtime can preserve ingress order even when callers use arbitrary
/// operation identifiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExternalIngressTicket {
    /// The first boundary at which this request may enter an input cut.
    pub(crate) eligible_boundary: u64,
    /// Monotonic order of external requests at that boundary.
    pub(crate) ingress_sequence: u64,
    /// Host-monotonic logical timestamp captured at admission.
    pub(crate) logical_time: ExecutionTime,
}

/// Runtime-owned admission of an external public call.
///
/// The supervisor resolves the reserved caller identity from the immutable
/// compiled surface, then asks this hook for the current boundary and ingress
/// sequence.  The hook is intentionally required for production calls: a
/// host-local counter or a guessed boundary would violate controlled-before-
/// external ordering after the Runtime advances.
pub(crate) trait RuntimeExternalIngress: Send + Sync {
    fn admit(
        &self,
        target_instance: &str,
        target_port: &str,
        caller: &RuntimeIngressIdentity,
        contract: &RuntimeMethodContract,
    ) -> Result<ExternalIngressTicket, PublicBackendError>;

    /// Release a reservation after a definitive target response or a local
    /// failure proved that the request was never transmitted.
    fn release(&self, _target_instance: &str, _target_port: &str, _ticket: ExternalIngressTicket) {}
}

/// The source/caller identity used for supervisor-originated Runtime ingress.
///
/// A runtime must admit this identity through its compiled execution contract.
/// The host never impersonates an authored graph caller, and never omits the
/// caller metadata merely because this request originated locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeIngressIdentity {
    pub(crate) source: String,
    pub(crate) caller: String,
}

impl Default for RuntimeIngressIdentity {
    fn default() -> Self {
        Self {
            source: PUBLIC_INGRESS_INSTANCE.to_owned(),
            caller: format!("{PUBLIC_INGRESS_INSTANCE}.{PUBLIC_INGRESS_FIELD}"),
        }
    }
}

/// Supervisor-owned ordering and reservation authority for one execution.
///
/// The coordinator is shared by public data operations, so every external
/// ticket is assigned from one locked sequence and the current boundary
/// observed by the supervisor state. The host clock supplies the metadata
/// timestamp, while the runtime boundary remains the source of eligibility
/// and is never guessed from a process-local request counter.
pub(crate) struct RuntimeExecutionCoordinator {
    state: ExecutionState,
    origin: Instant,
    ingress: Arc<Mutex<IngressState>>,
}

#[derive(Debug)]
struct IngressState {
    next_sequence: u64,
    reservations: BTreeMap<(String, String), usize>,
}

const MAX_EXTERNAL_INGRESS_PER_PORT: usize = 64;

impl std::fmt::Debug for RuntimeExecutionCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionCoordinator")
            .field("boundary", &self.current_boundary())
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl RuntimeExecutionCoordinator {
    pub(crate) fn new(state: ExecutionState) -> Self {
        Self {
            state,
            origin: Instant::now(),
            ingress: Arc::new(Mutex::new(IngressState {
                next_sequence: 1,
                reservations: BTreeMap::new(),
            })),
        }
    }

    /// Current completed boundary used for external eligibility.
    pub(crate) fn current_boundary(&self) -> u64 {
        self.state.runtime_boundary()
    }

    fn host_time(&self) -> ExecutionTime {
        ExecutionTime::from(self.origin.elapsed())
    }

    fn validate_external_identity(
        &self,
        caller: &RuntimeIngressIdentity,
    ) -> Result<(), PublicBackendError> {
        if caller.source != PUBLIC_INGRESS_INSTANCE
            || caller.caller != format!("{PUBLIC_INGRESS_INSTANCE}.{PUBLIC_INGRESS_FIELD}")
        {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "external Runtime ingress identity is not supervisor-owned".to_owned(),
            ));
        }
        Ok(())
    }

    fn admission_capacity(contract: &RuntimeMethodContract) -> usize {
        usize::try_from(contract.metadata.max_buffered_items)
            .unwrap_or(usize::MAX)
            .clamp(1, MAX_EXTERNAL_INGRESS_PER_PORT)
    }

    fn release_reservation(
        &self,
        target_instance: &str,
        target_port: &str,
        _ticket: ExternalIngressTicket,
    ) {
        let mut ingress = lock_unpoisoned(&self.ingress);
        let key = (target_instance.to_owned(), target_port.to_owned());
        if let Some(reservations) = ingress.reservations.get_mut(&key) {
            *reservations = reservations.saturating_sub(1);
            if *reservations == 0 {
                ingress.reservations.remove(&key);
            }
        }
    }
}

impl RuntimeExternalIngress for RuntimeExecutionCoordinator {
    fn admit(
        &self,
        target_instance: &str,
        target_port: &str,
        caller: &RuntimeIngressIdentity,
        contract: &RuntimeMethodContract,
    ) -> Result<ExternalIngressTicket, PublicBackendError> {
        self.validate_external_identity(caller)?;
        if !self.state.is_ready() {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "runtime graph is not Ready for external ingress".to_owned(),
            ));
        }
        if contract.metadata.shape != MethodShape::Call {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "external ingress requires a call method".to_owned(),
            ));
        }
        let key = (target_instance.to_owned(), target_port.to_owned());
        let mut ingress = lock_unpoisoned(&self.ingress);
        let capacity = Self::admission_capacity(contract);
        let reservations = ingress.reservations.get(&key).copied().unwrap_or_default();
        if reservations >= capacity {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "external Runtime ingress capacity is exhausted".to_owned(),
            ));
        }
        let eligible_boundary = self.current_boundary().checked_add(1).ok_or_else(|| {
            PublicBackendError::RejectedBeforeAdmission(
                "Runtime eligible boundary is exhausted".to_owned(),
            )
        })?;
        let ingress_sequence = ingress.next_sequence;
        let next_sequence = ingress_sequence.checked_add(1).ok_or_else(|| {
            PublicBackendError::RejectedBeforeAdmission(
                "Runtime external ingress sequence is exhausted".to_owned(),
            )
        })?;
        ingress.next_sequence = next_sequence;
        *ingress.reservations.entry(key).or_default() += 1;
        Ok(ExternalIngressTicket {
            eligible_boundary,
            ingress_sequence,
            logical_time: self.host_time(),
        })
    }

    fn release(&self, target_instance: &str, target_port: &str, ticket: ExternalIngressTicket) {
        self.release_reservation(target_instance, target_port, ticket);
    }
}

/// The generated public inventory and runtime bridge for one bundle.
#[derive(Clone)]
pub(crate) struct RuntimePublicSurface {
    /// Exact service/driver inventory used by the public adapter.
    pub(crate) services: Vec<ServiceMethods>,
    /// Exact port contracts used by the transport bridge.
    pub(crate) ports: Arc<BTreeMap<(String, String), RuntimeMethodContract>>,
    /// Explicit supervisor external caller identity.
    pub(crate) ingress: RuntimeIngressIdentity,
    /// Immutable simulation authority contract, when the bundle carries one.
    pub(crate) simulation: Option<SimulationDefinition>,
}

impl std::fmt::Debug for RuntimePublicSurface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimePublicSurface")
            .field("services", &self.services.len())
            .field("ports", &self.ports.len())
            .field("ingress", &self.ingress)
            .field("simulation", &self.simulation)
            .finish()
    }
}

impl RuntimePublicSurface {
    /// Extract exact public methods from the source bundle's retained artifact
    /// summaries.  No public metadata is synthesized from implementation
    /// names or from an empty service placeholder.
    pub(crate) fn from_bundle(bundle: &RuntimeBundle) -> Result<Self> {
        Self::from_source(bundle)
    }

    fn from_source(source: &RuntimeBundle) -> Result<Self> {
        let ingress = RuntimeIngressIdentity::default();
        let mut services = Vec::new();
        let mut ports = BTreeMap::new();
        // The root-local brain is an ordinary Runtime in the graph.  If it
        // owns a generated public method, that exact artifact contract belongs
        // in the same inventory as a service or driver; role is not a public
        // visibility boundary.
        for (instance, _executable) in source.executables_iter() {
            let instance = instance.clone();
            let Some(artifact) = source.runtime_record(&instance) else {
                anyhow::bail!("runtime `{instance}` has no retained Runtime artifact contract");
            };
            let mut service_methods = Vec::new();
            let mut runtime_methods = BTreeMap::new();

            let (_, outputs) = runtime_record_parts(artifact);
            for output in outputs.iter() {
                let Some(port) = output.port.as_deref() else {
                    continue;
                };
                let Some(signature) = output.signature.as_ref() else {
                    bail!(
                        "Runtime output `{instance}.{}` has a public method without a signature",
                        output.name
                    );
                };
                let role = output_role(signature);
                validate_signature(signature, port, signature.shape)?;
                let public_shape = match signature.shape {
                    phoxal::artifact::MethodShape::Call => MethodShape::Call,
                    phoxal::artifact::MethodShape::Observation => MethodShape::Observation,
                };
                let response_max_bytes = positive_bound(
                    output.max_bytes,
                    &format!("Runtime output `{instance}.{port}` response bytes"),
                )?;
                let max_buffered_items = output
                    .max_items
                    .or_else(|| singular_public_item_bound(signature))
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Runtime output `{instance}.{port}` has no bounded item count"
                        )
                    })?;
                let max_buffered_items = bounded_u32(
                    max_buffered_items,
                    &format!("Runtime output `{instance}.{port}` item count"),
                )?;
                let metadata = MethodMetadata {
                    endpoint: port.to_owned(),
                    shape: public_shape,
                    input_fqn: signature.request.clone(),
                    output_fqn: signature.response.clone(),
                    max_message_bytes: bounded_u32(
                        response_max_bytes,
                        &format!("Runtime output `{instance}.{port}` response bytes"),
                    )?,
                    max_buffered_items,
                    retained_latest: signature.retained_latest,
                    lease_valid_for_ms: signature.lease_valid_for_ms,
                };
                let contract = RuntimeMethodContract {
                    metadata: metadata.clone(),
                    role,
                    request_max_bytes: output.max_request_bytes.unwrap_or(response_max_bytes),
                    response_max_bytes,
                };
                insert_runtime_port(&mut runtime_methods, &mut service_methods, port, contract)?;
            }

            for input in runtime_record_parts(artifact).0 {
                if input.delivery != phoxal::artifact::InputDelivery::CallIngress {
                    continue;
                }
                let port = input.port.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Commands input `{instance}.{}` has no public method",
                        input.name
                    )
                })?;
                let signature = input.signature.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("Commands input `{instance}.{port}` has no generated signature")
                })?;
                validate_signature(signature, port, phoxal::artifact::MethodShape::Call)?;
                let request_max_bytes = positive_bound(
                    input.max_bytes,
                    &format!("Commands input `{instance}.{port}` request bytes"),
                )?;
                let max_buffered_items = bounded_u32(
                    positive_bound(
                        input.max_items,
                        &format!("Commands input `{instance}.{port}` item count"),
                    )?,
                    &format!("Commands input `{instance}.{port}` item count"),
                )?;
                let response_max_bytes = positive_bound(
                    input.response_max_bytes,
                    &format!("Call ingress `{instance}.{port}` response bytes"),
                )?;
                let metadata = MethodMetadata {
                    endpoint: port.to_owned(),
                    shape: MethodShape::Call,
                    input_fqn: signature.request.clone(),
                    output_fqn: signature.response.clone(),
                    max_message_bytes: bounded_u32(
                        response_max_bytes,
                        &format!("Commands input `{instance}.{port}` response bytes"),
                    )?,
                    max_buffered_items,
                    retained_latest: signature.retained_latest,
                    lease_valid_for_ms: signature.lease_valid_for_ms,
                };
                let contract = RuntimeMethodContract {
                    metadata: metadata.clone(),
                    role: RuntimeMethodRole::Commands,
                    request_max_bytes,
                    response_max_bytes,
                };
                insert_runtime_port(&mut runtime_methods, &mut service_methods, port, contract)?;
            }

            service_methods.sort_by(|left, right| left.endpoint.cmp(&right.endpoint));
            let service = ServiceMethods::new(instance.clone(), service_methods)?;
            for (port, contract) in runtime_methods {
                let key = (instance.clone(), port);
                if ports.insert(key.clone(), contract).is_some() {
                    bail!(
                        "source bundle contains duplicate public Runtime port `{}.{}`",
                        key.0,
                        key.1
                    );
                }
            }
            services.push(service);
        }
        let mut service_methods_by_instance = services
            .into_iter()
            .map(|service| (service.instance().to_owned(), service.methods().to_vec()))
            .collect::<BTreeMap<_, _>>();
        let simulation = source
            .simulation()
            .map(|simulation| {
                add_simulation_provider_metadata(
                    simulation,
                    &mut service_methods_by_instance,
                    &mut ports,
                )?;
                validate_simulation_actuation_bindings(simulation, &ports)?;
                simulation_definition(simulation)
            })
            .transpose()?;
        let mut services = service_methods_by_instance
            .into_iter()
            .map(|(instance, ports)| {
                ServiceMethods::new(instance, ports).map_err(anyhow::Error::from)
            })
            .collect::<Result<Vec<_>>>()?;
        services.sort_by(|left, right| left.instance().cmp(right.instance()));
        Ok(Self {
            services,
            ports: Arc::new(ports),
            ingress,
            simulation,
        })
    }

    /// Build the public execution definition from the exact extracted graph.
    pub(crate) fn execution(
        &self,
        execution_id: String,
        timeline_id: String,
        state: phoxal::communication::session::ExecutionState,
    ) -> Result<ExecutionDefinition> {
        let execution = ExecutionDefinition::new(
            phoxal::communication::session::ExecutionSummary {
                execution_id,
                timeline_id,
                state,
            },
            self.services.clone(),
        )
        .map_err(anyhow::Error::from)?;
        match &self.simulation {
            Some(simulation) => execution
                .with_simulation(simulation.clone())
                .map_err(Into::into),
            None => Ok(execution),
        }
    }
}

fn simulation_definition(source: &BundleSimulation) -> Result<SimulationDefinition> {
    let providers = source
        .providers
        .iter()
        .map(|provider| {
            SimulationProviderDefinition::new(
                provider.service_instance.clone(),
                provider.port.clone(),
                MethodShape::Observation,
                provider.input_fqn.clone(),
                provider.payload_fqn.clone(),
                provider.rate_microhertz,
            )
            .map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    SimulationDefinition::new(source.model_identity.clone(), source.quantum_ns, providers)
        .map_err(anyhow::Error::from)
}

fn add_simulation_provider_metadata(
    source: &BundleSimulation,
    service_methods: &mut BTreeMap<String, Vec<MethodMetadata>>,
    runtime_methods: &mut BTreeMap<(String, String), RuntimeMethodContract>,
) -> Result<()> {
    for provider in &source.providers {
        let metadata = MethodMetadata {
            endpoint: provider.port.clone(),
            shape: MethodShape::Observation,
            input_fqn: provider.input_fqn.clone(),
            output_fqn: provider.payload_fqn.clone(),
            max_message_bytes: provider.max_message_bytes,
            max_buffered_items: provider.max_buffered_items,
            retained_latest: provider.retained_latest,
            lease_valid_for_ms: provider.lease_valid_for_ms,
        };
        let key = (provider.service_instance.clone(), provider.port.clone());
        if let Some(existing) = runtime_methods.get(&key) {
            if existing.metadata != metadata {
                bail!(
                    "simulation provider metadata for `{}.{}` conflicts with the compiled public method",
                    key.0,
                    key.1
                );
            }
            continue;
        }
        runtime_methods.insert(
            key,
            RuntimeMethodContract {
                request_max_bytes: u64::from(provider.max_message_bytes),
                response_max_bytes: u64::from(provider.max_message_bytes),
                role: if provider.retained_latest {
                    RuntimeMethodRole::State
                } else {
                    RuntimeMethodRole::Sample
                },
                metadata: metadata.clone(),
            },
        );
        service_methods
            .entry(provider.service_instance.clone())
            .or_default()
            .push(metadata);
    }
    Ok(())
}

fn validate_simulation_actuation_bindings(
    source: &BundleSimulation,
    runtime_methods: &BTreeMap<(String, String), RuntimeMethodContract>,
) -> Result<()> {
    let mut expected = BTreeSet::new();
    for ((instance, port), contract) in runtime_methods {
        if contract.role == RuntimeMethodRole::Setpoint {
            expected.insert((instance.clone(), port.clone()));
        }
    }
    let actual = source
        .actuation_bindings
        .iter()
        .map(|binding| (binding.service_instance.clone(), binding.port.clone()))
        .collect::<BTreeSet<_>>();
    if !actual.is_subset(&expected) {
        bail!("simulation actuation bindings include unknown compiled setpoint ports");
    }
    for binding in &source.actuation_bindings {
        let key = (binding.service_instance.clone(), binding.port.clone());
        let contract = runtime_methods.get(&key).ok_or_else(|| {
            anyhow::anyhow!(
                "simulation actuation `{}.{}` is not a compiled public method",
                binding.service_instance,
                binding.port
            )
        })?;
        if contract.role != RuntimeMethodRole::Setpoint
            || contract.metadata.output_fqn != binding.payload_fqn
        {
            bail!(
                "simulation actuation `{}.{}` does not match its compiled setpoint payload",
                binding.service_instance,
                binding.port
            );
        }
    }
    Ok(())
}

fn insert_runtime_port(
    runtime_methods: &mut BTreeMap<String, RuntimeMethodContract>,
    service_methods: &mut Vec<MethodMetadata>,
    port: &str,
    contract: RuntimeMethodContract,
) -> Result<()> {
    if runtime_methods
        .insert(port.to_owned(), contract.clone())
        .is_some()
    {
        bail!("compiled Runtime graph serves duplicate public method `{port}`");
    }
    service_methods.push(contract.metadata);
    Ok(())
}

fn runtime_record_parts(
    record: &phoxal::artifact::RuntimeRecord,
) -> (
    &[phoxal::artifact::InputRecord],
    &[phoxal::artifact::OutputRecord],
) {
    let phoxal::artifact::RuntimeRecord::V0 {
        inputs, outputs, ..
    } = record;
    (inputs, outputs)
}

fn output_role(signature: &phoxal::artifact::MethodSignature) -> RuntimeMethodRole {
    match signature.shape {
        phoxal::artifact::MethodShape::Call => RuntimeMethodRole::Read,
        phoxal::artifact::MethodShape::Observation if signature.lease_valid_for_ms.is_some() => {
            RuntimeMethodRole::Setpoint
        }
        phoxal::artifact::MethodShape::Observation if signature.retained_latest => {
            RuntimeMethodRole::State
        }
        phoxal::artifact::MethodShape::Observation => RuntimeMethodRole::Sample,
    }
}

fn singular_public_item_bound(signature: &phoxal::artifact::MethodSignature) -> Option<u64> {
    (signature.shape == phoxal::artifact::MethodShape::Call
        || signature.retained_latest
        || signature.lease_valid_for_ms.is_some())
    .then_some(1)
}

fn positive_bound(value: Option<u64>, label: &str) -> Result<u64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{label} is missing"))?;
    if value == 0 {
        bail!("{label} must be positive");
    }
    Ok(value)
}

fn bounded_u32(value: u64, label: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("{label} exceeds the public u32 bound"))
}

fn validate_signature(
    signature: &phoxal::artifact::MethodSignature,
    port: &str,
    expected: phoxal::artifact::MethodShape,
) -> Result<()> {
    if signature.endpoint != port
        || signature.shape != expected
        || signature.service.is_empty()
        || signature.method.is_empty()
    {
        bail!("generated Runtime signature for `{port}` does not match its compiled method shape");
    }
    if signature.request.is_empty() || signature.response.is_empty() {
        bail!("generated Runtime signature for `{port}` has an empty message identity");
    }
    Ok(())
}

/// A production service bridge over the supervisor's internal Runtime bus.
#[derive(Clone)]
pub(crate) struct RuntimePublicBackend {
    bus: Connection,
    ports: Arc<BTreeMap<(String, String), RuntimeMethodContract>>,
    ingress: RuntimeIngressIdentity,
    external_ingress: Arc<dyn RuntimeExternalIngress>,
    next_command: Arc<AtomicU64>,
}

impl std::fmt::Debug for RuntimePublicBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimePublicBackend")
            .field("ports", &self.ports.len())
            .field("ingress", &self.ingress)
            .finish_non_exhaustive()
    }
}

impl RuntimePublicBackend {
    pub(crate) fn new(
        bus: Connection,
        surface: &RuntimePublicSurface,
        external_ingress: Arc<dyn RuntimeExternalIngress>,
    ) -> Self {
        Self {
            bus,
            ports: surface.ports.clone(),
            ingress: surface.ingress.clone(),
            external_ingress,
            next_command: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn call_inner(
        &self,
        operation: PublicOperation,
        binding: PublicBindingContext,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<PublicBackendOutcome, PublicBackendError> {
        let key = (
            binding.service_instance.clone(),
            binding.metadata.endpoint.clone(),
        );
        let contract = self.ports.get(&key).ok_or_else(|| {
            PublicBackendError::RejectedBeforeAdmission(format!(
                "public Runtime port `{}.{}` is not present in the admitted bundle",
                key.0, key.1
            ))
        })?;
        if operation != PublicOperation::Call
            || !matches!(
                contract.role,
                RuntimeMethodRole::Read | RuntimeMethodRole::Commands | RuntimeMethodRole::Setpoint
            )
            || binding.metadata != contract.metadata
        {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "public Runtime binding metadata does not match the compiled artifact".to_owned(),
            ));
        }
        if payload.len() as u64 > contract.request_max_bytes {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "public Runtime request exceeds its compiled request-byte bound".to_owned(),
            ));
        }
        let ticket = self
            .external_ingress
            .admit(&key.0, &key.1, &self.ingress, contract)?;
        if ticket.ingress_sequence == 0 {
            self.external_ingress.release(&key.0, &key.1, ticket);
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "Runtime external ingress sequence must be positive".to_owned(),
            ));
        }
        let command_id = self.next_command.fetch_add(1, Ordering::Relaxed);
        if command_id == 0 {
            self.external_ingress.release(&key.0, &key.1, ticket);
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "public Runtime command correlation exhausted".to_owned(),
            ));
        }
        let metadata = RuntimeWireMetadata::external_command(
            ticket.logical_time,
            command_id,
            ticket.eligible_boundary,
            ticket.ingress_sequence,
        );
        let attachment = match encode_runtime_metadata(&metadata) {
            Ok(attachment) => attachment,
            Err(error) => {
                self.external_ingress.release(&key.0, &key.1, ticket);
                return Ok(PublicBackendOutcome::NotSent(error.to_string()));
            }
        };
        let session = match self.bus.session() {
            Ok(session) => session,
            Err(error) => {
                self.external_ingress.release(&key.0, &key.1, ticket);
                return Ok(PublicBackendOutcome::NotSent(error.to_string()));
            }
        };
        let reply_key = self.bus.full_key(&port_key(
            &binding.service_instance,
            &binding.metadata.endpoint,
            "reply",
        ));
        let reply_key_expr = match OwnedKeyExpr::new(reply_key.clone()) {
            Ok(key) => key,
            Err(error) => {
                self.external_ingress.release(&key.0, &key.1, ticket);
                return Ok(PublicBackendOutcome::NotSent(format!(
                    "invalid Runtime reply key: {error}"
                )));
            }
        };
        let subscriber = match session
            .declare_subscriber(reply_key_expr)
            .with(zenoh::handlers::FifoChannel::new(8))
            .await
        {
            Ok(subscriber) => subscriber,
            Err(error) => {
                self.external_ingress.release(&key.0, &key.1, ticket);
                return Ok(PublicBackendOutcome::NotSent(error.to_string()));
            }
        };
        let request_key = self.bus.full_key(&port_key(
            &binding.service_instance,
            &binding.metadata.endpoint,
            "request",
        ));
        if let Err(error) = session
            .put(request_key, payload)
            .encoding(Encoding::from(PROTOBUF_ENCODING.to_owned()))
            .attachment(attachment)
            .await
        {
            return Ok(PublicBackendOutcome::OutcomeUnknown(format!(
                "Runtime request admission became uncertain: {error}"
            )));
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout.max(Duration::from_millis(1)))
            .ok_or_else(|| {
                PublicBackendError::Transport("Runtime deadline overflowed".to_owned())
            })?;
        loop {
            let sample = match tokio::time::timeout_at(deadline, subscriber.recv_async()).await {
                Ok(Ok(sample)) => sample,
                Ok(Err(error)) => {
                    return Ok(PublicBackendOutcome::OutcomeUnknown(format!(
                        "Runtime reply transport failed: {error}"
                    )));
                }
                Err(_) => {
                    return Ok(PublicBackendOutcome::OutcomeUnknown(
                        "Runtime reply deadline elapsed after request admission".to_owned(),
                    ));
                }
            };
            let wire = match WireSample::from_zenoh(sample) {
                Ok(wire) => wire,
                Err(_) => continue,
            };
            if wire.metadata().command_id != Some(command_id) {
                continue;
            }
            if wire.key() != reply_key
                || wire.metadata().source.as_deref() != Some(&key.0)
                || wire.metadata().eligible_boundary != Some(ticket.eligible_boundary)
                || wire.metadata().caller.as_deref() != Some(self.ingress.caller.as_str())
                || wire.metadata().ingress_sequence != Some(ticket.ingress_sequence)
            {
                return Ok(PublicBackendOutcome::OutcomeUnknown(
                    "Runtime reply identity did not match the admitted target".to_owned(),
                ));
            }
            match wire.metadata().wire_control() {
                Ok(WireControl::Busy | WireControl::Oversized)
                    if contract.role == RuntimeMethodRole::Read =>
                {
                    self.external_ingress.release(&key.0, &key.1, ticket);
                    return Ok(PublicBackendOutcome::RejectedBeforeAdmission(format!(
                        "Read refused: {:?}",
                        wire.metadata()
                            .wire_control()
                            .map_err(|e| PublicBackendError::Transport(e.to_string()))?
                    )));
                }
                Ok(WireControl::Rejected) => {
                    self.external_ingress.release(&key.0, &key.1, ticket);
                    return Ok(PublicBackendOutcome::RejectedBeforeAdmission(
                        wire.metadata().reason.clone().unwrap_or_else(|| {
                            "Runtime rejected the request before queue admission".to_owned()
                        }),
                    ));
                }
                Ok(WireControl::Data)
                    if wire.payload().len() as u64 <= contract.response_max_bytes =>
                {
                    self.external_ingress.release(&key.0, &key.1, ticket);
                    return Ok(PublicBackendOutcome::Received(wire.payload().to_vec()));
                }
                _ => {
                    return Ok(PublicBackendOutcome::OutcomeUnknown(
                        "Runtime reply violated its compiled transport contract".to_owned(),
                    ));
                }
            }
        }
    }
}

impl PublicSessionBackend for RuntimePublicBackend {
    fn call(
        &self,
        operation: PublicOperation,
        binding: PublicBindingContext,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<PublicBackendOutcome, PublicBackendError>> + Send>>
    {
        let backend = self.clone();
        Box::pin(async move {
            backend
                .call_inner(operation, binding, payload, timeout)
                .await
        })
    }

    fn subscribe(
        &self,
        operation: PublicOperation,
        binding: PublicBindingContext,
        request: SubscriptionRequest,
        capacity: usize,
    ) -> Result<PublicBackendSubscription, PublicBackendError> {
        let key = (
            binding.service_instance.clone(),
            binding.metadata.endpoint.clone(),
        );
        let contract = self.ports.get(&key).ok_or_else(|| {
            PublicBackendError::RejectedBeforeAdmission(
                "public Runtime subscription port is absent from the bundle".to_owned(),
            )
        })?;
        let role_matches = operation == PublicOperation::Observe
            && matches!(
                contract.role,
                RuntimeMethodRole::State
                    | RuntimeMethodRole::Sample
                    | RuntimeMethodRole::Event
                    | RuntimeMethodRole::Stream
            );
        if !role_matches || binding.metadata != contract.metadata {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "public Runtime subscription metadata does not match the bundle".to_owned(),
            ));
        }
        let capacity = capacity.clamp(1, MAX_RUNTIME_SUBSCRIBER_ITEMS);
        let (sender, records) = mpsc::channel(capacity);
        let bus = self.bus.clone();
        let binding = binding.clone();
        let contract = contract.clone();
        let spawn = tokio::runtime::Handle::try_current().map_err(|_| {
            PublicBackendError::Transport(
                "public Runtime subscription has no async runtime".to_owned(),
            )
        })?;
        spawn.spawn(async move {
            let session = match bus.session() {
                Ok(session) => session,
                Err(error) => {
                    let _ = sender
                        .send(Err(PublicBackendError::Transport(error.to_string())))
                        .await;
                    return;
                }
            };
            let relative = port_key(
                &binding.service_instance,
                &binding.metadata.endpoint,
                "publish",
            );
            let key = bus.full_key(&relative);
            let subscriber = match OwnedKeyExpr::new(key.clone()) {
                Ok(key) => match session
                    .declare_subscriber(key)
                    .with(zenoh::handlers::FifoChannel::new(capacity))
                    .await
                {
                    Ok(subscriber) => subscriber,
                    Err(error) => {
                        let _ = sender
                            .send(Err(PublicBackendError::Transport(error.to_string())))
                            .await;
                        return;
                    }
                },
                Err(error) => {
                    let _ = sender
                        .send(Err(PublicBackendError::Transport(format!(
                            "invalid Runtime publication key: {error}"
                        ))))
                        .await;
                    return;
                }
            };
            loop {
                let sample = match subscriber.recv_async().await {
                    Ok(sample) => sample,
                    Err(error) => {
                        let _ = sender
                            .send(Err(PublicBackendError::Transport(error.to_string())))
                            .await;
                        return;
                    }
                };
                let record = match runtime_record(sample, &contract, &request) {
                    Ok(record) => record,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                if sender.send(Ok(record)).await.is_err() {
                    return;
                }
            }
        });
        Ok(PublicBackendSubscription::new(None, records))
    }
}

fn runtime_record(
    sample: zenoh::sample::Sample,
    contract: &RuntimeMethodContract,
    request: &SubscriptionRequest,
) -> Result<SubscriptionRecord, PublicBackendError> {
    let wire = WireSample::from_zenoh(sample).map_err(|error| {
        PublicBackendError::Transport(format!("Runtime observation metadata is invalid: {error}"))
    })?;
    let control = wire.metadata().wire_control().map_err(|error| {
        PublicBackendError::Transport(format!("Runtime observation control is invalid: {error}"))
    })?;
    let kind = match control {
        WireControl::Data => {
            if wire.payload().len() as u64 > contract.response_max_bytes {
                return Err(PublicBackendError::Transport(
                    "Runtime observation exceeds its compiled byte bound".to_owned(),
                ));
            }
            RecordKind::Value
        }
        WireControl::Gap => RecordKind::Gap,
        WireControl::End => RecordKind::End,
        WireControl::Failed => RecordKind::Failed,
        WireControl::Rejected
        | WireControl::Withdraw
        | WireControl::Busy
        | WireControl::Oversized => {
            return Err(PublicBackendError::Transport(
                "Runtime observation used an incompatible command or Setpoint control".to_owned(),
            ));
        }
    };
    let payload = if control == WireControl::Data {
        wire.payload().to_vec()
    } else {
        if !wire.payload().is_empty() {
            return Err(PublicBackendError::Transport(
                "Runtime control observation carried a payload".to_owned(),
            ));
        }
        Vec::new()
    };
    Ok(SubscriptionRecord {
        session_id: request.session_id.clone(),
        binding_id: request.binding_id.clone(),
        subscription_id: request.subscription_id.clone(),
        execution_id: request.execution_id.clone(),
        timeline_id: request.timeline_id.clone(),
        revision: wire
            .metadata()
            .revision
            .or(wire.metadata().sequence)
            .unwrap_or_default(),
        kind,
        payload,
        dropped: 0,
        detail: if control == WireControl::Gap {
            Some("Runtime source reported a bounded observation gap".to_owned())
        } else {
            None
        },
    })
}

fn encode_runtime_metadata(metadata: &RuntimeWireMetadata) -> Result<Vec<u8>, PublicBackendError> {
    if metadata.encoded_len() > MAX_RUNTIME_METADATA_BYTES {
        return Err(PublicBackendError::RejectedBeforeAdmission(
            "Runtime metadata exceeds its bounded attachment size".to_owned(),
        ));
    }
    let mut encoded = Vec::with_capacity(metadata.encoded_len());
    metadata
        .encode(&mut encoded)
        .map_err(|error| PublicBackendError::Transport(error.to_string()))?;
    Ok(encoded)
}

/// Controlled Runtime boundary supplied by the runtime-semantics owner.
///
/// The supervisor performs public/session/lease fencing and observation
/// admission.  This hook is the only authority allowed to advance a runtime
/// boundary.  Keeping it explicit prevents a public bridge from claiming
/// completion after merely publishing sensor bytes.
pub(crate) trait RuntimeBoundaryHook: Send + Sync {
    /// Check receiver admission capacity before publishing any observation.
    fn validate_observation_admission(&self, observations: &[Observation]) -> Result<(), String>;
    fn acquire(
        &self,
        context: PublicSimulationContext,
        request: AcquireAuthorityRequest,
    ) -> BoundaryFuture<()>;
    fn admit_initial_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitInitialObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> BoundaryFuture<AdmitInitialObservationsResponse>;
    fn prepare_boundary(
        &self,
        context: PublicSimulationContext,
        request: PrepareBoundaryRequest,
    ) -> BoundaryFuture<PrepareBoundaryResponse>;
    fn admit_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> BoundaryFuture<AdmitObservationsResponse>;
    fn reset(
        &self,
        context: PublicSimulationContext,
        request: ResetRequest,
        next_timeline_id: String,
    ) -> BoundaryFuture<()>;
    fn release(
        &self,
        context: PublicSimulationContext,
        request: ReleaseAuthorityRequest,
    ) -> BoundaryFuture<()>;
    fn progress(
        &self,
        context: PublicSimulationContext,
        request: ProgressRequest,
    ) -> BoundaryFuture<ProgressResponse>;
}

type BoundaryFuture<T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send>>;

/// Concrete simulation bridge which validates the immutable bundle contract,
/// forwards observations to Runtime publication ports, and delegates exactly
/// one admitted boundary to [`RuntimeBoundaryHook`].
pub(crate) struct RuntimeSimulationBridge {
    bus: Connection,
    ports: Arc<BTreeMap<(String, String), RuntimeMethodContract>>,
    definition: Option<SimulationDefinition>,
    boundary: Arc<dyn RuntimeBoundaryHook>,
}

impl std::fmt::Debug for RuntimeSimulationBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeSimulationBridge")
            .field("ports", &self.ports.len())
            .field("has_definition", &self.definition.is_some())
            .finish_non_exhaustive()
    }
}

impl RuntimeSimulationBridge {
    pub(crate) fn new(
        bus: Connection,
        surface: &RuntimePublicSurface,
        definition: Option<SimulationDefinition>,
        boundary: Arc<dyn RuntimeBoundaryHook>,
    ) -> Self {
        Self {
            bus,
            ports: surface.ports.clone(),
            definition,
            boundary,
        }
    }

    fn ensure_definition(
        &self,
        context: &PublicSimulationContext,
    ) -> Result<&SimulationDefinition, PublicBackendError> {
        let definition = self.definition.as_ref().ok_or_else(|| {
            PublicBackendError::RejectedBeforeAdmission(
                "the admitted bundle has no immutable simulation definition".to_owned(),
            )
        })?;
        if definition.model_identity() != context.model_identity
            || definition.quantum_ns() != context.quantum_ns
        {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "simulation context does not match the immutable bundle definition".to_owned(),
            ));
        }
        Ok(definition)
    }

    fn validate_provider_set(
        &self,
        definition: &SimulationDefinition,
        observations: &[Observation],
    ) -> Result<(), PublicBackendError> {
        let required = definition
            .providers()
            .iter()
            .map(|provider| (provider.service_instance(), provider.port()))
            .collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        for observation in observations {
            let membership = observation.membership.as_ref().ok_or_else(|| {
                PublicBackendError::RejectedBeforeAdmission(
                    "simulation observation is missing its membership".to_owned(),
                )
            })?;
            if !seen.insert((membership.producer.as_str(), membership.port.as_str())) {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "simulation observations contain a duplicate provider".to_owned(),
                ));
            }
            let provider = definition
                .providers()
                .iter()
                .find(|provider| {
                    provider.service_instance() == membership.producer
                        && provider.port() == membership.port
                })
                .ok_or_else(|| {
                    PublicBackendError::RejectedBeforeAdmission(
                        "simulation observation is not in the immutable provider set".to_owned(),
                    )
                })?;
            let contract = self
                .ports
                .get(&(membership.producer.clone(), membership.port.clone()))
                .ok_or_else(|| {
                    PublicBackendError::RejectedBeforeAdmission(
                        "simulation provider is absent from the compiled Runtime graph".to_owned(),
                    )
                })?;
            if contract.metadata.shape != provider.shape()
                || contract.metadata.input_fqn != provider.input_fqn()
                || contract.metadata.output_fqn != provider.payload_fqn()
                || observation.payload.len() as u64 > contract.response_max_bytes
            {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "simulation observation does not match its immutable provider metadata"
                        .to_owned(),
                ));
            }
        }
        if seen != required {
            return Err(PublicBackendError::RejectedBeforeAdmission(
                "simulation observations do not contain the complete immutable provider set"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    async fn publish_observations(
        &self,
        context: &PublicSimulationContext,
        observations: &[Observation],
    ) -> Result<Vec<phoxal::communication::simulation::ProductMembership>, PublicBackendError> {
        let session = self
            .bus
            .session()
            .map_err(|error| PublicBackendError::Transport(error.to_string()))?;
        let mut memberships = Vec::with_capacity(observations.len());
        for observation in observations {
            let membership = observation.membership.as_ref().ok_or_else(|| {
                PublicBackendError::RejectedBeforeAdmission(
                    "simulation observation is missing its membership".to_owned(),
                )
            })?;
            if membership.sequence == 0 {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "simulation observation sequence exhausted".to_owned(),
                ));
            }
            if matches!(
                membership.disposition,
                phoxal::communication::simulation::ProductDisposition::NotDue
                    | phoxal::communication::simulation::ProductDisposition::Empty
            ) {
                memberships.push(membership.clone());
                continue;
            }
            let stamp = ObservationStamp::new(
                membership.producer.clone(),
                ExecutionTime::from_nanos(membership.capture_time_ns),
                None,
            );
            let metadata = RuntimeWireMetadata::observed(&stamp, membership.sequence)
                .with_delivery_identity(
                    &context.execution_id,
                    &context.timeline_id,
                    membership.capture_boundary,
                    0,
                )
                .with_eligible_boundary(membership.capture_boundary);
            let attachment = encode_runtime_metadata(&metadata)?;
            let key =
                self.bus
                    .full_key(&port_key(&membership.producer, &membership.port, "publish"));
            session
                .put(key, observation.payload.clone())
                .encoding(Encoding::from(PROTOBUF_ENCODING.to_owned()))
                .attachment(attachment)
                .await
                .map_err(|error| PublicBackendError::Transport(error.to_string()))?;
            memberships.push(membership.clone());
        }
        Ok(memberships)
    }
}

impl PublicSimulationBackend for RuntimeSimulationBridge {
    fn acquire(
        &self,
        context: PublicSimulationContext,
        request: AcquireAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        let definition = self.definition.clone();
        let boundary = self.boundary.clone();
        Box::pin(async move {
            let definition = definition.ok_or_else(|| {
                PublicBackendError::RejectedBeforeAdmission(
                    "the admitted bundle has no immutable simulation definition".to_owned(),
                )
            })?;
            if request.model_identity != definition.model_identity()
                || request.quantum_ns != definition.quantum_ns()
                || request.providers.len() != definition.providers().len()
            {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "simulation authority does not match the immutable bundle definition"
                        .to_owned(),
                ));
            }
            let expected = definition
                .providers()
                .iter()
                .map(|provider| {
                    (
                        provider.service_instance().to_owned(),
                        provider.port().to_owned(),
                        provider.shape() as i32,
                        provider.input_fqn().to_owned(),
                        provider.payload_fqn().to_owned(),
                    )
                })
                .collect::<BTreeSet<_>>();
            let observed = request
                .providers
                .iter()
                .map(|provider| {
                    (
                        provider.service_instance.clone(),
                        provider.port.clone(),
                        provider.shape,
                        provider.input_fqn.clone(),
                        provider.payload_fqn.clone(),
                    )
                })
                .collect::<BTreeSet<_>>();
            if observed != expected {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "simulation provider requirements do not match the immutable bundle definition"
                        .to_owned(),
                ));
            }
            boundary
                .acquire(context, request)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn admit_initial_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitInitialObservationsRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AdmitInitialObservationsResponse, PublicBackendError>>
                + Send,
        >,
    > {
        let bridge = Arc::new(self.clone_for_async());
        Box::pin(async move {
            let definition = bridge.ensure_definition(&context)?;
            bridge.validate_provider_set(definition, &request.observations)?;
            bridge
                .boundary
                .validate_observation_admission(&request.observations)
                .map_err(PublicBackendError::RejectedBeforeAdmission)?;
            let published = bridge
                .publish_observations(&context, &request.observations)
                .await?;
            bridge
                .boundary
                .admit_initial_observations(context, request, published)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn prepare_boundary(
        &self,
        context: PublicSimulationContext,
        request: PrepareBoundaryRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PrepareBoundaryResponse, PublicBackendError>> + Send>>
    {
        let bridge = Arc::new(self.clone_for_async());
        Box::pin(async move {
            bridge.ensure_definition(&context)?;
            bridge
                .boundary
                .prepare_boundary(context, request)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn admit_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitObservationsRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AdmitObservationsResponse, PublicBackendError>> + Send>>
    {
        let bridge = Arc::new(self.clone_for_async());
        Box::pin(async move {
            let definition = bridge.ensure_definition(&context)?;
            bridge.validate_provider_set(definition, &request.observations)?;
            bridge
                .boundary
                .validate_observation_admission(&request.observations)
                .map_err(PublicBackendError::RejectedBeforeAdmission)?;
            let published = bridge
                .publish_observations(&context, &request.observations)
                .await?;
            bridge
                .boundary
                .admit_observations(context, request, published)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn reset(
        &self,
        context: PublicSimulationContext,
        request: ResetRequest,
        next_timeline_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        let definition = self.definition.clone();
        let boundary = self.boundary.clone();
        Box::pin(async move {
            if definition.is_none() {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "the admitted bundle has no immutable simulation definition".to_owned(),
                ));
            }
            boundary
                .reset(context, request, next_timeline_id)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn release(
        &self,
        context: PublicSimulationContext,
        request: ReleaseAuthorityRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublicBackendError>> + Send>> {
        let boundary = self.boundary.clone();
        Box::pin(async move {
            boundary
                .release(context, request)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }

    fn progress(
        &self,
        context: PublicSimulationContext,
        request: ProgressRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ProgressResponse, PublicBackendError>> + Send>> {
        let definition = self.definition.clone();
        let boundary = self.boundary.clone();
        Box::pin(async move {
            if definition.is_none() {
                return Err(PublicBackendError::RejectedBeforeAdmission(
                    "the admitted bundle has no immutable simulation definition".to_owned(),
                ));
            }
            boundary
                .progress(context, request)
                .await
                .map_err(PublicBackendError::Transport)
        })
    }
}

impl RuntimeSimulationBridge {
    fn clone_for_async(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            ports: self.ports.clone(),
            definition: self.definition.clone(),
            boundary: self.boundary.clone(),
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::bundle::TestExecutable;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    use phoxal::communication::simulation::{CutReceipt, TransitionKey};

    use super::*;

    fn ready_state() -> ExecutionState {
        let state = ExecutionState::new();
        state.mark_ready();
        state
    }

    fn external_contract(
        role: RuntimeMethodRole,
        max_buffered_items: u32,
    ) -> RuntimeMethodContract {
        let metadata = MethodMetadata {
            endpoint: "operation".to_owned(),
            shape: role.shape(),
            input_fqn: "fixture.Request".to_owned(),
            output_fqn: "fixture.Response".to_owned(),
            max_message_bytes: 128,
            max_buffered_items,
            retained_latest: role == RuntimeMethodRole::State,
            lease_valid_for_ms: None,
        };
        RuntimeMethodContract {
            metadata,
            role,
            request_max_bytes: 128,
            response_max_bytes: 128,
        }
    }

    #[test]
    fn private_runtime_role_mapping_is_explicit() {
        let signature =
            |shape, retained_latest, lease_valid_for_ms| phoxal::artifact::MethodSignature {
                endpoint: "method".to_owned(),
                service: "fixture.Service".to_owned(),
                method: "Method".to_owned(),
                shape,
                request: "fixture.Request".to_owned(),
                response: "fixture.Response".to_owned(),
                retained_latest,
                lease_valid_for_ms,
            };
        assert_eq!(
            output_role(&signature(
                phoxal::artifact::MethodShape::Observation,
                true,
                None
            )),
            RuntimeMethodRole::State
        );
        assert_eq!(
            output_role(&signature(
                phoxal::artifact::MethodShape::Observation,
                false,
                Some(100)
            )),
            RuntimeMethodRole::Setpoint
        );
        assert_eq!(
            output_role(&signature(phoxal::artifact::MethodShape::Call, false, None)),
            RuntimeMethodRole::Read
        );
    }

    #[test]
    fn simulation_surface_retains_provider_metadata_and_exact_bindings() {
        let source = BundleSimulation {
            protocol: "phoxal.simulation.v1".to_owned(),
            mode: "controlled".to_owned(),
            model_identity: "model-digest".to_owned(),
            quantum_ns: 10_000_000,
            providers: vec![phoxal::artifact::bundle::BundleSimulationProvider {
                rate_microhertz: 100_000_000,
                service_fqn: "fixture.Sensor".into(),
                method: "Sample".into(),
                service_instance: "imu".to_owned(),
                port: "sample".to_owned(),
                shape: phoxal::artifact::MethodShape::Observation,
                retained_latest: false,
                lease_valid_for_ms: None,
                input_fqn: "google.protobuf.Empty".to_owned(),
                payload_fqn: "fixture.Imu".to_owned(),
                max_message_bytes: 1024,
                max_buffered_items: 16,
            }],
            actuation_bindings: vec![phoxal::artifact::bundle::BundleActuationBinding {
                service_instance: "motion".to_owned(),
                port: "actuators".to_owned(),
                payload_fqn: "fixture.Actuators".to_owned(),
                actuator_ids: vec!["left".to_owned(), "right".to_owned()],
            }],
        };
        let mut service_methods = BTreeMap::new();
        let mut runtime_methods = BTreeMap::from([(
            ("motion".to_owned(), "actuators".to_owned()),
            RuntimeMethodContract {
                metadata: MethodMetadata {
                    endpoint: "actuators".to_owned(),
                    shape: MethodShape::Call,
                    input_fqn: "fixture.Empty".to_owned(),
                    output_fqn: "fixture.Actuators".to_owned(),
                    max_message_bytes: 2048,
                    max_buffered_items: 1,
                    retained_latest: false,
                    lease_valid_for_ms: Some(100),
                },
                role: RuntimeMethodRole::Setpoint,
                request_max_bytes: 2048,
                response_max_bytes: 2048,
            },
        )]);

        add_simulation_provider_metadata(&source, &mut service_methods, &mut runtime_methods)
            .expect("provider metadata is admitted");
        validate_simulation_actuation_bindings(&source, &runtime_methods)
            .expect("exact setpoint binding is admitted");
        let definition = simulation_definition(&source).expect("public simulation definition");

        assert_eq!(service_methods["imu"][0].endpoint, "sample");
        assert_eq!(definition.providers()[0].payload_fqn(), "fixture.Imu");
        assert_eq!(definition.model_identity(), "model-digest");
        assert_eq!(definition.quantum_ns(), 10_000_000);
    }

    #[test]
    fn runtime_metadata_is_bounded_before_encoding() {
        let metadata = RuntimeWireMetadata {
            source: Some("x".repeat(2_000)),
            ..Default::default()
        };
        assert!(encode_runtime_metadata(&metadata).is_err());
    }

    #[test]
    fn external_coordinator_uses_ready_boundary_sequence_and_bounded_capacity() {
        let coordinator = RuntimeExecutionCoordinator::new(ready_state());
        let contract = external_contract(RuntimeMethodRole::Commands, 2);
        let caller = RuntimeIngressIdentity::default();
        let first = coordinator
            .admit("service", "operation", &caller, &contract)
            .expect("first external ticket");
        let second = coordinator
            .admit("service", "operation", &caller, &contract)
            .expect("second external ticket");
        assert_eq!(first.eligible_boundary, 1);
        assert_eq!(second.eligible_boundary, 1);
        assert_eq!(first.ingress_sequence, 1);
        assert_eq!(second.ingress_sequence, 2);
        assert!(first.logical_time <= second.logical_time);
        assert!(matches!(
            coordinator.admit("service", "operation", &caller, &contract),
            Err(PublicBackendError::RejectedBeforeAdmission(detail))
                if detail.contains("capacity")
        ));
        RuntimeExternalIngress::release(&coordinator, "service", "operation", first);
        let third = coordinator
            .admit("service", "operation", &caller, &contract)
            .expect("released capacity is reusable");
        assert_eq!(third.ingress_sequence, 3);
    }

    #[test]
    fn public_surface_uses_brain_artifact_and_exact_service_bounds() {
        let brain_artifact = serde_json::json!({
            "runtime": {
                "outputs": [{
                    "name": "state_output",
                    "port": "state",
                    "max_items": 1,
                    "max_bytes": 64,
                    "signature": {
                        "endpoint": "state",
                        "service": "fixture.Brain",
                        "method": "State",
                        "shape": "observation",
                        "request": "google.protobuf.Empty",
                        "response": "fixture.State",
                        "retained_latest": true,
                        "lease_valid_for_ms": null
                    }
                }]
            }
        });
        let service_artifact = serde_json::json!({
            "runtime": {
                "inputs": [{
                    "name": "command",
                    "delivery": "call_ingress",
                    "response_max_bytes": 256,
                    "response_max_items": 1,
                    "port": "command",
                    "max_items": 2,
                    "max_bytes": 128,
                    "signature": {
                        "endpoint": "command",
                        "service": "fixture.Service",
                        "method": "Command",
                        "shape": "call",
                        "request": "fixture.Request",
                        "response": "fixture.Response",
                        "retained_latest": false,
                        "lease_valid_for_ms": null
                    }
                }],
                "outputs": []
            }
        });
        let bundle = RuntimeBundle::for_test(
            std::path::Path::new("/tmp/fixture"),
            "robot",
            vec![
                TestExecutable::with_artifact("brain", brain_artifact),
                TestExecutable::with_artifact("service", service_artifact),
            ],
        );
        let surface = RuntimePublicSurface::from_bundle(&bundle).expect("artifact contracts");

        assert_eq!(
            surface
                .services
                .iter()
                .map(ServiceMethods::instance)
                .collect::<Vec<_>>(),
            vec!["brain", "service"]
        );
        let brain_state = surface
            .ports
            .get(&("brain".to_owned(), "state".to_owned()))
            .expect("brain public method");
        assert_eq!(brain_state.metadata.input_fqn, "google.protobuf.Empty");
        assert_eq!(brain_state.metadata.output_fqn, "fixture.State");
        assert_eq!(brain_state.metadata.max_message_bytes, 64);

        let command = surface
            .ports
            .get(&("service".to_owned(), "command".to_owned()))
            .expect("service command");
        assert_eq!(command.metadata.max_message_bytes, 256);
        assert_eq!(command.request_max_bytes, 128);
        assert_eq!(command.response_max_bytes, 256);
    }

    #[derive(Debug)]
    struct TestExternalIngress {
        tickets: Mutex<VecDeque<ExternalIngressTicket>>,
        seen: Mutex<Vec<(String, String, String)>>,
    }

    impl RuntimeExternalIngress for TestExternalIngress {
        fn admit(
            &self,
            target_instance: &str,
            target_port: &str,
            caller: &RuntimeIngressIdentity,
            _contract: &RuntimeMethodContract,
        ) -> Result<ExternalIngressTicket, PublicBackendError> {
            assert_eq!(caller.source, "supervisor");
            assert_eq!(caller.caller, "supervisor.public");
            self.seen.lock().expect("test lock").push((
                target_instance.to_owned(),
                target_port.to_owned(),
                caller.caller.clone(),
            ));
            self.tickets
                .lock()
                .expect("test lock")
                .pop_front()
                .ok_or(PublicBackendError::Capacity)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_ingress_carries_exact_boundary_and_sequence_order() {
        let (owner, bus) = phoxal::runtime::connection::ConnectionOwner::open(
            phoxal::runtime::connection::ConnectionConfig::for_external(
                phoxal::identity::ExecutionId::mint(),
                None,
                Vec::new(),
            ),
        )
        .await
        .expect("test bus opens");
        let metadata = MethodMetadata {
            endpoint: "command".to_owned(),
            shape: MethodShape::Call,
            input_fqn: "fixture.Request".to_owned(),
            output_fqn: "fixture.Response".to_owned(),
            max_message_bytes: 256,
            max_buffered_items: 2,
            retained_latest: false,
            lease_valid_for_ms: None,
        };
        let contract = RuntimeMethodContract {
            metadata: metadata.clone(),
            role: RuntimeMethodRole::Commands,
            request_max_bytes: 128,
            response_max_bytes: 256,
        };
        let surface = RuntimePublicSurface {
            services: Vec::new(),
            ports: Arc::new(BTreeMap::from([(
                ("service".to_owned(), "command".to_owned()),
                contract,
            )])),
            ingress: RuntimeIngressIdentity::default(),
            simulation: None,
        };
        let external = Arc::new(TestExternalIngress {
            tickets: Mutex::new(VecDeque::from([
                ExternalIngressTicket {
                    eligible_boundary: 17,
                    ingress_sequence: 42,
                    logical_time: ExecutionTime::from_nanos(123),
                },
                ExternalIngressTicket {
                    eligible_boundary: 17,
                    ingress_sequence: 43,
                    logical_time: ExecutionTime::from_nanos(124),
                },
            ])),
            seen: Mutex::new(Vec::new()),
        });
        let backend = RuntimePublicBackend::new(bus.clone(), &surface, external.clone());
        let session = bus.session().expect("bus session");
        let request_key = bus.full_key(&port_key("service", "command", "request"));
        let reply_key = bus.full_key(&port_key("service", "command", "reply"));
        let request_subscriber = session
            .declare_subscriber(OwnedKeyExpr::new(request_key).expect("request key"))
            .with(zenoh::handlers::FifoChannel::new(4))
            .await
            .expect("request subscriber");
        let responder = tokio::spawn(async move {
            for expected_sequence in [42_u64, 43_u64] {
                let sample = request_subscriber
                    .recv_async()
                    .await
                    .expect("request sample");
                let wire = WireSample::from_zenoh(sample).expect("request metadata");
                assert_eq!(wire.metadata().eligible_boundary, Some(17));
                assert_eq!(wire.metadata().ingress_sequence, Some(expected_sequence));
                assert_eq!(wire.metadata().caller.as_deref(), Some("supervisor.public"));
                let mut response_metadata = wire.metadata().clone();
                response_metadata.source = Some("service".to_owned());
                let attachment =
                    encode_runtime_metadata(&response_metadata).expect("reply metadata");
                session
                    .put(reply_key.clone(), vec![expected_sequence as u8])
                    .encoding(Encoding::from(PROTOBUF_ENCODING.to_owned()))
                    .attachment(attachment)
                    .await
                    .expect("reply sample");
            }
        });
        let binding = PublicBindingContext {
            session_id: vec![1],
            binding_id: vec![2],
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            service_instance: "service".to_owned(),
            metadata,
        };
        let first = backend
            .call_inner(
                PublicOperation::Call,
                binding.clone(),
                vec![1],
                Duration::from_secs(1),
            )
            .await
            .expect("first call");
        let second = backend
            .call_inner(
                PublicOperation::Call,
                binding,
                vec![2],
                Duration::from_secs(1),
            )
            .await
            .expect("second call");
        assert_eq!(first, PublicBackendOutcome::Received(vec![42]));
        assert_eq!(second, PublicBackendOutcome::Received(vec![43]));
        responder.await.expect("responder completes");
        assert_eq!(
            external.seen.lock().expect("test lock").as_slice(),
            [
                (
                    "service".to_owned(),
                    "command".to_owned(),
                    "supervisor.public".to_owned(),
                ),
                (
                    "service".to_owned(),
                    "command".to_owned(),
                    "supervisor.public".to_owned(),
                ),
            ]
        );
        owner.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_read_uses_the_same_supervisor_ticket_and_reply_correlation() {
        let (owner, bus) = phoxal::runtime::connection::ConnectionOwner::open(
            phoxal::runtime::connection::ConnectionConfig::for_external(
                phoxal::identity::ExecutionId::mint(),
                None,
                Vec::new(),
            ),
        )
        .await
        .expect("test bus opens");
        let metadata = MethodMetadata {
            endpoint: "read".to_owned(),
            shape: MethodShape::Call,
            input_fqn: "fixture.Request".to_owned(),
            output_fqn: "fixture.Response".to_owned(),
            max_message_bytes: 256,
            max_buffered_items: 1,
            retained_latest: false,
            lease_valid_for_ms: None,
        };
        let surface = RuntimePublicSurface {
            services: Vec::new(),
            ports: Arc::new(BTreeMap::from([(
                ("provider".to_owned(), "read".to_owned()),
                RuntimeMethodContract {
                    metadata: metadata.clone(),
                    role: RuntimeMethodRole::Read,
                    request_max_bytes: 128,
                    response_max_bytes: 256,
                },
            )])),
            ingress: RuntimeIngressIdentity::default(),
            simulation: None,
        };
        let external = Arc::new(TestExternalIngress {
            tickets: Mutex::new(VecDeque::from([ExternalIngressTicket {
                eligible_boundary: 9,
                ingress_sequence: 7,
                logical_time: ExecutionTime::from_nanos(1234),
            }])),
            seen: Mutex::new(Vec::new()),
        });
        let backend = RuntimePublicBackend::new(bus.clone(), &surface, external);
        let session = bus.session().expect("bus session");
        let request_key = bus.full_key(&port_key("provider", "read", "request"));
        let reply_key = bus.full_key(&port_key("provider", "read", "reply"));
        let request_subscriber = session
            .declare_subscriber(OwnedKeyExpr::new(request_key).expect("request key"))
            .with(zenoh::handlers::FifoChannel::new(2))
            .await
            .expect("request subscriber");
        let responder = tokio::spawn(async move {
            let sample = request_subscriber
                .recv_async()
                .await
                .expect("read request sample");
            let wire = WireSample::from_zenoh(sample).expect("read request metadata");
            assert_eq!(wire.metadata().logical_time_nanos, Some(1234));
            assert_eq!(wire.metadata().eligible_boundary, Some(9));
            assert_eq!(wire.metadata().ingress_sequence, Some(7));
            assert_eq!(wire.metadata().caller_rank, None);
            assert_eq!(wire.metadata().caller.as_deref(), Some("supervisor.public"));
            let mut reply_metadata = wire.metadata().clone();
            reply_metadata.source = Some("provider".to_owned());
            session
                .put(reply_key, vec![8, 9])
                .encoding(Encoding::from(PROTOBUF_ENCODING.to_owned()))
                .attachment(encode_runtime_metadata(&reply_metadata).expect("reply metadata"))
                .await
                .expect("read reply");
        });
        let binding = PublicBindingContext {
            session_id: vec![1],
            binding_id: vec![2],
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            service_instance: "provider".to_owned(),
            metadata,
        };
        let result = backend
            .call_inner(
                PublicOperation::Call,
                binding,
                vec![4],
                Duration::from_secs(1),
            )
            .await
            .expect("read call");
        assert_eq!(result, PublicBackendOutcome::Received(vec![8, 9]));
        responder.await.expect("responder completes");
        owner.close().await;
    }

    #[derive(Debug)]
    struct RecordingBoundary {
        prepares: AtomicUsize,
    }

    impl RuntimeBoundaryHook for RecordingBoundary {
        fn validate_observation_admission(
            &self,
            _observations: &[Observation],
        ) -> Result<(), String> {
            Ok(())
        }
        fn acquire(
            &self,
            _context: PublicSimulationContext,
            _request: AcquireAuthorityRequest,
        ) -> BoundaryFuture<()> {
            Box::pin(async { Ok(()) })
        }

        fn admit_initial_observations(
            &self,
            _context: PublicSimulationContext,
            request: AdmitInitialObservationsRequest,
            _published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
        ) -> BoundaryFuture<AdmitInitialObservationsResponse> {
            Box::pin(async move {
                Ok(AdmitInitialObservationsResponse {
                    receipt: Some(CutReceipt {
                        transition_key: request.transition_key,
                        correlation_id: request.correlation_id,
                        status: phoxal::communication::simulation::PhaseStatus::InitialAdmitted,
                        ..Default::default()
                    }),
                })
            })
        }

        fn prepare_boundary(
            &self,
            _context: PublicSimulationContext,
            request: PrepareBoundaryRequest,
        ) -> BoundaryFuture<PrepareBoundaryResponse> {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(PrepareBoundaryResponse {
                    receipt: Some(CutReceipt {
                        transition_key: request.transition_key,
                        correlation_id: request.correlation_id,
                        status: phoxal::communication::simulation::PhaseStatus::Prepared,
                        ..Default::default()
                    }),
                    actuation: Vec::new(),
                })
            })
        }

        fn admit_observations(
            &self,
            _context: PublicSimulationContext,
            request: AdmitObservationsRequest,
            _published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
        ) -> BoundaryFuture<AdmitObservationsResponse> {
            Box::pin(async move {
                Ok(AdmitObservationsResponse {
                    receipt: Some(CutReceipt {
                        transition_key: request.transition_key,
                        correlation_id: request.correlation_id,
                        status:
                            phoxal::communication::simulation::PhaseStatus::ObservationsAdmitted,
                        ..Default::default()
                    }),
                })
            })
        }

        fn reset(
            &self,
            _context: PublicSimulationContext,
            _request: ResetRequest,
            _next_timeline_id: String,
        ) -> BoundaryFuture<()> {
            Box::pin(async { Ok(()) })
        }

        fn release(
            &self,
            _context: PublicSimulationContext,
            _request: ReleaseAuthorityRequest,
        ) -> BoundaryFuture<()> {
            Box::pin(async { Ok(()) })
        }

        fn progress(
            &self,
            _context: PublicSimulationContext,
            _request: ProgressRequest,
        ) -> BoundaryFuture<ProgressResponse> {
            Box::pin(async { Err("not used in this test".to_owned()) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bridge_forwards_prepare_as_one_exact_phase() {
        let (owner, bus) = phoxal::runtime::connection::ConnectionOwner::open(
            phoxal::runtime::connection::ConnectionConfig::for_external(
                phoxal::identity::ExecutionId::mint(),
                None,
                Vec::new(),
            ),
        )
        .await
        .expect("test bus opens");
        let metadata = MethodMetadata {
            endpoint: "state".to_owned(),
            shape: MethodShape::Observation,
            input_fqn: "google.protobuf.Empty".to_owned(),
            output_fqn: "example.Payload".to_owned(),
            max_message_bytes: 1024,
            max_buffered_items: 1,
            retained_latest: true,
            lease_valid_for_ms: None,
        };
        let contract = RuntimeMethodContract {
            metadata: metadata.clone(),
            role: RuntimeMethodRole::State,
            request_max_bytes: 1024,
            response_max_bytes: 1024,
        };
        let surface = RuntimePublicSurface {
            services: Vec::new(),
            ports: Arc::new(BTreeMap::from([(
                ("sensor".to_owned(), "state".to_owned()),
                contract,
            )])),
            ingress: RuntimeIngressIdentity::default(),
            simulation: None,
        };
        let definition = SimulationDefinition::new(
            "model-digest",
            1_000,
            vec![
                SimulationProviderDefinition::new(
                    "sensor",
                    "state",
                    MethodShape::Observation,
                    "google.protobuf.Empty",
                    "example.Payload",
                    100_000_000,
                )
                .expect("provider"),
            ],
        )
        .expect("simulation definition");
        let boundary = Arc::new(RecordingBoundary {
            prepares: AtomicUsize::new(0),
        });
        let bridge =
            RuntimeSimulationBridge::new(bus, &surface, Some(definition), boundary.clone());
        let context = PublicSimulationContext {
            principal: "simulator".to_owned(),
            session_id: vec![1],
            authority_grant: vec![2],
            correlation_id: vec![3],
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            completed_boundary: 4,
            model_identity: "model-digest".to_owned(),
            quantum_ns: 1_000,
        };
        let request = PrepareBoundaryRequest {
            transition_key: Some(TransitionKey {
                session_id: vec![1],
                execution_id: "execution".to_owned(),
                timeline_id: "timeline".to_owned(),
                authority_grant: vec![2],
                boundary: 4,
                operation_sequence: 1,
            }),
            correlation_id: vec![3],
        };
        let response = bridge
            .prepare_boundary(context, request)
            .await
            .expect("prepare phase succeeds");
        assert_eq!(
            response.receipt.expect("prepare receipt").status,
            phoxal::communication::simulation::PhaseStatus::Prepared
        );
        assert_eq!(boundary.prepares.load(Ordering::SeqCst), 1);
        owner.close().await;
    }
}
