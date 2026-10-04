//! Supervisor-owned execution admission and controlled boundary protocol.
//!
//! A public simulation advance is not complete when a local counter changes.
//! This module sends one exact invocation to every due runtime, waits for the
//! corresponding acceptance and product receipts, and commits the supervisor
//! boundary only after the whole required roster has replied.

mod capacity;
mod initialization;
use capacity::validate_controlled_capacity;
mod observation_admission;
mod read_views;
mod trace;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use zenoh::bytes::Encoding;
use zenoh::key_expr::OwnedKeyExpr;

use super::bundle::RuntimeBundle;
use super::public_backend::RuntimeBoundaryHook;
use super::state::{ExecutionState, TimeMode};
use crate::runtime::transport::server::PublicSimulationContext;
use phoxal::artifact::simulation::{
    ScenarioCaptureEvidence, ScenarioExecutionReport, ScenarioObservationEvidence,
    ScenarioStepEvidence,
};
use phoxal::communication::execution as wire;
use phoxal::communication::execution::RuntimeWireMetadata;
use phoxal::communication::simulation::{
    AcquireAuthorityRequest, AdmitInitialObservationsRequest, AdmitInitialObservationsResponse,
    AdmitObservationsRequest, AdmitObservationsResponse, CutReceipt, PrepareBoundaryRequest,
    PrepareBoundaryResponse, ProgressRequest, ProgressResponse, ReleaseAuthorityRequest,
    ResetRequest, TransitionKey,
};
use phoxal::runtime::ExecutionTime;
use phoxal::runtime::connection::Connection;
use phoxal::runtime::execution_protocol;
use phoxal::runtime::transport::{self, WireControl, WireSample};
use phoxal::scenario::plan_support::{
    Action as ScenarioAction, Capture as ScenarioCapture, Program,
};
use serde::Serialize;

const CONTROL_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_RUNTIME_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PRODUCT_RECEIPTS: usize = 4096;
const ADMISSION_RETRY: Duration = Duration::from_millis(100);

type BoundaryFuture<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, String>> + Send>>;

type Subscriber =
    zenoh::pubsub::Subscriber<zenoh::handlers::FifoChannelHandler<zenoh::sample::Sample>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeExecutionMode {
    Hardware,
    Controlled,
}

impl RuntimeExecutionMode {
    fn wire(self) -> wire::ExecutionMode {
        match self {
            Self::Hardware => wire::ExecutionMode::Hardware,
            Self::Controlled => wire::ExecutionMode::Controlled,
        }
    }
}

struct RuntimeInstance {
    instance: String,
    period_ns: u64,
    timeout: Duration,
    input_sources: BTreeMap<String, BTreeSet<(String, String)>>,
    product_ports: BTreeSet<String>,
    actuation_ports: BTreeSet<String>,
    actuation_max_bytes: BTreeMap<String, u64>,
    admit_response: Subscriber,
    ready: Subscriber,
    accepted: Subscriber,
    failures: Subscriber,
    reset_response: Subscriber,
    initialize_response: Subscriber,
    delivery_ack: Subscriber,
    pin_response: Option<Subscriber>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ExpectedDelivery {
    source: String,
    target: String,
    port: String,
    direction: String,
    sequence: u64,
    item: u32,
    bytes: u64,
}

struct BoundaryState {
    mode: RuntimeExecutionMode,
    quantum_ns: u64,
    timeline_id: String,
    fault: Option<String>,
    committed_boundary: u64,
    admitted_observation_boundary: u64,
    prepared_transition: Option<TransitionKey>,
    actuations: BTreeMap<(String, String), phoxal::communication::simulation::Actuation>,
}

struct ProtocolInner {
    bus: Connection,
    execution_id: String,
    state: ExecutionState,
    instances: Vec<RuntimeInstance>,
    artifacts: BTreeMap<String, phoxal::artifact::RuntimeRecord>,
    observation_providers:
        BTreeMap<(String, String), phoxal::artifact::bundle::BundleSimulationProvider>,
    connections: BTreeMap<
        phoxal::artifact::bundle::EndpointReference,
        Vec<phoxal::artifact::bundle::EndpointReference>,
    >,
    delivery_routes: BTreeMap<(String, String, String), BTreeSet<String>>,
    observation_acknowledgements: BTreeMap<String, Subscriber>,
    request_routes: RequestRoutes,
    scenario: Option<ScenarioDriver>,
    boundary: Mutex<BoundaryState>,
    failed: CancellationToken,
}

struct ScenarioDriver {
    program: Program,
    delivery_ack: Subscriber,
    captures: BTreeMap<String, ScenarioCaptureSubscription>,
    command_replies: BTreeMap<(String, String), Subscriber>,
    state: Mutex<ScenarioDriverState>,
}

struct ScenarioCaptureSubscription {
    kind: &'static str,
    policy: phoxal::scenario::CapturePolicy,
    subscriber: Subscriber,
}

struct ScenarioDeliveryExpectation<'a> {
    label: &'a str,
    kind: &'static str,
    timeline_id: &'a str,
    target: String,
    port: &'a str,
    sequence: u64,
    bytes: u64,
    production_boundary: u64,
    eligible_boundary: u64,
}

#[derive(Default)]
struct ScenarioDriverState {
    steps: Vec<ScenarioStepEvidence>,
    captures: BTreeMap<String, ScenarioCaptureEvidence>,
    command_labels: BTreeMap<u64, String>,
    command_replies: BTreeMap<String, Vec<u8>>,
}

/// Supervisor-side owner of private runtime admission and controlled progress.
#[derive(Clone)]
pub(crate) struct RuntimeExecutionProtocol {
    inner: Arc<ProtocolInner>,
}

impl std::fmt::Debug for RuntimeExecutionProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionProtocol")
            .field("execution_id", &self.inner.execution_id)
            .field("instances", &self.inner.instances.len())
            .finish_non_exhaustive()
    }
}

impl ScenarioDriver {
    async fn open(bus: &Connection, program: Program) -> Result<Self> {
        let delivery_ack = declare(bus, "supervisor", "delivery-ack").await?;
        let mut captures = BTreeMap::new();
        for capture in program.captures() {
            let (name, signature, kind, policy) = match capture {
                ScenarioCapture::State {
                    name,
                    signature,
                    policy,
                } => (name, signature, "state", *policy),
                ScenarioCapture::Sample {
                    name,
                    signature,
                    policy,
                } => (name, signature, "sample", *policy),
                ScenarioCapture::Event {
                    name,
                    signature,
                    policy,
                } => (name, signature, "event", *policy),
                ScenarioCapture::NativeBody { .. } => continue,
            };
            let (instance, _) = scenario_capture_target(name).with_context(|| {
                format!("scenario capture `{name}` must be named `<instance>/<label>`")
            })?;
            let subscriber = bus
                .session()?
                .declare_subscriber(bus.full_key(&transport::port_key(
                    instance,
                    &signature.endpoint,
                    "publish",
                )))
                .with(zenoh::handlers::FifoChannel::new(
                    usize::try_from(policy.capacity())
                        .unwrap_or(MAX_PRODUCT_RECEIPTS)
                        .min(MAX_PRODUCT_RECEIPTS),
                ))
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            captures.insert(
                name.clone(),
                ScenarioCaptureSubscription {
                    kind,
                    policy,
                    subscriber,
                },
            );
        }
        let mut command_replies = BTreeMap::new();
        for step in program.steps() {
            let ScenarioAction::Command {
                target_instance,
                service_signature,
                ..
            } = &step.action
            else {
                continue;
            };
            let key = (
                target_instance.clone(),
                service_signature.endpoint.to_owned(),
            );
            if command_replies.contains_key(&key) {
                continue;
            }
            let subscriber = bus
                .session()?
                .declare_subscriber(bus.full_key(&transport::port_key(
                    target_instance,
                    &service_signature.endpoint,
                    "reply",
                )))
                .with(zenoh::handlers::FifoChannel::new(MAX_PRODUCT_RECEIPTS))
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            command_replies.insert(key, subscriber);
        }
        Ok(Self {
            program,
            delivery_ack,
            captures,
            command_replies,
            state: Mutex::new(ScenarioDriverState::default()),
        })
    }
}

fn scenario_capture_target(name: &str) -> Option<(&str, &str)> {
    name.split_once('/').or_else(|| name.split_once('.'))
}

impl RuntimeExecutionProtocol {
    /// Declare response subscribers before any child process is launched.
    pub(crate) async fn open(
        bus: Connection,
        source: &RuntimeBundle,
        state: ExecutionState,
        scenario_program: Option<Program>,
    ) -> Result<Self> {
        let mut instances = Vec::new();
        let mut artifacts = BTreeMap::<String, phoxal::artifact::RuntimeRecord>::new();
        for instance in source.instances().keys() {
            let instance = instance.clone();
            source
                .executable(&instance)
                .with_context(|| format!("runtime `{instance}` has no verified executable"))?;
            let record = source
                .runtime_record(&instance)
                .with_context(|| format!("runtime `{instance}` has no runtime record"))?
                .clone();
            artifacts.insert(instance.clone(), record.clone());
            let phoxal::artifact::RuntimeRecord::V0 {
                period_ms,
                timeout_ms,
                inputs,
                outputs,
                ..
            } = record.clone();
            let period_ns = period_ms.checked_mul(1_000_000).with_context(|| {
                format!("runtime `{instance}` authored period overflows nanoseconds")
            })?;
            if period_ns == 0 {
                bail!("runtime `{instance}` authored period must be positive");
            }
            let timeout = if timeout_ms == 0 {
                DEFAULT_RUNTIME_TIMEOUT
            } else {
                Duration::from_millis(timeout_ms)
            };
            let input_sources = input_sources(source, &instance, &inputs)?;
            let product_ports = outputs
                .iter()
                .filter(|output| is_observation_method(output) || is_leased_method(output))
                .filter_map(|output| output.port.clone())
                .chain(
                    inputs
                        .iter()
                        .filter(|input| {
                            input.delivery == phoxal::artifact::InputDelivery::CallIngress
                                && input
                                    .signature
                                    .as_ref()
                                    .is_some_and(|signature| signature.lease_valid_for_ms.is_none())
                        })
                        .filter_map(|input| input.port.clone()),
                )
                .collect::<BTreeSet<_>>();
            let actuation_ports = outputs
                .iter()
                .filter(|output| is_leased_method(output))
                .filter(|output| {
                    source.simulation().is_some_and(|simulation| {
                        simulation.actuation_bindings.iter().any(|binding| {
                            binding.service_instance == instance
                                && output.port.as_deref() == Some(binding.port.as_str())
                        })
                    })
                })
                .map(|output| output.port.clone().unwrap_or_else(|| output.name.clone()))
                .collect::<BTreeSet<_>>();
            let actuation_max_bytes = outputs
                .iter()
                .filter(|output| is_leased_method(output))
                .filter_map(|output| {
                    output
                        .port
                        .as_ref()
                        .or(Some(&output.name))
                        .map(|port| (port.clone(), output.max_bytes.unwrap_or(u64::MAX)))
                })
                .collect::<BTreeMap<_, _>>();
            let admit_response = declare(&bus, &instance, "admit-response").await?;
            let ready = declare(&bus, &instance, "ready").await?;
            let accepted = declare(&bus, &instance, "accepted").await?;
            let failures = declare(&bus, &instance, "failure").await?;
            let reset_response = declare(&bus, &instance, "reset-response").await?;
            let initialize_response = declare(&bus, &instance, "initialize-state-response").await?;
            let delivery_ack = declare(&bus, &instance, "delivery-ack").await?;
            let pin_response = if outputs.iter().any(is_call_method) {
                Some(declare(&bus, &instance, "pin-read-views-response").await?)
            } else {
                None
            };
            instances.push(RuntimeInstance {
                instance,
                period_ns,
                timeout,
                input_sources,
                product_ports,
                actuation_ports,
                actuation_max_bytes,
                admit_response,
                ready,
                accepted,
                failures,
                reset_response,
                initialize_response,
                delivery_ack,
                pin_response,
            });
        }
        let (delivery_routes, request_routes) =
            graph_delivery_routes(source.connections(), &artifacts)?;
        let mut observation_acknowledgements = BTreeMap::new();
        if let Some(simulation) = source.simulation() {
            for provider in &simulation.providers {
                if observation_acknowledgements.contains_key(&provider.service_instance) {
                    continue;
                }
                let subscriber = bus
                    .session()?
                    .declare_subscriber(execution_protocol::key(
                        &bus,
                        &provider.service_instance,
                        "delivery-ack",
                    ))
                    .with(zenoh::handlers::FifoChannel::new(MAX_PRODUCT_RECEIPTS))
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                observation_acknowledgements.insert(provider.service_instance.clone(), subscriber);
            }
        }
        let scenario = match scenario_program {
            Some(program) => Some(ScenarioDriver::open(&bus, program).await?),
            None => None,
        };
        Ok(Self {
            inner: Arc::new(ProtocolInner {
                execution_id: bus.execution().to_string(),
                bus,
                state: state.clone(),
                instances,
                artifacts,
                observation_providers: source
                    .simulation()
                    .into_iter()
                    .flat_map(|simulation| &simulation.providers)
                    .map(|provider| {
                        (
                            (provider.service_instance.clone(), provider.port.clone()),
                            provider.clone(),
                        )
                    })
                    .collect(),
                connections: source.connections().clone(),
                delivery_routes,
                observation_acknowledgements,
                request_routes,
                scenario,
                boundary: Mutex::new(BoundaryState {
                    mode: RuntimeExecutionMode::Hardware,
                    quantum_ns: 0,
                    timeline_id: state.time_domain().timeline.to_string(),
                    fault: None,
                    committed_boundary: state.runtime_boundary(),
                    admitted_observation_boundary: state.runtime_boundary(),
                    prepared_transition: None,
                    actuations: BTreeMap::new(),
                }),
                failed: CancellationToken::new(),
            }),
        })
    }

    /// Admit every required runtime with one selected scheduling mode.
    pub(crate) async fn admit_all(
        &self,
        mode: RuntimeExecutionMode,
        quantum_ns: u64,
        timeline_id: &str,
    ) -> Result<()> {
        if matches!(mode, RuntimeExecutionMode::Controlled) && quantum_ns == 0 {
            bail!("controlled execution requires a positive quantum");
        }
        if matches!(mode, RuntimeExecutionMode::Hardware) && quantum_ns != 0 {
            bail!("hardware execution cannot carry a simulation quantum");
        }
        if matches!(mode, RuntimeExecutionMode::Controlled) {
            validate_controlled_capacity(
                &self.inner.instances,
                &self.inner.artifacts,
                &self.inner.connections,
                &self.inner.observation_providers,
                self.inner
                    .scenario
                    .as_ref()
                    .map(|scenario| &scenario.program),
                quantum_ns,
            )?;
        }
        for runtime in &self.inner.instances {
            if matches!(mode, RuntimeExecutionMode::Controlled)
                && !runtime.period_ns.is_multiple_of(quantum_ns)
            {
                bail!(
                    "runtime `{}` period {} ns is not an exact multiple of quantum {} ns",
                    runtime.instance,
                    runtime.period_ns,
                    quantum_ns
                );
            }
            let request = self.admission_request(runtime, mode, quantum_ns, timeline_id);
            send(&self.inner.bus, &runtime.instance, "admit", &request).await?;
        }
        // Every runtime can start independently. Retry each pending admission
        // concurrently, under one graph deadline, rather than serializing
        // retries behind the first slow process and restarting its budget.
        let deadline = tokio::time::Instant::now() + crate::runtime::process::STARTUP_TIMEOUT;
        let mut admissions = tokio::task::JoinSet::new();
        for index in 0..self.inner.instances.len() {
            let protocol = self.clone();
            let timeline = timeline_id.to_owned();
            admissions.spawn(async move {
                let runtime = &protocol.inner.instances[index];
                let response = loop {
                    match tokio::time::timeout(
                        ADMISSION_RETRY,
                        recv_admission(&runtime.admit_response),
                    )
                    .await
                    {
                        Ok(response) => break response?,
                        Err(_) => {
                            let request =
                                protocol.admission_request(runtime, mode, quantum_ns, &timeline);
                            send(&protocol.inner.bus, &runtime.instance, "admit", &request).await?;
                        }
                    }
                };
                tracing::debug!(runtime = %runtime.instance, "received runtime admission response");
                if !response.admitted {
                    bail!(
                        "runtime `{}` refused execution admission: {}",
                        runtime.instance,
                        response
                            .detail
                            .unwrap_or_else(|| "unspecified refusal".to_owned())
                    );
                }
                if !response.unsupported_contracts.is_empty() {
                    bail!(
                        "runtime `{}` returned unsupported contracts after admission: {}",
                        runtime.instance,
                        response.unsupported_contracts.join(", ")
                    );
                }
                tracing::debug!(runtime = %runtime.instance, "waiting for runtime Ready");
                let ready = recv_ready(&runtime.ready).await?;
                tracing::debug!(runtime = %runtime.instance, "received runtime Ready");
                if ready.execution_id != protocol.inner.execution_id
                    || ready.timeline_id != timeline
                    || ready.runtime_instance != runtime.instance
                {
                    bail!(
                        "runtime `{}` published a Ready message for another execution",
                        runtime.instance
                    );
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        tokio::time::timeout_at(deadline, async {
            while let Some(result) = admissions.join_next().await {
                result.context("runtime admission task failed")??;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("runtime graph admission and Ready timed out")??;
        let mut boundary = self.inner.boundary.lock().await;
        boundary.mode = mode;
        boundary.quantum_ns = quantum_ns;
        boundary.timeline_id = timeline_id.to_owned();
        boundary.committed_boundary = self.inner.state.runtime_boundary();
        boundary.admitted_observation_boundary = boundary.committed_boundary;
        boundary.prepared_transition = None;
        boundary.actuations.clear();
        boundary.fault = None;
        Ok(())
    }

    fn admission_request(
        &self,
        _runtime: &RuntimeInstance,
        mode: RuntimeExecutionMode,
        quantum_ns: u64,
        timeline_id: &str,
    ) -> wire::AdmitExecutionRequest {
        wire::AdmitExecutionRequest {
            execution_id: self.inner.execution_id.clone(),
            timeline_id: timeline_id.to_owned(),
            required_contracts: vec![wire::ContractRequirement {
                protocol: wire::PROTOCOL.to_owned(),
                capabilities: execution_protocol::REQUIRED_CAPABILITIES
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
            }],
            mode: mode.wire(),
            quantum_ns,
        }
    }

    pub(crate) async fn wait_failed(&self) {
        self.inner.failed.cancelled().await;
    }

    pub(crate) async fn failure_reason(&self) -> Option<String> {
        self.inner.boundary.lock().await.fault.clone()
    }

    pub(crate) async fn scenario_report(&self) -> Option<ScenarioExecutionReport> {
        let scenario = self.inner.scenario.as_ref()?;
        self.drain_scenario_records().await;
        let state = scenario.state.lock().await;
        Some(ScenarioExecutionReport::V0 {
            scenario_name: scenario.program.scenario_name().to_owned(),
            steps: state.steps.clone(),
            captures: state
                .captures
                .values()
                .cloned()
                .map(|mut capture| {
                    capture.terminal = true;
                    capture
                })
                .collect(),
            command_replies: state.command_replies.clone(),
        })
    }

    async fn publish_scenario_actions(
        &self,
        boundary: u64,
        logical_time_ns: u64,
        timeline_id: &str,
    ) -> Result<(), String> {
        let Some(scenario) = self.inner.scenario.as_ref() else {
            return Ok(());
        };
        let quantum_ns = u64::from(scenario.program.quantum().micros()) * 1_000;
        let run_valid_until_ns = u64::from(scenario.program.transition_count())
            .checked_add(1)
            .and_then(|count| count.checked_mul(quantum_ns))
            .ok_or_else(|| "scenario validity bound overflowed".to_owned())?;
        for (ordinal, step) in scenario.program.steps().iter().enumerate() {
            if u64::from(step.boundary) != boundary {
                continue;
            }
            let sequence = u64::try_from(ordinal)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| "scenario action sequence overflowed".to_owned())?;
            let eligible_boundary = boundary
                .checked_add(1)
                .ok_or_else(|| "scenario eligible boundary overflowed".to_owned())?;
            match &step.action {
                ScenarioAction::Setpoint {
                    target_instance,
                    consumer_signature,
                    encoded_payload,
                    validity,
                } => {
                    let mut metadata = RuntimeWireMetadata::data(
                        "supervisor",
                        ExecutionTime::from_nanos(logical_time_ns),
                        sequence,
                    )
                    .with_delivery_identity(
                        self.inner.execution_id.clone(),
                        timeline_id.to_owned(),
                        boundary,
                        0,
                    )
                    .with_eligible_boundary(eligible_boundary);
                    metadata.expires_at_nanos = Some(match validity {
                        phoxal::scenario::plan_support::Validity::Permanent => run_valid_until_ns,
                        phoxal::scenario::plan_support::Validity::Lease { valid_for_ms } => {
                            logical_time_ns
                                .checked_add(valid_for_ms.checked_mul(1_000_000).ok_or_else(
                                    || "scenario lease validity overflowed".to_owned(),
                                )?)
                                .ok_or_else(|| "scenario lease expiry overflowed".to_owned())?
                        }
                    });
                    publish_scenario_sample(
                        &self.inner.bus,
                        "supervisor",
                        &consumer_signature.endpoint,
                        "publish",
                        encoded_payload,
                        metadata,
                        WireControl::Data,
                    )
                    .await?;
                    self.wait_scenario_delivery(ScenarioDeliveryExpectation {
                        label: &step.label,
                        kind: "setpoint",
                        timeline_id,
                        target: format!("{target_instance}.{}", consumer_signature.endpoint),
                        port: &consumer_signature.endpoint,
                        sequence,
                        bytes: encoded_payload.len() as u64,
                        production_boundary: boundary,
                        eligible_boundary,
                    })
                    .await?;
                }
                ScenarioAction::Withdraw {
                    target_instance,
                    producer_signature,
                } => {
                    let metadata = RuntimeWireMetadata::data(
                        "supervisor",
                        ExecutionTime::from_nanos(logical_time_ns),
                        sequence,
                    )
                    .with_delivery_identity(
                        self.inner.execution_id.clone(),
                        timeline_id.to_owned(),
                        boundary,
                        0,
                    )
                    .with_eligible_boundary(eligible_boundary);
                    publish_scenario_sample(
                        &self.inner.bus,
                        "supervisor",
                        &producer_signature.endpoint,
                        "publish",
                        &[],
                        metadata,
                        WireControl::Withdraw,
                    )
                    .await?;
                    self.wait_scenario_delivery(ScenarioDeliveryExpectation {
                        label: &step.label,
                        kind: "withdraw",
                        timeline_id,
                        target: format!("{target_instance}.{}", producer_signature.endpoint),
                        port: &producer_signature.endpoint,
                        sequence,
                        bytes: 0,
                        production_boundary: boundary,
                        eligible_boundary,
                    })
                    .await?;
                }
                ScenarioAction::Command {
                    target_instance,
                    service_signature,
                    request_encoded,
                    label,
                    ..
                } => {
                    let metadata = RuntimeWireMetadata::external_command(
                        ExecutionTime::from_nanos(logical_time_ns),
                        sequence,
                        eligible_boundary,
                        sequence,
                    );
                    publish_scenario_sample(
                        &self.inner.bus,
                        target_instance,
                        &service_signature.endpoint,
                        "request",
                        request_encoded,
                        metadata,
                        WireControl::Data,
                    )
                    .await?;
                    let mut state = scenario.state.lock().await;
                    state.command_labels.insert(sequence, label.clone());
                    state.steps.push(ScenarioStepEvidence {
                        label: step.label.clone(),
                        kind: "command".to_owned(),
                        production_boundary: boundary,
                        eligible_boundary,
                    });
                }
            }
        }
        Ok(())
    }

    async fn wait_scenario_delivery(
        &self,
        expectation: ScenarioDeliveryExpectation<'_>,
    ) -> Result<(), String> {
        let scenario = self
            .inner
            .scenario
            .as_ref()
            .ok_or_else(|| "scenario delivery requested without a scenario".to_owned())?;
        let acknowledgement =
            tokio::time::timeout(DEFAULT_RUNTIME_TIMEOUT, scenario.delivery_ack.recv_async())
                .await
                .map_err(|_| {
                    format!(
                        "scenario step `{}` delivery acknowledgement timed out",
                        expectation.label
                    )
                })?
                .map_err(|error| error.to_string())?;
        let acknowledgement: wire::DeliveryAck =
            decode(acknowledgement).map_err(|error| error.to_string())?;
        if !scenario_delivery_ack_matches(
            &acknowledgement,
            &self.inner.execution_id,
            expectation.timeline_id,
            &expectation.target,
            expectation.port,
            expectation.sequence,
            expectation.bytes,
            expectation.production_boundary,
        ) {
            return Err(format!(
                "scenario step `{}` received a mismatched or refused delivery acknowledgement: {}",
                expectation.label,
                acknowledgement
                    .detail
                    .unwrap_or_else(|| "identity mismatch".to_owned())
            ));
        }
        scenario
            .state
            .lock()
            .await
            .steps
            .push(ScenarioStepEvidence {
                label: expectation.label.to_owned(),
                kind: expectation.kind.to_owned(),
                production_boundary: expectation.production_boundary,
                eligible_boundary: expectation.eligible_boundary,
            });
        Ok(())
    }

    async fn drain_scenario_records(&self) {
        let Some(scenario) = self.inner.scenario.as_ref() else {
            return;
        };
        let mut state = scenario.state.lock().await;
        for (name, capture) in &scenario.captures {
            state
                .captures
                .entry(name.clone())
                .or_insert_with(|| ScenarioCaptureEvidence {
                    name: name.clone(),
                    kind: capture.kind.to_owned(),
                    records: Vec::new(),
                    gap_before_first: false,
                    complete: true,
                    terminal: false,
                });
            while let Ok(Some(sample)) = capture.subscriber.try_recv() {
                let Ok(sample) = WireSample::from_zenoh(sample) else {
                    continue;
                };
                let entry =
                    state
                        .captures
                        .entry(name.clone())
                        .or_insert_with(|| ScenarioCaptureEvidence {
                            name: name.clone(),
                            kind: capture.kind.to_owned(),
                            records: Vec::new(),
                            gap_before_first: false,
                            complete: true,
                            terminal: false,
                        });
                let metadata = sample.metadata();
                let Some(source) = metadata.publisher().map(str::to_owned) else {
                    entry.complete = false;
                    entry.gap_before_first = true;
                    continue;
                };
                let Some(sequence) = metadata.sequence else {
                    entry.complete = false;
                    entry.gap_before_first = true;
                    continue;
                };
                let Ok(capture_time) = metadata.logical_time() else {
                    entry.complete = false;
                    entry.gap_before_first = true;
                    continue;
                };
                let capacity = usize::try_from(capture.policy.capacity()).unwrap_or(usize::MAX);
                match capture.policy {
                    phoxal::scenario::CapturePolicy::Latest => {
                        entry.records.clear();
                    }
                    phoxal::scenario::CapturePolicy::BestEffortHistory { .. }
                        if entry.records.len() >= capacity =>
                    {
                        entry.records.remove(0);
                        entry.gap_before_first = true;
                    }
                    phoxal::scenario::CapturePolicy::RequiredHistory { .. }
                        if entry.records.len() >= capacity =>
                    {
                        entry.complete = false;
                        continue;
                    }
                    _ => {}
                }
                entry.records.push(ScenarioObservationEvidence {
                    payload: sample.payload().to_vec(),
                    source,
                    capture_time_ns: capture_time.as_nanos(),
                    sequence,
                });
            }
        }
        for subscriber in scenario.command_replies.values() {
            while let Ok(Some(sample)) = subscriber.try_recv() {
                let Ok(sample) = WireSample::from_zenoh(sample) else {
                    continue;
                };
                let Some(command_id) = sample.metadata().command_id else {
                    continue;
                };
                let Some(label) = state.command_labels.get(&command_id).cloned() else {
                    continue;
                };
                state
                    .command_replies
                    .entry(label)
                    .or_insert_with(|| sample.payload().to_vec());
            }
        }
    }

    async fn reset_scenario_evidence(&self) {
        let Some(scenario) = self.inner.scenario.as_ref() else {
            return;
        };
        for capture in scenario.captures.values() {
            while let Ok(Some(_)) = capture.subscriber.try_recv() {}
        }
        for subscriber in scenario.command_replies.values() {
            while let Ok(Some(_)) = subscriber.try_recv() {}
        }
        *scenario.state.lock().await = ScenarioDriverState::default();
    }

    async fn admit_initial_observations_inner(
        &self,
        context: PublicSimulationContext,
        request: AdmitInitialObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> Result<AdmitInitialObservationsResponse, String> {
        let mut boundary = self.inner.boundary.lock().await;
        if let Some(fault) = &boundary.fault {
            return Err(format!("controlled execution has failed: {fault}"));
        }
        if boundary.mode != RuntimeExecutionMode::Controlled {
            return Err("hardware execution has no controlled simulation barrier".to_owned());
        }
        let key = request
            .transition_key
            .clone()
            .ok_or_else(|| "initial observation request has no transition key".to_owned())?;
        if !transition_matches(&context, &boundary, &key)
            || key.boundary != 0
            || boundary.committed_boundary != 0
            || boundary.admitted_observation_boundary != 0
            || boundary.prepared_transition.is_some()
        {
            return Err("initial observation transition identity or boundary is stale".to_owned());
        }
        validate_published_observations(
            &request.observations,
            &published_observations,
            key.boundary,
        )?;
        if let Err(error) = self.initialize_states(&key.timeline_id).await {
            return self.fail_boundary(&mut boundary, 0, error);
        }
        if let Err(error) = self
            .wait_observation_admission(&request.observations, &key.timeline_id, 0)
            .await
        {
            return self.fail_boundary(&mut boundary, 0, error);
        }
        let receipt = make_cut_receipt(
            &key,
            &request.correlation_id,
            &request,
            &published_observations,
            phoxal::communication::simulation::PhaseStatus::InitialAdmitted,
            0,
        );
        trace::admission(&key, 0, &published_observations, std::iter::empty());
        Ok(AdmitInitialObservationsResponse {
            receipt: Some(receipt),
        })
    }

    async fn prepare_boundary_inner(
        &self,
        context: PublicSimulationContext,
        request: PrepareBoundaryRequest,
    ) -> Result<PrepareBoundaryResponse, String> {
        let mut boundary = self.inner.boundary.lock().await;
        if let Some(fault) = &boundary.fault {
            return Err(format!("controlled execution has failed: {fault}"));
        }
        if boundary.mode != RuntimeExecutionMode::Controlled {
            return Err("hardware execution has no controlled simulation barrier".to_owned());
        }
        let key = request
            .transition_key
            .clone()
            .ok_or_else(|| "prepare request has no transition key".to_owned())?;
        if !transition_matches(&context, &boundary, &key)
            || key.boundary != boundary.committed_boundary
            || boundary.prepared_transition.is_some()
        {
            return Err("prepare transition identity or boundary is stale".to_owned());
        }
        let Some(target) = key.boundary.checked_add(1) else {
            return self.fail_boundary(
                &mut boundary,
                key.boundary,
                "controlled boundary is exhausted".to_owned(),
            );
        };
        let Some(logical_time_ns) = key.boundary.checked_mul(boundary.quantum_ns) else {
            return self.fail_boundary(
                &mut boundary,
                key.boundary,
                "controlled logical time is exhausted".to_owned(),
            );
        };
        if let Err(error) = self
            .pin_read_views(&boundary.timeline_id, key.boundary)
            .await
        {
            return self.fail_boundary(&mut boundary, key.boundary, error);
        }
        if let Err(error) = self
            .publish_scenario_actions(key.boundary, logical_time_ns, &boundary.timeline_id)
            .await
        {
            return self.fail_boundary(&mut boundary, key.boundary, error);
        }
        let due = self
            .inner
            .instances
            .iter()
            .filter(|runtime| {
                key.boundary == 0 || logical_time_ns.is_multiple_of(runtime.period_ns)
            })
            .collect::<Vec<_>>();
        for runtime in &due {
            let invocation = wire::Invocation {
                execution_id: self.inner.execution_id.clone(),
                timeline_id: boundary.timeline_id.clone(),
                runtime_instance: runtime.instance.clone(),
                boundary: key.boundary,
                logical_time_ns,
            };
            if let Err(error) =
                send(&self.inner.bus, &runtime.instance, "invoke", &invocation).await
            {
                return self.fail_boundary(&mut boundary, key.boundary, error.to_string());
            }
        }
        let mut product_receipt_count = 0_usize;
        for runtime in due {
            let timeout = runtime.timeout.max(Duration::from_millis(1));
            let accepted = match tokio::time::timeout(
                timeout,
                recv_acceptance(
                    &runtime.accepted,
                    &runtime.failures,
                    &self.inner.execution_id,
                    &boundary.timeline_id,
                    &runtime.instance,
                    key.boundary,
                    &runtime.product_ports,
                ),
            )
            .await
            {
                Ok(Ok(accepted)) => accepted,
                Ok(Err(error)) => {
                    return self.fail_boundary(&mut boundary, key.boundary, error);
                }
                Err(_) => {
                    return self.fail_boundary(
                        &mut boundary,
                        key.boundary,
                        format!("runtime `{}` invocation timed out", runtime.instance),
                    );
                }
            };
            product_receipt_count =
                match product_receipt_count.checked_add(accepted.required_products.len()) {
                    Some(count) => count,
                    None => {
                        return self.fail_boundary(
                            &mut boundary,
                            key.boundary,
                            "required product receipt count overflow".to_owned(),
                        );
                    }
                };
            if product_receipt_count > MAX_PRODUCT_RECEIPTS {
                return self.fail_boundary(
                    &mut boundary,
                    key.boundary,
                    "required product receipt set is too large".to_owned(),
                );
            }
            if let Err(error) = validate_products(&accepted, &runtime.product_ports) {
                return self.fail_boundary(&mut boundary, key.boundary, error);
            }
            if let Err(error) = validate_input_receipts(&accepted, &runtime.input_sources) {
                return self.fail_boundary(&mut boundary, key.boundary, error);
            }
            let accepted_actuations = match validate_actuations(&accepted, runtime, logical_time_ns)
            {
                Ok(actuations) => actuations,
                Err(error) => {
                    return self.fail_boundary(&mut boundary, key.boundary, error);
                }
            };
            let deliveries = match expected_deliveries(
                &runtime.instance,
                &accepted,
                &self.inner.delivery_routes,
                &self.inner.request_routes,
            ) {
                Ok(deliveries) => deliveries,
                Err(error) => {
                    return self.fail_boundary(&mut boundary, key.boundary, error);
                }
            };
            tracing::debug!(
                target: "phoxal::boundary",
                runtime = %runtime.instance,
                boundary = key.boundary,
                expected_deliveries = ?deliveries,
                "waiting for runtime delivery admission"
            );
            if let Err(error) = wait_delivery_acknowledgements(DeliveryAckWait {
                acknowledgements: &runtime.delivery_ack,
                failures: &runtime.failures,
                execution_id: &self.inner.execution_id,
                timeline_id: &boundary.timeline_id,
                instance: &runtime.instance,
                boundary: key.boundary,
                expected: &deliveries,
                timeout,
            })
            .await
            {
                return self.fail_boundary(&mut boundary, key.boundary, error);
            }
            trace::invocation(&accepted);
            for actuation in accepted_actuations {
                let route = (runtime.instance.clone(), actuation.port.clone());
                boundary.actuations.insert(
                    route,
                    make_actuation(
                        runtime,
                        &self.inner.execution_id,
                        actuation,
                        key.boundary,
                        logical_time_ns,
                        target,
                    ),
                );
            }
        }
        self.drain_scenario_records().await;
        let end_time_ns = target
            .checked_mul(boundary.quantum_ns)
            .ok_or("actuator validity boundary overflow")?;
        if let Some(((producer, port), _)) = boundary
            .actuations
            .iter()
            .find(|(_, command)| command.valid_until_ns < end_time_ns)
        {
            let error = format!("actuator {producer}/{port} expires before boundary {target}");
            return self.fail_boundary(&mut boundary, key.boundary, error);
        }
        let actuations = boundary.actuations.values().cloned().collect::<Vec<_>>();
        boundary.prepared_transition = Some(key.clone());
        let memberships = actuations
            .iter()
            .filter_map(|actuation| actuation.membership.clone())
            .collect::<Vec<_>>();
        let receipt = make_cut_receipt(
            &key,
            &request.correlation_id,
            &request,
            &memberships,
            phoxal::communication::simulation::PhaseStatus::Prepared,
            boundary.admitted_observation_boundary,
        );
        Ok(PrepareBoundaryResponse {
            receipt: Some(receipt),
            actuation: actuations,
        })
    }

    async fn admit_observations_inner(
        &self,
        context: PublicSimulationContext,
        request: AdmitObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> Result<AdmitObservationsResponse, String> {
        let mut boundary = self.inner.boundary.lock().await;
        if let Some(fault) = &boundary.fault {
            return Err(format!("controlled execution has failed: {fault}"));
        }
        if boundary.mode != RuntimeExecutionMode::Controlled {
            return Err("hardware execution has no controlled simulation barrier".to_owned());
        }
        let key = request
            .transition_key
            .clone()
            .ok_or_else(|| "observation admission has no transition key".to_owned())?;
        if !transition_matches(&context, &boundary, &key)
            || key.boundary != boundary.committed_boundary
            || boundary.prepared_transition.as_ref() != Some(&key)
        {
            return Err("observation transition identity or boundary is stale".to_owned());
        }
        let target = key
            .boundary
            .checked_add(1)
            .ok_or_else(|| "controlled boundary is exhausted".to_owned())?;
        validate_published_observations(&request.observations, &published_observations, target)?;
        if let Err(error) = self
            .wait_observation_admission(&request.observations, &key.timeline_id, target)
            .await
        {
            return self.fail_boundary(&mut boundary, key.boundary, error);
        }
        if let Err(error) = self.inner.state.complete_runtime_boundary(target) {
            return self.fail_boundary(&mut boundary, key.boundary, error);
        }
        boundary.committed_boundary = target;
        boundary.admitted_observation_boundary = target;
        boundary.prepared_transition = None;
        trace::admission(
            &key,
            target,
            &published_observations,
            boundary.actuations.values(),
        );
        let receipt = make_cut_receipt(
            &key,
            &request.correlation_id,
            &request,
            &published_observations,
            phoxal::communication::simulation::PhaseStatus::ObservationsAdmitted,
            target,
        );
        Ok(AdmitObservationsResponse {
            receipt: Some(receipt),
        })
    }

    fn fail_boundary<T>(
        &self,
        boundary: &mut BoundaryState,
        at: u64,
        reason: String,
    ) -> Result<T, String> {
        boundary.fault = Some(format!("boundary {at}: {reason}"));
        self.inner.failed.cancel();
        Err(boundary.fault.clone().unwrap_or(reason))
    }

    fn fail_reset(&self, boundary: &mut BoundaryState, reason: String) -> Result<(), String> {
        boundary.fault = Some(format!("reset failed: {reason}"));
        self.inner.failed.cancel();
        Err(boundary.fault.clone().unwrap_or(reason))
    }

    async fn reset_inner(
        &self,
        context: PublicSimulationContext,
        request: ResetRequest,
        next_timeline_id: String,
    ) -> Result<(), String> {
        let mut boundary = self.inner.boundary.lock().await;
        if boundary.mode != RuntimeExecutionMode::Controlled {
            return Err("hardware execution refuses controlled reset".to_owned());
        }
        if boundary.fault.is_some()
            || context.execution_id != self.inner.execution_id
            || request.execution_id != self.inner.execution_id
            || request.timeline_id != boundary.timeline_id
            || request.completed_boundary != boundary.committed_boundary
            || context.completed_boundary != request.completed_boundary
            || next_timeline_id.is_empty()
            || next_timeline_id == boundary.timeline_id
        {
            return Err("reset identity, timeline, or boundary is stale".to_owned());
        }
        let timeline = parse_timeline_id(&next_timeline_id)
            .map_err(|error| format!("reset timeline is invalid: {error}"))?;
        for runtime in &self.inner.instances {
            let reset = wire::ResetExecutionRequest {
                execution_id: self.inner.execution_id.clone(),
                retired_timeline_id: boundary.timeline_id.clone(),
                next_timeline_id: next_timeline_id.clone(),
                completed_boundary: boundary.committed_boundary,
            };
            if let Err(error) = send(&self.inner.bus, &runtime.instance, "reset", &reset).await {
                return self.fail_reset(&mut boundary, error.to_string());
            }
            let response = match tokio::time::timeout(
                runtime.timeout.max(Duration::from_millis(1)),
                recv_reset(&runtime.reset_response),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => return self.fail_reset(&mut boundary, error.to_string()),
                Err(_) => {
                    return self.fail_reset(
                        &mut boundary,
                        format!("runtime `{}` reset timed out", runtime.instance),
                    );
                }
            };
            if !response.accepted {
                return self.fail_reset(
                    &mut boundary,
                    format!(
                        "runtime `{}` refused reset: {}",
                        runtime.instance,
                        response
                            .detail
                            .unwrap_or_else(|| "unspecified refusal".to_owned())
                    ),
                );
            }
        }
        if let Err(error) = self
            .inner
            .state
            .replace_time_domain_with(TimeMode::Simulated, timeline)
        {
            return self.fail_reset(&mut boundary, error.to_string());
        }
        self.inner.state.reset_runtime_boundary();
        self.reset_scenario_evidence().await;
        boundary.actuations.clear();
        boundary.timeline_id = next_timeline_id;
        boundary.committed_boundary = 0;
        boundary.admitted_observation_boundary = 0;
        boundary.prepared_transition = None;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn scenario_delivery_ack_matches(
    acknowledgement: &wire::DeliveryAck,
    execution_id: &str,
    timeline_id: &str,
    target: &str,
    port: &str,
    sequence: u64,
    bytes: u64,
    production_boundary: u64,
) -> bool {
    acknowledgement.execution_id == execution_id
        && acknowledgement.timeline_id == timeline_id
        && acknowledgement.boundary == production_boundary
        && acknowledgement.source == "supervisor"
        && acknowledgement.target == target
        && acknowledgement.port == port
        && acknowledgement.direction == "publish"
        && acknowledgement.sequence == sequence
        && acknowledgement.item == 0
        && acknowledgement.bytes == bytes
        && acknowledgement.admitted
}

fn input_sources(
    source: &RuntimeBundle,
    consumer_instance: &str,
    inputs: &[phoxal::artifact::InputRecord],
) -> Result<BTreeMap<String, BTreeSet<(String, String)>>> {
    let mut providers = BTreeMap::<String, BTreeSet<(String, String)>>::new();
    for (consumer, sources) in source.connections() {
        if consumer.instance != consumer_instance
            || !inputs.iter().any(|input| input.name == consumer.endpoint)
        {
            continue;
        }
        for source in sources {
            providers
                .entry(consumer.endpoint.clone())
                .or_default()
                .insert((source.instance.clone(), source.endpoint.clone()));
        }
    }
    for input in inputs
        .iter()
        .filter(|input| input.delivery == phoxal::artifact::InputDelivery::CallIngress)
    {
        let port = input.port.as_deref().unwrap_or(&input.name);
        let callers = providers.entry(input.name.clone()).or_default();
        callers.insert(("supervisor".into(), port.to_owned()));
        for (consumer, sources) in source.connections() {
            if sources
                .iter()
                .any(|source| source.instance == consumer_instance && source.endpoint == port)
            {
                callers.insert((consumer.instance.clone(), port.to_owned()));
            }
        }
    }
    Ok(providers)
}

/// The launch-facing fields of one runtime record.
pub(super) fn record_parts(
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

type DeliveryRouteKey = (String, String, String);
type DeliveryRoutes = BTreeMap<DeliveryRouteKey, BTreeSet<String>>;
type RequestRoutes = BTreeMap<DeliveryRouteKey, String>;

fn graph_delivery_routes(
    connections: &BTreeMap<
        phoxal::artifact::bundle::EndpointReference,
        Vec<phoxal::artifact::bundle::EndpointReference>,
    >,
    artifacts: &BTreeMap<String, phoxal::artifact::RuntimeRecord>,
) -> Result<(DeliveryRoutes, RequestRoutes)> {
    let mut routes = DeliveryRoutes::new();
    let mut request_routes = RequestRoutes::new();
    for (consumer, sources) in connections {
        let consumer_instance = &consumer.instance;
        let consumer_port = consumer.endpoint.as_str();
        let consumer_artifact = artifacts
            .get(consumer_instance)
            .with_context(|| format!("connection consumer `{consumer}` has no runtime artifact"))?;
        let phoxal::artifact::RuntimeRecord::V0 { inputs, .. } = consumer_artifact;
        let input = inputs.iter().find(|input| {
            input.name == consumer_port || input.port.as_deref() == Some(consumer_port)
        });
        let input = input
            .with_context(|| format!("connection consumer `{consumer}` has no declared input"))?;
        let role = input.delivery;
        let receiving_field = format!("{consumer_instance}.{}", input.name);
        for source in sources {
            let source_instance = source.instance.as_str();
            let source_port = source.endpoint.as_str();
            match role {
                phoxal::artifact::InputDelivery::CallCompletions => {
                    // A required operation declares its contract on the
                    // completion field; requests route to the bound provider
                    // and replies return through the same receiving field.
                    let target = artifacts.get(source_instance).with_context(|| {
                        format!("request target `{source}` has no runtime artifact")
                    })?;
                    let phoxal::artifact::RuntimeRecord::V0 { inputs, .. } = target;
                    let fields = inputs
                        .iter()
                        .filter(|field| {
                            field.delivery == phoxal::artifact::InputDelivery::CallIngress
                                && field.port.as_deref() == Some(source_port)
                        })
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>();
                    let [field] = fields.as_slice() else {
                        bail!(
                            "request target `{source}` must resolve to exactly one receiving field"
                        );
                    };
                    request_routes.insert(
                        (
                            consumer_instance.to_owned(),
                            source_instance.to_owned(),
                            source_port.to_owned(),
                        ),
                        format!("{source_instance}.{field}"),
                    );
                    let reply_route = routes
                        .entry((
                            source_instance.to_owned(),
                            source_port.to_owned(),
                            "reply".to_owned(),
                        ))
                        .or_default();
                    reply_route.insert(receiving_field.clone());
                    // The runtime's generated call path issues commands
                    // under its stable virtual field `generated_call` and
                    // delivers the completion into the same connected
                    // receiving field, so that identity is a valid reply
                    // target alias for the connected caller.
                    reply_route.insert(format!("{consumer_instance}.generated_call"));
                }
                phoxal::artifact::InputDelivery::CallResult
                | phoxal::artifact::InputDelivery::CallTarget => {
                    let target = artifacts.get(source_instance).with_context(|| {
                        format!("request target `{source}` has no runtime artifact")
                    })?;
                    let phoxal::artifact::RuntimeRecord::V0 {
                        inputs, outputs, ..
                    } = target;
                    let fields = if role == phoxal::artifact::InputDelivery::CallResult {
                        outputs
                            .iter()
                            .filter(|field| {
                                is_call_method(field) && field.port.as_deref() == Some(source_port)
                            })
                            .map(|field| field.name.as_str())
                            .collect::<Vec<_>>()
                    } else {
                        inputs
                            .iter()
                            .filter(|field| {
                                field.delivery == phoxal::artifact::InputDelivery::CallIngress
                                    && field.port.as_deref() == Some(source_port)
                            })
                            .map(|field| field.name.as_str())
                            .collect::<Vec<_>>()
                    };
                    let [field] = fields.as_slice() else {
                        bail!(
                            "request target `{source}` must resolve to exactly one receiving field"
                        );
                    };
                    request_routes.insert(
                        (
                            consumer_instance.to_owned(),
                            source_instance.to_owned(),
                            source_port.to_owned(),
                        ),
                        format!("{source_instance}.{field}"),
                    );
                    let reply_route = routes
                        .entry((
                            source_instance.to_owned(),
                            source_port.to_owned(),
                            "reply".to_owned(),
                        ))
                        .or_default();
                    reply_route.insert(receiving_field.clone());
                    // The runtime's generated call path issues commands
                    // under its stable virtual field `generated_call` and
                    // delivers the completion into the same connected
                    // receiving field, so that identity is a valid reply
                    // target alias for the connected caller.
                    reply_route.insert(format!("{consumer_instance}.generated_call"));
                }
                phoxal::artifact::InputDelivery::ObservationLatest
                | phoxal::artifact::InputDelivery::ObservationHistory
                | phoxal::artifact::InputDelivery::LeasedValue => {
                    routes
                        .entry((
                            source_instance.to_owned(),
                            source_port.to_owned(),
                            "publish".to_owned(),
                        ))
                        .or_default()
                        .insert(receiving_field.clone());
                }
                _ => bail!("connection consumer `{consumer}` does not accept graph traffic"),
            }
        }
    }
    Ok((routes, request_routes))
}

impl RuntimeBoundaryHook for RuntimeExecutionProtocol {
    fn validate_observation_admission(
        &self,
        observations: &[phoxal::communication::simulation::Observation],
    ) -> Result<(), String> {
        self.observation_deliveries(observations).map(|_| ())
    }
    fn acquire(
        &self,
        context: PublicSimulationContext,
        _request: AcquireAuthorityRequest,
    ) -> BoundaryFuture<()> {
        let protocol = self.clone();
        Box::pin(async move {
            let boundary = protocol.inner.boundary.lock().await;
            if boundary.mode != RuntimeExecutionMode::Controlled
                || context.execution_id != protocol.inner.execution_id
                || context.timeline_id != boundary.timeline_id
            {
                return Err(
                    "simulation authority requires a controlled admitted execution".to_owned(),
                );
            }
            if let Some(fault) = &boundary.fault {
                return Err(format!("controlled execution has failed: {fault}"));
            }
            Ok(())
        })
    }

    fn admit_initial_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitInitialObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> BoundaryFuture<AdmitInitialObservationsResponse> {
        let protocol = self.clone();
        Box::pin(async move {
            protocol
                .admit_initial_observations_inner(context, request, published_observations)
                .await
        })
    }

    fn prepare_boundary(
        &self,
        context: PublicSimulationContext,
        request: PrepareBoundaryRequest,
    ) -> BoundaryFuture<PrepareBoundaryResponse> {
        let protocol = self.clone();
        Box::pin(async move { protocol.prepare_boundary_inner(context, request).await })
    }

    fn admit_observations(
        &self,
        context: PublicSimulationContext,
        request: AdmitObservationsRequest,
        published_observations: Vec<phoxal::communication::simulation::ProductMembership>,
    ) -> BoundaryFuture<AdmitObservationsResponse> {
        let protocol = self.clone();
        Box::pin(async move {
            protocol
                .admit_observations_inner(context, request, published_observations)
                .await
        })
    }

    fn reset(
        &self,
        context: PublicSimulationContext,
        request: ResetRequest,
        next_timeline_id: String,
    ) -> BoundaryFuture<()> {
        let protocol = self.clone();
        Box::pin(async move {
            protocol
                .reset_inner(context, request, next_timeline_id)
                .await
        })
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
        context: PublicSimulationContext,
        _request: ProgressRequest,
    ) -> BoundaryFuture<ProgressResponse> {
        let protocol = self.clone();
        Box::pin(async move {
            let Ok(boundary) = protocol.inner.boundary.try_lock() else {
                return Ok(ProgressResponse {
                    execution_id: protocol.inner.execution_id.clone(),
                    timeline_id: context.timeline_id,
                    completed_boundary: context.completed_boundary,
                    failed: protocol.inner.failed.is_cancelled(),
                    session_id: context.session_id,
                    authority_grant: context.authority_grant,
                    correlation_id: context.correlation_id,
                    ..Default::default()
                });
            };
            if context.execution_id != protocol.inner.execution_id
                || context.timeline_id != boundary.timeline_id
            {
                return Err("progress identity is stale".to_owned());
            }
            Ok(ProgressResponse {
                execution_id: protocol.inner.execution_id.clone(),
                timeline_id: boundary.timeline_id.clone(),
                completed_boundary: boundary.committed_boundary,
                failed: boundary.fault.is_some(),
                detail: boundary.fault.clone(),
                session_id: context.session_id,
                authority_grant: context.authority_grant,
                correlation_id: context.correlation_id,
            })
        })
    }
}

async fn declare(bus: &Connection, instance: &str, leg: &str) -> Result<Subscriber> {
    let expression = OwnedKeyExpr::new(execution_protocol::key(bus, instance, leg))
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let session = bus.session()?;
    session
        .declare_subscriber(expression)
        .with(zenoh::handlers::FifoChannel::new(CONTROL_CHANNEL_CAPACITY))
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

async fn send<M: Message>(bus: &Connection, instance: &str, leg: &str, message: &M) -> Result<()> {
    let session = bus.session()?;
    session
        .put(
            execution_protocol::key(bus, instance, leg),
            execution_protocol::encode(message)?,
        )
        .encoding(Encoding::from(
            execution_protocol::PROTOBUF_ENCODING.to_owned(),
        ))
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(())
}

async fn publish_scenario_sample(
    bus: &Connection,
    instance: &str,
    port: &str,
    direction: &str,
    payload: &[u8],
    mut metadata: RuntimeWireMetadata,
    control: WireControl,
) -> Result<(), String> {
    metadata.control = control as u32;
    let attachment = metadata
        .encode_bounded()
        .map_err(|error| error.to_string())?;
    bus.session()
        .map_err(|error| error.to_string())?
        .put(
            bus.full_key(&transport::port_key(instance, port, direction)),
            payload.to_vec(),
        )
        .encoding(Encoding::from(transport::PROTOBUF_ENCODING.to_owned()))
        .attachment(attachment)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn recv_admission(subscriber: &Subscriber) -> Result<wire::AdmitExecutionResponse> {
    decode(
        subscriber
            .recv_async()
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
    )
}

async fn recv_ready(subscriber: &Subscriber) -> Result<wire::Ready> {
    decode(
        subscriber
            .recv_async()
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
    )
}

async fn recv_reset(subscriber: &Subscriber) -> Result<wire::ResetExecutionResponse> {
    decode(
        subscriber
            .recv_async()
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
    )
}

fn expected_deliveries(
    instance: &str,
    accepted: &wire::InvocationAccepted,
    delivery_routes: &BTreeMap<(String, String, String), BTreeSet<String>>,
    request_routes: &RequestRoutes,
) -> Result<Vec<ExpectedDelivery>, String> {
    if accepted.required_deliveries.len() > MAX_PRODUCT_RECEIPTS {
        return Err("runtime delivery receipt set is too large".to_owned());
    }
    let mut expected = BTreeSet::new();
    for receipt in &accepted.required_deliveries {
        if receipt.port.is_empty()
            || receipt.direction.is_empty()
            || receipt.direction == "completion"
        {
            return Err("runtime delivery receipt has an invalid port or direction".to_owned());
        }
        let targets = match receipt.direction.as_str() {
            "request" => {
                let target = (!receipt.target.is_empty())
                    .then_some(receipt.target.as_str())
                    .ok_or_else(|| {
                        "runtime request delivery receipt is missing its target".to_owned()
                    })?;
                let receiver = request_routes
                    .get(&(instance.to_owned(), target.to_owned(), receipt.port.clone()))
                    .ok_or_else(|| {
                        "runtime request delivery receipt is not in the admitted graph".to_owned()
                    })?;
                vec![receiver.clone()]
            }
            "publish" | "reply" => {
                let key = (
                    instance.to_owned(),
                    receipt.port.clone(),
                    receipt.direction.clone(),
                );
                let Some(graph_targets) = delivery_routes.get(&key) else {
                    // An output with no graph receiver is not a required
                    // delivery obligation for this execution.
                    continue;
                };
                if receipt.direction == "reply" && receipt.target.is_empty() {
                    return Err(
                        "runtime reply delivery receipt is missing its caller target".to_owned(),
                    );
                }
                if receipt.target.is_empty() {
                    graph_targets.iter().cloned().collect()
                } else {
                    if !graph_targets.contains(&receipt.target) {
                        return Err(
                            "runtime delivery receipt names a target outside the admitted graph"
                                .to_owned(),
                        );
                    }
                    vec![receipt.target.clone()]
                }
            }
            _ => return Err("runtime delivery receipt has an unknown direction".to_owned()),
        };
        for target in targets {
            let delivery = ExpectedDelivery {
                source: instance.to_owned(),
                target,
                port: receipt.port.clone(),
                direction: receipt.direction.clone(),
                sequence: receipt.sequence,
                item: receipt.item,
                bytes: receipt.bytes,
            };
            if !expected.insert(delivery) {
                return Err("runtime returned duplicate delivery receipt identity".to_owned());
            }
        }
    }
    Ok(expected.into_iter().collect())
}

struct DeliveryAckWait<'a> {
    acknowledgements: &'a Subscriber,
    failures: &'a Subscriber,
    execution_id: &'a str,
    timeline_id: &'a str,
    instance: &'a str,
    boundary: u64,
    expected: &'a [ExpectedDelivery],
    timeout: Duration,
}

async fn wait_delivery_acknowledgements(wait: DeliveryAckWait<'_>) -> Result<(), String> {
    let DeliveryAckWait {
        acknowledgements,
        failures,
        execution_id,
        timeline_id,
        instance,
        boundary,
        expected,
        timeout,
    } = wait;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut pending = expected.iter().cloned().collect::<BTreeSet<_>>();
    while !pending.is_empty() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "runtime `{instance}` delivery acknowledgement timed out at boundary {boundary}"
            ));
        }
        let delivery = tokio::time::timeout(
            remaining,
            recv_delivery_ack(
                acknowledgements,
                failures,
                execution_id,
                timeline_id,
                instance,
                boundary,
                &pending,
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "runtime `{instance}` delivery acknowledgement timed out at boundary {boundary}"
            )
        })??;
        pending.remove(&delivery);
    }
    Ok(())
}

async fn recv_delivery_ack(
    acknowledgements: &Subscriber,
    failures: &Subscriber,
    execution_id: &str,
    timeline_id: &str,
    instance: &str,
    boundary: u64,
    pending: &BTreeSet<ExpectedDelivery>,
) -> Result<ExpectedDelivery, String> {
    loop {
        tokio::select! {
            result = acknowledgements.recv_async() => {
                let acknowledgement: wire::DeliveryAck = decode(
                    result.map_err(|error| error.to_string())?
                ).map_err(|error: anyhow::Error| error.to_string())?;
                let Some(candidate) = delivery_ack_candidate(
                    &acknowledgement,
                    execution_id,
                    timeline_id,
                    instance,
                    boundary,
                    pending,
                ) else {
                    tracing::debug!(
                        target: "phoxal::boundary",
                        acknowledgement = ?acknowledgement,
                        "ignoring unmatched runtime delivery acknowledgement"
                    );
                    // This is a duplicate acknowledgement for a delivery
                    // already removed from the pending set, or a stale
                    // identity from another accepted cut.
                    continue;
                };
                if acknowledgement.admitted {
                    return Ok(candidate);
                }
                return Err(acknowledgement.detail.unwrap_or_else(|| {
                    format!(
                        "receiver `{}` refused delivery admission for `{}`",
                        candidate.target, candidate.port
                    )
                }));
            }
            result = failures.recv_async() => {
                let failure: wire::RuntimeFailure = decode(
                    result.map_err(|error| error.to_string())?
                ).map_err(|error: anyhow::Error| error.to_string())?;
                if failure.execution_id == execution_id
                    && failure.timeline_id == timeline_id
                    && failure.runtime_instance == instance
                    && failure.boundary == boundary
                {
                    return Err(failure.reason);
                }
            }
        }
    }
}

fn delivery_ack_candidate(
    acknowledgement: &wire::DeliveryAck,
    execution_id: &str,
    timeline_id: &str,
    instance: &str,
    boundary: u64,
    pending: &BTreeSet<ExpectedDelivery>,
) -> Option<ExpectedDelivery> {
    if acknowledgement.execution_id != execution_id
        || acknowledgement.timeline_id != timeline_id
        || acknowledgement.boundary != boundary
        || acknowledgement.source != instance
    {
        return None;
    }
    let candidate = ExpectedDelivery {
        source: acknowledgement.source.clone(),
        target: acknowledgement.target.clone(),
        port: acknowledgement.port.clone(),
        direction: acknowledgement.direction.clone(),
        sequence: acknowledgement.sequence,
        item: acknowledgement.item,
        bytes: acknowledgement.bytes,
    };
    pending.get(&candidate).cloned()
}

async fn recv_acceptance(
    accepted: &Subscriber,
    failures: &Subscriber,
    execution_id: &str,
    timeline_id: &str,
    instance: &str,
    boundary: u64,
    product_ports: &BTreeSet<String>,
) -> Result<wire::InvocationAccepted, String> {
    loop {
        tokio::select! {
            result = accepted.recv_async() => {
                let message: wire::InvocationAccepted = decode(result.map_err(|error| error.to_string())?).map_err(|error: anyhow::Error| error.to_string())?;
                if message.execution_id != execution_id || message.timeline_id != timeline_id || message.runtime_instance != instance || message.boundary != boundary {
                    // A replayed or delayed acceptance can remain in the
                    // bounded control queue after a retry.  It is evidence
                    // for another invocation, never permission to complete
                    // this one, so discard it without reinvoking the runtime.
                    continue;
                }
                validate_products(&message, product_ports)?;
                return Ok(message);
            }
            result = failures.recv_async() => {
                let failure: wire::RuntimeFailure = decode(result.map_err(|error| error.to_string())?).map_err(|error: anyhow::Error| error.to_string())?;
                if failure.execution_id == execution_id && failure.timeline_id == timeline_id && failure.runtime_instance == instance && failure.boundary == boundary {
                    return Err(failure.reason);
                }
            }
        }
    }
}

fn validate_products(
    accepted: &wire::InvocationAccepted,
    product_ports: &BTreeSet<String>,
) -> Result<(), String> {
    if accepted.required_products.len() > MAX_PRODUCT_RECEIPTS {
        return Err("runtime product receipt set is too large".to_owned());
    }
    let mut seen = BTreeSet::new();
    for receipt in &accepted.required_products {
        if receipt.port.is_empty()
            || (receipt.items == 0 && receipt.bytes != 0)
            || !product_ports.contains(&receipt.port)
            || !seen.insert(receipt.port.as_str())
        {
            return Err(
                "runtime product receipt is malformed or not in the admitted graph".to_owned(),
            );
        }
    }
    if seen.len() != product_ports.len() {
        return Err("runtime product receipts omit an admitted output".to_owned());
    }
    Ok(())
}

fn transition_matches(
    context: &PublicSimulationContext,
    boundary: &BoundaryState,
    transition: &TransitionKey,
) -> bool {
    transition.session_id == context.session_id
        && transition.execution_id == context.execution_id
        && transition.timeline_id == context.timeline_id
        && transition.timeline_id == boundary.timeline_id
        && transition.authority_grant == context.authority_grant
        && context.completed_boundary == boundary.committed_boundary
}

fn membership_digest(
    memberships: &[phoxal::communication::simulation::ProductMembership],
) -> Vec<u8> {
    let mut encoded = memberships
        .iter()
        .map(Message::encode_to_vec)
        .collect::<Vec<_>>();
    encoded.sort();
    let mut hasher = Sha256::new();
    for member in encoded {
        hasher.update((member.len() as u64).to_be_bytes());
        hasher.update(member);
    }
    hasher.finalize().to_vec()
}

fn make_cut_receipt<Request: Message>(
    transition: &TransitionKey,
    correlation_id: &[u8],
    _request: &Request,
    memberships: &[phoxal::communication::simulation::ProductMembership],
    status: phoxal::communication::simulation::PhaseStatus,
    admitted_observation_boundary: u64,
) -> CutReceipt {
    CutReceipt {
        transition_key: Some(transition.clone()),
        correlation_id: correlation_id.to_vec(),
        membership_digest: membership_digest(memberships),
        products: memberships.to_vec(),
        prepared_boundary: transition.boundary,
        admitted_observation_boundary,
        status,
    }
}

fn make_actuation(
    runtime: &RuntimeInstance,
    execution_id: &str,
    actuation: wire::Actuation,
    boundary: u64,
    logical_time_ns: u64,
    sequence: u64,
) -> phoxal::communication::simulation::Actuation {
    let payload_digest = Sha256::digest(&actuation.payload).to_vec();
    let membership = phoxal::communication::simulation::ProductMembership {
        producer: runtime.instance.clone(),
        port: actuation.port,
        producer_incarnation: producer_incarnation(execution_id, &runtime.instance),
        sequence,
        capture_boundary: boundary,
        capture_time_ns: logical_time_ns,
        disposition: phoxal::communication::simulation::ProductDisposition::Present,
        item_count: 1,
        encoded_bytes: actuation.payload.len() as u64,
        payload_digest,
    };
    phoxal::communication::simulation::Actuation {
        membership: Some(membership),
        payload: actuation.payload,
        valid_until_ns: actuation.valid_until_ns,
    }
}

/// The per-execution producer incarnation token.
///
/// This token distinguishes products of one execution from the next across
/// the simulation protocol: it derives from the execution identity and the
/// producing runtime, so two executions of the same runtime produce
/// different tokens and a stale authority's products are distinguishable.
/// It is never derived from executable bytes.
fn producer_incarnation(execution_id: &str, producer: &str) -> Vec<u8> {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(execution_id.as_bytes());
    hasher.update(producer.as_bytes());
    hasher.finalize().to_vec()
}

fn validate_published_observations(
    observations: &[phoxal::communication::simulation::Observation],
    published: &[phoxal::communication::simulation::ProductMembership],
    expected_boundary: u64,
) -> Result<(), String> {
    if published.len() > MAX_PRODUCT_RECEIPTS || published.len() != observations.len() {
        return Err("published observation memberships are not complete".to_owned());
    }
    let expected = observations
        .iter()
        .map(|observation| {
            observation
                .membership
                .as_ref()
                .ok_or_else(|| "observation has no membership".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_set = expected
        .iter()
        .map(|membership| membership.encode_to_vec())
        .collect::<BTreeSet<_>>();
    let published_set = published
        .iter()
        .map(Message::encode_to_vec)
        .collect::<BTreeSet<_>>();
    if expected_set != published_set {
        return Err("published observation memberships do not match the requested cut".to_owned());
    }
    let mut seen = BTreeSet::new();
    for (observation, membership) in observations.iter().zip(expected) {
        if membership.producer.is_empty()
            || membership.port.is_empty()
            || membership.producer_incarnation.is_empty()
            || membership.sequence == 0
            || membership.capture_boundary != expected_boundary
            || membership.encoded_bytes != observation.payload.len() as u64
            || membership.payload_digest != Sha256::digest(&observation.payload).as_slice()
            || !seen.insert((membership.producer.as_str(), membership.port.as_str()))
        {
            return Err("published observation membership has invalid identity".to_owned());
        }
    }
    Ok(())
}

fn validate_input_receipts(
    accepted: &wire::InvocationAccepted,
    input_sources: &BTreeMap<String, BTreeSet<(String, String)>>,
) -> Result<(), String> {
    if accepted.required_inputs.len() > MAX_PRODUCT_RECEIPTS {
        return Err("runtime input receipt set is too large".to_owned());
    }
    let mut seen = BTreeSet::new();
    for receipt in &accepted.required_inputs {
        if receipt.source.is_empty()
            || receipt.port.is_empty()
            || receipt.input.is_empty()
            || (receipt.items == 0 && receipt.bytes != 0)
            || !input_sources.get(&receipt.input).is_some_and(|sources| {
                sources.contains(&(receipt.source.clone(), receipt.port.clone()))
            })
            || !seen.insert((
                receipt.input.as_str(),
                receipt.source.as_str(),
                receipt.port.as_str(),
            ))
        {
            return Err(format!(
                "runtime {} input receipt {} <- {}.{} is malformed, duplicated, or not in the admitted graph",
                accepted.runtime_instance, receipt.input, receipt.source, receipt.port
            ));
        }
    }
    Ok(())
}

fn validate_actuations(
    accepted: &wire::InvocationAccepted,
    runtime: &RuntimeInstance,
    logical_time_ns: u64,
) -> Result<Vec<wire::Actuation>, String> {
    if accepted.actuations.len() > MAX_PRODUCT_RECEIPTS {
        return Err("runtime actuation set is too large".to_owned());
    }
    let mut seen = BTreeSet::new();
    for actuation in &accepted.actuations {
        let Some(max_bytes) = runtime.actuation_max_bytes.get(&actuation.port) else {
            return Err("runtime returned an actuation for a foreign port".to_owned());
        };
        if actuation.payload.is_empty()
            || actuation.payload.len() as u64 > *max_bytes
            || actuation.valid_until_ns <= logical_time_ns
            || !seen.insert(actuation.port.as_str())
        {
            return Err("runtime returned an invalid or duplicate actuation".to_owned());
        }
    }
    if !runtime
        .actuation_ports
        .iter()
        .all(|port| seen.contains(port.as_str()))
    {
        return Err("runtime omitted a required actuation".to_owned());
    }
    Ok(accepted
        .actuations
        .iter()
        .filter(|actuation| runtime.actuation_ports.contains(&actuation.port))
        .cloned()
        .collect())
}

fn decode<M: Message + Default>(sample: zenoh::sample::Sample) -> Result<M> {
    if sample.encoding().to_string() != execution_protocol::PROTOBUF_ENCODING {
        bail!("private execution response used an unexpected encoding");
    }
    execution_protocol::decode(sample.payload().to_bytes().as_ref())
}

fn parse_timeline_id(value: &str) -> Result<phoxal::identity::TimelineId, String> {
    let digits = value
        .strip_prefix('t')
        .ok_or_else(|| "timeline must use the canonical t-prefixed form".to_owned())?;
    if digits.len() != 16 || !digits.is_ascii() {
        return Err("timeline must contain exactly 16 hexadecimal digits".to_owned());
    }
    let raw = u64::from_str_radix(digits, 16)
        .map_err(|_| "timeline contains a non-hexadecimal digit".to_owned())?;
    phoxal::identity::TimelineId::from_raw(raw)
        .ok_or_else(|| "timeline identity cannot be zero".to_owned())
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ArtifactSummary {
    runtime: phoxal::artifact::RuntimeRecord,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ArtifactInput {
    name: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    max_items: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    port: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ArtifactOutput {
    name: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    port: Option<String>,
    #[serde(default)]
    signature: Option<ArtifactSignature>,
    #[serde(default)]
    input: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    max_items: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    max_request_bytes: Option<u64>,
    #[serde(default)]
    every_steps: Option<u64>,
    #[serde(default)]
    bootstrap: bool,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ArtifactSignature {
    shape: phoxal::artifact::MethodShape,
    #[serde(default)]
    retained_latest: bool,
    #[serde(default)]
    lease_valid_for_ms: Option<u64>,
}

fn is_observation_method(output: &phoxal::artifact::OutputRecord) -> bool {
    output
        .signature
        .as_ref()
        .is_some_and(|signature| signature.shape == phoxal::artifact::MethodShape::Observation)
}

fn is_call_method(output: &phoxal::artifact::OutputRecord) -> bool {
    output
        .signature
        .as_ref()
        .is_some_and(|signature| signature.shape == phoxal::artifact::MethodShape::Call)
}

fn is_leased_method(output: &phoxal::artifact::OutputRecord) -> bool {
    output
        .signature
        .as_ref()
        .is_some_and(|signature| signature.lease_valid_for_ms.is_some())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::runtime::bundle::{RuntimeBundle, TestExecutable};
    use phoxal::communication::execution::RuntimeWireMetadata;
    use phoxal::communication::simulation::{
        AdmitInitialObservationsRequest, AdmitObservationsRequest, PrepareBoundaryRequest,
        ResetRequest, TransitionKey,
    };
    use phoxal::identity::{ExecutionId, ParticipantId, TimelineId};
    use phoxal::runtime::ExecutionTime;
    use phoxal::runtime::connection::{Connection, ConnectionConfig, ConnectionOwner};
    use phoxal::runtime::transport::{PROTOBUF_ENCODING, port_key};

    async fn send_test_acceptance(bus: &Connection, invocation: &wire::Invocation, output: bool) {
        super::send(
            bus,
            &invocation.runtime_instance,
            "accepted",
            &wire::InvocationAccepted {
                execution_id: invocation.execution_id.clone(),
                timeline_id: invocation.timeline_id.clone(),
                runtime_instance: invocation.runtime_instance.clone(),
                boundary: invocation.boundary,
                required_products: if invocation.runtime_instance == "producer" {
                    vec![wire::ProductReceipt {
                        port: "value".to_owned(),
                        sequence: u64::from(output),
                        items: u32::from(output),
                        bytes: u64::from(output),
                    }]
                } else {
                    Vec::new()
                },
                required_inputs: Vec::new(),
                actuations: Vec::new(),
                required_deliveries: if output {
                    vec![wire::DeliveryReceipt {
                        port: "value".to_owned(),
                        direction: "publish".to_owned(),
                        target: String::new(),
                        sequence: 1,
                        item: 0,
                        bytes: 1,
                    }]
                } else {
                    Vec::new()
                },
            },
        )
        .await
        .expect("invocation acceptance publishes");
    }

    async fn publish_test_value(bus: &Connection, execution_id: &str, timeline_id: &str) {
        let metadata =
            RuntimeWireMetadata::data("producer", ExecutionTime::from_nanos(1_000_000), 1)
                .with_delivery_identity(execution_id, timeline_id, 1, 0);
        let attachment = metadata
            .encode_bounded()
            .expect("delivery metadata encodes");
        bus.session()
            .expect("producer session")
            .put(
                bus.full_key(&port_key("producer", "value", "publish")),
                vec![42_u8],
            )
            .encoding(Encoding::from(PROTOBUF_ENCODING.to_owned()))
            .attachment(attachment)
            .await
            .expect("controlled value publishes");
    }

    #[test]
    fn input_receipts_distinguish_consumer_fields_from_producer_ports() {
        let graph = std::collections::BTreeMap::from([
            (
                "encoders".into(),
                std::collections::BTreeSet::from([("wheel".into(), "encoder".into())]),
            ),
            (
                "diagnostics".into(),
                std::collections::BTreeSet::from([("wheel".into(), "encoder".into())]),
            ),
        ]);
        let mut accepted = wire::InvocationAccepted {
            required_inputs: vec![wire::InputReceipt {
                input: "encoders".into(),
                source: "wheel".into(),
                port: "encoder".into(),
                sequence: 1,
                items: 1,
                bytes: 8,
            }],
            ..Default::default()
        };
        assert!(super::validate_input_receipts(&accepted, &graph).is_ok());
        let mut second = accepted.required_inputs[0].clone();
        second.input = "diagnostics".into();
        accepted.required_inputs.push(second);
        assert!(super::validate_input_receipts(&accepted, &graph).is_ok());
        accepted.required_inputs[1].input = "encoders".into();
        assert!(super::validate_input_receipts(&accepted, &graph).is_err());
        accepted.required_inputs[1].input = "foreign".into();
        assert!(super::validate_input_receipts(&accepted, &graph).is_err());
    }

    #[test]
    fn due_boundaries_include_zero_and_only_exact_periods() {
        let quantum = 10;
        let period = 20;
        assert!(0_u64 == 0 || 0_u64.is_multiple_of(period));
        assert!(20_u64.is_multiple_of(period));
        assert!(!10_u64.is_multiple_of(period));
        assert!(quantum > 0);
    }

    #[test]
    fn scenario_delivery_acknowledgement_requires_exact_timeline_and_target() {
        let mut acknowledgement = wire::DeliveryAck {
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            boundary: 7,
            source: "supervisor".to_owned(),
            target: "motion.manual".to_owned(),
            port: "manual".to_owned(),
            direction: "publish".to_owned(),
            sequence: 9,
            item: 0,
            bytes: 4,
            admitted: true,
            detail: None,
        };
        let matches = |acknowledgement: &wire::DeliveryAck| {
            scenario_delivery_ack_matches(
                acknowledgement,
                "execution",
                "timeline",
                "motion.manual",
                "manual",
                9,
                4,
                7,
            )
        };

        assert!(matches(&acknowledgement));
        acknowledgement.timeline_id = "stale-timeline".to_owned();
        assert!(!matches(&acknowledgement));
        acknowledgement.timeline_id = "timeline".to_owned();
        acknowledgement.target = "motion.autonomous".to_owned();
        assert!(!matches(&acknowledgement));
    }

    #[test]
    fn delivery_ack_candidates_remove_out_of_order_items_without_loss() {
        let make = |item| ExpectedDelivery {
            source: "producer".to_owned(),
            target: "consumer.value".to_owned(),
            port: "value".to_owned(),
            direction: "publish".to_owned(),
            sequence: 9,
            item,
            bytes: 4,
        };
        let mut pending = BTreeSet::from([make(0), make(1), make(2)]);
        for item in [2_u32, 0, 1] {
            let acknowledgement = wire::DeliveryAck {
                execution_id: "execution".to_owned(),
                timeline_id: "timeline".to_owned(),
                boundary: 7,
                source: "producer".to_owned(),
                target: "consumer.value".to_owned(),
                port: "value".to_owned(),
                direction: "publish".to_owned(),
                sequence: 9,
                item,
                bytes: 4,
                admitted: true,
                detail: None,
            };
            let candidate = delivery_ack_candidate(
                &acknowledgement,
                "execution",
                "timeline",
                "producer",
                7,
                &pending,
            )
            .expect("out-of-order acknowledgement matches pending identity");
            assert!(pending.remove(&candidate));
        }
        assert!(pending.is_empty());
    }

    #[test]
    fn empty_product_and_input_batches_complete_without_new_data() {
        let accepted = wire::InvocationAccepted {
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            runtime_instance: "runtime".to_owned(),
            boundary: 4,
            required_products: vec![wire::ProductReceipt {
                port: "optional".to_owned(),
                sequence: 0,
                items: 0,
                bytes: 0,
            }],
            required_inputs: Vec::new(),
            actuations: Vec::new(),
            required_deliveries: Vec::new(),
        };
        let product_ports = BTreeSet::from(["optional".to_owned()]);
        assert!(validate_products(&accepted, &product_ports).is_ok());
        assert!(
            validate_products(
                &wire::InvocationAccepted {
                    required_products: Vec::new(),
                    ..accepted.clone()
                },
                &product_ports
            )
            .is_err()
        );
        assert!(
            validate_products(
                &wire::InvocationAccepted {
                    required_products: vec![
                        accepted.required_products[0].clone(),
                        accepted.required_products[0].clone()
                    ],
                    ..accepted.clone()
                },
                &product_ports
            )
            .is_err()
        );
        assert!(
            validate_products(
                &wire::InvocationAccepted {
                    required_products: vec![wire::ProductReceipt {
                        port: "optional".to_owned(),
                        sequence: 0,
                        items: 0,
                        bytes: 1,
                    }],
                    ..accepted.clone()
                },
                &product_ports,
            )
            .is_err()
        );
    }

    async fn initialize_test_runtime(
        bus: &Connection,
        subscriber: &super::Subscriber,
        instance: &str,
        ports: &[&str],
    ) {
        let request: wire::InitializeStateRequest =
            super::decode(subscriber.recv_async().await.unwrap()).unwrap();
        super::send(
            bus,
            instance,
            "initialize-state-response",
            &wire::InitializeStateResponse {
                execution_id: request.execution_id,
                timeline_id: request.timeline_id,
                runtime_instance: instance.into(),
                required_products: ports
                    .iter()
                    .map(|port| wire::ProductReceipt {
                        port: (*port).into(),
                        sequence: 0,
                        items: 0,
                        bytes: 0,
                    })
                    .collect(),
                required_deliveries: Vec::new(),
            },
        )
        .await
        .unwrap();
    }

    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pending_runtime_does_not_block_another_runtimes_admission_retry() {
        let temporary = tempfile::tempdir().expect("router temporary directory");
        let socket = temporary.path().join("router.sock");
        let endpoint = format!("unixsock-stream/{}", socket.display());
        let execution = ExecutionId::mint();
        let router = super::super::router::start_embedded_router(
            execution,
            endpoint.clone(),
            Arc::new(|_| {}),
        )
        .await
        .expect("local router starts");
        let (supervisor_owner, supervisor_bus) = ConnectionOwner::open(
            ConnectionConfig::for_external(execution, None, vec![endpoint.clone()]),
        )
        .await
        .expect("supervisor bus opens");
        let (producer_owner, producer_bus) =
            ConnectionOwner::open(ConnectionConfig::for_participant(
                execution,
                ParticipantId::new("producer").expect("producer identity"),
                vec![endpoint.clone()],
            ))
            .await
            .expect("producer bus opens");
        let (consumer_owner, consumer_bus) =
            ConnectionOwner::open(ConnectionConfig::for_participant(
                execution,
                ParticipantId::new("consumer").expect("consumer identity"),
                vec![endpoint],
            ))
            .await
            .expect("consumer bus opens");

        let source = RuntimeBundle::for_test_with_connections(
            Path::new("."),
            "parallel-admission",
            vec![
                TestExecutable::with_artifact(
                    "consumer",
                    serde_json::json!({"runtime": {"inputs": [], "outputs": []}, "descriptors": []}),
                ),
                TestExecutable::with_artifact(
                    "producer",
                    serde_json::json!({"runtime": {"inputs": [], "outputs": []}, "descriptors": []}),
                ),
            ],
            BTreeMap::new(),
        );
        let state = ExecutionState::new();
        let protocol = RuntimeExecutionProtocol::open(supervisor_bus, &source, state.clone(), None)
            .await
            .expect("protocol opens");
        let consumer_admit = super::declare(&consumer_bus, "consumer", "admit")
            .await
            .expect("consumer subscription");
        let producer_admit = super::declare(&producer_bus, "producer", "admit")
            .await
            .expect("producer subscription");
        let (retried, retry_seen) = tokio::sync::oneshot::channel();
        let producer_actor = tokio::spawn(async move {
            producer_admit
                .recv_async()
                .await
                .expect("initial producer request");
            let request: wire::AdmitExecutionRequest = super::decode(
                producer_admit
                    .recv_async()
                    .await
                    .expect("independent producer retry"),
            )
            .expect("request decodes");
            retried.send(()).expect("consumer still waiting");
            send_admission_and_ready(&producer_bus, "producer", request).await;
        });
        let consumer_actor = tokio::spawn(async move {
            let request: wire::AdmitExecutionRequest =
                super::decode(consumer_admit.recv_async().await.expect("consumer request"))
                    .expect("request decodes");
            // The first peer remains pending until the second is retried.
            // A sequential admission waiter deadlocks this real transport flow.
            retry_seen
                .await
                .expect("producer was independently retried");
            send_admission_and_ready(&consumer_bus, "consumer", request).await;
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            protocol.admit_all(
                RuntimeExecutionMode::Controlled,
                1_000_000,
                &state.time_domain().timeline.to_string(),
            ),
        )
        .await
        .expect("all peers must make progress within one bounded startup")
        .expect("all peers admitted");
        producer_actor.await.expect("producer completed");
        consumer_actor.await.expect("consumer completed");
        consumer_owner.close().await;
        producer_owner.close().await;
        supervisor_owner.close().await;
        router.close().await.expect("router closes");
    }

    async fn send_admission_and_ready(
        bus: &Connection,
        instance: &str,
        request: wire::AdmitExecutionRequest,
    ) {
        super::send(
            bus,
            instance,
            "admit-response",
            &wire::AdmitExecutionResponse {
                admitted: true,
                ..Default::default()
            },
        )
        .await
        .expect("admission response publishes");
        super::send(
            bus,
            instance,
            "ready",
            &wire::Ready {
                execution_id: request.execution_id,
                timeline_id: request.timeline_id,
                runtime_instance: instance.to_owned(),
            },
        )
        .await
        .expect("Ready publishes");
    }

    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_router_delivery_ack_completes_before_slow_receiver_due() {
        let temporary = tempfile::tempdir().expect("router temporary directory");
        let socket = temporary.path().join("router.sock");
        let endpoint = format!("unixsock-stream/{}", socket.display());
        let execution = ExecutionId::mint();
        let router = super::super::router::start_embedded_router(
            execution,
            endpoint.clone(),
            Arc::new(|_| {}),
        )
        .await
        .expect("local router starts");
        let (supervisor_owner, supervisor_bus) = ConnectionOwner::open(
            ConnectionConfig::for_external(execution, None, vec![endpoint.clone()]),
        )
        .await
        .expect("supervisor bus opens");
        let (producer_owner, producer_bus) =
            ConnectionOwner::open(ConnectionConfig::for_participant(
                execution,
                ParticipantId::new("producer").expect("producer identity"),
                vec![endpoint.clone()],
            ))
            .await
            .expect("producer bus opens");
        let (consumer_owner, consumer_bus) =
            ConnectionOwner::open(ConnectionConfig::for_participant(
                execution,
                ParticipantId::new("consumer").expect("consumer identity"),
                vec![endpoint],
            ))
            .await
            .expect("consumer bus opens");
        let source = RuntimeBundle::for_test_with_connections(
            Path::new("."),
            "controlled-delivery-fixture",
            vec![
                TestExecutable::with_artifact(
                    "producer",
                    serde_json::json!({
                        "runtime": {
                            "schema": "phoxal/artifact/v0",
                            "record": "runtime",
                            "period_ms": 1,
                            "timeout_ms": 500,
                            "inputs": [],
                            "outputs": [
                                {"name": "value", "port": "value", "max_items": 1, "max_bytes": 1,
                                 "signature": {"shape": "observation", "retained_latest": true, "lease_valid_for_ms": null}}
                            ]
                        },
                        "descriptors": []
                    }),
                ),
                TestExecutable::with_artifact(
                    "consumer",
                    serde_json::json!({
                        "runtime": {
                            "schema": "phoxal/artifact/v0",
                            "record": "runtime",
                            "period_ms": 2,
                            "timeout_ms": 500,
                            "inputs": [
                                {"name": "value", "delivery": "observation_latest", "port": "value", "max_items": 1, "max_bytes": 1}
                            ],
                            "outputs": []
                        },
                        "descriptors": []
                    }),
                ),
            ],
            BTreeMap::from([(
                "consumer.value".to_owned(),
                serde_json::json!("producer.value"),
            )]),
        );
        let state = ExecutionState::new();
        let protocol =
            RuntimeExecutionProtocol::open(supervisor_bus.clone(), &source, state.clone(), None)
                .await
                .expect("protocol opens");
        let producer_admit = super::declare(&producer_bus, "producer", "admit")
            .await
            .expect("producer admission subscriber");
        let producer_invoke = super::declare(&producer_bus, "producer", "invoke")
            .await
            .expect("producer invocation subscriber");
        let consumer_admit = super::declare(&consumer_bus, "consumer", "admit")
            .await
            .expect("consumer admission subscriber");
        let consumer_invoke = super::declare(&consumer_bus, "consumer", "invoke")
            .await
            .expect("consumer invocation subscriber");
        let value_subscriber = consumer_bus
            .session()
            .expect("consumer value session")
            .declare_subscriber(
                OwnedKeyExpr::new(consumer_bus.full_key(&port_key("producer", "value", "publish")))
                    .expect("consumer value key"),
            )
            .with(zenoh::handlers::FifoChannel::new(4))
            .await
            .expect("consumer value subscriber");

        let producer_initialize = super::declare(&producer_bus, "producer", "initialize-state")
            .await
            .unwrap();
        let consumer_initialize = super::declare(&consumer_bus, "consumer", "initialize-state")
            .await
            .unwrap();
        let producer_actor_bus = producer_bus.clone();
        let producer_actor = tokio::spawn(async move {
            let admission_sample =
                tokio::time::timeout(Duration::from_secs(2), producer_admit.recv_async())
                    .await
                    .expect("producer admission arrives")
                    .expect("producer admission subscriber remains open");
            let admission: wire::AdmitExecutionRequest =
                super::decode(admission_sample).expect("producer admission decodes");
            assert!(
                admission.required_contracts[0]
                    .capabilities
                    .iter()
                    .any(|capability| capability == "delivery-ack")
            );
            super::send(
                &producer_actor_bus,
                "producer",
                "admit-response",
                &wire::AdmitExecutionResponse {
                    admitted: true,
                    unsupported_contracts: Vec::new(),
                    detail: None,
                },
            )
            .await
            .expect("producer admission response publishes");
            super::send(
                &producer_actor_bus,
                "producer",
                "ready",
                &wire::Ready {
                    execution_id: admission.execution_id.clone(),
                    timeline_id: admission.timeline_id.clone(),
                    runtime_instance: "producer".to_owned(),
                },
            )
            .await
            .expect("producer Ready publishes");
            initialize_test_runtime(
                &producer_actor_bus,
                &producer_initialize,
                "producer",
                &["value"],
            )
            .await;
            for boundary in 0..=2_u64 {
                let invocation_sample =
                    tokio::time::timeout(Duration::from_secs(2), producer_invoke.recv_async())
                        .await
                        .expect("producer invocation arrives")
                        .expect("producer invocation subscriber remains open");
                let invocation: wire::Invocation =
                    super::decode(invocation_sample).expect("producer invocation decodes");
                assert_eq!(invocation.boundary, boundary);
                if boundary == 1 {
                    publish_test_value(
                        &producer_actor_bus,
                        &invocation.execution_id,
                        &invocation.timeline_id,
                    )
                    .await;
                }
                send_test_acceptance(&producer_actor_bus, &invocation, boundary == 1).await;
            }
        });

        let consumer_actor_bus = consumer_bus.clone();
        let consumer_actor = tokio::spawn(async move {
            let admission_sample =
                tokio::time::timeout(Duration::from_secs(2), consumer_admit.recv_async())
                    .await
                    .expect("consumer admission arrives")
                    .expect("consumer admission subscriber remains open");
            let admission: wire::AdmitExecutionRequest =
                super::decode(admission_sample).expect("consumer admission decodes");
            super::send(
                &consumer_actor_bus,
                "consumer",
                "admit-response",
                &wire::AdmitExecutionResponse {
                    admitted: true,
                    unsupported_contracts: Vec::new(),
                    detail: None,
                },
            )
            .await
            .expect("consumer admission response publishes");
            super::send(
                &consumer_actor_bus,
                "consumer",
                "ready",
                &wire::Ready {
                    execution_id: admission.execution_id.clone(),
                    timeline_id: admission.timeline_id.clone(),
                    runtime_instance: "consumer".to_owned(),
                },
            )
            .await
            .expect("consumer Ready publishes");
            initialize_test_runtime(&consumer_actor_bus, &consumer_initialize, "consumer", &[])
                .await;
            let mut retained_values = Vec::new();
            loop {
                tokio::select! {
                    invocation_sample = consumer_invoke.recv_async() => {
                        let invocation: wire::Invocation = super::decode(
                            invocation_sample.expect("consumer invocation subscriber remains open")
                        ).expect("consumer invocation decodes");
                        match invocation.boundary {
                            0 => send_test_acceptance(&consumer_actor_bus, &invocation, false).await,
                            2 => {
                                loop {
                                    match value_subscriber.try_recv() {
                                        Ok(Some(sample)) => retained_values.push(
                                            phoxal::runtime::transport::WireSample::from_zenoh(sample)
                                                .expect("retained value decodes")
                                        ),
                                        Ok(None) => break,
                                        Err(error) => panic!("consumer value subscriber failed: {error}"),
                                    }
                                }
                                assert_eq!(retained_values.len(), 1, "slow consumer sees one value at its due invocation");
                                let value = &retained_values[0];
                                assert_eq!(value.payload(), [42]);
                                assert_eq!(value.metadata().boundary, Some(1));
                                assert_eq!(value.metadata().item, Some(0));
                                assert_eq!(value.metadata().sequence, Some(1));
                                send_test_acceptance(&consumer_actor_bus, &invocation, false).await;
                                break;
                            }
                            boundary => panic!("unexpected consumer invocation boundary {boundary}"),
                        }
                    }
                    sample = value_subscriber.recv_async() => {
                        let value = phoxal::runtime::transport::WireSample::from_zenoh(
                            sample.expect("consumer value subscriber remains open")
                        ).expect("controlled value decodes");
                        assert_eq!(value.payload(), [42]);
                        assert_eq!(value.metadata().boundary, Some(1));
                        assert_eq!(value.metadata().item, Some(0));
                        assert_eq!(value.metadata().sequence, Some(1));
                        retained_values.push(value);
                        super::send(
                            &consumer_actor_bus,
                            "producer",
                            "delivery-ack",
                            &wire::DeliveryAck {
                                execution_id: admission.execution_id.clone(),
                                timeline_id: admission.timeline_id.clone(),
                                boundary: 1,
                                source: "producer".to_owned(),
                                target: "consumer.value".to_owned(),
                                port: "value".to_owned(),
                                direction: "publish".to_owned(),
                                sequence: 1,
                                item: 0,
                                bytes: 1,
                                admitted: true,
                                detail: None,
                            },
                        )
                        .await
                        .expect("delivery acknowledgement publishes");
                    }
                }
            }
        });

        let timeline = state.time_domain().timeline.to_string();
        protocol
            .admit_all(RuntimeExecutionMode::Controlled, 1_000_000, &timeline)
            .await
            .expect("controlled admission and Ready complete");
        let mut context = PublicSimulationContext {
            principal: "simulator".to_owned(),
            session_id: vec![1],
            authority_grant: vec![2],
            correlation_id: vec![3],
            execution_id: execution.to_string(),
            timeline_id: timeline.clone(),
            completed_boundary: 0,
            model_identity: "unused".to_owned(),
            quantum_ns: 1_000_000,
        };
        {
            let _blocked_boundary = protocol.inner.boundary.lock().await;
            let progress = tokio::time::timeout(Duration::from_millis(50), protocol.progress(context.clone(), ProgressRequest::default())).await
                .expect("progress remains live while invocation or observation admission holds the boundary")
                .expect("progress snapshot");
            assert_eq!(progress.completed_boundary, 0);
            assert!(!progress.failed);
        }
        let initial_key = TransitionKey {
            session_id: vec![1],
            execution_id: execution.to_string(),
            timeline_id: timeline.clone(),
            authority_grant: vec![2],
            boundary: 0,
            operation_sequence: 1,
        };
        protocol
            .admit_initial_observations(
                context.clone(),
                AdmitInitialObservationsRequest {
                    transition_key: Some(initial_key),
                    observations: Vec::new(),
                    membership_digest: Vec::new(),
                    correlation_id: vec![3],
                },
                Vec::new(),
            )
            .await
            .expect("boundary-zero observations are admitted without invocation");
        for boundary in 0..=2_u64 {
            context.completed_boundary = boundary;
            let transition = TransitionKey {
                session_id: vec![1],
                execution_id: execution.to_string(),
                timeline_id: timeline.clone(),
                authority_grant: vec![2],
                boundary,
                operation_sequence: boundary + 2,
            };
            let response = protocol
                .prepare_boundary(
                    context.clone(),
                    PrepareBoundaryRequest {
                        transition_key: Some(transition.clone()),
                        correlation_id: vec![4 + boundary as u8],
                    },
                )
                .await
                .expect("controlled boundary prepares");
            assert_eq!(
                response.receipt.expect("prepare receipt").status,
                phoxal::communication::simulation::PhaseStatus::Prepared
            );
            let response = protocol
                .admit_observations(
                    context.clone(),
                    AdmitObservationsRequest {
                        transition_key: Some(transition),
                        observations: Vec::new(),
                        membership_digest: Vec::new(),
                        correlation_id: vec![4 + boundary as u8],
                    },
                    Vec::new(),
                )
                .await
                .expect("controlled observation cut completes");
            assert_eq!(
                response
                    .receipt
                    .expect("observation admission receipt")
                    .status,
                phoxal::communication::simulation::PhaseStatus::ObservationsAdmitted
            );
        }
        assert_eq!(state.runtime_boundary(), 3);
        producer_actor.await.expect("producer actor completes");
        consumer_actor.await.expect("consumer actor completes");
        consumer_owner.close().await;
        producer_owner.close().await;
        supervisor_owner.close().await;
        router.close().await.expect("router closes");
    }

    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_router_control_exchange_commits_only_the_complete_cut() {
        let temporary = tempfile::tempdir().expect("router temporary directory");
        let socket = temporary.path().join("router.sock");
        let endpoint = format!("unixsock-stream/{}", socket.display());
        let execution = ExecutionId::mint();
        let router = super::super::router::start_embedded_router(
            execution,
            endpoint.clone(),
            Arc::new(|_| {}),
        )
        .await
        .expect("local router starts");
        let (supervisor_owner, supervisor_bus) = ConnectionOwner::open(
            ConnectionConfig::for_external(execution, None, vec![endpoint.clone()]),
        )
        .await
        .expect("supervisor bus opens");
        let (runtime_owner, runtime_bus) =
            ConnectionOwner::open(ConnectionConfig::for_participant(
                execution,
                ParticipantId::new("brain").expect("runtime identity"),
                vec![endpoint],
            ))
            .await
            .expect("runtime bus opens");
        let mut source = RuntimeBundle::for_test(
            Path::new("."),
            "controlled-fixture",
            vec![TestExecutable::with_artifact(
                "brain",
                serde_json::json!({
                    "runtime": {
                        "schema": "phoxal/artifact/v0",
                        "record": "runtime",
                        "period_ms": 2,
                        "timeout_ms": 500,
                        "inputs": [],
                        "outputs": [
                            {"name": "state", "port": "state", "max_bytes": 64,
                             "signature": {"shape": "observation", "retained_latest": true, "lease_valid_for_ms": null}},
                            {"name": "target", "port": "target", "max_bytes": 8,
                             "signature": {"shape": "observation", "retained_latest": false, "lease_valid_for_ms": 100}}
                        ]
                    },
                    "descriptors": []
                }),
            )],
        );
        source.set_simulation(phoxal::artifact::bundle::BundleSimulation {
            protocol: "phoxal.simulation.v1".into(),
            mode: "controlled".into(),
            model_identity: "fixture".into(),
            quantum_ns: 1_000_000,
            providers: vec![],
            actuation_bindings: vec![phoxal::artifact::bundle::BundleActuationBinding {
                service_instance: "brain".into(),
                port: "target".into(),
                payload_fqn: "fixture.Target".into(),
                actuator_ids: vec!["motor".into()],
            }],
        });
        let state = ExecutionState::new();
        let protocol =
            RuntimeExecutionProtocol::open(supervisor_bus.clone(), &source, state.clone(), None)
                .await
                .expect("protocol opens");
        let admit = super::declare(&runtime_bus, "brain", "admit")
            .await
            .expect("runtime admission subscriber");
        let invoke = super::declare(&runtime_bus, "brain", "invoke")
            .await
            .expect("runtime invocation subscriber");
        let reset = super::declare(&runtime_bus, "brain", "reset")
            .await
            .expect("runtime reset subscriber");
        let initialize = super::declare(&runtime_bus, "brain", "initialize-state")
            .await
            .unwrap();
        let actor_bus = runtime_bus.clone();
        let timeline = state.time_domain().timeline.to_string();
        let actor_timeline = timeline.clone();
        let actor = tokio::spawn(async move {
            let admission_sample = tokio::time::timeout(Duration::from_secs(2), admit.recv_async())
                .await
                .expect("admission request arrives")
                .expect("admission subscriber remains open");
            let admission: wire::AdmitExecutionRequest =
                super::decode(admission_sample).expect("admission decodes");
            assert_eq!(admission.mode, wire::ExecutionMode::Controlled);
            assert_eq!(admission.quantum_ns, 1_000_000);
            super::send(
                &actor_bus,
                "brain",
                "admit-response",
                &wire::AdmitExecutionResponse {
                    admitted: true,
                    unsupported_contracts: Vec::new(),
                    detail: None,
                },
            )
            .await
            .expect("admission response publishes");
            super::send(
                &actor_bus,
                "brain",
                "ready",
                &wire::Ready {
                    execution_id: admission.execution_id.clone(),
                    timeline_id: admission.timeline_id.clone(),
                    runtime_instance: "brain".to_owned(),
                },
            )
            .await
            .expect("ready response publishes");
            initialize_test_runtime(&actor_bus, &initialize, "brain", &["state", "target"]).await;
            let invocation_sample =
                tokio::time::timeout(Duration::from_secs(2), invoke.recv_async())
                    .await
                    .expect("invocation arrives")
                    .expect("invocation subscriber remains open");
            let invocation: wire::Invocation =
                super::decode(invocation_sample).expect("invocation decodes");
            assert_eq!(invocation.boundary, 0);
            assert_eq!(invocation.logical_time_ns, 0);
            super::send(
                &actor_bus,
                "brain",
                "accepted",
                &wire::InvocationAccepted {
                    execution_id: invocation.execution_id.clone(),
                    timeline_id: invocation.timeline_id.clone(),
                    runtime_instance: invocation.runtime_instance.clone(),
                    boundary: invocation.boundary,
                    required_products: vec![
                        wire::ProductReceipt {
                            port: "state".to_owned(),
                            sequence: 1,
                            items: 1,
                            bytes: 1,
                        },
                        wire::ProductReceipt {
                            port: "target".to_owned(),
                            sequence: 1,
                            items: 1,
                            bytes: 1,
                        },
                    ],
                    required_inputs: Vec::new(),
                    actuations: vec![wire::Actuation {
                        port: "target".to_owned(),
                        payload: vec![7],
                        valid_until_ns: 2_000_000,
                    }],
                    required_deliveries: Vec::new(),
                },
            )
            .await
            .expect("acceptance publishes");
            let reset_sample = tokio::time::timeout(Duration::from_secs(2), reset.recv_async())
                .await
                .expect("reset request arrives")
                .expect("reset subscriber remains open");
            let request: wire::ResetExecutionRequest =
                super::decode(reset_sample).expect("reset decodes");
            assert_eq!(request.retired_timeline_id, actor_timeline);
            super::send(
                &actor_bus,
                "brain",
                "reset-response",
                &wire::ResetExecutionResponse {
                    accepted: true,
                    detail: None,
                },
            )
            .await
            .expect("reset response publishes");
        });
        protocol
            .admit_all(RuntimeExecutionMode::Controlled, 1_000_000, &timeline)
            .await
            .expect("admission and private Ready identity validation complete");
        let context = PublicSimulationContext {
            principal: "simulator".to_owned(),
            session_id: vec![1],
            authority_grant: vec![2],
            correlation_id: vec![3],
            execution_id: execution.to_string(),
            timeline_id: timeline.clone(),
            completed_boundary: 0,
            model_identity: "unused".to_owned(),
            quantum_ns: 1_000_000,
        };
        let initial_key = TransitionKey {
            session_id: vec![1],
            execution_id: execution.to_string(),
            timeline_id: timeline.clone(),
            authority_grant: vec![2],
            boundary: 0,
            operation_sequence: 1,
        };
        protocol
            .admit_initial_observations(
                context.clone(),
                AdmitInitialObservationsRequest {
                    transition_key: Some(initial_key),
                    observations: Vec::new(),
                    membership_digest: Vec::new(),
                    correlation_id: vec![3],
                },
                Vec::new(),
            )
            .await
            .expect("boundary-zero observations are admitted");
        let transition = TransitionKey {
            session_id: vec![1],
            execution_id: execution.to_string(),
            timeline_id: timeline.clone(),
            authority_grant: vec![2],
            boundary: 0,
            operation_sequence: 2,
        };
        let prepare_request = PrepareBoundaryRequest {
            transition_key: Some(transition.clone()),
            correlation_id: vec![4],
        };
        let response = protocol
            .prepare_boundary(context.clone(), prepare_request)
            .await
            .expect("complete runtime cut prepares");
        assert_eq!(response.actuation.len(), 1);
        let original_actuation = response.actuation.clone();
        assert_eq!(
            response.actuation[0]
                .membership
                .as_ref()
                .expect("actuation membership")
                .producer,
            "brain"
        );
        assert_eq!(response.actuation[0].payload, vec![7]);
        let response = protocol
            .admit_observations(
                context.clone(),
                AdmitObservationsRequest {
                    transition_key: Some(transition.clone()),
                    observations: Vec::new(),
                    membership_digest: Vec::new(),
                    correlation_id: vec![4],
                },
                Vec::new(),
            )
            .await
            .expect("observation admission completes");
        assert_eq!(
            response.receipt.expect("admission receipt").status,
            phoxal::communication::simulation::PhaseStatus::ObservationsAdmitted
        );
        assert_eq!(state.runtime_boundary(), 1);
        let held_transition = TransitionKey {
            boundary: 1,
            operation_sequence: 3,
            ..transition.clone()
        };
        let held_context = PublicSimulationContext {
            completed_boundary: 1,
            ..context.clone()
        };
        let held = protocol
            .prepare_boundary(
                held_context.clone(),
                PrepareBoundaryRequest {
                    transition_key: Some(held_transition.clone()),
                    correlation_id: vec![5],
                },
            )
            .await
            .expect("the slower owner need not run at the intermediate physics boundary");
        assert_eq!(
            held.actuation, original_actuation,
            "holding never rewrites capture time, sequence, or expiry"
        );
        protocol
            .admit_observations(
                held_context.clone(),
                AdmitObservationsRequest {
                    transition_key: Some(held_transition),
                    observations: Vec::new(),
                    membership_digest: Vec::new(),
                    correlation_id: vec![5],
                },
                Vec::new(),
            )
            .await
            .expect("held actuator completes the second transition");
        let stale = protocol
            .prepare_boundary(
                context,
                PrepareBoundaryRequest {
                    transition_key: Some(transition),
                    correlation_id: vec![4],
                },
            )
            .await;
        assert!(stale.is_err(), "duplicate old boundary cannot reinvoke");
        assert_eq!(
            state.runtime_boundary(),
            2,
            "stale request cannot fabricate progress"
        );
        let next_timeline = TimelineId::mint().to_string();
        protocol
            .reset(
                PublicSimulationContext {
                    principal: "simulator".to_owned(),
                    session_id: vec![1],
                    authority_grant: vec![2],
                    correlation_id: vec![4],
                    execution_id: execution.to_string(),
                    timeline_id: timeline.clone(),
                    completed_boundary: 2,
                    model_identity: "unused".to_owned(),
                    quantum_ns: 1_000_000,
                },
                ResetRequest {
                    authority_grant: vec![2],
                    execution_id: execution.to_string(),
                    timeline_id: timeline,
                    completed_boundary: 2,
                    session_id: vec![1],
                    correlation_id: vec![4],
                },
                next_timeline.clone(),
            )
            .await
            .expect("reset response commits a fresh fence");
        assert_eq!(state.runtime_boundary(), 0);
        assert!(
            protocol
                .progress(
                    PublicSimulationContext {
                        principal: "simulator".to_owned(),
                        session_id: vec![1],
                        authority_grant: vec![2],
                        correlation_id: vec![5],
                        execution_id: execution.to_string(),
                        timeline_id: next_timeline,
                        completed_boundary: 0,
                        model_identity: "unused".to_owned(),
                        quantum_ns: 1_000_000,
                    },
                    ProgressRequest::default(),
                )
                .await
                .is_ok()
        );
        actor.await.expect("runtime actor completes");
        runtime_owner.close().await;
        supervisor_owner.close().await;
        router.close().await.expect("router closes");
    }
    #[test]
    fn every_receiving_field_has_its_own_delivery_obligation() {
        let consumer = fixture_record(
            serde_json::json!([
                {"name": "near", "delivery": "observation_history", "max_bytes": 1024},
                {"name": "far", "delivery": "observation_history", "max_bytes": 1024}
            ]),
            serde_json::json!([]),
        );
        let artifacts = BTreeMap::from([("consumer".to_owned(), consumer)]);
        let connections = BTreeMap::from([
            (
                crate::runtime::bundle_endpoint("consumer", "near"),
                vec![crate::runtime::bundle_endpoint("sensor", "range")],
            ),
            (
                crate::runtime::bundle_endpoint("consumer", "far"),
                vec![crate::runtime::bundle_endpoint("sensor", "range")],
            ),
        ]);
        let (routes, requests) = graph_delivery_routes(&connections, &artifacts).unwrap();
        let accepted = wire::InvocationAccepted {
            required_deliveries: vec![wire::DeliveryReceipt {
                port: "range".to_owned(),
                direction: "publish".to_owned(),
                sequence: 1,
                item: 0,
                bytes: 20,
                ..Default::default()
            }],
            ..Default::default()
        };
        let expected = expected_deliveries("sensor", &accepted, &routes, &requests).unwrap();
        assert_eq!(
            expected.len(),
            2,
            "each receiving field must acknowledge independently"
        );
        let mut pending = expected.into_iter().collect::<BTreeSet<_>>();
        let mut ack = wire::DeliveryAck {
            execution_id: "execution".to_owned(),
            timeline_id: "timeline".to_owned(),
            boundary: 1,
            source: "sensor".to_owned(),
            target: "consumer.near".to_owned(),
            port: "range".to_owned(),
            direction: "publish".to_owned(),
            sequence: 1,
            item: 0,
            bytes: 20,
            admitted: true,
            detail: None,
        };
        let first =
            delivery_ack_candidate(&ack, "execution", "timeline", "sensor", 1, &pending).unwrap();
        assert!(pending.remove(&first));
        assert_eq!(pending.len(), 1);
        assert!(
            delivery_ack_candidate(&ack, "execution", "timeline", "sensor", 1, &pending).is_none()
        );
        ack.target = "consumer.far".to_owned();
        ack.admitted = false;
        ack.detail = Some("receiver queue saturated".to_owned());
        assert!(
            delivery_ack_candidate(&ack, "execution", "timeline", "sensor", 1, &pending).is_some(),
            "a rejected second field must remain an unsatisfied obligation"
        );
    }

    fn fixture_record(
        inputs: serde_json::Value,
        outputs: serde_json::Value,
    ) -> phoxal::artifact::RuntimeRecord {
        let artifact = crate::runtime::bundle::normalize_fixture_artifact(serde_json::json!({
            "runtime": {
                "inputs": inputs,
                "outputs": outputs,
            }
        }));
        let summary: phoxal::artifact::ArtifactSummary =
            serde_json::from_value(artifact).expect("fixture runtime record decodes");
        summary.runtime
    }
    #[test]
    fn keyed_connections_authorize_both_request_and_reply_delivery_legs() {
        for role in ["call_target", "call_result"] {
            let caller = fixture_record(
                serde_json::json!([{"name": "emergency", "delivery": role, "max_bytes": 1024}]),
                serde_json::json!([]),
            );
            let target = if role == "call_result" {
                fixture_record(
                    serde_json::json!([]),
                    serde_json::json!([{"name": "handler", "port": "emergency",
                        "signature": {"endpoint": "emergency", "service": "fixture.Service", "method": "Emergency", "shape": "call", "request": "fixture.Request", "response": "fixture.Response", "retained_latest": false, "lease_valid_for_ms": null}}]),
                )
            } else {
                fixture_record(
                    serde_json::json!([{"name": "handler", "delivery": "call_ingress", "port": "emergency"}]),
                    serde_json::json!([]),
                )
            };
            let artifacts =
                BTreeMap::from([("brain".to_owned(), caller), ("motion".to_owned(), target)]);
            let connections = BTreeMap::from([(
                crate::runtime::bundle_endpoint("brain", "emergency"),
                vec![crate::runtime::bundle_endpoint("motion", "emergency")],
            )]);
            let (replies, requests) = graph_delivery_routes(&connections, &artifacts).unwrap();
            assert_eq!(
                requests,
                BTreeMap::from([(
                    (
                        "brain".to_owned(),
                        "motion".to_owned(),
                        "emergency".to_owned()
                    ),
                    "motion.handler".to_owned()
                )])
            );
            assert_eq!(
                replies,
                BTreeMap::from([(
                    (
                        "motion".to_owned(),
                        "emergency".to_owned(),
                        "reply".to_owned()
                    ),
                    // The connected receiving field and the runtime's
                    // stable generated-call caller alias both route this
                    // reply leg.
                    BTreeSet::from([
                        "brain.emergency".to_owned(),
                        "brain.generated_call".to_owned(),
                    ])
                )])
            );
        }
    }
    #[test]
    fn reply_obligations_select_one_caller_and_refuse_broadcast_or_unknown_targets() {
        let routes = BTreeMap::from([(
            (
                "server".to_owned(),
                "commands".to_owned(),
                "reply".to_owned(),
            ),
            BTreeSet::from([
                "alice.first".to_owned(),
                "alice.second".to_owned(),
                "bob.first".to_owned(),
            ]),
        )]);
        let mut accepted = wire::InvocationAccepted {
            required_deliveries: vec![wire::DeliveryReceipt {
                port: "commands".to_owned(),
                direction: "reply".to_owned(),
                target: "alice.first".to_owned(),
                sequence: 1,
                item: 0,
                bytes: 2,
            }],
            ..Default::default()
        };
        let expected = expected_deliveries("server", &accepted, &routes, &BTreeMap::new()).unwrap();
        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].target, "alice.first");
        accepted.required_deliveries[0].target.clear();
        assert!(expected_deliveries("server", &accepted, &routes, &BTreeMap::new()).is_err());
        accepted.required_deliveries[0].target = "mallory".to_owned();
        assert!(expected_deliveries("server", &accepted, &routes, &BTreeMap::new()).is_err());
    }
}
