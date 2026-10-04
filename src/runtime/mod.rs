//! Supervisor-owned source-bundle execution host.
//!
//! The host admits one immutable source bundle, starts the embedded router,
//! launches its recorded Runtime processes, and serves the public session on
//! the same execution transport. Runtime admission is the readiness proof.
//! There is no legacy observer, MessagePack control plane, or second serving
//! path.

pub(crate) mod adapter;
pub(crate) mod bundle;
pub(crate) mod execution;
pub(crate) mod lock;
pub(crate) mod process;
pub(crate) mod public_backend;
pub(crate) mod router;
pub(crate) mod session_table;
pub(crate) mod signal;
pub(crate) mod state;
pub(crate) mod systemd;
pub(crate) mod transport;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::rendezvous::RuntimeRendezvous;
use anyhow::{Context, Result, bail};
use phoxal::communication::DeploymentTarget;
use phoxal::communication::session::ExecutionState as PublicExecutionState;
use phoxal::communication_transport::PublicTransportLimits;

use self::adapter::SupervisorAdapter;
use self::transport::server::{PrincipalPolicy, PublicSessionServer};
use crate::scenario_admission::{ScenarioLaunchMode, admit_simulation_run};
use phoxal::identity::ExecutionId;
use phoxal::runtime::connection::{Connection, ConnectionConfig, ConnectionOwner};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use bundle::RuntimeBundle;
use execution::{RuntimeExecutionMode, RuntimeExecutionProtocol};
use process::ProcessSupervisor;
use public_backend::{RuntimeExecutionCoordinator, RuntimePublicBackend, RuntimePublicSurface};
use state::{ExecutionState, TimeMode};

struct ExecutionLaunch {
    runtime: RuntimeBundle,
    endpoint: String,
    target: DeploymentTarget,
    ready_file: Option<PathBuf>,
    scenario_result: Option<PathBuf>,
    simulation_run: Option<PathBuf>,
    shutdown: CancellationToken,
    scenario_program: Option<phoxal::scenario::plan_support::Program>,
}

/// One typed graph endpoint reference from fixture-readable parts.
#[cfg(test)]
pub(crate) fn bundle_endpoint(
    instance: &str,
    endpoint: &str,
) -> phoxal::artifact::bundle::EndpointReference {
    phoxal::artifact::bundle::EndpointReference {
        instance: instance.to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

pub(super) struct RunRequest<'a> {
    pub(super) requested_root: &'a Path,
    /// Explicit execution state directory owning this run's sockets and
    /// locks. The supervisor never infers `.phoxal`, Cargo workspace roots,
    /// or release-directory ancestry from the bundle location.
    pub(super) state_dir: &'a Path,
    pub(super) target: DeploymentTarget,
    pub(super) ready_file: Option<&'a Path>,
    pub(super) scenario_result: Option<&'a Path>,
    pub(super) simulation_run: Option<&'a Path>,
    pub(super) owner_pid: Option<u32>,
    pub(super) listen: Option<&'a str>,
    pub(super) launch_mode: ScenarioLaunchMode,
}

/// Execute one compiled source bundle and publish readiness atomically.
///
/// The optional file is an internal local-orchestration handoff. It becomes
/// visible only after every required Runtime has completed admission and the
/// public execution status is Ready.
pub async fn run(request: RunRequest<'_>) -> Result<()> {
    let RunRequest {
        requested_root,
        state_dir,
        target,
        ready_file,
        scenario_result,
        simulation_run,
        owner_pid,
        listen,
        launch_mode,
    } = request;
    let canonical = requested_root.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize bundle root {}",
            requested_root.display()
        )
    })?;
    let paths = RuntimeRendezvous::for_state_dir(state_dir);
    let lock = lock::SupervisorLock::acquire(&paths.supervisor_lock())?;
    admit_simulation_run(launch_mode, simulation_run.is_some())?;
    let mut runtime = RuntimeBundle::open(&canonical)?;
    let scenario_program = match simulation_run {
        Some(path) => Some(admit_run_specification(&mut runtime, path)?),
        None => None,
    };
    tracing::info!(
        bundle = %runtime.root().display(),
        robot = runtime.robot_id(),
        lock = %lock.path().display(),
        scope = target.scope(),
        supervisor_id = target.supervisor(),
        launch_mode = ?launch_mode,
        "phoxal-supervisor starting"
    );

    let state = ExecutionState::new();
    let shutdown = CancellationToken::new();
    signal::cancel_on_termination(shutdown.clone())?;
    let owner_guard = owner_pid.map(|owner_pid| {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            while process_is_alive(owner_pid) {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            tracing::warn!(owner_pid, "command-scoped execution owner exited");
            shutdown.cancel();
        })
    });
    let endpoint = match listen {
        Some(endpoint) => endpoint.to_owned(),
        None => router_endpoint(&paths.checked_supervisor_socket()?),
    };
    let outcome = execute(
        ExecutionLaunch {
            runtime,
            endpoint,
            target,
            ready_file: ready_file.map(Path::to_owned),
            scenario_result: scenario_result.map(Path::to_owned),
            simulation_run: simulation_run.map(Path::to_owned),
            shutdown: shutdown.clone(),
            scenario_program,
        },
        &state,
    )
    .await;
    shutdown.cancel();
    if let Some(owner_guard) = owner_guard {
        owner_guard.abort();
    }
    outcome
}

fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 does not modify the target process. It only asks the
    // kernel whether the process exists and is visible to this caller.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn admit_run_specification(
    runtime: &mut RuntimeBundle,
    path: &Path,
) -> Result<phoxal::scenario::plan_support::Program> {
    const MAX_RUN_SPECIFICATION_BYTES: u64 = 16 * 1024 * 1024;
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "simulation run specification is missing: {}",
            path.display()
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!(
            "simulation run specification must be a regular non-symlink file: {}",
            path.display()
        );
    }
    if metadata.len() > MAX_RUN_SPECIFICATION_BYTES {
        bail!(
            "simulation run specification is {} bytes; cap is {}",
            metadata.len(),
            MAX_RUN_SPECIFICATION_BYTES
        );
    }
    let bytes = fs::read(path).with_context(|| {
        format!(
            "failed to read simulation run specification {}",
            path.display()
        )
    })?;
    let specification: phoxal::artifact::simulation_run::SimulationRunSpecification =
        serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "failed to decode simulation run specification {}",
                path.display()
            )
        })?;
    let phoxal::artifact::simulation_run::SimulationRunSpecification::V0 {
        bundle,
        model,
        program,
        bindings,
        captures,
        execution,
        ..
    } = &specification;

    let manifest_bytes = fs::read(runtime.root().join("manifest.json"))
        .context("failed to read immutable bundle manifest for run admission")?;
    let manifest_digest = format!("{:x}", Sha256::digest(&manifest_bytes));
    if manifest_digest != bundle.manifest_sha256 {
        bail!(
            "simulation run references bundle manifest {}, but selected bundle is {}",
            bundle.manifest_sha256,
            manifest_digest
        );
    }
    if runtime.robot_id() != bundle.robot_id {
        bail!(
            "simulation run references robot `{}`, but selected bundle is `{}`",
            bundle.robot_id,
            runtime.robot_id()
        );
    }
    let decoded = phoxal::scenario::plan_support::Program::decode(&program.bytes)
        .map_err(|error| anyhow::anyhow!("simulation program decode failed: {error}"))?;
    decoded
        .verify_identity()
        .map_err(|error| anyhow::anyhow!("simulation program identity failed: {error}"))?;
    if decoded.transition_count() != execution.transitions {
        bail!(
            "simulation program declares {} transitions; specification declares {}",
            decoded.transition_count(),
            execution.transitions
        );
    }
    if captures.len() != decoded.captures().len() {
        bail!(
            "simulation specification declares {} captures; program declares {}",
            captures.len(),
            decoded.captures().len()
        );
    }
    validate_program_contract(&decoded, bindings, captures)?;
    let simulation = runtime
        .simulation()
        .ok_or_else(|| anyhow::anyhow!("simulation run requires a controlled simulation bundle"))?;
    if simulation.model_identity != model.model_identity {
        bail!(
            "simulation model `{}` does not match bundle model `{}`",
            model.model_identity,
            simulation.model_identity
        );
    }
    if simulation.quantum_ns != execution.quantum_ns {
        bail!(
            "simulation run quantum {} does not match bundle quantum {}",
            execution.quantum_ns,
            simulation.quantum_ns
        );
    }
    validate_simulation_quantum(
        decoded.scenario_name(),
        decoded.quantum().micros(),
        execution.quantum_ns,
    )
    .map_err(|mismatch| anyhow::anyhow!("{mismatch}"))?;
    runtime.apply_simulation_bindings(bindings)?;
    Ok(decoded)
}

fn validate_program_contract(
    program: &phoxal::scenario::plan_support::Program,
    bindings: &[phoxal::artifact::simulation_run::SimulationBinding],
    captures: &[phoxal::artifact::simulation_run::SimulationCaptureRequirement],
) -> Result<()> {
    use phoxal::artifact::simulation_run::{
        SimulationBinding, SimulationCapturePolicy, SimulationCaptureRequirement,
    };
    use phoxal::scenario::plan_support::{Action, Capture};

    let mut expected = std::collections::BTreeMap::<(String, String), SimulationBinding>::new();
    for step in program.steps() {
        let (target_instance, signature, payload_bytes) = match &step.action {
            Action::Setpoint {
                target_instance,
                consumer_signature,
                encoded_payload,
                ..
            } => (target_instance, consumer_signature, encoded_payload.len()),
            Action::Withdraw {
                target_instance,
                producer_signature,
            } => (target_instance, producer_signature, 0),
            Action::Command { .. } => continue,
        };
        let key = (target_instance.clone(), signature.endpoint.to_owned());
        let max_message_bytes = u32::try_from(payload_bytes)
            .map_err(|_| anyhow::anyhow!("simulation program payload exceeds u32"))?;
        let candidate = SimulationBinding {
            target_instance: target_instance.clone(),
            source_instance: "supervisor".to_owned(),
            signature: signature.clone(),
            max_message_bytes,
            replaces_authored_source: false,
        };
        expected
            .entry(key)
            .and_modify(|existing| {
                existing.max_message_bytes = existing.max_message_bytes.max(max_message_bytes);
            })
            .or_insert(candidate);
    }
    if bindings.len() != expected.len() {
        bail!(
            "simulation specification declares {} bindings; program requires {}",
            bindings.len(),
            expected.len()
        );
    }
    for binding in bindings {
        let key = (
            binding.target_instance.clone(),
            binding.signature.endpoint.clone(),
        );
        let expected = expected.get(&key).with_context(|| {
            format!(
                "simulation specification declares binding {}.{} absent from the program",
                binding.target_instance, binding.signature.endpoint
            )
        })?;
        if binding.source_instance != expected.source_instance
            || binding.signature != expected.signature
            || binding.max_message_bytes != expected.max_message_bytes
        {
            bail!(
                "simulation binding {}.{} does not match the canonical program",
                binding.target_instance,
                binding.signature.endpoint
            );
        }
    }

    let expected_captures = program
        .captures()
        .iter()
        .map(|capture| match capture {
            Capture::State {
                name,
                signature,
                policy,
            }
            | Capture::Sample {
                name,
                signature,
                policy,
            }
            | Capture::Event {
                name,
                signature,
                policy,
            } => {
                let (instance, _) = name.split_once('/').with_context(|| {
                    format!("simulation capture `{name}` has no configured instance")
                })?;
                Ok(SimulationCaptureRequirement::Observation {
                    instance: instance.to_owned(),
                    signature: signature.clone(),
                    policy: match policy {
                        phoxal::scenario::CapturePolicy::Latest => SimulationCapturePolicy::Latest,
                        phoxal::scenario::CapturePolicy::BestEffortHistory { capacity } => {
                            SimulationCapturePolicy::BestEffortHistory {
                                capacity: *capacity,
                            }
                        }
                        phoxal::scenario::CapturePolicy::RequiredHistory { capacity } => {
                            SimulationCapturePolicy::RequiredHistory {
                                capacity: *capacity,
                            }
                        }
                    },
                })
            }
            Capture::NativeBody { name, .. } => Ok(SimulationCaptureRequirement::RootBody {
                body: name.clone(),
                every_steps: 1,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    if captures != expected_captures {
        bail!("simulation capture requirements do not match the canonical program");
    }
    Ok(())
}

async fn execute(launch: ExecutionLaunch, state: &ExecutionState) -> Result<()> {
    let ExecutionLaunch {
        runtime,
        endpoint,
        target,
        ready_file,
        scenario_result,
        simulation_run,
        shutdown,
        scenario_program,
    } = launch;
    let execution = ExecutionId::mint();
    let source = &runtime;
    let router_loss: Arc<OnceLock<String>> = Arc::default();
    let router_lost = {
        let router_loss = Arc::clone(&router_loss);
        let shutdown = shutdown.clone();
        Arc::new(move |reason: String| {
            let _ = router_loss.set(reason);
            shutdown.cancel();
        }) as self::router::RouterLost
    };
    let router = self::router::start_embedded_router(execution, endpoint.clone(), router_lost)
        .await
        .context("the embedded router did not start")?;

    let (owner, bus) = match ConnectionOwner::open(ConnectionConfig::for_external(
        execution,
        None,
        vec![endpoint.clone()],
    ))
    .await
    {
        Ok(opened) => opened,
        Err(error) => {
            return Err(abort_router_startup(
                router,
                anyhow::anyhow!("failed to open supervisor bus: {error}"),
            )
            .await);
        }
    };
    if let Err(error) = verify_router_identity(&bus, execution, &endpoint).await {
        let _ = owner.close().await;
        let _ = router.close().await;
        return Err(error);
    }

    let surface = match RuntimePublicSurface::from_bundle(&runtime) {
        Ok(surface) => surface,
        Err(error) => {
            let _ = owner.close().await;
            let _ = router.close().await;
            return Err(error);
        }
    };
    if surface.simulation.is_some() && state.time_domain().mode != TimeMode::Simulated {
        state
            .replace_time_domain(TimeMode::Simulated)
            .map_err(anyhow::Error::msg)?;
    }
    let protocol = Arc::new(
        RuntimeExecutionProtocol::open(bus.clone(), source, state.clone(), scenario_program)
            .await
            .context("failed to open Runtime execution protocol")?,
    );
    let public = match start_public_session(
        &bus,
        &target,
        &surface,
        state,
        execution,
        Arc::clone(&protocol),
    )
    .await
    {
        Ok(public) => public,
        Err(error) => {
            let _ = owner.close().await;
            let _ = router.close().await;
            return Err(error);
        }
    };
    let watchdog = match notify_systemd(shutdown.clone()) {
        Ok(watchdog) => watchdog,
        Err(error) => {
            let _ = public.close().await;
            let _ = owner.close().await;
            let _ = router.close().await;
            return Err(error);
        }
    };

    let mut processes =
        match ProcessSupervisor::launch(source, execution, &endpoint, simulation_run.as_deref())
            .await
        {
            Ok(processes) => processes,
            Err(error) => {
                let error = anyhow::anyhow!("failed to launch the Runtime graph: {error:#}");
                mark_execution_failed(&public, execution, &error).await;
                return finish_run(
                    Err(error),
                    RunResources {
                        processes: None,
                        public,
                        owner,
                        router,
                        watchdog,
                        shutdown,
                        router_loss,
                    },
                )
                .await;
            }
        };

    let (mode, quantum_ns) = match surface.simulation.as_ref() {
        Some(definition) => (RuntimeExecutionMode::Controlled, definition.quantum_ns()),
        None => (RuntimeExecutionMode::Hardware, 0),
    };
    let timeline = state.time_domain().timeline.to_string();
    let admission = tokio::select! {
        result = protocol.admit_all(mode, quantum_ns, &timeline) => result,
        result = processes.monitor(&shutdown) => {
            match result {
                Ok(()) => {
                    return finish_run(
                        Ok(()),
                        RunResources {
                            processes: Some(processes), public, owner, router,
                            watchdog, shutdown, router_loss,
                        },
                    ).await;
                }
                Err(error) => Err(error.context("required Runtime process failed during admission")),
            }
        }
    };
    if let Err(error) = admission {
        let error = anyhow::anyhow!("Runtime execution admission failed: {error:#}");
        let _ = processes.stop().await;
        mark_execution_failed(&public, execution, &error).await;
        return finish_run(
            Err(error),
            RunResources {
                processes: None,
                public,
                owner,
                router,
                watchdog,
                shutdown,
                router_loss,
            },
        )
        .await;
    }
    state.mark_ready();
    let _ = public
        .set_status(phoxal::communication::session::SupervisorState::Ready, None)
        .await;
    let _ = public
        .set_execution_state(&execution.to_string(), PublicExecutionState::Ready)
        .await;
    if let Some(path) = ready_file.as_deref()
        && let Err(error) = publish_readiness(path, execution)
    {
        let error = anyhow::anyhow!("failed to publish supervisor readiness: {error:#}");
        let _ = processes.stop().await;
        mark_execution_failed(&public, execution, &error).await;
        return finish_run(
            Err(error),
            RunResources {
                processes: None,
                public,
                owner,
                router,
                watchdog,
                shutdown,
                router_loss,
            },
        )
        .await;
    }

    let outcome = tokio::select! {
        failure = async {
            protocol.wait_failed().await;
            protocol.failure_reason().await
        }, if matches!(mode, RuntimeExecutionMode::Controlled) => {
            let error = anyhow::anyhow!(
                "controlled Runtime boundary failed: {}",
                failure.unwrap_or_else(|| "unspecified boundary failure".to_owned())
            );
            mark_execution_failed(&public, execution, &error).await;
            Err(error)
        }
        () = shutdown.cancelled() => Ok(()),
        result = processes.monitor(&shutdown) => {
            match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    let error = anyhow::anyhow!("required Runtime process failed: {error:#}");
                    mark_execution_failed(&public, execution, &error).await;
                    Err(error)
                }
            }
        }
    };
    let outcome = match (scenario_result.as_deref(), protocol.scenario_report().await) {
        (Some(path), Some(report)) => publish_json(path, &report)
            .context("failed to publish scenario execution evidence")
            .and(outcome),
        _ => outcome,
    };
    finish_run(
        outcome,
        RunResources {
            processes: Some(processes),
            public,
            owner,
            router,
            watchdog,
            shutdown,
            router_loss,
        },
    )
    .await
}

fn publish_readiness(path: &Path, execution: ExecutionId) -> Result<()> {
    if path.exists() {
        bail!("readiness path {} already exists", path.display());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("readiness path {} has no parent", path.display()))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("phoxal-ready"),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    let body =
        format!("{{\"schema\":\"phoxal/supervisor-ready/v0\",\"execution\":\"{execution}\"}}\n");
    file.write_all(body.as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to publish readiness from {} to {}",
            temporary.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn publish_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    if path.exists() {
        bail!("result path {} already exists", path.display());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("result path {} has no parent", path.display()))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("phoxal-result"),
        std::process::id()
    ));
    let body = serde_json::to_vec(value).context("failed to encode result JSON")?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(&body)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to publish result from {} to {}",
            temporary.display(),
            path.display()
        )
    })?;
    Ok(())
}

async fn start_public_session(
    bus: &Connection,
    target: &DeploymentTarget,
    surface: &RuntimePublicSurface,
    state: &ExecutionState,
    execution: ExecutionId,
    protocol: Arc<RuntimeExecutionProtocol>,
) -> Result<PublicSessionServer> {
    let coordinator = Arc::new(RuntimeExecutionCoordinator::new(state.clone()));
    let mut adapter = SupervisorAdapter::with_defaults(
        target.clone(),
        env!("CARGO_PKG_VERSION"),
        phoxal::VERSION,
    )?;
    let timeline = state.time_domain().timeline.to_string();
    adapter.install_execution(surface.execution(
        execution.to_string(),
        timeline,
        PublicExecutionState::Preparing,
    )?)?;
    adapter.set_status(
        phoxal::communication::session::SupervisorState::Preparing,
        Some("Runtime processes are being admitted".to_owned()),
    )?;
    let session = bus.session()?.clone();
    let backend = Arc::new(RuntimePublicBackend::new(bus.clone(), surface, coordinator));
    match surface.simulation.clone() {
        Some(definition) => Ok(PublicSessionServer::start_with_backends(
            session,
            adapter,
            backend,
            Arc::new(public_backend::RuntimeSimulationBridge::new(
                bus.clone(),
                surface,
                Some(definition),
                protocol,
            )),
            PrincipalPolicy::Any,
            PublicTransportLimits::default(),
        )
        .await?),
        None => Ok(PublicSessionServer::start_with_backend(
            session,
            adapter,
            backend,
            PrincipalPolicy::Any,
            PublicTransportLimits::default(),
        )
        .await?),
    }
}

async fn mark_execution_failed(
    public: &PublicSessionServer,
    execution: ExecutionId,
    error: &anyhow::Error,
) {
    let detail = format!("{error:#}");
    let _ = public
        .set_status(
            phoxal::communication::session::SupervisorState::Failed,
            Some(detail),
        )
        .await;
    let _ = public
        .set_execution_state(&execution.to_string(), PublicExecutionState::Failed)
        .await;
}

struct RunResources {
    processes: Option<ProcessSupervisor>,
    public: PublicSessionServer,
    owner: ConnectionOwner,
    router: self::router::EmbeddedRouter,
    watchdog: Option<tokio::task::JoinHandle<Result<()>>>,
    shutdown: CancellationToken,
    router_loss: Arc<OnceLock<String>>,
}

async fn finish_run(outcome: Result<()>, resources: RunResources) -> Result<()> {
    let RunResources {
        mut processes,
        public,
        owner,
        router,
        watchdog,
        shutdown,
        router_loss,
    } = resources;
    shutdown.cancel();
    let process_outcome = match processes.as_mut() {
        Some(processes) => processes.stop().await,
        None => Ok(()),
    };
    let public_outcome = public.close().await.map_err(anyhow::Error::from);
    let watchdog_outcome = match watchdog {
        Some(task) => task
            .await
            .context("the systemd watchdog task panicked")
            .and_then(std::convert::identity),
        None => Ok(()),
    };
    let close = owner.close().await;
    if !close.is_clean() {
        tracing::warn!(%close, "supervisor bus did not close cleanly");
    }
    if let Err(error) = router.close().await {
        tracing::warn!(error = %error, "embedded router did not close cleanly");
    }
    process_outcome?;
    outcome.and(watchdog_outcome).and(public_outcome)?;
    if let Some(reason) = router_loss.get() {
        bail!("{reason}");
    }
    Ok(())
}

async fn abort_router_startup(
    router: self::router::EmbeddedRouter,
    error: anyhow::Error,
) -> anyhow::Error {
    if let Err(close_error) = router.close().await {
        tracing::warn!(error = %close_error, "embedded router did not close after startup failure");
    }
    error
}

/// Tell systemd the supervisor is up, and keep its watchdog fed until stop.
fn notify_systemd(
    shutdown: CancellationToken,
) -> Result<Option<tokio::task::JoinHandle<Result<()>>>> {
    let notify = self::systemd::notify::SdNotify::from_env().unwrap_or_else(|error| {
        tracing::warn!("ignoring an unusable systemd notify socket: {error:#}");
        None
    });
    let Some(notify) = notify else {
        return Ok(None);
    };
    notify.notify_ready()?;
    let Some(interval) = notify.watchdog_interval() else {
        return Ok(None);
    };
    Ok(Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => notify.notify_watchdog()?,
            }
        }
    })))
}

fn router_endpoint(socket: &Path) -> String {
    format!("unixsock-stream/{}", socket.display())
}

/// Compare the controlled simulation's declared quantum (ns) against
/// the scenario program's quantum (us) without truncating either
/// side. Returns a human-readable diagnostic naming the scenario,
/// the program's quantum in both units, and the simulation's
/// quantum in nanoseconds when the two disagree.
fn validate_simulation_quantum(
    scenario_name: &str,
    program_quantum_micros: u32,
    simulation_quantum_ns: u64,
) -> Result<(), String> {
    let required_ns = u128::from(program_quantum_micros)
        .checked_mul(1_000)
        .ok_or_else(|| {
            format!(
                "scenario program `{scenario_name}` declares quantum {program_quantum_micros} micros; \
                 the converted nanosecond value overflows u128 and cannot be compared against \
                 the simulation's quantum"
            )
        })?;
    if u128::from(simulation_quantum_ns) != required_ns {
        return Err(format!(
            "scenario program `{scenario_name}` declares quantum {program_quantum_micros} \
             micros ({required_ns} ns) but the controlled simulation provides \
             {simulation_quantum_ns} ns; refusing to admit mismatched timing"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod quantum_validation_tests {
    use super::validate_simulation_quantum;

    /// 2 ms / 2 ms is the canonical equal case. The previous
    /// integer-truncation comparison accepted the value because both
    /// sides resolved to 2_000 micros; this regression ensures the
    /// nanosecond-aware comparison still accepts it.
    #[test]
    fn equal_quantum_in_nanoseconds_is_accepted() {
        validate_simulation_quantum("scenarios/Equal", 2_000, 2_000_000)
            .expect("equal quantum must validate");
    }

    /// 1 ms / 1 ms is the second canonical equal case. The previous
    /// unsigned-micros comparison admitted it; the new check must
    /// do the same without silently dropping precision.
    #[test]
    fn one_millisecond_quantum_is_accepted() {
        validate_simulation_quantum("scenarios/OneMs", 1_000, 1_000_000)
            .expect("1 ms quantum must validate");
    }

    /// The truncation defect: 2_000_001 ns vs 2_000 us. The old
    /// comparison divided simulation by 1_000 first, producing 2_000
    /// micros on both sides and admitting the bundle. The
    /// nanosecond-aware comparison refuses.
    #[test]
    fn mismatched_quantum_one_ns_over_is_refused() {
        let err = validate_simulation_quantum("scenarios/OneNsOver", 2_000, 2_000_001)
            .expect_err("2_000_001 ns vs 2_000 us must refuse");
        let expected_actual = format!("{}", 2_000_001u64);
        let expected_required = format!("{}", 2_000_000u64);
        assert!(
            err.contains(&expected_actual) && err.contains(&expected_required),
            "diagnostic must name both sides in nanoseconds; got `{err}`"
        );
    }

    /// 1_999_999 ns vs 2_000 us — the reverse truncation defect.
    /// The old comparison would have produced 1_999 micros on the
    /// simulation side, refused; this regression preserves that
    /// refusal with a nanosecond diagnostic.
    #[test]
    fn mismatched_quantum_one_ns_under_is_refused() {
        let err = validate_simulation_quantum("scenarios/OneNsUnder", 2_000, 1_999_999)
            .expect_err("1_999_999 ns vs 2_000 us must refuse");
        let expected_actual = format!("{}", 1_999_999u64);
        let expected_required = format!("{}", 2_000_000u64);
        assert!(
            err.contains(&expected_actual) && err.contains(&expected_required),
            "diagnostic must name both sides in nanoseconds; got `{err}`"
        );
    }

    /// A 10_000_000 ns / 10_000 us case is exactly representable and
    /// must validate, demonstrating that the new comparison does not
    /// introduce a regression for values the previous truncation
    /// handled correctly.
    #[test]
    fn ten_millisecond_quantum_is_accepted() {
        validate_simulation_quantum("scenarios/TenMs", 10_000, 10_000_000)
            .expect("10 ms quantum must validate");
    }
}

async fn verify_router_identity(
    bus: &Connection,
    expected: ExecutionId,
    endpoint: &str,
) -> Result<()> {
    let executions = ConnectionOwner::probe_routers(endpoint).await?;
    match executions.as_slice() {
        [reported] if *reported == expected => {}
        [reported] => bail!("router reports {reported}, expected {expected}"),
        [] => bail!("router on {endpoint} reports no execution identity"),
        many => bail!("router endpoint {endpoint} reports {} routers", many.len()),
    }
    anyhow::ensure!(
        bus.execution() == expected,
        "supervisor bus execution mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incompatible_bundle_and_run_specification_fail_before_launch() -> Result<()> {
        use phoxal::artifact::simulation_run::{
            SimulationApplicationReference, SimulationBundleReference, SimulationExecutionBounds,
            SimulationModelReference, SimulationProgram, SimulationRunSpecification,
        };

        let directory = tempfile::tempdir()?;
        fs::write(directory.path().join("manifest.json"), b"canonical bundle")?;
        let mut runtime = RuntimeBundle::for_test(directory.path(), "fixture", Vec::new());
        let specification = SimulationRunSpecification::V0 {
            bundle: SimulationBundleReference {
                robot_id: "fixture".to_owned(),
                manifest_sha256: "0".repeat(64),
            },
            model: SimulationModelReference {
                scene: "simulation/scene.xml".to_owned(),
                model_identity: "model".to_owned(),
            },
            simulator: SimulationApplicationReference {
                package: "phoxal-simulator".to_owned(),
                version: "0.0.0-dev.1".to_owned(),
                binary: "phoxal-simulator".to_owned(),
            },
            program: SimulationProgram { bytes: Vec::new() },
            bindings: Vec::new(),
            captures: Vec::new(),
            execution: SimulationExecutionBounds {
                quantum_ns: 2_000_000,
                transitions: 1,
                host_deadline_ms: 1_000,
                shutdown_grace_ms: 100,
            },
        };
        let path = directory.path().join("run.json");
        fs::write(&path, serde_json::to_vec(&specification)?)?;
        let error = admit_run_specification(&mut runtime, &path)
            .expect_err("a run cannot target another immutable bundle");
        assert!(error.to_string().contains("references bundle manifest"));
        Ok(())
    }

    #[test]
    fn readiness_is_published_once_after_admission() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("ready.json");
        let execution = ExecutionId::mint();
        publish_readiness(&path, execution)?;
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        assert_eq!(value["schema"], "phoxal/supervisor-ready/v0");
        assert_eq!(value["execution"], execution.to_string());
        assert!(publish_readiness(&path, execution).is_err());
        Ok(())
    }
}
