//! Controlled-simulation acceptance through the actual supervisor
//! executable, Zenoh transport, execution protocol, and the standalone
//! countdown processes.
//!
//! This is the process-tier reset and time-evidence path: the public
//! session acquires exclusive simulation authority over the admitted
//! execution, drives the three-phase admission cycle, and receives exact
//! capture metadata back. A simulated provider replaces the mission
//! countdown's retained status port at an immutable cadence, so the
//! brain's mission advances on simulator-submitted observations under
//! logical time — the real job needs twenty-five 20 ms boundaries, and
//! the simulated completion is admitted long before that. A simulation
//! reset through the execution protocol re-initializes the runtime
//! processes (`RuntimeRunner::reset` reached over the transport), retires
//! the old timeline and grant, and the fresh timeline re-initializes both
//! runtimes: their fresh bootstrap publications are received as live
//! values, the mission's fresh request is served and its fresh reply
//! returns, and a second mission reaches success on fresh simulated
//! observations alone — no old result, capture, or retired ownership
//! advances it.
//!
//! Build the external binaries first (the harness build does not build
//! other packages' binaries):
//!
//! ```sh
//! cargo build --locked -p phoxal-supervisor --bin phoxal-supervisor
//! cargo build --locked -p phoxal-countdown-fixture
//! cargo build --locked -p phoxal-countdown-brain-fixture
//! cargo test --locked -p phoxal-supervisor-runtime-fixture -p phoxal-supervisor --features phoxal-supervisor-runtime-fixture/test-fixtures
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used)]
#![recursion_limit = "256"]

mod support;

#[path = "fixtures/brain/src/contract.rs"]
mod brain_contract;

// The consumer contract names its provider payload through this module, so
// the include mirrors the consumer crate's own module layout.
#[path = "fixtures/brain/src/provider.rs"]
mod provider;

#[path = "fixtures/brain/src/expected_record.rs"]
mod brain_expected_record;

/// The generated provider operation marker the brain's contract names,
/// mirrored here with the same served identity so the included contract
/// compiles outside the consumer crate.
pub struct Start;
impl phoxal::contracts::Operation for Start {
    type Request = provider::StartRequest;
    type Response = provider::StartResponse;
    const METHOD: phoxal::contracts::CallMethod<Self::Request, Self::Response> =
        phoxal::contracts::CallMethod::new(
            "phoxal.tests.authoring.countdown.v1.Start",
            "start",
            "start",
            "phoxal.tests.authoring.countdown.v1.StartRequest",
            "phoxal.tests.authoring.countdown.v1.StartResponse",
            None,
            &[],
        );
}

use provider as contract;

#[path = "fixtures/countdown/src/expected_record.rs"]
mod expected_record;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use brain_contract::BrainApi;
use contract::CountdownState;
use phoxal::communication::session::SupervisorState;
use phoxal::communication::simulation::{
    AcquireAuthorityRequest, AdmitInitialObservationsRequest, AdmitObservationsRequest,
    Observation, PrepareBoundaryRequest, ProductDisposition, ProductMembership,
    ProviderRequirement, ResetRequest, TransitionKey,
};
use phoxal::session::{Connection, ConnectionConfig, ObservationItem, Supervisor, connect};
use prost::Message as _;
use sha2::{Digest, Sha256};

/// Slack for the supervisor to bind, admit the runtime, and reach Ready.
const STARTUP: Duration = Duration::from_secs(20);
/// Budget for one boundary cycle through the transport.
const PHASE: Duration = Duration::from_secs(10);
/// The controlled quantum: every runtime's 20 ms period is due at every
/// boundary.
const QUANTUM_NS: u64 = 20_000_000;
/// The immutable source cadence of the simulated status provider: 50 Hz is
/// due at every 20 ms boundary.
const RATE_MICROHERTZ: u64 = 50_000_000;
/// The real mission job needs 500 ms of logical time — twenty-five
/// boundaries — so a simulated completion at boundary two can only be
/// simulator data.
const MISSION_JOB: u64 = 42;
const MISSION_REAL_BOUNDARIES: u64 = 25;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_controlled_simulation_resets_through_the_process_boundary() {
    let bundle = build_bundle();
    let root = bundle.root.canonicalize().expect("bundle root resolves");
    let socket = support::execution_dir(&root).join("supervisor.sock");
    let endpoint = format!("unixsock-stream/{}", socket.display());

    let mut supervisor =
        support::SupervisorProcess::launch_mode(&root, "simulation-e2e", "controlled");

    let connection = tokio::time::timeout(STARTUP, connect_when_bound(&endpoint, &mut supervisor))
        .await
        .expect("the supervisor binds its public session endpoint");
    let supervisor_session = connection
        .supervisor("simulation-e2e")
        .await
        .expect("the supervisor accepts the public session");
    tokio::time::timeout(STARTUP, wait_until_ready(&supervisor_session))
        .await
        .expect("the authored runtime reaches Ready")
        .expect("the authored runtime remains healthy while reaching Ready");

    let executions = supervisor_session
        .management()
        .executions()
        .await
        .expect("execution inventory");
    let execution_id = executions
        .first()
        .expect("one admitted execution")
        .execution_id
        .clone();
    let execution = supervisor_session
        .execution(&execution_id)
        .await
        .expect("select the admitted execution");
    let brain = execution
        .service("brain")
        .await
        .expect("select the brain consumer instance");
    let mission = brain
        .method(BrainApi::MISSION)
        .await
        .expect("bind the consumer's mission-state method");
    let phases = ObservationWatch::watch(&mission).await;
    let simulation = supervisor_session.simulation();

    // Exclusive authority over the controlled execution, through the
    // transport, naming the immutable definition exactly.
    let acquired = simulation
        .acquire_authority(AcquireAuthorityRequest {
            execution_id: execution_id.clone(),
            model_identity: "countdown-sim-fixture".to_owned(),
            quantum_ns: QUANTUM_NS,
            providers: vec![ProviderRequirement {
                service_instance: "mission_countdown".to_owned(),
                port: "status".to_owned(),
                shape: phoxal::communication::session::MethodShape::Observation as i32,
                input_fqn: "google.protobuf.Empty".to_owned(),
                payload_fqn: "phoxal.tests.authoring.countdown.v1.CountdownState".to_owned(),
                rate_microhertz: RATE_MICROHERTZ,
            }],
            correlation_id: vec![1],
            ..Default::default()
        })
        .await
        .expect("simulation authority is acquired through the transport");
    let timeline = acquired.timeline_id.clone();
    let grant = acquired.authority_grant.clone();
    assert!(!timeline.is_empty(), "the authority names its timeline");
    assert_eq!(
        acquired.boundary, 0,
        "the authority starts at boundary zero"
    );
    assert!(acquired.lease_ms > 0, "the authority is lease-bound");
    eprintln!("[trace] acquired timeline {timeline} at boundary 0");

    // The initial cut at boundary zero: the idle status value.
    let initial_state = CountdownState {
        active_job_id: None,
        last_job_id: None,
        last_outcome: contract::Outcome::Unspecified,
    };
    let initial = admit_initial(
        &simulation,
        &execution_id,
        &timeline,
        &grant,
        &initial_state,
    )
    .await
    .expect("the initial observation cut is admitted");
    assert_capture_echo(&initial, 0, 1, "initial");

    // The brain mission starts its job at the first admitted boundary and
    // advances on the simulated status alone.
    let active = CountdownState {
        active_job_id: Some(MISSION_JOB),
        last_job_id: None,
        last_outcome: contract::Outcome::Unspecified,
    };
    let completed = CountdownState {
        active_job_id: None,
        last_job_id: Some(MISSION_JOB),
        last_outcome: contract::Outcome::Completed,
    };
    // Normal authenticated public ingress is fenced to the admitted controlled timeline.
    // A distinct public caller can submit while the authority owner advances time.
    let caller_connection = connect_when_bound(&endpoint, &mut supervisor).await;
    let caller = caller_connection
        .supervisor("simulation-e2e")
        .await
        .expect("external public session");
    let caller_execution = caller
        .execution(&execution_id)
        .await
        .expect("external execution");
    let caller_brain = caller_execution
        .service("brain")
        .await
        .expect("external brain");
    let external_target = caller_brain
        .method(phoxal::contracts::CallMethod::<
            brain_contract::ConsumedEventState,
            phoxal::contracts::Empty,
        >::new(
            "phoxal.tests.authoring.consumer.v1.ConsumedEventState",
            "target",
            "target",
            "phoxal.tests.authoring.consumer.v1.ConsumedEventState",
            "google.protobuf.Empty",
            Some(100),
            &[],
        ))
        .await
        .expect("controlled leased input");
    let external_target = std::sync::Arc::new(external_target);
    let (receipt_owner, receipt_bus) = phoxal::runtime::connection::ConnectionOwner::open(
        phoxal::runtime::connection::ConnectionConfig::for_external(
            phoxal::identity::ExecutionId::parse(&execution_id).expect("execution id"),
            None,
            vec![endpoint.clone()],
        ),
    )
    .await
    .expect("receipt observer");
    let receipts = receipt_bus
        .session()
        .expect("observer session")
        .declare_subscriber(phoxal::runtime::execution_protocol::key(
            &receipt_bus,
            "supervisor",
            "delivery-ack",
        ))
        .with(zenoh::handlers::FifoChannel::new(16))
        .await
        .expect("receipt subscription");
    // Round-trip on the same reliable link after declaring the observer:
    // the router has processed its subscription before the external caller publishes.
    let bootstrap = receipt_bus
        .session()
        .expect("observer session")
        .get(
            phoxal::communication::DeploymentTarget::new("local", "simulation-e2e")
                .expect("target")
                .bootstrap_key(),
        )
        .timeout(PHASE)
        .await
        .expect("observer link barrier");
    assert!(
        bootstrap
            .recv_async()
            .await
            .expect("bootstrap response")
            .result()
            .is_ok()
    );
    let pending_target = external_target.clone();
    let pending = tokio::spawn(async move {
        pending_target
            .call(
                brain_contract::ConsumedEventState {
                    last_job_id: None,
                    last_outcome: None,
                    handled_count: 73,
                },
                STARTUP,
            )
            .await
    });
    wait_external_lease_admission(&receipts, &execution_id, &timeline, 1).await;
    let (mut boundary, mut sequence, mut operation) = (0_u64, 1_u64, 2_u64);
    let mut admitted_boundary = 0_u64;
    for (step, state) in [&active, &completed].iter().enumerate() {
        cycle_prepare(
            &simulation,
            &execution_id,
            &timeline,
            &grant,
            boundary,
            operation,
        )
        .await
        .expect("the boundary preparation invokes the runtimes");
        let target = boundary + 1;
        sequence += 1;
        let receipt = cycle_admit(
            &simulation,
            &execution_id,
            &timeline,
            &grant,
            boundary,
            operation,
            target,
            sequence,
            state,
        )
        .await
        .unwrap_or_else(|panic| panic!("boundary {target} admission failed: {panic}"));
        assert_capture_echo(&receipt, target, sequence, &format!("cycle {step}"));
        boundary = target;
        admitted_boundary = target;
        operation += 1;
    }
    assert!(matches!(
        pending
            .await
            .expect("public call task")
            .expect("controlled public call"),
        phoxal::session::CallOutcome::Received(_)
    ));
    assert_eq!(
        fs::read_to_string(root.join("controlled-lease.marker")).expect("runtime marker"),
        "73"
    );
    // Renew at 40ms, before the original 100ms expiry. Withdrawal below is
    // checked at 100ms, before this renewed lease's 140ms expiry.
    let renewing_target = external_target.clone();
    let mut renewal = Some(tokio::spawn(async move {
        renewing_target
            .call(
                brain_contract::ConsumedEventState {
                    last_job_id: None,
                    last_outcome: None,
                    handled_count: 74,
                },
                STARTUP,
            )
            .await
    }));
    wait_external_lease_admission(&receipts, &execution_id, &timeline, 2).await;
    let mut withdrawal = None;
    // The mission holds a 100 ms logical delay after the completion it
    // observes, so keep admitting the completed status — one boundary per
    // cycle — until it latches success, well inside the real job's
    // twenty-five-boundary duration.
    for step in 0..12 {
        cycle_prepare(
            &simulation,
            &execution_id,
            &timeline,
            &grant,
            boundary,
            operation,
        )
        .await
        .expect("the boundary preparation invokes the runtimes");
        let target = boundary + 1;
        sequence += 1;
        let receipt = cycle_admit(
            &simulation,
            &execution_id,
            &timeline,
            &grant,
            boundary,
            operation,
            target,
            sequence,
            &completed,
        )
        .await
        .unwrap_or_else(|panic| panic!("boundary {target} admission failed: {panic}"));
        assert_capture_echo(&receipt, target, sequence, &format!("hold {step}"));
        boundary = target;
        admitted_boundary = target;
        operation += 1;
        if step == 1 {
            assert!(matches!(
                renewal
                    .take()
                    .expect("renewal task")
                    .await
                    .expect("renewal join")
                    .expect("renewal call"),
                phoxal::session::CallOutcome::Received(_)
            ));
            assert_eq!((boundary - 1) * QUANTUM_NS, 60_000_000);
            assert_eq!(
                fs::read_to_string(root.join("controlled-lease.marker")).expect("renewal marker"),
                "74",
                "renewal is consumed at 60ms, before the original 100ms expiry"
            );
            let withdrawing_target = external_target.clone();
            withdrawal = Some(tokio::spawn(async move {
                withdrawing_target.withdraw(STARTUP).await
            }));
            wait_external_lease_admission(&receipts, &execution_id, &timeline, 3).await;
        }
        if step == 3 {
            assert!(matches!(
                withdrawal
                    .take()
                    .expect("withdrawal task")
                    .await
                    .expect("withdrawal join")
                    .expect("withdrawal call"),
                phoxal::session::CallOutcome::Received(_)
            ));
            assert_eq!((boundary - 1) * QUANTUM_NS, 100_000_000);
            assert_eq!(
                fs::read_to_string(root.join("controlled-lease.marker"))
                    .expect("withdrawal marker"),
                "absent",
                "withdrawal removes renewed evidence before 140ms expiry"
            );
        }
        if let Some(mission) = phases.value() {
            eprintln!(
                "[trace] hold {step}: mission phase {}",
                mission.phase as i32
            );
            if mission.phase == brain_contract::MissionPhase::Succeeded {
                break;
            }
        }
    }
    // The mission completed on simulated data: the real job cannot have
    // finished before boundary twenty-five.
    assert!(admitted_boundary < MISSION_REAL_BOUNDARIES);
    phases
        .wait_value(
            |mission| mission.phase == brain_contract::MissionPhase::Succeeded,
            "first-timeline mission success",
        )
        .await;
    eprintln!("[trace] mission Succeeded on simulated data by boundary {admitted_boundary}");

    assert_eq!(
        fs::read_to_string(root.join("controlled-lease.marker")).expect("runtime marker"),
        "absent",
        "withdrawn lease remains absent"
    );
    // Prepare one more transition, then prove the immutable cadence and
    // capture-time fidelity are enforced before anything is admitted: a
    // not-due claim at a due boundary, and a capture time that disagrees
    // with its boundary, are both refused.
    cycle_prepare(
        &simulation,
        &execution_id,
        &timeline,
        &grant,
        admitted_boundary,
        operation,
    )
    .await
    .expect("the refusal probes prepare a real transition");
    let probe_target = admitted_boundary + 1;
    let mut stale = target_observation(probe_target, sequence + 1, &completed);
    stale.disposition = ProductDisposition::NotDue;
    let refusal = simulation
        .admit_observations(AdmitObservationsRequest {
            transition_key: Some(key(
                &execution_id,
                &timeline,
                &grant,
                admitted_boundary,
                operation,
            )),
            observations: vec![Observation {
                membership: Some(stale.clone()),
                payload: Vec::new(),
            }],
            membership_digest: digest_of(&[stale]),
            correlation_id: vec![20],
        })
        .await
        .expect_err("a cadence violation must be refused");
    eprintln!("[trace] cadence refusal: {refusal:?}");
    let mut skewed = target_observation(probe_target, sequence + 2, &completed);
    skewed.capture_time_ns = probe_target * QUANTUM_NS + 1;
    let refusal = simulation
        .admit_observations(AdmitObservationsRequest {
            transition_key: Some(key(
                &execution_id,
                &timeline,
                &grant,
                admitted_boundary,
                operation,
            )),
            observations: vec![Observation {
                membership: Some(skewed.clone()),
                payload: completed.encode_to_vec(),
            }],
            membership_digest: digest_of(&[skewed]),
            correlation_id: vec![21],
        })
        .await
        .expect_err("a capture-time violation must be refused");
    eprintln!("[trace] capture-time refusal: {refusal:?}");
    // Complete the prepared transition so the boundary is committed again
    // before the reset.
    let probe_target = admitted_boundary + 1;
    sequence += 3;
    let receipt = cycle_admit(
        &simulation,
        &execution_id,
        &timeline,
        &grant,
        admitted_boundary,
        operation,
        probe_target,
        sequence,
        &completed,
    )
    .await
    .expect("the refused probes leave the transition completable");
    assert_capture_echo(&receipt, probe_target, sequence, "post-probe");
    admitted_boundary = probe_target;

    // The first-timeline watcher is owned and closed before the reset
    // retires its binding.
    phases.close().await;

    // Reset from the completed boundary: the runtime processes are reset
    // through the execution protocol and a fresh timeline begins.
    let reset = simulation
        .reset(ResetRequest {
            authority_grant: grant.clone(),
            execution_id: execution_id.clone(),
            timeline_id: timeline.clone(),
            completed_boundary: admitted_boundary,
            session_id: Vec::new(),
            correlation_id: vec![30],
        })
        .await
        .expect("the simulation reset crosses the process boundary");
    let next_timeline = reset.next_timeline_id.clone();
    let next_grant = reset.authority_grant.clone();
    assert_ne!(next_timeline, timeline, "the reset mints a fresh timeline");
    assert_ne!(next_grant, grant, "the reset retires the old grant");
    assert_eq!(
        reset.previous_timeline_id, timeline,
        "the reset names the retired timeline"
    );
    eprintln!("[trace] reset to timeline {next_timeline} from boundary {admitted_boundary}");

    // Old received data is fenced: the retired grant cannot mutate anything.
    let refused = simulation
        .admit_observations(AdmitObservationsRequest {
            transition_key: Some(key(
                &execution_id,
                &timeline,
                &grant,
                admitted_boundary,
                operation,
            )),
            observations: vec![Observation {
                membership: Some(target_observation(
                    admitted_boundary + 1,
                    sequence + 3,
                    &completed,
                )),
                payload: completed.encode_to_vec(),
            }],
            membership_digest: digest_of(&[target_observation(
                admitted_boundary + 1,
                sequence + 3,
                &completed,
            )]),
            correlation_id: vec![31],
        })
        .await;
    let refusal = refused.expect_err("the retired grant must be refused");
    eprintln!("[trace] retired-grant refusal: {refusal:?}");

    let retired = external_target
        .call(
            brain_contract::ConsumedEventState {
                last_job_id: None,
                last_outcome: None,
                handled_count: 99,
            },
            PHASE,
        )
        .await;
    assert!(
        matches!(
            retired,
            Err(_)
                | Ok(phoxal::session::CallOutcome::NotSent(_))
                | Ok(phoxal::session::CallOutcome::RejectedBeforeAdmission(_))
        ),
        "retired timeline must be refused before admission: {retired:?}"
    );
    // The reset retired every timeline-scoped observation binding. Rebind
    // the mission and the real service's status NOW — before the fresh
    // timeline's first publication — so both watchers receive the fresh
    // bootstrap records as they happen, never by replay.
    let execution = supervisor_session
        .execution(&execution_id)
        .await
        .expect("re-select the execution for the fresh timeline");
    let brain = execution
        .service("brain")
        .await
        .expect("re-select the brain consumer instance");
    let mission = brain
        .method(BrainApi::MISSION)
        .await
        .expect("rebind the mission method after the reset");
    let phases = ObservationWatch::watch(&mission).await;
    let countdown = execution
        .service("countdown")
        .await
        .expect("re-select the countdown service");
    let status_handle = countdown
        .method(contract::CountdownApi::STATUS)
        .await
        .expect("rebind the countdown status method after the reset");
    let statuses = ObservationWatch::watch(&status_handle).await;

    // The fresh timeline's initial cut re-initializes every runtime, and
    // both fresh bootstrap publications are actually received: the brain's
    // mission restarts from a non-succeeded phase, and the service's
    // status is the fresh idle state — not the retired timeline's values.
    let initial = admit_initial(
        &simulation,
        &execution_id,
        &next_timeline,
        &next_grant,
        &initial_state,
    )
    .await
    .expect("the fresh timeline admits its initial cut");
    assert_capture_echo(&initial, 0, 1, "post-reset initial");
    let fresh_mission = phases
        .wait_value(
            |mission| mission.phase != brain_contract::MissionPhase::Succeeded,
            "fresh mission bootstrap",
        )
        .await;
    assert_ne!(
        fresh_mission.phase,
        brain_contract::MissionPhase::Succeeded,
        "the retired timeline's succeeded phase did not survive the reset"
    );
    eprintln!(
        "[trace] fresh mission bootstrap received (phase {})",
        fresh_mission.phase as i32
    );
    let fresh_status = statuses
        .wait_value(|_status| true, "fresh service bootstrap")
        .await;
    assert_eq!(
        (fresh_status.active_job_id, fresh_status.last_job_id),
        (None, None),
        "the service's re-initialized idle status is received on the fresh timeline"
    );
    eprintln!("[trace] fresh service status received (idle)");

    assert_eq!(
        fs::read_to_string(root.join("controlled-lease.marker")).expect("reset marker"),
        "absent",
        "reset has no fresh lease evidence"
    );
    // Fresh traffic: the mission issues a new request, the real service
    // serves it (its status changes to the fresh active job), the reply
    // returns, and the mission completes a second time on fresh simulated
    // observations with restarted capture metadata.
    let (mut boundary, mut sequence, mut operation) = (0_u64, 1_u64, 2_u64);
    for (step, state) in [&active, &completed].iter().enumerate() {
        cycle_prepare(
            &simulation,
            &execution_id,
            &next_timeline,
            &next_grant,
            boundary,
            operation,
        )
        .await
        .expect("the fresh timeline invokes the runtimes");
        let target = boundary + 1;
        sequence += 1;
        let receipt = cycle_admit(
            &simulation,
            &execution_id,
            &next_timeline,
            &next_grant,
            boundary,
            operation,
            target,
            sequence,
            state,
        )
        .await
        .unwrap_or_else(|panic| panic!("post-reset boundary {target} failed: {panic}"));
        assert_capture_echo(
            &receipt,
            target,
            sequence,
            &format!("post-reset cycle {step}"),
        );
        boundary = target;
        operation += 1;
    }
    // The fresh request was served: the service's own status publishes the
    // fresh active job — new received data from the real runtime process,
    // not the simulator and not the retired timeline.
    let served = statuses
        .wait_value(
            |status| status.active_job_id == Some(MISSION_JOB),
            "fresh request served by the real service",
        )
        .await;
    assert_eq!(served.last_job_id, None, "the fresh job has not completed");
    eprintln!("[trace] fresh request served (active job {MISSION_JOB})");

    // Drive the remaining boundaries while the watchers observe in the
    // background; the second mission success consumes the fresh reply and
    // the fresh simulated completion alone.
    for step in 0..12 {
        cycle_prepare(
            &simulation,
            &execution_id,
            &next_timeline,
            &next_grant,
            boundary,
            operation,
        )
        .await
        .expect("the fresh timeline invokes the runtimes");
        let target = boundary + 1;
        sequence += 1;
        let receipt = cycle_admit(
            &simulation,
            &execution_id,
            &next_timeline,
            &next_grant,
            boundary,
            operation,
            target,
            sequence,
            &completed,
        )
        .await
        .unwrap_or_else(|panic| panic!("post-reset boundary {target} failed: {panic}"));
        assert_capture_echo(
            &receipt,
            target,
            sequence,
            &format!("post-reset hold {step}"),
        );
        boundary = target;
        operation += 1;
        if let Some(mission) = phases.value() {
            eprintln!(
                "[trace] post-reset hold {step}: mission phase {}",
                mission.phase as i32
            );
            if mission.phase == brain_contract::MissionPhase::Succeeded {
                break;
            }
        }
    }
    let final_phase = phases
        .wait_value(|_mission| true, "second-timeline final mission phase")
        .await;
    eprintln!(
        "[trace] second-timeline mission phase at close: {}",
        final_phase.phase as i32
    );
    assert_eq!(
        final_phase.phase,
        brain_contract::MissionPhase::Succeeded,
        "the second mission must reach success on fresh data"
    );
    assert!(
        boundary < MISSION_REAL_BOUNDARIES,
        "the second success again precedes the real job's completion"
    );
    eprintln!("[trace] mission Succeeded again on the fresh timeline by boundary {boundary}");

    phases.close().await;
    statuses.close().await;

    tokio::time::timeout(STARTUP, supervisor_session.close())
        .await
        .expect("the supervisor session closes in time")
        .expect("the supervisor session closes cleanly");
    tokio::time::timeout(STARTUP, connection.close())
        .await
        .expect("the public connection closes in time")
        .expect("the public connection closes cleanly");
    caller_connection
        .close()
        .await
        .expect("external caller closes");
    receipt_owner.close().await;
    supervisor.shutdown().await;
}

async fn wait_external_lease_admission(
    receipts: &zenoh::pubsub::Subscriber<
        zenoh::handlers::FifoChannelHandler<zenoh::sample::Sample>,
    >,
    execution: &str,
    timeline: &str,
    sequence: u64,
) {
    tokio::time::timeout(PHASE, async {
        loop {
            let sample = receipts.recv_async().await.expect("delivery receipt");
            let receipt = phoxal::communication::execution::DeliveryAck::decode(
                sample.payload().to_bytes().as_ref(),
            )
            .expect("decode delivery receipt");
            eprintln!("[trace] external delivery receipt: {receipt:?}");
            if receipt.source == "supervisor"
                && receipt.target == "brain.target"
                && receipt.direction == "request"
                && receipt.sequence == sequence
            {
                assert_eq!(receipt.execution_id, execution);
                assert_eq!(receipt.timeline_id, timeline);
                assert!(
                    receipt.admitted,
                    "request admission refused: {:?}",
                    receipt.detail
                );
                return;
            }
        }
    })
    .await
    .expect("normal external request reaches the receiver before advancing time");
}

/// One transition key; the session fills its own session identifier.
#[allow(clippy::too_many_arguments)]
fn key(
    execution_id: &str,
    timeline: &str,
    grant: &[u8],
    boundary: u64,
    operation: u64,
) -> TransitionKey {
    TransitionKey {
        session_id: Vec::new(),
        execution_id: execution_id.to_owned(),
        timeline_id: timeline.to_owned(),
        authority_grant: grant.to_vec(),
        boundary,
        operation_sequence: operation,
    }
}

/// One present status observation membership captured exactly at a
/// boundary's logical time.
fn target_observation(boundary: u64, sequence: u64, state: &CountdownState) -> ProductMembership {
    let payload = state.encode_to_vec();
    ProductMembership {
        producer: "mission_countdown".to_owned(),
        port: "status".to_owned(),
        producer_incarnation: b"simulation-fixture".to_vec(),
        sequence,
        capture_boundary: boundary,
        capture_time_ns: boundary * QUANTUM_NS,
        disposition: ProductDisposition::Present,
        item_count: 1,
        encoded_bytes: payload.len() as u64,
        payload_digest: Sha256::digest(&payload).to_vec(),
    }
}

/// The canonical membership digest: SHA-256 over the length-prefixed,
/// deterministically ordered encoded membership frames.
fn digest_of(memberships: &[ProductMembership]) -> Vec<u8> {
    let mut encoded = memberships
        .iter()
        .map(ProductMembership::encode_to_vec)
        .collect::<Vec<_>>();
    encoded.sort();
    let mut hasher = Sha256::new();
    for member in encoded {
        hasher.update((member.len() as u64).to_be_bytes());
        hasher.update(member);
    }
    hasher.finalize().to_vec()
}

/// One full membership list digest for a set of observations.
fn cut_digest(observations: &[Observation]) -> Vec<u8> {
    digest_of(
        &observations
            .iter()
            .map(|observation| observation.membership.clone().unwrap_or_default())
            .collect::<Vec<_>>(),
    )
}

/// Admits the initial cut of one timeline: the idle status value.
async fn admit_initial(
    simulation: &phoxal::session::Simulation,
    execution_id: &str,
    timeline: &str,
    grant: &[u8],
    state: &CountdownState,
) -> Result<phoxal::communication::simulation::CutReceipt, String> {
    let observation = Observation {
        membership: Some(target_observation(0, 1, state)),
        payload: state.encode_to_vec(),
    };
    let request = AdmitInitialObservationsRequest {
        transition_key: Some(key(execution_id, timeline, grant, 0, 1)),
        membership_digest: cut_digest(std::slice::from_ref(&observation)),
        observations: vec![observation],
        correlation_id: vec![2],
    };
    let response = simulation
        .admit_initial_observations(request)
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(response.receipt.unwrap_or_default())
}

/// Prepares one boundary transition: the supervisor invokes the due
/// runtimes at the target boundary's logical time.
#[allow(clippy::too_many_arguments)]
async fn cycle_prepare(
    simulation: &phoxal::session::Simulation,
    execution_id: &str,
    timeline: &str,
    grant: &[u8],
    boundary: u64,
    operation: u64,
) -> Result<phoxal::communication::simulation::CutReceipt, String> {
    let request = PrepareBoundaryRequest {
        transition_key: Some(key(execution_id, timeline, grant, boundary, operation)),
        correlation_id: vec![3],
    };
    let response = simulation
        .prepare_boundary(request)
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(response.receipt.unwrap_or_default())
}

/// Admits one observation cut captured at the target boundary.
#[allow(clippy::too_many_arguments)]
async fn cycle_admit(
    simulation: &phoxal::session::Simulation,
    execution_id: &str,
    timeline: &str,
    grant: &[u8],
    boundary: u64,
    operation: u64,
    target: u64,
    sequence: u64,
    state: &CountdownState,
) -> Result<phoxal::communication::simulation::CutReceipt, String> {
    let observation = Observation {
        membership: Some(target_observation(target, sequence, state)),
        payload: state.encode_to_vec(),
    };
    let request = AdmitObservationsRequest {
        transition_key: Some(key(execution_id, timeline, grant, boundary, operation)),
        membership_digest: cut_digest(std::slice::from_ref(&observation)),
        observations: vec![observation],
        correlation_id: vec![4],
    };
    let response = simulation
        .admit_observations(request)
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(response.receipt.unwrap_or_default())
}

/// Asserts the exact capture metadata a real boundary echoes back: the
/// boundary, its logical capture time, the sequence, and the digest.
fn assert_capture_echo(
    receipt: &phoxal::communication::simulation::CutReceipt,
    boundary: u64,
    sequence: u64,
    checkpoint: &str,
) {
    let product = receipt
        .products
        .first()
        .unwrap_or_else(|| panic!("{checkpoint}: the receipt echoes no products"));
    assert_eq!(
        product.capture_boundary, boundary,
        "{checkpoint}: the receipt echoes the capture boundary"
    );
    assert_eq!(
        product.capture_time_ns,
        boundary * QUANTUM_NS,
        "{checkpoint}: the receipt echoes the boundary's logical capture time"
    );
    assert_eq!(
        product.sequence, sequence,
        "{checkpoint}: the receipt echoes the producer sequence"
    );
    assert_eq!(
        receipt.membership_digest,
        digest_of(std::slice::from_ref(product)),
        "{checkpoint}: the receipt digest covers the echoed membership"
    );
    eprintln!(
        "[trace] {checkpoint}: boundary {} capture {} ns sequence {sequence}",
        product.capture_boundary, product.capture_time_ns
    );
}

/// One long-lived typed observation watcher: a single subscription feeds
/// a state channel that distinguishes **no observed value** from a real
/// value, records stream termination and failure, and is explicitly
/// closed by its owner.
struct ObservationWatch<T> {
    state: tokio::sync::watch::Receiver<WatchState<T>>,
    task: tokio::task::JoinHandle<()>,
}

enum WatchState<T> {
    /// Subscribed; no record has arrived yet.
    Pending,
    /// The latest received value.
    Value(T),
    /// The owner ended the stream before a matching value.
    Ended,
    /// The stream failed.
    Failed(String),
}

impl<T: Clone + phoxal::contracts::ProstPayload> ObservationWatch<T> {
    /// Subscribes once and starts the background record loop.
    async fn watch(handle: &phoxal::session::ObservationHandle<T>) -> Self {
        let mut observations = tokio::time::timeout(PHASE, handle.observe())
            .await
            .expect("the observation subscription starts")
            .expect("observe starts cleanly");
        let (sender, state) = tokio::sync::watch::channel(WatchState::Pending);
        let task = tokio::spawn(async move {
            loop {
                match observations.recv().await {
                    Some(Ok(ObservationItem::Value { value, .. })) => {
                        let _ = sender.send(WatchState::Value(value));
                    }
                    Some(Ok(ObservationItem::InitialAbsent { .. })) => continue,
                    Some(Ok(ObservationItem::Gap { .. })) => continue,
                    Some(Ok(ObservationItem::End { .. })) => {
                        let _ = sender.send(WatchState::Ended);
                        return;
                    }
                    Some(Ok(ObservationItem::Failed { detail, .. })) => {
                        let _ = sender.send(WatchState::Failed(detail));
                        return;
                    }
                    Some(Err(error)) => {
                        let _ = sender.send(WatchState::Failed(format!("{error:?}")));
                        return;
                    }
                    None => {
                        let _ = sender.send(WatchState::Ended);
                        return;
                    }
                }
            }
        });
        Self { state, task }
    }

    /// The latest received value, if any record has arrived.
    fn value(&self) -> Option<T> {
        match &*self.state.borrow() {
            WatchState::Value(value) => Some(value.clone()),
            _ => None,
        }
    }

    /// Waits until a real received value satisfies the predicate. Fails on
    /// stream termination or failure before a match, and on timeout — a
    /// closing channel or a default state can never satisfy this wait.
    async fn wait_value(&self, predicate: impl Fn(&T) -> bool, what: &str) -> T {
        let mut receiver = self.state.clone();
        tokio::time::timeout(PHASE * 3, async {
            loop {
                match &*receiver.borrow() {
                    WatchState::Value(value) if predicate(value) => return value.clone(),
                    WatchState::Ended => {
                        panic!("the {what} stream ended before {what} was observed")
                    }
                    WatchState::Failed(detail) => {
                        panic!("the {what} stream failed before {what} was observed: {detail}")
                    }
                    _ => {}
                }
                if receiver.changed().await.is_err() {
                    panic!("the {what} watcher channel closed before a value arrived");
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {what} value arrived within the bounded window"))
    }

    /// Stops the background loop and reaps its task: the abort is issued
    /// and the owned task is awaited to completion, so a watcher is never
    /// left running after its scope ends.
    async fn close(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Connect as soon as the supervisor's embedded router is listening.
async fn connect_when_bound(
    endpoint: &str,
    supervisor: &mut support::SupervisorProcess,
) -> Connection {
    loop {
        assert!(
            !supervisor.is_finished(),
            "the supervisor exited before it was reachable at {endpoint}"
        );
        let config = ConnectionConfig::new(endpoint, "local", "simulation-e2e")
            .expect("the session config is valid");
        match connect(config).await {
            Ok(connection) => return connection,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Wait for the public supervisor projection to observe Runtime admission.
async fn wait_until_ready(supervisor: &Supervisor) -> phoxal::Result<()> {
    loop {
        let status = supervisor.management().status().await?;
        match status.state {
            SupervisorState::Ready => return Ok(()),
            SupervisorState::Failed => {
                anyhow::bail!(
                    "supervisor entered Failed before Runtime Ready: {:?}",
                    status.detail
                )
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

/// Locates the compiled authored brain consumer fixture beside this
/// package's Runtime fixture binary.
fn consumer_binary() -> PathBuf {
    support::fixtures::fixture_binary("brain", "countdown-brain-fixture")
}

struct TestBundle {
    _temporary_root: tempfile::TempDir,
    root: PathBuf,
}

/// Locates the compiled authored countdown fixture beside this package's
/// Runtime fixture binary.
fn countdown_binary() -> PathBuf {
    support::fixtures::fixture_binary("countdown", "countdown-authoring-fixture")
}

fn build_bundle() -> TestBundle {
    let temporary_root = tempfile::tempdir().expect("temporary bundle root");
    let root = temporary_root.path().join("bundle");
    fs::create_dir_all(root.join("bin")).expect("bundle bin directory");

    let source = countdown_binary();
    let executable = root.join("bin/countdown");
    fs::copy(source, &executable).expect("copy the compiled countdown fixture");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
        .expect("make the compiled countdown fixture executable");
    let consumer = consumer_binary();
    let consumer_executable = root.join("bin/brain");
    fs::copy(consumer, &consumer_executable).expect("copy the compiled brain consumer fixture");
    fs::set_permissions(&consumer_executable, fs::Permissions::from_mode(0o755))
        .expect("make the compiled brain consumer fixture executable");
    let driver = root.join("bin/native-driver");
    fs::write(
        &driver,
        "#!/bin/sh\necho physical fixture must not launch >&2\nexit 73\n",
    )
    .expect("native driver marker");
    fs::set_permissions(&driver, fs::Permissions::from_mode(0o755)).expect("driver executable");
    let native_artifact = serde_json::json!({"runtime": {
        "schema":"phoxal/artifact/v0", "record":"runtime", "config_schema":{"type":"null"},
        "period_ms":20,"timeout_ms":100,"init_timeout_ms":1000,
        "inputs":[{"name":"actuator","port":"actuator","delivery":"leased_value",
            "request_fqn":"phoxal.component.actuator.v1.ActuatorCommand","max_items":1,"max_bytes":4096,
            "signature":{"endpoint":"actuator","service":"phoxal.component.actuator.v1.ActuatorCommand",
                "method":"actuator","shape":"call","request":"phoxal.component.actuator.v1.ActuatorCommand",
                "response":"google.protobuf.Empty","retained_latest":false,"lease_valid_for_ms":100}}],
        "outputs":[{"name":"status","port":"status","max_items":1,"max_bytes":4096,"bootstrap":true,
            "signature":{"endpoint":"status","service":"phoxal.tests.authoring.countdown.v1.CountdownState",
                "method":"status","shape":"observation","request":"google.protobuf.Empty",
                "response":"phoxal.tests.authoring.countdown.v1.CountdownState","retained_latest":true,"lease_valid_for_ms":null}}]
    }});
    use phoxal::artifact::bundle::{BundleComponent, InstanceRole};
    use phoxal::artifact::document::{ComponentDocument, ComponentModel};
    let simulation = serde_json::json!({
        "protocol": "phoxal.simulation.v1",
        "mode": "controlled",
        "model_identity": "countdown-sim-fixture",
        "quantum_ns": QUANTUM_NS,
        "providers": [{
            "rate_microhertz": RATE_MICROHERTZ,
            "service_fqn": "phoxal.tests.authoring.countdown.v1.CountdownState",
            "method": "status",
            "service_instance": "mission_countdown",
            "port": "status",
            "shape": "observation",
            "retained_latest": true,
            "lease_valid_for_ms": null,
            "input_fqn": "google.protobuf.Empty",
            "payload_fqn": "phoxal.tests.authoring.countdown.v1.CountdownState",
            "max_message_bytes": 4096,
            "max_buffered_items": 1
        }],
        // The brain's leased actuator projection is the native
        // actuation the simulator serves for the mission motor.
        "actuation_bindings": [{
            "service_instance": "brain",
            "port": "command",
            "payload_fqn": "phoxal.component.actuator.v1.ActuatorCommand",
            "actuator_ids": ["mission_countdown.motor"]
        }]
    });
    support::write_bundle(
        &root,
        "countdown-authoring",
        vec![
            ("brain", brain_artifact()),
            ("countdown", countdown_artifact()),
            ("native-driver", native_artifact),
        ],
        vec![
            ("brain", InstanceRole::Brain, "brain"),
            ("countdown", InstanceRole::Service, "countdown"),
            ("mission_countdown", InstanceRole::Driver, "native-driver"),
        ],
        vec![
            ("brain.countdown_finished", "countdown.finished"),
            ("brain.countdown_status", "mission_countdown.status"),
            ("brain.start_countdown", "countdown.start"),
            ("mission_countdown.actuator", "brain.command"),
        ],
        vec![BundleComponent {
            instance: "mission_countdown".to_owned(),
            driver: true,
            package: "phoxal-countdown-fixture".to_owned(),
            source: "fixture".to_owned(),
            mount_site: "mount".to_owned(),
            definition: ComponentDocument::V0 {
                model: ComponentModel {
                    file: "model.xml".into(),
                    root_body: "root".to_owned(),
                },
                capabilities: std::collections::BTreeMap::from([("motor".to_owned(), serde_json::from_value(serde_json::json!({"kind":"motor","target":{"kind":"actuator","id":"motor"}})).expect("native motor capability"))]),
                assets: Vec::new(),
            },
        }],
        Some(simulation),
    );
    TestBundle {
        _temporary_root: temporary_root,
        root,
    }
}

/// The brain consumer fixture's expected record, shared with that binary's
/// own unit test.
fn brain_artifact() -> serde_json::Value {
    serde_json::json!({
        "runtime": serde_json::to_value(brain_expected_record::expected_runtime_record())
            .expect("the expected brain record serializes")
    })
}

/// The countdown fixture's complete expected runtime record, shared with
/// the binary's own unit test.
fn countdown_artifact() -> serde_json::Value {
    serde_json::json!({
        "runtime": serde_json::to_value(expected_record::expected_runtime_record())
            .expect("the expected runtime record serializes")
    })
}
