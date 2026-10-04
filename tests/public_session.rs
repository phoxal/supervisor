//! Acceptance through the actual supervisor executable and runtime child.

#![allow(clippy::expect_used, clippy::unwrap_used)]
#![recursion_limit = "256"]

mod support;

#[path = "fixtures/runtime/src/contract.rs"]
mod contract;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use contract::{InspectionApi, InspectionReadRequest};
use phoxal::session::{CallOutcome, Connection, ConnectionConfig, ObservationItem, connect};

/// How long the supervisor is given to bind its socket. Binding is synchronous
/// inside `host::run`, so this is slack for the compile-time-sized fixture
/// staging around it rather than a readiness poll budget.
const STARTUP: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_attaches_to_a_live_supervisor() {
    let bundle = build_bundle();
    let root = bundle
        .root
        .canonicalize()
        .expect("the staged bundle root resolves");
    let socket = support::execution_dir(&root).join("supervisor.sock");

    let mut supervisor = support::SupervisorProcess::launch(&root, "local");

    let endpoint = format!("unixsock-stream/{}", socket.display());
    let connection = tokio::time::timeout(STARTUP, connect_when_bound(&endpoint, &mut supervisor))
        .await
        .expect("the supervisor binds its socket");
    let supervisor_session = connection
        .supervisor("local")
        .await
        .expect("the supervisor accepts the public session");
    let info = supervisor_session
        .info()
        .await
        .expect("the supervisor info answers");
    assert!(
        !info.framework_version.is_empty() && !info.supervisor_version.is_empty(),
        "the supervisor reports its framework and package versions"
    );
    tokio::time::timeout(STARTUP, async {
        loop {
            assert!(
                !supervisor.is_finished(),
                "supervisor exited during admission"
            );
            let status = supervisor_session
                .management()
                .status()
                .await
                .expect("public status");
            match status.state {
                phoxal::communication::session::SupervisorState::Ready => break,
                phoxal::communication::session::SupervisorState::Failed => {
                    panic!("admission failed: {:?}", status.detail)
                }
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("runtime admission reaches Ready");
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
        .expect("select admitted execution");
    let brain = execution
        .service("brain")
        .await
        .expect("select brain service instance");

    let status = brain
        .method(InspectionApi::STATUS)
        .await
        .expect("bind generated observation method");
    let mut observations = status.observe().await.expect("start observation");
    tokio::time::timeout(STARTUP, async {
        loop {
            let observation = observations
                .recv()
                .await
                .expect("observation stream remains open")
                .expect("observation decodes");
            match observation {
                ObservationItem::InitialAbsent { .. } => continue,
                ObservationItem::Value { value, .. } => {
                    assert!(value.active);
                    break;
                }
                unexpected => panic!("unexpected observation record: {unexpected:?}"),
            }
        }
    })
    .await
    .expect("the generated runtime observation arrives");

    let read = brain
        .method(InspectionApi::READ)
        .await
        .expect("bind generated call method");
    let outcome = read
        .call(
            InspectionReadRequest {
                key: "status".to_owned(),
            },
            STARTUP,
        )
        .await
        .expect("call transport completes");
    assert!(
        matches!(
            outcome,
            CallOutcome::Received(response)
                if response.state.is_some_and(|state| state.active && state.count > 0)
        ),
        "the generated call completes through the real runtime"
    );

    // The derived standard encoder observation — spliced in from this
    // component's declared capability beside the authored contract —
    // delivers live samples through the supervisor.
    let encoder = brain
        .method(phoxal::contracts::ObservationMethod::<
            phoxal::contracts::component::encoder::EncoderSample,
        >::new(
            "phoxal.robotics.v1.EncoderSample",
            "encoder",
            "encoder",
            "google.protobuf.Empty",
            "phoxal.robotics.v1.EncoderSample",
            false,
            None,
            &[],
        ))
        .await
        .expect("bind the derived standard encoder method");
    let mut samples = encoder.observe().await.expect("observe encoder samples");
    tokio::time::timeout(STARTUP, async {
        loop {
            let sample = samples
                .recv()
                .await
                .expect("encoder stream remains open")
                .expect("encoder sample decodes");
            match sample {
                ObservationItem::InitialAbsent { .. } => continue,
                ObservationItem::Value { value, .. } => {
                    assert!(
                        value.position_rad.is_some() && value.velocity_radps.is_some(),
                        "the component publishes live encoder measurements: {value:?}"
                    );
                    break;
                }
                unexpected => panic!("unexpected encoder record: {unexpected:?}"),
            }
        }
    })
    .await
    .expect("the standard component observation arrives");

    // The component-specific calibrate operation replies through the same
    // supervisor session as the standard surface.
    let calibrate = brain
        .method(InspectionApi::CALIBRATE)
        .await
        .expect("bind the custom calibrate method");
    let outcome = calibrate
        .call(phoxal::contracts::Empty {}, STARTUP)
        .await
        .expect("calibrate transport completes");
    assert!(
        matches!(outcome, CallOutcome::Received(phoxal::contracts::Empty {})),
        "the custom operation replies through the real runtime: {outcome:?}"
    );
    supervisor_session
        .close()
        .await
        .expect("the supervisor session closes cleanly");
    connection
        .close()
        .await
        .expect("the connection closes cleanly");
    supervisor.shutdown().await;
}

/// Connect as soon as the supervisor is listening.
///
/// The supervisor is started in-process and this is the only wait in the test:
/// there is no readiness contract to poll, because a bound socket *is* the
/// readiness - `host::run` binds synchronously and fails the run otherwise.
async fn connect_when_bound(
    endpoint: &str,
    supervisor: &mut support::SupervisorProcess,
) -> Connection {
    loop {
        assert!(
            !supervisor.is_finished(),
            "the supervisor exited before it was reachable at {endpoint}"
        );
        let config = ConnectionConfig::new(endpoint, "local", "session-attach-test")
            .expect("the session config is valid");
        match connect(config).await {
            Ok(connection) => return connection,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

struct TestBundle {
    _temporary_root: tempfile::TempDir,
    root: PathBuf,
}

fn build_bundle() -> TestBundle {
    let temporary_root = tempfile::tempdir().expect("temporary bundle root");
    let root = temporary_root.path().join("bundle");
    fs::create_dir_all(root.join("bin")).expect("bundle bin directory");

    let source = support::fixtures::fixture_binary("runtime", "supervisor-test-runtime");
    let executable = root.join("bin/brain");
    fs::copy(source, &executable).expect("copy compiled Runtime fixture");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))
        .expect("make compiled Runtime fixture executable");
    support::write_bundle(
        &root,
        "session-attachment",
        vec![("brain", support::reference_runtime_artifact())],
        vec![(
            "brain",
            phoxal::artifact::bundle::InstanceRole::Brain,
            "brain",
        )],
        vec![],
        vec![],
        None,
    );
    TestBundle {
        _temporary_root: temporary_root,
        root,
    }
}
