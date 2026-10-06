//! Acceptance through the actual supervisor executable and runtime child.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

#[path = "fixtures/runtime/src/contract.rs"]
mod contract;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use phoxal::communication::session::SupervisorState;
use phoxal::session::{Connection, ConnectionConfig, Supervisor, connect};

const STARTUP: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hardware_public_lease_reaches_generated_input_expires_and_withdraws() {
    use phoxal::session::CallOutcome;
    let bundle = build_bundle();
    let endpoint = format!(
        "unixsock-stream/{}",
        support::execution_dir(&bundle.root)
            .join("supervisor.sock")
            .display()
    );
    let mut child = support::SupervisorProcess::launch(&bundle.root, "lease-hardware");
    let connection = tokio::time::timeout(STARTUP, connect_when_bound(&endpoint, &mut child))
        .await
        .expect("public socket");
    let supervisor = connection
        .supervisor("lease-hardware")
        .await
        .expect("public session");
    tokio::time::timeout(STARTUP, wait_until_ready(&supervisor))
        .await
        .expect("ready deadline")
        .expect("ready");
    let executions = supervisor
        .management()
        .executions()
        .await
        .expect("execution inventory");
    let execution = supervisor
        .execution(&executions[0].execution_id)
        .await
        .expect("execution");
    let brain = execution.service("brain").await.expect("brain");
    let target = brain
        .method(phoxal::contracts::CallMethod::<
            contract::InspectionState,
            phoxal::contracts::Empty,
        >::new(
            "example.inspection.v1.InspectionState",
            "target",
            "target",
            "example.inspection.v1.InspectionState",
            "google.protobuf.Empty",
            Some(500),
            &[],
        ))
        .await
        .expect("leased target");
    let marker = bundle.root.join("leased-target.marker");
    for count in [42, 7] {
        assert!(matches!(
            target
                .call(
                    contract::InspectionState {
                        count,
                        active: true
                    },
                    STARTUP
                )
                .await
                .expect("public leased call"),
            CallOutcome::Received(_)
        ));
        tokio::time::timeout(STARTUP, async {
            while fs::read_to_string(&marker).ok().as_deref() != Some(&count.to_string()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("generated runtime accepts lease at its hardware clock");
        if count == 42 {
            tokio::time::timeout(STARTUP, async {
                while fs::read_to_string(&marker).ok().as_deref() != Some("absent") {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("lease expires without renewal");
        }
    }
    // This hardware path proves the public withdrawal reply and eventual absence.
    // The controlled process suite proves removal before expiry and renewal while live.
    assert!(matches!(
        target.withdraw(STARTUP).await.expect("explicit withdrawal"),
        CallOutcome::Received(_)
    ));
    tokio::time::timeout(STARTUP, async {
        while fs::read_to_string(&marker).ok().as_deref() != Some("absent") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("withdrawal removes the accepted lease");
    connection.close().await.expect("connection closes");
    child.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_runtime_crosses_supervisor_and_zenoh_before_termination() {
    let bundle = build_bundle();
    let root = bundle
        .root
        .canonicalize()
        .expect("bundle root canonicalizes");
    let socket = support::execution_dir(&root).join("supervisor.sock");
    let endpoint = format!("unixsock-stream/{}", socket.display());

    let mut supervisor = support::SupervisorProcess::launch(&root, "runtime-e2e");

    let connection =
        tokio::time::timeout(STARTUP, connect_when_bound(&endpoint, &mut supervisor)).await;
    let connection = connection.expect("the supervisor binds its public session endpoint");
    let supervisor_session = connection
        .supervisor("runtime-e2e")
        .await
        .expect("the supervisor accepts the public session");
    tokio::time::timeout(STARTUP, wait_until_ready(&supervisor_session))
        .await
        .expect("runtime reaches Ready before the startup deadline")
        .expect("runtime remains healthy while reaching Ready");
    tokio::time::timeout(STARTUP, wait_for_step(&root))
        .await
        .expect("runtime executes a bounded step")
        .expect("runtime step marker is readable");

    supervisor_session
        .close()
        .await
        .expect("the supervisor session closes cleanly");
    connection
        .close()
        .await
        .expect("the public connection closes cleanly");
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialization_failure_terminates_the_supervisor_during_admission() {
    let bundle = build_bundle();
    // The real authored initializer writes this marker. A directory at that
    // path makes initialization fail without a synthetic execution flag.
    fs::create_dir(bundle.root.join("reference-runtime.marker")).expect("block the marker write");
    let mut supervisor = support::SupervisorProcess::launch(&bundle.root, "failed-init");
    tokio::time::timeout(STARTUP, async {
        while !supervisor.is_finished() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a failed child terminates startup before its admission timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn termination_cancels_admission_and_reaps_an_unresponsive_child() {
    let bundle = build_bundle();
    // Trusted executable contents may change. This child intentionally
    // never speaks the admission protocol, so shutdown must cancel the wait.
    fs::write(bundle.root.join("bin/brain"), "#!/bin/sh\nexec sleep 120\n")
        .expect("stage an unresponsive child");
    let endpoint = format!(
        "unixsock-stream/{}",
        support::execution_dir(&bundle.root)
            .join("supervisor.sock")
            .display()
    );
    let mut supervisor = support::SupervisorProcess::launch(&bundle.root, "cancel-admission");
    let connection = tokio::time::timeout(STARTUP, connect_when_bound(&endpoint, &mut supervisor))
        .await
        .expect("the router binds during admission");
    supervisor.shutdown().await;
    connection.close().await.expect("close the connection");
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
        let config = ConnectionConfig::new(endpoint, "local", "runtime-e2e")
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
                anyhow::bail!("supervisor entered Failed before Runtime Ready")
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

async fn wait_for_step(root: &Path) -> phoxal::Result<()> {
    loop {
        if fs::read(root.join("reference-runtime.marker")).is_ok_and(|value| value == b"stepped") {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
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
        "runtime-reference",
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
