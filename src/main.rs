//! Launch the execution supervisor for one immutable bundle.
#![allow(unused_imports, dead_code)]

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

mod config;
mod rendezvous;
mod runtime;
mod scenario_admission;
mod transport;

use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod application_contract;

use application_contract::APPLICATION_CONTRACT;

/// Multi-thread: Zenoh refuses to run on Tokio's current-thread scheduler, and
/// the router runs in this process.
#[tokio::main]

async fn main() -> ExitCode {
    // Parse first: help, version, and misuse all end the process without ever
    // needing a subscriber installed.
    let cli = config::Cli::parse();
    let target = match phoxal::communication::DeploymentTarget::new(cli.scope, cli.supervisor_id) {
        Ok(target) => target,
        Err(error) => {
            eprintln!("phoxal-supervisor: invalid deployment identity: {error}");
            return ExitCode::from(2);
        }
    };
    init_tracing();
    let launch_mode: scenario_admission::ScenarioLaunchMode = cli.launch_mode.into();
    let state_dir = cli.state_dir.clone();
    match runtime::run(runtime::RunRequest {
        requested_root: &cli.bundle_root,
        state_dir: &state_dir,
        target,
        ready_file: cli.ready_file.as_deref(),
        scenario_result: cli.scenario_result.as_deref(),
        simulation_run: cli.simulation_run.as_deref(),
        owner_pid: cli.owner_pid,
        listen: cli.listen.as_deref(),
        launch_mode,
    })
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Stderr is the supervisor's diagnostic channel under systemd,
            // where it is the journal. One rendered chain, not a panic.
            eprintln!("phoxal-supervisor: {error:#}");
            ExitCode::from(1)
        }
    }
}

/// The supervisor's own diagnostics, on stderr.
///
/// Under systemd stderr is the journal; interactively it is the terminal the
/// operator launched from, and locally `phoxal` captures it to a file. Either
/// way it is the only channel the supervisor has before the bus exists, and it
/// is deliberately not the bus log stream: the process that retains everyone
/// else's records cannot also be a client of its own retention.
///
/// The default quietens the transport rather than the supervisor: Zenoh's own
/// info-level chatter says nothing about this robot, and the unix-socket link
/// warns on every ordinary participant disconnect. `RUST_LOG` replaces the
/// whole default when it is set.
fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
}

const DEFAULT_LOG_FILTER: &str = "info,zenoh=warn,zenoh_link_unixsock_stream=error";
