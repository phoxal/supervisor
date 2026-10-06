use clap::Parser;
use std::path::PathBuf;

/// Observe one compiled bundle's execution.
///
/// `version` is spelled out rather than left to clap's `#[command(version)]`
/// shorthand so the printed line is unambiguously this package's version, and
/// `name` is fixed so it stays `phoxal-supervisor <version>` however the binary
/// was invoked.
#[derive(Debug, Parser)]
#[command(
    name = "phoxal-supervisor",
    version = env!("CARGO_PKG_VERSION"),
    about = ABOUT,
    long_about = LONG_ABOUT,
)]
pub(super) struct Cli {
    /// The compiled bundle directory whose execution to observe.
    #[arg(value_name = "BUNDLE_ROOT")]
    pub(super) bundle_root: PathBuf,

    /// Explicit execution state directory owning sockets and locks. The
    /// supervisor never infers a project, release, or workspace location.
    #[arg(long, value_name = "STATE_DIR")]
    pub(super) state_dir: PathBuf,

    /// Deployment namespace governed by router authorization.
    #[arg(long, value_name = "SCOPE")]
    pub(super) scope: String,

    /// Stable supervisor target within the deployment namespace.
    #[arg(long, value_name = "ID")]
    pub(super) supervisor_id: String,

    /// Explicit router endpoint for this execution.
    #[arg(long, value_name = "ENDPOINT")]
    pub(super) listen: Option<String>,

    /// Internal machine-readable readiness handoff for local orchestration.
    #[arg(long, value_name = "PATH", hide = true)]
    pub(super) ready_file: Option<PathBuf>,

    /// Internal machine-readable scenario evidence handoff for local orchestration.
    #[arg(long, value_name = "PATH", hide = true)]
    pub(super) scenario_result: Option<PathBuf>,

    /// Simulation-only run specification. Hardware launch rejects this before
    /// starting the robot process graph.
    #[arg(long, value_name = "PATH", hide = true)]
    pub(super) simulation_run: Option<PathBuf>,

    /// Simulator-owned native implementation context, refused by hardware launches.
    #[arg(long, value_name = "PATH", hide = true)]
    pub(super) simulation_context: Option<PathBuf>,

    /// Command-scoped owner whose loss cancels the complete execution.
    #[arg(long, value_name = "PID", hide = true)]
    pub(super) owner_pid: Option<u32>,

    /// Launch mode the supervisor runs under. `controlled` is the
    /// default for local development; `hardware` is the on-robot launch.
    #[arg(
        long,
        value_name = "MODE",
        value_enum,
        default_value_t = LaunchModeArg::Controlled
    )]
    pub(super) launch_mode: LaunchModeArg,
}

/// Launch scheduling modes accepted by clap. Lives here so the
/// configuration layer stays free of supervisor runtime types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(super) enum LaunchModeArg {
    Controlled,
    Hardware,
}

impl From<LaunchModeArg> for crate::scenario_admission::ScenarioLaunchMode {
    fn from(value: LaunchModeArg) -> Self {
        match value {
            LaunchModeArg::Controlled => Self::Controlled,
            LaunchModeArg::Hardware => Self::Hardware,
        }
    }
}

const ABOUT: &str = "phoxal-supervisor - the Phoxal Framework execution observer";

const LONG_ABOUT: &str = "\
phoxal-supervisor - the Phoxal Framework execution observer

<BUNDLE_ROOT> is a compiled bundle directory: manifest.json, assets/, and bin/.
Build one with `cargo phoxal build`. The required --scope and --supervisor-id
select the installed public routing namespace; neither value comes from the
bundle. The supervisor runs the router, reports which of the robot's runtimes
are present, and retains their logs and telemetry. It admits and launches the selected runtime processes.

`--version` reports this supervisor package's own version. The linked framework version is available through an authenticated session information request.";
