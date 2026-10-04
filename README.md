# Phoxal Supervisor

The standalone application that launches an admitted robot bundle, coordinates execution, and serves lifecycle and session management.
Shared wire contracts and pure bundle validation belong to the framework SDK.
Linux and macOS are supported.
Windows and other operating systems are unsupported and unqualified.

A robot selects this application with `supervisor.source` in `robot.yaml`, using the same local path or full pinned Git source format as its participants.
The developer tool acquires that selection and stages the application in the bundle.
The robot's Cargo manifest contains genuine Rust library dependencies, not a supervisor dependency.
Hardware preparation checks bundle, launch, execution, and target interfaces.
Simulation preparation additionally requires the simulation interface.
Application compatibility is determined by its embedded interface record and target, independent of package versions.
Executable contents are trusted.

Launch a relocated bundle with an explicit state directory:

```sh
phoxal-supervisor /path/to/bundle \
  --state-dir /path/to/state --scope hardware --supervisor-id local \
  --launch-mode hardware
```

The directory is used literally for the lock and socket; no project or release-directory inference is required.
A hardware launch refuses a simulation run specification before starting children.

Run the application's unit suite with `cargo test`.

## Maintainer qualification

The repository is one root binary package; fixture programs are standalone inputs under `tests/fixtures/`.
Private unit tests stay with their implementation, process assertions live under `tests/`, and bounded process/fixture helpers live under `tests/support/`.
SDK contract attachment tests belong to framework.
The application binary is selected by Cargo through `CARGO_BIN_EXE_phoxal-supervisor`, not a stale manually built executable.

Run deterministic unit checks with `cargo test`.
For host acceptance, select a compatible public tool explicitly and run:

```sh
PHOXAL_TOOL=/path/to/cargo-phoxal cargo test --features host-acceptance
```

The harness copies fixture programs into temporary directories, builds them from explicit independent manifests, and prepares brain/composition projects through that tool.
It uses the SDK actually selected by this application, and propagates a local development overlay only when Cargo selected one.
The ordinary registry-backed path does not require a sibling checkout.
The test-owned acquisition cache is under Cargo's target tree and uses ordinary tool lifecycle locks.
All process waits and cleanup remain bounded.
The process qualification workflow runs this host lane separately from deterministic CI.
Strict CI and release qualification use `--locked` to require an up-to-date committed lockfile.

## Publication

Review release-plz version and generated changelog PRs before publication.
Successful main-branch CI enables the standard crates.io release job.
The application consumes the released SDK through ordinary public dependencies without sibling checkouts or local overlays.
Application versions are independent of SDK versions; compatibility follows the actual interfaces required by the operation.
