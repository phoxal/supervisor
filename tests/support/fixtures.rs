//! Standalone sample programs staged away from the authored checkout.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

fn target() -> PathBuf {
    super::supervisor_binary()
        .parent()
        .expect("application binary directory")
        .parent()
        .expect("Cargo target directory")
        .to_owned()
}

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).expect("fixture directory");
    for entry in std::fs::read_dir(source).expect("authored fixture inputs") {
        let entry = entry.expect("fixture entry");
        if matches!(entry.file_name().to_str(), Some("target" | ".phoxal")) {
            continue;
        }
        let path = destination.join(entry.file_name());
        if entry.file_type().expect("fixture type").is_dir() {
            copy_tree(&entry.path(), &path);
        } else {
            std::fs::copy(entry.path(), path).expect("fixture input copy");
        }
    }
}

fn successful(output: Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stage() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("isolated fixture projects");
    let owner = Path::new(env!("CARGO_MANIFEST_DIR"));
    copy_tree(&owner.join("tests/fixtures"), root.path());

    // Qualify precisely the SDK this application resolved. A local owner
    // overlay is propagated only when Cargo actually selected path packages;
    // published SDK builds use the fixtures' ordinary registry dependencies.
    let metadata = cargo()
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(owner.join("Cargo.toml"))
        .current_dir(owner)
        .output()
        .expect("selected SDK metadata");
    assert!(
        metadata.status.success(),
        "SDK metadata: {}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata.stdout).expect("Cargo metadata JSON");
    let mut patch = String::from("\n[patch.phoxal]\n");
    let mut local = false;
    for package in metadata["packages"].as_array().expect("Cargo packages") {
        let name = package["name"].as_str().expect("package name");
        if matches!(name, "phoxal" | "phoxal-build" | "phoxal-macros")
            && package["source"].is_null()
        {
            let path = Path::new(package["manifest_path"].as_str().expect("SDK manifest"))
                .parent()
                .expect("SDK package directory");
            patch.push_str(&format!("{name} = {{ path = {:?} }}\n", path));
            local = true;
        }
    }
    for entry in std::fs::read_dir(root.path()).expect("staged programs") {
        let program = entry.expect("staged program").path();
        if local {
            let config = program.join(".cargo/config.toml");
            let original =
                std::fs::read_to_string(&config).expect("fixture registry configuration");
            std::fs::write(config, original + &patch).expect("selected local SDK overlay");
        }
        for name in ["robot.yaml", "robot.substitution.yaml"] {
            let file = program.join(name);
            if file.is_file() {
                let mut document: serde_yaml::Value =
                    serde_yaml::from_str(&std::fs::read_to_string(&file).expect("fixture robot"))
                        .expect("robot YAML");
                document["supervisor"]["source"]["path"] =
                    serde_yaml::Value::String("../supervisor".to_owned());
                std::fs::write(
                    file,
                    serde_yaml::to_string(&document).expect("fixture robot YAML"),
                )
                .expect("isolated supervisor selection");
            }
        }
    }
    // Keep authored paths relative while selecting this actual application,
    // rather than creating a second supervisor fixture implementation.
    std::os::unix::fs::symlink(owner, root.path().join("supervisor"))
        .expect("owning application source selection");
    root
}

fn tool(program: &Path) -> Command {
    let mut command =
        Command::new(std::env::var_os("PHOXAL_TOOL").unwrap_or_else(|| "cargo-phoxal".into()));
    command
        .current_dir(program)
        .env("CARGO_TARGET_DIR", target());
    // This test-owned installation cache is coordinated by the application's
    // ordinary acquisition locks, not a second fixture compatibility store.
    command.env("PHOXAL_HOME", target().join("supervisor-test-home"));
    command
}

pub fn fixture_binary(program: &str, binary: &str) -> PathBuf {
    let stage = stage();
    let source = stage.path().join(program);
    if source.join("robot.yaml").is_file() {
        successful(
            tool(&source)
                .arg("prepare")
                .output()
                .expect("public fixture preparation"),
            "fixture preparation",
        );
    }
    successful(
        cargo()
            .args(["build", "--manifest-path"])
            .arg(source.join("Cargo.toml"))
            .current_dir(&source)
            .env("CARGO_TARGET_DIR", target())
            .output()
            .expect("fixture Cargo build"),
        "fixture build",
    );
    // These unit checks validate the fake countdown/brain behavior relied on
    // by the process assertions. Generic SDK attachment checks live in the
    // framework suite instead.
    if matches!(program, "countdown" | "brain") {
        successful(
            cargo()
                .args(["test", "--bin", binary, "--manifest-path"])
                .arg(source.join("Cargo.toml"))
                .current_dir(&source)
                .env("CARGO_TARGET_DIR", target())
                .output()
                .expect("fixture sanity tests"),
            "fixture sanity tests",
        );
    }
    let path = target().join("debug").join(binary);
    assert!(path.is_file(), "fixture binary {}", path.display());
    path
}

pub fn composition_bundle(substitution: bool) -> (tempfile::TempDir, PathBuf) {
    let stage = stage();
    let source = stage.path().join("composition");
    if substitution {
        std::fs::copy(
            source.join("robot.substitution.yaml"),
            source.join("robot.yaml"),
        )
        .expect("isolated substituted composition");
    }
    let bundle = stage.path().join("bundle");
    successful(
        tool(&source)
            .args(["build", "--output"])
            .arg(&bundle)
            .output()
            .expect("public bundle build"),
        "composition build",
    );
    (stage, bundle)
}
