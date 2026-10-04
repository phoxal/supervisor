//! Reading the supervisor's resolved manifest and admitting its graph.
//!
//! Opening a bundle decodes the one shared `phoxal/bundle/v0` manifest,
//! validates it through the shared pure admission, verifies every stored
//! executable path and resolved contract, and stops before launching
//! anything. The supervisor later launches only that admitted graph.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use phoxal::artifact::bundle::{AdmittedBundle, BundleManifest, EndpointReference, InstanceRole};

const MANIFEST_FILE: &str = "manifest.json";
const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

/// One admitted resolved bundle with its on-disk executables verified.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeBundle {
    root: PathBuf,
    admitted: AdmittedBundle,
    executables: BTreeMap<String, VerifiedExecutable>,
}

/// One instance's verified executable facts.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedExecutable {
    /// Bundle-relative executable path of the referenced artifact.
    pub(crate) relative: PathBuf,
    /// Resolved absolute executable path inside the bundle root.
    pub(crate) path: PathBuf,
}

impl RuntimeBundle {
    /// Decodes, validates, and verifies one resolved bundle.
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let root = root.canonicalize().with_context(|| {
            format!(
                "phoxal-supervisor takes a compiled bundle directory; {} is not one",
                root.display()
            )
        })?;
        if !root.is_dir() {
            bail!(
                "compiled bundle root is not a directory: {}",
                root.display()
            );
        }
        let manifest_path = root.join(MANIFEST_FILE);
        let bytes = bounded_file(&manifest_path, MAX_MANIFEST_BYTES)?;
        let manifest = serde_json::from_slice::<BundleManifest>(&bytes)
            .with_context(|| format!("{} is not a supported compiled bundle", root.display()))?;
        let admitted = AdmittedBundle::validate(manifest)
            .map_err(|message| anyhow::anyhow!("invalid runtime bundle: {message}"))?;
        let host = phoxal::artifact::bundle::host_execution_target();
        if admitted.target != host {
            bail!(
                "bundle targets {} but this supervisor runs on {host}",
                admitted.target
            );
        }
        let mut verified_artifacts = BTreeMap::new();
        for artifact in admitted.artifacts.values() {
            let relative = safe_relative_path(&artifact.path)?;
            let path = root.join(&relative);
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("bundle executable is missing: {}", path.display()))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                bail!(
                    "bundle executable is not a regular file: {}",
                    path.display()
                );
            }
            let canonical = path
                .canonicalize()
                .with_context(|| format!("cannot resolve bundle executable: {}", path.display()))?;
            if !canonical.starts_with(&root) {
                bail!("bundle executable escapes its root: {}", path.display());
            }
            // Executable contents are trusted: presence, suitability, and
            // confinement are checked, never digested.
            verified_artifacts.insert(
                artifact.id.clone(),
                VerifiedExecutable {
                    relative,
                    path: canonical,
                },
            );
        }
        let supervisor = root.join(safe_relative_path(&admitted.supervisor.path)?);
        let supervisor_metadata = fs::symlink_metadata(&supervisor)
            .with_context(|| format!("bundle supervisor is missing: {}", supervisor.display()))?;
        if !supervisor_metadata.is_file() || supervisor_metadata.file_type().is_symlink() {
            bail!(
                "bundle supervisor is not a regular file: {}",
                supervisor.display()
            );
        }
        let executables = admitted
            .instances
            .iter()
            .filter_map(|(id, instance)| {
                verified_artifacts
                    .get(&instance.artifact)
                    .cloned()
                    .map(|executable| (id.clone(), executable))
            })
            .collect();
        Ok(Self {
            root,
            admitted,
            executables,
        })
    }

    /// The admitted bundle root.
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Authored robot identity.
    pub(crate) fn robot_id(&self) -> &str {
        &self.admitted.robot_id
    }

    /// The immutable controlled-simulation definition, when carried.
    pub(crate) fn simulation(&self) -> Option<&phoxal::artifact::bundle::BundleSimulation> {
        self.admitted.simulation.as_ref()
    }

    /// The execution graph with typed endpoint references.
    pub(crate) fn connections(&self) -> &BTreeMap<EndpointReference, Vec<EndpointReference>> {
        &self.admitted.execution_connections
    }

    /// Every launch instance keyed by identity.
    pub(crate) fn instances(&self) -> &BTreeMap<String, phoxal::artifact::bundle::BundleInstance> {
        &self.admitted.instances
    }

    /// Every mounted component keyed by identity.
    pub(crate) fn components(
        &self,
    ) -> &BTreeMap<String, phoxal::artifact::bundle::BundleComponent> {
        &self.admitted.components
    }

    /// Component model source directories by instance.
    pub(crate) fn component_sources(&self) -> &BTreeMap<String, String> {
        &self.admitted.component_sources
    }

    /// Portable robot model resources.
    pub(crate) fn model(&self) -> Option<&phoxal::artifact::bundle::BundleModelAssets> {
        self.admitted.model.as_ref()
    }

    /// The verified executable one instance launches.
    pub(crate) fn executable(&self, instance: &str) -> Option<&VerifiedExecutable> {
        self.executables.get(instance)
    }

    /// Every launch instance with its verified executable, in stable order.
    pub(crate) fn executables_iter(&self) -> impl Iterator<Item = (&String, &VerifiedExecutable)> {
        self.executables.iter()
    }

    /// The canonical runtime record selected for one launch instance.
    pub(crate) fn runtime_record(
        &self,
        instance: &str,
    ) -> Option<&phoxal::artifact::RuntimeRecord> {
        self.admitted.instance_runtime(instance)
    }

    /// The launch role of one instance.
    pub(crate) fn instance_role(&self, instance: &str) -> Option<InstanceRole> {
        self.admitted.instances.get(instance).map(|i| i.role)
    }

    /// Apply simulation-only source bindings to this in-memory execution
    /// graph. The immutable manifest and its bytes remain unchanged.
    pub(crate) fn apply_simulation_bindings(
        &mut self,
        bindings: &[phoxal::artifact::simulation_run::SimulationBinding],
    ) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for binding in bindings {
            if binding.source_instance != "supervisor" {
                bail!(
                    "simulation binding for {}.{} has unsupported source instance `{}`",
                    binding.target_instance,
                    binding.signature.endpoint,
                    binding.source_instance
                );
            }
            if binding.signature.shape != phoxal::artifact::MethodShape::Call
                || binding.signature.lease_valid_for_ms.is_none()
            {
                bail!(
                    "simulation binding for {}.{} is not a leased generated call",
                    binding.target_instance,
                    binding.signature.endpoint
                );
            }
            let target = EndpointReference {
                instance: binding.target_instance.clone(),
                endpoint: binding.signature.endpoint.clone(),
            };
            if !seen.insert(target.clone()) {
                bail!("simulation run declares conflicting producers for `{target}`");
            }
            let replaces_authored_source =
                self.admitted.execution_connections.contains_key(&target);
            if binding.replaces_authored_source != replaces_authored_source {
                bail!(
                    "simulation binding for `{target}` records replaces_authored_source={}, but the immutable graph requires {}",
                    binding.replaces_authored_source,
                    replaces_authored_source
                );
            }
            let source = EndpointReference {
                instance: binding.source_instance.clone(),
                endpoint: binding.signature.endpoint.clone(),
            };
            self.admitted
                .execution_connections
                .insert(target, vec![source]);
        }
        Ok(())
    }
}

fn bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let link_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect bundle manifest {}", path.display()))?;
    if link_metadata.file_type().is_symlink() {
        bail!(
            "bundle manifest must not be a symbolic link: {}",
            path.display()
        );
    }
    let file = fs::File::open(path)
        .with_context(|| format!("cannot read bundle manifest {}", path.display()))?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("bundle manifest is not a regular file: {}", path.display());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        bail!("bundle manifest exceeds {maximum} bytes");
    }
    Ok(bytes)
}

fn safe_relative_path(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("executable path `{value}` is not bundle-relative");
    }
    Ok(path.to_owned())
}

/// One fixture executable for admission-level tests.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestExecutable {
    pub(crate) instance: String,
    pub(crate) role: &'static str,
    pub(crate) relative: String,
    pub(crate) artifact: Option<serde_json::Value>,
}

#[cfg(test)]
impl TestExecutable {
    pub(crate) fn for_test(instance: impl Into<String>, path: impl Into<String>) -> Self {
        let instance = instance.into();
        Self {
            role: if instance == "brain" {
                "brain"
            } else {
                "service"
            },
            instance,
            relative: path.into(),
            artifact: None,
        }
    }

    pub(crate) fn with_artifact(instance: impl Into<String>, artifact: serde_json::Value) -> Self {
        let artifact = normalize_fixture_artifact(artifact);
        let instance = instance.into();
        Self {
            role: if instance == "brain" {
                "brain"
            } else {
                "service"
            },
            relative: format!("bin/{instance}"),
            instance,
            artifact: Some(artifact),
        }
    }
}

#[cfg(test)]
impl RuntimeBundle {
    /// Builds one bundle directly from fixture executables and authored-form
    /// connections, bypassing on-disk verification for admission-level tests.
    pub(crate) fn for_test(root: &Path, robot_id: &str, executables: Vec<TestExecutable>) -> Self {
        Self::for_test_with_connections(root, robot_id, executables, BTreeMap::new())
    }

    pub(crate) fn for_test_with_connections(
        root: &Path,
        robot_id: &str,
        executables: Vec<TestExecutable>,
        connections: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        use phoxal::artifact::bundle::BundleInstance;
        let mut artifacts = BTreeMap::new();
        let mut instances = BTreeMap::new();
        let mut verified = BTreeMap::new();
        for executable in executables {
            let artifact_id = format!("artifact-{}", executable.instance);
            let runtime = executable
                .artifact
                .as_ref()
                .map(|artifact| {
                    let summary: phoxal::artifact::ArtifactSummary =
                        serde_json::from_value(artifact.clone())
                            .expect("fixture artifact contract decodes");
                    summary.runtime
                })
                .unwrap_or_else(minimal_runtime_record);
            artifacts.insert(
                artifact_id.clone(),
                phoxal::artifact::bundle::BundleArtifactRecord {
                    id: artifact_id.clone(),
                    path: executable.relative.clone(),
                    provenance: None,
                    runtime,
                    descriptors: vec![phoxal::artifact::DescriptorSummary {
                        sha256: "0".repeat(64),
                        bytes: 8,
                        files: vec!["fixture.proto".to_owned()],
                    }],
                },
            );
            instances.insert(
                executable.instance.clone(),
                BundleInstance {
                    id: executable.instance.clone(),
                    role: match executable.role {
                        "brain" => InstanceRole::Brain,
                        "driver" => InstanceRole::Driver,
                        _ => InstanceRole::Service,
                    },
                    artifact: artifact_id,
                    config: phoxal::artifact::bundle::InstanceConfig::absent(),
                },
            );
            verified.insert(
                executable.instance.clone(),
                VerifiedExecutable {
                    relative: PathBuf::from(&executable.relative),
                    path: root.join(&executable.relative),
                },
            );
        }
        let mut execution_connections = BTreeMap::new();
        for (consumer, sources) in connections {
            let consumer = EndpointReference::parse(&consumer)
                .unwrap_or_else(|message| panic!("fixture connection: {message}"));
            let sources = match sources {
                serde_json::Value::String(source) => vec![source],
                serde_json::Value::Array(sources) => sources
                    .into_iter()
                    .map(|source| {
                        source
                            .as_str()
                            .expect("fixture source is a string")
                            .to_owned()
                    })
                    .collect(),
                other => panic!("fixture connection sources must be strings: {other}"),
            };
            let sources = sources
                .iter()
                .map(|source| {
                    EndpointReference::parse(source)
                        .unwrap_or_else(|message| panic!("fixture source: {message}"))
                })
                .collect();
            execution_connections.insert(consumer, sources);
        }
        Self {
            root: root.to_owned(),
            admitted: AdmittedBundle {
                robot_id: robot_id.to_owned(),
                target: phoxal::artifact::bundle::host_execution_target(),
                supervisor: phoxal::artifact::bundle::BundleSupervisor {
                    path: "bin/supervisor".to_owned(),
                },
                artifacts,
                instances,
                execution_connections,
                components: BTreeMap::new(),
                component_sources: BTreeMap::new(),
                model: None,
                simulation: None,
            },
            executables: verified,
        }
    }

    pub(crate) fn set_simulation(
        &mut self,
        simulation: phoxal::artifact::bundle::BundleSimulation,
    ) {
        self.admitted.simulation = Some(simulation);
    }
}

/// Completes one fixture artifact with the fields its test does not care
/// about, so partial inline fixtures decode into the strict shared record
/// family without weakening that record's production validation.
#[cfg(test)]
pub(crate) fn normalize_fixture_artifact(mut artifact: serde_json::Value) -> serde_json::Value {
    let Some(runtime) = artifact
        .get_mut("runtime")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return artifact;
    };
    runtime
        .entry("schema")
        .or_insert_with(|| serde_json::json!("phoxal/artifact/v0"));
    runtime
        .entry("record")
        .or_insert_with(|| serde_json::json!("runtime"));
    runtime
        .entry("period_ms")
        .or_insert_with(|| serde_json::json!(20));
    runtime
        .entry("timeout_ms")
        .or_insert_with(|| serde_json::json!(100));
    runtime
        .entry("init_timeout_ms")
        .or_insert_with(|| serde_json::json!(1_000));
    runtime
        .entry("config_schema")
        .or_insert_with(|| serde_json::json!({"type": "object"}));
    for field in ["inputs", "outputs"] {
        runtime
            .entry(field)
            .or_insert_with(|| serde_json::json!([]));
    }
    let mut runtime = std::mem::take(runtime);
    for field in ["inputs", "outputs"] {
        let Some(records) = runtime
            .get_mut(field)
            .and_then(serde_json::Value::as_array_mut)
        else {
            continue;
        };
        for record in records.iter_mut() {
            let Some(record) = record.as_object_mut() else {
                continue;
            };
            if field == "inputs" {
                for (key, default) in [
                    ("delivery", serde_json::json!("observation_latest")),
                    ("max_age_ms", serde_json::Value::Null),
                    ("max_items", serde_json::Value::Null),
                    ("max_bytes", serde_json::Value::Null),
                    ("port", serde_json::Value::Null),
                    ("signature", serde_json::Value::Null),
                    ("request_fqn", serde_json::Value::Null),
                    ("response_fqn", serde_json::Value::Null),
                ] {
                    record.entry(key).or_insert_with(|| default.clone());
                }
            } else {
                for (key, default) in [
                    ("port", serde_json::Value::Null),
                    ("signature", serde_json::Value::Null),
                    ("max_items", serde_json::Value::Null),
                    ("max_bytes", serde_json::Value::Null),
                    ("max_request_bytes", serde_json::Value::Null),
                    ("every_steps", serde_json::Value::Null),
                    ("bootstrap", serde_json::json!(false)),
                    ("timeout_ms", serde_json::Value::Null),
                ] {
                    record.entry(key).or_insert_with(|| default.clone());
                }
            }
            if let Some(signature) = record
                .get_mut("signature")
                .and_then(serde_json::Value::as_object_mut)
            {
                for (key, default) in [
                    ("endpoint", serde_json::json!("fixture")),
                    ("service", serde_json::json!("fixture.Service")),
                    ("method", serde_json::json!("Fixture")),
                    ("shape", serde_json::json!("observation")),
                    ("request", serde_json::json!("google.protobuf.Empty")),
                    ("response", serde_json::json!("fixture.Payload")),
                    ("retained_latest", serde_json::json!(false)),
                    ("lease_valid_for_ms", serde_json::Value::Null),
                ] {
                    signature.entry(key).or_insert_with(|| default.clone());
                }
            }
        }
    }
    let artifact = artifact
        .as_object_mut()
        .expect("fixture artifact is an object");
    artifact.insert("runtime".to_owned(), serde_json::Value::Object(runtime));
    artifact.entry("descriptors").or_insert_with(|| {
        serde_json::json!([{
            "sha256": "0".repeat(64),
            "bytes": 8,
            "files": ["fixture.proto"],
        }])
    });
    serde_json::Value::Object(artifact.clone())
}

#[cfg(test)]
fn minimal_runtime_record() -> phoxal::artifact::RuntimeRecord {
    phoxal::artifact::RuntimeRecord::V0 {
        record: phoxal::artifact::RUNTIME_RECORD.to_owned(),
        conversions: Vec::new(),
        period_ms: 20,
        timeout_ms: 100,
        init_timeout_ms: 1_000,
        config_schema: serde_json::json!({"type": "object"}),
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_without_a_supported_manifest_is_not_a_bundle() {
        let dir = tempfile::tempdir().expect("temporary directory");
        std::fs::write(dir.path().join("robot.yaml"), "schema: phoxal/robot/v0\n")
            .expect("source fixture");
        let error = RuntimeBundle::open(dir.path()).expect_err("authored YAML is not a bundle");
        assert!(format!("{error:#}").contains("manifest.json"), "{error:#}");
    }

    #[test]
    fn an_unsupported_bundle_format_is_refused() {
        let dir = tempfile::tempdir().expect("temporary directory");
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema": "phoxal/bundle/vX",
                "robot_id": "fixture",
            }))
            .expect("unknown manifest"),
        )
        .expect("unknown manifest writes");
        let error = RuntimeBundle::open(dir.path()).expect_err("unknown format refuses");
        assert!(
            format!("{error:#}").contains("phoxal/bundle/v0"),
            "{error:#}"
        );
    }
}
