//! Owned process group for supervisor executable acceptance.
//!
//! Each test target includes this module separately, so a helper is dead
//! code in every target that does not call it; the allow keeps the shared
//! module compilable for every consumer.

#![allow(dead_code)]

use std::{path::Path, path::PathBuf, time::Duration};

/// The application binary Cargo builds for this integration target.
pub fn supervisor_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_phoxal-supervisor"))
}

pub mod fixtures;

pub fn reference_runtime_artifact() -> serde_json::Value {
    serde_json::json!({
        "runtime": {
            "schema": "phoxal/artifact/v0",
            "record": "runtime",
            "period_ms": 20,
            "timeout_ms": 100,
            "init_timeout_ms": 1000,
            "config_schema": {"type": "null"},
            "inputs": [{
                "name": "read",
                "response_max_bytes": 4096,
                "response_max_items": 8,
                "delivery": "call_ingress",
                "max_items": 8,
                "max_bytes": 4096,
                "port": "read",
                "signature": {
                    "endpoint": "read",
                    "service": "example.inspection.v1.Read",
                    "method": "read",
                    "shape": "call",
                    "request": "example.inspection.v1.InspectionReadRequest",
                    "response": "example.inspection.v1.InspectionReadResponse",
                    "retained_latest": false,
                    "lease_valid_for_ms": null
                }
            }, {
                "name": "calibrate",
                "response_max_bytes": 1024,
                "response_max_items": 4,
                "delivery": "call_ingress",
                "max_items": 4,
                "max_bytes": 1024,
                "port": "calibrate",
                "signature": {
                    "endpoint": "calibrate",
                    "service": "example.inspection.v1.Calibrate",
                    "method": "calibrate",
                    "shape": "call",
                    "request": "google.protobuf.Empty",
                    "response": "google.protobuf.Empty",
                    "retained_latest": false,
                    "lease_valid_for_ms": null
                }
            }],
            "outputs": [
                {
                    "name": "status",
                                        "port": "status",
                    "max_items": 1,
                    "max_bytes": 4096,
                    "bootstrap": true,
                                        "signature": {
                        "endpoint": "status",
                        "service": "example.inspection.v1.InspectionState",
                        "method": "status",
                        "shape": "observation",
                        "request": "google.protobuf.Empty",
                        "response": "example.inspection.v1.InspectionState",
                        "retained_latest": true,
                        "lease_valid_for_ms": null
                    }
                },
                {
                    "name": "encoder",
                                        "port": "encoder",
                    "max_items": 16,
                    "max_bytes": 8192,
                    "bootstrap": false,
                                        "signature": {
                        "endpoint": "encoder",
                        "service": "phoxal.robotics.v1.EncoderSample",
                        "method": "encoder",
                        "shape": "observation",
                        "request": "google.protobuf.Empty",
                        "response": "phoxal.robotics.v1.EncoderSample",
                        "retained_latest": false,
                        "lease_valid_for_ms": null
                    }
                }
            ]
        }
    })
}

/// Constructs the current resolved manifest from the fixture's typed contracts.
pub fn write_bundle(
    root: &Path,
    robot_id: &str,
    artifacts: Vec<(&str, serde_json::Value)>,
    instances: Vec<(&str, phoxal::artifact::bundle::InstanceRole, &str)>,
    connections: Vec<(&str, &str)>,
    components: Vec<phoxal::artifact::bundle::BundleComponent>,
    simulation: Option<serde_json::Value>,
) {
    use phoxal::artifact::bundle::{
        AdmittedBundle, BundleArtifactRecord, BundleConnection, BundleInstance, BundleManifest,
        BundleSupervisor, InstanceConfig, host_execution_target,
    };
    std::fs::copy(supervisor_binary(), root.join("bin/supervisor"))
        .expect("copy the selected supervisor into the bundle");
    let manifest = BundleManifest::V0 {
        robot_id: robot_id.to_owned(),
        target: host_execution_target(),
        supervisor: BundleSupervisor {
            path: "bin/supervisor".to_owned(),
        },
        artifacts: artifacts
            .into_iter()
            .map(|(id, artifact)| BundleArtifactRecord {
                id: id.to_owned(),
                path: format!("bin/{id}"),
                provenance: None,
                runtime: serde_json::from_value(artifact["runtime"].clone())
                    .expect("decode the fixture runtime contract"),
                descriptors: Vec::new(),
            })
            .collect(),
        instances: instances
            .into_iter()
            .map(|(id, role, artifact)| BundleInstance {
                id: id.to_owned(),
                role,
                artifact: artifact.to_owned(),
                config: InstanceConfig::absent(),
            })
            .collect(),
        connections: connections
            .into_iter()
            .map(|(consumer, source)| BundleConnection {
                consumer: phoxal::artifact::bundle::EndpointReference::parse(consumer)
                    .expect("consumer endpoint"),
                sources: vec![
                    phoxal::artifact::bundle::EndpointReference::parse(source)
                        .expect("source endpoint"),
                ],
            })
            .collect(),
        components,
        component_sources: Default::default(),
        model: None,
        simulation: simulation
            .map(|value| serde_json::from_value(value).expect("simulation contract")),
    };
    AdmittedBundle::validate(manifest.clone()).expect("admit the fixture's resolved bundle");
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize the resolved manifest"),
    )
    .expect("write the resolved manifest");
}

pub fn execution_dir(bundle: &Path) -> PathBuf {
    bundle.with_file_name("execution-state")
}

pub struct SupervisorProcess {
    child: tokio::process::Child,
    group: i32,
}

impl SupervisorProcess {
    pub fn launch(bundle: &Path, id: &str) -> Self {
        let mut command = tokio::process::Command::new(supervisor_binary());
        command
            .arg(bundle)
            .arg("--state-dir")
            .arg(execution_dir(bundle))
            .args(["--scope", "local", "--supervisor-id", id]);
        command.process_group(0).kill_on_drop(true);
        let child = command.spawn().expect("launch supervisor executable");
        let group = child.id().expect("live supervisor PID") as i32;
        Self { child, group }
    }

    pub fn is_finished(&mut self) -> bool {
        self.child
            .try_wait()
            .expect("query supervisor exit")
            .is_some()
    }

    pub async fn shutdown(&mut self) {
        // SAFETY: this positive PID belongs to the live child retained by this guard.
        assert_eq!(unsafe { libc::kill(self.group, libc::SIGTERM) }, 0);
        let status = tokio::time::timeout(Duration::from_secs(15), self.child.wait())
            .await
            .expect("bounded supervisor shutdown")
            .expect("reap supervisor");
        assert!(status.success(), "supervisor shutdown failed: {status}");
    }
}

impl Drop for SupervisorProcess {
    fn drop(&mut self) {
        // SAFETY: the child was started in its own process group. Kill only that group,
        // including runtime children, when an assertion unwinds before normal shutdown.
        unsafe {
            libc::kill(-self.group, libc::SIGKILL);
        }
    }
}
