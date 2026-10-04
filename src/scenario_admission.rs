//! Simulation-run admission policy owned by the supervisor binary.

/// Launch mode requested for a bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScenarioLaunchMode {
    Controlled,
    Hardware,
}

/// Refuse a simulation run specification on a hardware launch before any
/// participant process starts.
pub(super) fn admit_simulation_run(
    mode: ScenarioLaunchMode,
    has_run_specification: bool,
) -> anyhow::Result<()> {
    if has_run_specification && matches!(mode, ScenarioLaunchMode::Hardware) {
        anyhow::bail!(
            "simulation run specifications are nondeployable and cannot be used for hardware launch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hardware_launch_refuses_a_run_specification() {
        let error =
            admit_simulation_run(ScenarioLaunchMode::Hardware, true).expect_err("hardware refusal");
        assert!(error.to_string().contains("nondeployable"));
    }

    #[test]
    fn controlled_launch_accepts_a_run_specification() {
        admit_simulation_run(ScenarioLaunchMode::Controlled, true).expect("controlled admission");
    }

    #[test]
    fn ordinary_hardware_bundle_is_accepted() {
        admit_simulation_run(ScenarioLaunchMode::Hardware, false).expect("ordinary hardware");
    }
}
