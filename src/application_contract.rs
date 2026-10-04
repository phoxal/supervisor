//! The supervisor's embedded application contract.
//!
//! The record is ordinary static Rust data with compile-time construction
//! and an explicit portable byte encoding, placed in a link section so a
//! build host can inspect a foreign-target application without executing
//! it. The human `--version` output reports release provenance only; this
//! record is the compatibility boundary.

use phoxal::artifact::application::{
    APPLICATION_RECORD_BYTES, ApplicationContract, BUNDLE_CONTRACT, EXECUTION_PROTOCOL_CONTRACT,
    SIMULATION_PROTOCOL_CONTRACT, SUPERVISOR_LAUNCH_CONTRACT, encode_application_contract,
};

/// The interfaces this supervisor actually consumes.
pub const APPLICATION_CONTRACT: ApplicationContract = ApplicationContract {
    bundle: Some(BUNDLE_CONTRACT),
    launch: SUPERVISOR_LAUNCH_CONTRACT,
    execution: Some(EXECUTION_PROTOCOL_CONTRACT),
    simulation: Some(SIMULATION_PROTOCOL_CONTRACT),
    target: phoxal::artifact::application::HOST_EXECUTION_TARGET,
};

/// The portable encoded record embedded in this executable.
#[used]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__phoxal_app"))]
#[cfg_attr(target_os = "linux", unsafe(link_section = ".phoxal_app"))]
static EMBEDDED_APPLICATION_CONTRACT: [u8; APPLICATION_RECORD_BYTES] =
    encode_application_contract(&APPLICATION_CONTRACT);

#[cfg(test)]
mod tests {
    use super::*;
    use phoxal::artifact::application::decode_application_contract;

    #[test]
    fn the_embedded_record_round_trips_through_the_portable_encoding() {
        let decoded = decode_application_contract(&EMBEDDED_APPLICATION_CONTRACT)
            .expect("the embedded record decodes");
        assert!(decoded.matches(&APPLICATION_CONTRACT));
    }
}
