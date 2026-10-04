//! The supervisor process-boundary runtime binary.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Phoxal supports Linux and macOS only");

phoxal::api!();

mod contract;
mod reference_runtime;

use crate::reference_runtime::MARKER;
use phoxal::runtime::RuntimeLaunch;

fn main() -> phoxal::Result<()> {
    let bundle_root = RuntimeLaunch::parse()?.bundle_root;
    *MARKER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        Some(bundle_root.join("reference-runtime.marker"));
    phoxal::runtime::run::<crate::reference_runtime::ReferenceRuntime>()
}
