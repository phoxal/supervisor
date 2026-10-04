//! Generates the consumer's robot API from the countdown provider's
//! prepared contract products, so the brain names generated provider
//! descriptors instead of hand-mirroring them.
fn main() -> Result<(), phoxal::build::Error> {
    println!("cargo:rerun-if-changed=.phoxal/prepared");
    phoxal::build::api(phoxal::build::BuildApiConfig::default())
}
