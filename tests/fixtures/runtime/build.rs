//! Generates this component's derived standard endpoint fragment beside the
//! authored Runtime contract.

fn main() -> Result<(), phoxal::build::Error> {
    phoxal::build::api(phoxal::build::BuildApiConfig::default())
}
