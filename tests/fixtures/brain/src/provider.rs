//! The countdown provider's authored contract, included from the sibling
//! fixture so both binaries share the canonical `FinishedEvent` payload.

#[path = "../../countdown/src/contract.rs"]
mod contract;

pub use contract::*;
