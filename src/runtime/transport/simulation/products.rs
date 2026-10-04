//! Closed observation and actuation membership validation.

use super::authority::{simulation_adapter_error, simulation_rejected};
use super::*;

pub(super) fn canonical_membership_digest(
    memberships: &[phoxal::communication::simulation::ProductMembership],
) -> Result<[u8; 32], PublicTransportError> {
    let mut encoded = memberships
        .iter()
        .map(Message::encode_to_vec)
        .collect::<Vec<_>>();
    encoded.sort();
    let mut hasher = Sha256::new();
    for member in encoded {
        let length = u64::try_from(member.len())
            .map_err(|_| simulation_rejected("simulation membership is too large"))?;
        hasher.update(length.to_be_bytes());
        hasher.update(member);
    }
    Ok(hasher.finalize().into())
}

pub(super) fn observation_memberships(
    observations: &[phoxal::communication::simulation::Observation],
) -> Result<Vec<phoxal::communication::simulation::ProductMembership>, PublicTransportError> {
    observations
        .iter()
        .map(|observation| {
            observation
                .membership
                .clone()
                .ok_or_else(|| simulation_rejected("simulation observation has no membership"))
        })
        .collect()
}

pub(super) fn validate_actuation_cut(
    actuation: &[phoxal::communication::simulation::Actuation],
    prepared_boundary: u64,
) -> Result<Vec<phoxal::communication::simulation::ProductMembership>, PublicTransportError> {
    let mut memberships = Vec::with_capacity(actuation.len());
    let mut seen = BTreeSet::new();
    for item in actuation {
        let membership = item
            .membership
            .clone()
            .ok_or_else(|| simulation_rejected("simulation actuator is missing its membership"))?;
        let digest: [u8; 32] = Sha256::digest(&item.payload).into();
        let complete = !membership.producer.is_empty()
            && !membership.port.is_empty()
            && !membership.producer_incarnation.is_empty()
            && membership.sequence != 0
            && membership.capture_boundary <= prepared_boundary
            && membership.disposition
                == phoxal::communication::simulation::ProductDisposition::Present
            && membership.item_count == 1
            && membership.encoded_bytes == item.payload.len() as u64
            && membership.payload_digest == digest
            && item.valid_until_ns > membership.capture_time_ns
            && seen.insert((membership.producer.clone(), membership.port.clone()));
        if !complete {
            return Err(simulation_rejected(
                "simulation actuator membership is incomplete or duplicated",
            ));
        }
        memberships.push(membership);
    }
    Ok(memberships)
}

pub(super) async fn validate_observation_cut(
    observations: &[phoxal::communication::simulation::Observation],
    expected_boundary: u64,
    definition: &crate::runtime::adapter::SimulationDefinition,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    execution_id: &str,
    limits: (usize, usize),
    supplied_digest: &[u8],
) -> Result<[u8; 32], PublicTransportError> {
    let (max_product_bytes, max_cut_bytes) = limits;
    let memberships = observation_memberships(observations)?;
    let expected = definition
        .providers()
        .iter()
        .map(|provider| (provider.service_instance(), provider.port()))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut cut_bytes = 0_usize;
    let adapter_guard = adapter.lock().await;
    for (observation, membership) in observations.iter().zip(&memberships) {
        if membership.producer.is_empty()
            || membership.port.is_empty()
            || membership.producer_incarnation.is_empty()
            || membership.sequence == 0
            || membership.capture_boundary != expected_boundary
            || expected_boundary.checked_mul(definition.quantum_ns())
                != Some(membership.capture_time_ns)
            || membership.payload_digest.len() != 32
            || !seen.insert((membership.producer.as_str(), membership.port.as_str()))
        {
            return Err(simulation_rejected(
                "simulation observation membership is incomplete or duplicated",
            ));
        }
        let disposition = membership.disposition;
        if disposition == phoxal::communication::simulation::ProductDisposition::Unspecified {
            return Err(simulation_rejected(
                "simulation observation disposition is unspecified",
            ));
        }
        let provider = definition
            .providers()
            .iter()
            .find(|provider| {
                provider.service_instance() == membership.producer
                    && provider.port() == membership.port
            })
            .ok_or_else(|| simulation_rejected("observation has no agreed provider"))?;
        let due = provider.due(expected_boundary, definition.quantum_ns());
        if due == (disposition == phoxal::communication::simulation::ProductDisposition::NotDue) {
            return Err(simulation_rejected(
                "observation disposition disagrees with the immutable source cadence",
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&observation.payload).into();
        if membership.payload_digest != digest
            || membership.encoded_bytes != observation.payload.len() as u64
        {
            return Err(simulation_rejected(
                "simulation observation payload digest or byte count is inconsistent",
            ));
        }
        match disposition {
            phoxal::communication::simulation::ProductDisposition::Present
                if membership.item_count == 0 =>
            {
                return Err(simulation_rejected(
                    "present simulation observations must contain an item",
                ));
            }
            phoxal::communication::simulation::ProductDisposition::Empty
            | phoxal::communication::simulation::ProductDisposition::NotDue
                if membership.item_count != 0 || !observation.payload.is_empty() =>
            {
                return Err(simulation_rejected(
                    "empty or not-due observations must have no payload",
                ));
            }
            _ => {}
        }
        let payload_bytes = usize::try_from(membership.encoded_bytes)
            .map_err(|_| simulation_rejected("simulation observation byte count overflows"))?;
        if payload_bytes > max_product_bytes {
            return Err(simulation_rejected(
                "simulation observation exceeds its negotiated product byte cap",
            ));
        }
        cut_bytes = cut_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| simulation_rejected("simulation observation cut overflows"))?;
        let metadata = adapter_guard
            .validate_simulation_observation(execution_id, &membership.producer, &membership.port)
            .map_err(|error| simulation_adapter_error("simulation", error))?;
        if payload_bytes > usize::try_from(metadata.max_message_bytes).unwrap_or(usize::MAX) {
            return Err(simulation_rejected(
                "simulation observation exceeds its generated port byte bound",
            ));
        }
    }
    if seen != expected {
        return Err(simulation_rejected(
            "simulation observations do not contain the complete required provider set",
        ));
    }
    let digest = canonical_membership_digest(&memberships)?;
    if supplied_digest != digest {
        return Err(simulation_rejected(
            "simulation observation membership digest is inconsistent",
        ));
    }
    let encoded_len = observations
        .iter()
        .map(Message::encoded_len)
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| simulation_rejected("simulation observation cut overflows"))?;
    if cut_bytes > max_cut_bytes || encoded_len > max_cut_bytes {
        return Err(simulation_rejected(
            "simulation observation cut exceeds its negotiated byte cap",
        ));
    }
    Ok(digest)
}

pub(super) fn normalize_receipt(
    receipt: &mut Option<phoxal::communication::simulation::CutReceipt>,
    admission: &SimulationPhaseAdmission,
    correlation_id: &[u8],
    status: phoxal::communication::simulation::PhaseStatus,
    memberships: Vec<phoxal::communication::simulation::ProductMembership>,
    admitted_observation_boundary: u64,
) -> Result<[u8; 32], PublicTransportError> {
    let digest = canonical_membership_digest(&memberships)?;
    let receipt = receipt
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation backend omitted its phase receipt"))?;
    receipt.transition_key = Some(admission.transition_key.clone());
    receipt.correlation_id = correlation_id.to_vec();
    receipt.membership_digest = digest.to_vec();
    receipt.products = memberships;
    receipt.prepared_boundary = admission.transition_key.boundary;
    receipt.admitted_observation_boundary = admitted_observation_boundary;
    receipt.status = status;
    Ok(digest)
}
