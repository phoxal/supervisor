//! Admission of complete simulation transitions.

use super::authority::{
    authorize_live_simulation_grant, authorize_simulation_grant, release_backend_authority,
    simulation_adapter_error, simulation_backend_error, simulation_rejected,
    validate_simulation_correlation,
};
use super::products::*;
use super::*;

pub(crate) async fn begin_simulation_phase(
    route: &PublicRoute,
    transition_key: &TransitionKey,
    correlation_id: &[u8],
    operation: PublicOperation,
    context: PhaseAdmissionContext<'_>,
    now_ms: u64,
) -> Result<SimulationPhaseAdmission, PublicTransportError> {
    let PhaseAdmissionContext {
        adapter,
        authority,
        backend: simulation_backend,
    } = context;
    validate_simulation_correlation(correlation_id)?;
    if transition_key.session_id.is_empty()
        || transition_key.execution_id.is_empty()
        || transition_key.timeline_id.is_empty()
        || transition_key.authority_grant.is_empty()
        || transition_key.operation_sequence == 0
    {
        return Err(simulation_rejected(
            "simulation transition key is incomplete",
        ));
    }
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
    authorize_simulation_grant(route, &transition_key.authority_grant, current)?;
    if Instant::now() >= current.lease_deadline {
        let expired = guard.take();
        drop(guard);
        if let Some(expired) = expired {
            release_backend_authority(expired, simulation_backend).await;
            return Err(simulation_rejected("simulation authority lease expired"));
        }
        return Err(simulation_rejected("simulation authority lease expired"));
    }
    if let Some(failure) = &current.failure {
        return Err(simulation_rejected(failure));
    }
    if current.active_phase.is_some() {
        return Err(simulation_rejected("simulation phase is still executing"));
    }
    if current.resetting {
        return Err(simulation_rejected("simulation reset is in progress"));
    }
    if transition_key.session_id != current.session_id
        || transition_key.execution_id != current.execution_id
        || transition_key.timeline_id != current.timeline_id
    {
        return Err(simulation_rejected(
            "simulation transition identity does not match authority",
        ));
    }
    if transition_key.operation_sequence <= current.accepted_sequence_watermark {
        return Err(simulation_rejected(
            "simulation operation sequence is stale",
        ));
    }
    if transition_key.operation_sequence
        != current
            .accepted_sequence_watermark
            .checked_add(1)
            .ok_or_else(|| simulation_rejected("simulation operation sequence is exhausted"))?
    {
        return Err(simulation_rejected(
            "simulation operation sequence is out of order",
        ));
    }
    if current
        .in_flight
        .as_ref()
        .is_some_and(|key| key != transition_key)
    {
        return Err(simulation_rejected(
            "another simulation transition is already in flight",
        ));
    }
    match operation {
        PublicOperation::AdmitInitialObservations => {
            if transition_key.boundary != 0
                || current.boundary != 0
                || current.accepted_sequence_watermark != 0
                || current.in_flight.is_some()
            {
                return Err(simulation_rejected(
                    "initial observations are only admitted once at boundary zero",
                ));
            }
        }
        PublicOperation::PrepareBoundary => {
            if transition_key.boundary != current.boundary
                || current.in_flight.is_some()
                || current.accepted_sequence_watermark == 0
            {
                return Err(simulation_rejected(
                    "prepare boundary is not the current admitted boundary",
                ));
            }
        }
        PublicOperation::AdmitObservations => {
            if transition_key.boundary != current.boundary
                || current.in_flight.as_ref() != Some(transition_key)
            {
                return Err(simulation_rejected(
                    "observation admission does not match the prepared transition",
                ));
            }
        }
        _ => {
            return Err(simulation_rejected(
                "operation is not a three-phase simulation transition",
            ));
        }
    }
    let mut adapter_guard = adapter.lock().await;
    if !adapter_guard.simulation_session_active(route.principal(), &current.session_id, now_ms) {
        return Err(simulation_rejected(
            "simulation session is no longer active",
        ));
    }
    adapter_guard
        .authorize_simulation_session(route, &current.session_id, now_ms)
        .map_err(|error| simulation_adapter_error(operation.segment(), error))?;
    let summary = adapter_guard
        .simulation_execution(&current.execution_id)
        .map_err(|error| simulation_adapter_error(operation.segment(), error))?;
    if summary.timeline_id != current.timeline_id {
        return Err(simulation_rejected(
            "simulation timeline was invalidated by the supervisor",
        ));
    }
    let definition = adapter_guard
        .simulation_definition(&current.execution_id)
        .map_err(|error| simulation_adapter_error(operation.segment(), error))?;
    let context = PublicSimulationContext {
        principal: current.principal.clone(),
        session_id: current.session_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: correlation_id.to_vec(),
        execution_id: current.execution_id.clone(),
        timeline_id: current.timeline_id.clone(),
        completed_boundary: current.boundary,
        model_identity: current.model_identity.clone(),
        quantum_ns: current.quantum_ns,
    };
    current.in_flight = Some(transition_key.clone());
    current.active_phase = Some(operation);
    Ok(SimulationPhaseAdmission {
        context,
        transition_key: transition_key.clone(),
        definition,
        operation,
        adapter: adapter.clone(),
        route: route.clone(),
        authorized_at_ms: now_ms,
        started: Instant::now(),
    })
}

pub(crate) async fn admit_initial_observations(
    route: &PublicRoute,
    request: AdmitInitialObservationsRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<AdmitInitialObservationsResponse, PublicTransportError> {
    let key = request
        .transition_key
        .clone()
        .ok_or_else(|| simulation_rejected("initial observation request has no transition key"))?;
    {
        let guard = authority.lock().await;
        if let Some(current) = guard.as_ref() {
            authorize_live_simulation_grant(route, &key.authority_grant, current, adapter, now_ms)
                .await?;
        }
    }
    let (max_product_bytes, max_cut_bytes) = {
        let guard = authority.lock().await;
        let current = guard
            .as_ref()
            .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
        (current.max_product_bytes, current.max_cut_bytes)
    };
    let admission = begin_simulation_phase(
        route,
        &key,
        &request.correlation_id,
        PublicOperation::AdmitInitialObservations,
        PhaseAdmissionContext {
            adapter,
            authority,
            backend: simulation_backend,
        },
        now_ms,
    )
    .await?;
    if let Err(error) = validate_observation_cut(
        &request.observations,
        0,
        &admission.definition,
        adapter,
        &admission.context.execution_id,
        (max_product_bytes, max_cut_bytes),
        &request.membership_digest,
    )
    .await
    {
        clear_phase_admission(authority, &admission).await;
        return Err(error);
    }
    let result = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.admit_initial_observations(admission.context.clone(), request.clone()),
    )
    .await;
    let mut response = phase_backend_result(result, authority, &admission).await?;
    let memberships = observation_memberships(&request.observations)?;
    phase_validation_result(
        normalize_receipt(
            &mut response.receipt,
            &admission,
            &request.correlation_id,
            phoxal::communication::simulation::PhaseStatus::InitialAdmitted,
            memberships,
            0,
        ),
        authority,
        &admission,
    )
    .await?;
    complete_phase(authority, &admission, response, false).await
}

pub(crate) async fn prepare_boundary(
    route: &PublicRoute,
    request: PrepareBoundaryRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<PrepareBoundaryResponse, PublicTransportError> {
    let key = request
        .transition_key
        .clone()
        .ok_or_else(|| simulation_rejected("prepare request has no transition key"))?;
    {
        let guard = authority.lock().await;
        if let Some(current) = guard.as_ref() {
            authorize_live_simulation_grant(route, &key.authority_grant, current, adapter, now_ms)
                .await?;
        }
    }
    let admission = begin_simulation_phase(
        route,
        &key,
        &request.correlation_id,
        PublicOperation::PrepareBoundary,
        PhaseAdmissionContext {
            adapter,
            authority,
            backend: simulation_backend,
        },
        now_ms,
    )
    .await?;
    let result = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.prepare_boundary(admission.context.clone(), request.clone()),
    )
    .await;
    let mut response = phase_backend_result(result, authority, &admission).await?;
    let memberships = phase_validation_result(
        validate_actuation_cut(&response.actuation, key.boundary),
        authority,
        &admission,
    )
    .await?;
    phase_validation_result(
        normalize_receipt(
            &mut response.receipt,
            &admission,
            &request.correlation_id,
            phoxal::communication::simulation::PhaseStatus::Prepared,
            memberships,
            key.boundary,
        ),
        authority,
        &admission,
    )
    .await?;
    complete_phase(authority, &admission, response, false).await
}

pub(crate) async fn admit_observations(
    route: &PublicRoute,
    request: AdmitObservationsRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<AdmitObservationsResponse, PublicTransportError> {
    let key = request
        .transition_key
        .clone()
        .ok_or_else(|| simulation_rejected("observation admission has no transition key"))?;
    {
        let guard = authority.lock().await;
        if let Some(current) = guard.as_ref() {
            authorize_live_simulation_grant(route, &key.authority_grant, current, adapter, now_ms)
                .await?;
        }
    }
    let (max_product_bytes, max_cut_bytes) = {
        let guard = authority.lock().await;
        let current = guard
            .as_ref()
            .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
        (current.max_product_bytes, current.max_cut_bytes)
    };
    let admission = begin_simulation_phase(
        route,
        &key,
        &request.correlation_id,
        PublicOperation::AdmitObservations,
        PhaseAdmissionContext {
            adapter,
            authority,
            backend: simulation_backend,
        },
        now_ms,
    )
    .await?;
    let expected_boundary = key
        .boundary
        .checked_add(1)
        .ok_or_else(|| simulation_rejected("simulation boundary overflow"))?;
    if let Err(error) = validate_observation_cut(
        &request.observations,
        expected_boundary,
        &admission.definition,
        adapter,
        &admission.context.execution_id,
        (max_product_bytes, max_cut_bytes),
        &request.membership_digest,
    )
    .await
    {
        clear_phase_admission(authority, &admission).await;
        return Err(error);
    }
    let result = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.admit_observations(admission.context.clone(), request.clone()),
    )
    .await;
    let mut response = phase_backend_result(result, authority, &admission).await?;
    let memberships = observation_memberships(&request.observations)?;
    phase_validation_result(
        normalize_receipt(
            &mut response.receipt,
            &admission,
            &request.correlation_id,
            phoxal::communication::simulation::PhaseStatus::ObservationsAdmitted,
            memberships,
            expected_boundary,
        ),
        authority,
        &admission,
    )
    .await?;
    complete_phase(authority, &admission, response, true).await
}

pub(crate) async fn clear_phase_admission(
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    admission: &SimulationPhaseAdmission,
) {
    if let Some(current) = authority.lock().await.as_mut()
        && current.in_flight.as_ref() == Some(&admission.transition_key)
    {
        current.active_phase = None;
        if admission.operation != PublicOperation::AdmitObservations {
            current.in_flight = None;
        }
    }
}

pub(crate) async fn phase_backend_result<T>(
    result: Result<Result<T, PublicBackendError>, tokio::time::error::Elapsed>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    admission: &SimulationPhaseAdmission,
) -> Result<T, PublicTransportError> {
    let error = match result {
        Ok(Ok(response)) => return Ok(response),
        Ok(Err(
            error @ (PublicBackendError::RejectedBeforeAdmission(_) | PublicBackendError::Capacity),
        )) => {
            clear_phase_admission(authority, admission).await;
            return Err(simulation_backend_error(
                admission.operation.segment(),
                error,
            ));
        }
        Ok(Err(error)) => simulation_backend_error(admission.operation.segment(), error),
        Err(_) => PublicTransportError::Timeout {
            operation: admission.operation.segment().to_owned(),
        },
    };
    if let Some(current) = authority.lock().await.as_mut()
        && current.grant == admission.context.authority_grant
        && current.timeline_id == admission.context.timeline_id
    {
        current.failure = Some(bounded_error_detail(&error.to_string()));
        current.active_phase = None;
    }
    Err(error)
}

/// A malformed reply after backend mutation is terminal.
/// Repeating the request must never repeat that mutation.
async fn phase_validation_result<T>(
    result: Result<T, PublicTransportError>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    admission: &SimulationPhaseAdmission,
) -> Result<T, PublicTransportError> {
    if let Err(error) = &result
        && let Some(current) = authority.lock().await.as_mut()
        && current.grant == admission.context.authority_grant
        && current.timeline_id == admission.context.timeline_id
    {
        current.failure = Some(bounded_error_detail(&error.to_string()));
        current.active_phase = None;
    }
    result
}

pub(crate) async fn complete_phase<Response>(
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    admission: &SimulationPhaseAdmission,
    response: Response,
    completes_transition: bool,
) -> Result<Response, PublicTransportError> {
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority is no longer active"))?;
    if current.grant != admission.context.authority_grant
        || current.timeline_id != admission.context.timeline_id
        || current.session_id != admission.context.session_id
        || current.principal != admission.context.principal
        || current.resetting
        || current.failure.is_some()
        || current.active_phase != Some(admission.operation)
        || current.in_flight.as_ref() != Some(&admission.transition_key)
    {
        return Err(simulation_rejected(
            "simulation authority changed while a phase was executing",
        ));
    }
    authorize_live_simulation_grant(
        &admission.route,
        &admission.context.authority_grant,
        current,
        &admission.adapter,
        admission.authorized_at_ms.saturating_add(
            admission
                .started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        ),
    )
    .await?;
    current.active_phase = None;
    current.lease_deadline = Instant::now()
        .checked_add(SIMULATION_AUTHORITY_LEASE)
        .ok_or_else(|| simulation_rejected("simulation authority lease overflows the clock"))?;
    if completes_transition {
        current.boundary = admission
            .transition_key
            .boundary
            .checked_add(1)
            .ok_or_else(|| simulation_rejected("simulation boundary overflow"))?;
        current.accepted_sequence_watermark = admission.transition_key.operation_sequence;
        current.in_flight = None;
    } else if admission.operation == PublicOperation::AdmitInitialObservations {
        current.accepted_sequence_watermark = admission.transition_key.operation_sequence;
        current.in_flight = None;
    }
    Ok(response)
}
