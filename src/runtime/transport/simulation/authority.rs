//! Exclusive grant lifecycle, reset, progress, and revocation.

use super::*;

pub(crate) async fn acquire_simulation_authority(
    route: &PublicRoute,
    request: AcquireAuthorityRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<AcquireAuthorityResponse, PublicTransportError> {
    if request.execution_id.is_empty()
        || request.model_identity.is_empty()
        || !request.model_identity.is_ascii()
        || request.quantum_ns == 0
        || request.providers.is_empty()
        || request.session_id.is_empty()
        || request.correlation_id.is_empty()
        || request.correlation_id.len() > MAX_SIMULATION_CORRELATION_BYTES
    {
        return Err(simulation_rejected(
            "acquire-authority requires execution, model, quantum, and providers",
        ));
    }
    let max_product_bytes = usize::try_from(request.max_product_bytes)
        .map_err(|_| simulation_rejected("simulation product byte cap is not representable"))?;
    let max_cut_bytes = usize::try_from(request.max_cut_bytes)
        .map_err(|_| simulation_rejected("simulation cut byte cap is not representable"))?;
    let max_product_bytes = if max_product_bytes == 0 {
        MAX_SIMULATION_PRODUCT_BYTES
    } else {
        max_product_bytes
    };
    let max_cut_bytes = if max_cut_bytes == 0 {
        MAX_SIMULATION_CUT_BYTES
    } else {
        max_cut_bytes
    };
    if max_product_bytes > MAX_SIMULATION_PRODUCT_BYTES
        || max_cut_bytes > MAX_SIMULATION_CUT_BYTES
        || max_product_bytes > max_cut_bytes
    {
        return Err(simulation_rejected(
            "simulation byte caps exceed the supported bounded lane",
        ));
    }
    let mut adapter_guard = adapter.lock().await;
    adapter_guard
        .authorize_simulation_session(route, &request.session_id, now_ms)
        .map_err(|error| simulation_adapter_error("acquire-authority", error))?;
    let summary = adapter_guard
        .simulation_execution(&request.execution_id)
        .map_err(|error| simulation_adapter_error("acquire-authority", error))?;
    let definition = adapter_guard
        .simulation_definition(&request.execution_id)
        .map_err(|error| simulation_adapter_error("acquire-authority", error))?;
    if request.model_identity != definition.model_identity()
        || request.quantum_ns != definition.quantum_ns()
        || request.providers.len() != definition.providers().len()
    {
        return Err(simulation_rejected(
            "simulation request does not match the supervisor-owned definition",
        ));
    }
    let expected_providers = definition
        .providers()
        .iter()
        .map(|provider| {
            (
                provider.service_instance().to_owned(),
                provider.port().to_owned(),
                provider.shape() as i32,
                provider.input_fqn().to_owned(),
                provider.payload_fqn().to_owned(),
                provider.rate_microhertz(),
            )
        })
        .collect::<BTreeSet<_>>();
    let requested_providers = request
        .providers
        .iter()
        .map(|provider| {
            (
                provider.service_instance.clone(),
                provider.port.clone(),
                provider.shape,
                provider.input_fqn.clone(),
                provider.payload_fqn.clone(),
                provider.rate_microhertz,
            )
        })
        .collect::<BTreeSet<_>>();
    if requested_providers != expected_providers {
        return Err(simulation_rejected(
            "simulation providers do not exactly match the supervisor-owned definition",
        ));
    }
    let definition_model_identity = definition.model_identity().to_owned();
    let definition_quantum_ns = definition.quantum_ns();
    let state = summary.state;
    if !matches!(
        state,
        phoxal::communication::session::ExecutionState::Ready
            | phoxal::communication::session::ExecutionState::Active
    ) {
        return Err(simulation_rejected(
            "simulation authority requires a ready or active execution",
        ));
    }
    let mut providers = BTreeSet::new();
    for provider in &request.providers {
        if provider.service_instance.is_empty()
            || provider.port.is_empty()
            || provider.payload_fqn.is_empty()
            || !provider.payload_fqn.is_ascii()
            || !providers.insert((provider.service_instance.as_str(), provider.port.as_str()))
        {
            return Err(simulation_rejected(
                "simulation providers must be complete and unique",
            ));
        }
        adapter_guard
            .validate_simulation_provider(
                &request.execution_id,
                &provider.service_instance,
                &provider.port,
                provider.shape,
                &provider.input_fqn,
                &provider.payload_fqn,
            )
            .map_err(|error| simulation_adapter_error("acquire-authority", error))?;
    }
    drop(adapter_guard);
    let mut authority_guard = authority.lock().await;
    if let Some(current) = authority_guard.as_ref() {
        let mut adapter_guard = adapter.lock().await;
        let current_is_live = Instant::now() < current.lease_deadline
            && adapter_guard.simulation_session_active(
                &current.principal,
                &current.session_id,
                now_ms,
            )
            && adapter_guard
                .simulation_execution(&current.execution_id)
                .is_ok_and(|summary| summary.timeline_id == current.timeline_id);
        drop(adapter_guard);
        if current_is_live {
            return Err(simulation_rejected(
                "simulation authority is already held by another client",
            ));
        }
        if let Some(stale) = authority_guard.take() {
            release_backend_authority(stale, simulation_backend).await;
        }
    }
    let mut grant = vec![0_u8; SIMULATION_GRANT_BYTES];
    getrandom::fill(&mut grant).map_err(|_| {
        PublicTransportError::Transport("cannot obtain simulation authority entropy".to_owned())
    })?;
    let lease_deadline = Instant::now()
        .checked_add(SIMULATION_AUTHORITY_LEASE)
        .ok_or_else(|| simulation_rejected("simulation authority lease overflows the clock"))?;
    let context = PublicSimulationContext {
        principal: route.principal().to_owned(),
        session_id: request.session_id.clone(),
        authority_grant: grant.clone(),
        correlation_id: request.correlation_id.clone(),
        execution_id: request.execution_id.clone(),
        timeline_id: summary.timeline_id.clone(),
        completed_boundary: 0,
        model_identity: definition_model_identity.clone(),
        quantum_ns: definition_quantum_ns,
    };
    let acquire = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.acquire(context, request.clone()),
    )
    .await
    .map_err(|_| PublicTransportError::Timeout {
        operation: "acquire-authority".to_owned(),
    })?;
    acquire.map_err(|error| simulation_backend_error("acquire-authority", error))?;
    *authority_guard = Some(SimulationAuthority {
        principal: route.principal().to_owned(),
        session_id: request.session_id.clone(),
        grant: grant.clone(),
        execution_id: request.execution_id.clone(),
        timeline_id: summary.timeline_id.clone(),
        model_identity: definition_model_identity.clone(),
        quantum_ns: definition_quantum_ns,
        boundary: 0,
        lease_deadline,
        in_flight: None,
        accepted_sequence_watermark: 0,
        max_product_bytes,
        max_cut_bytes,
        active_phase: None,
        failure: None,
        resetting: false,
    });
    Ok(AcquireAuthorityResponse {
        authority_grant: grant,
        timeline_id: summary.timeline_id,
        boundary: 0,
        lease_ms: SIMULATION_AUTHORITY_LEASE.as_millis() as u32,
        session_id: request.session_id,
        execution_id: request.execution_id,
        model_identity: definition_model_identity,
        quantum_ns: definition_quantum_ns,
        correlation_id: request.correlation_id,
        max_product_bytes: max_product_bytes as u64,
        max_cut_bytes: max_cut_bytes as u64,
    })
}

pub(crate) async fn revoke_simulation_for_session(
    route: &PublicRoute,
    session_id: &[u8],
    _adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
) {
    let mut guard = authority.lock().await;
    let Some(current) = guard.as_ref() else {
        return;
    };
    if current.principal != route.principal() || current.session_id != session_id {
        return;
    }
    let Some(current) = guard.take() else {
        return;
    };
    drop(guard);
    release_backend_authority(current, simulation_backend).await;
}

pub(crate) async fn revoke_simulation_authority(
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
) {
    let current = authority.lock().await.take();
    if let Some(current) = current {
        release_backend_authority(current, simulation_backend).await;
    }
}

pub(crate) async fn release_backend_authority(
    current: SimulationAuthority,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
) {
    let context = PublicSimulationContext {
        principal: current.principal.clone(),
        session_id: current.session_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: Vec::new(),
        execution_id: current.execution_id.clone(),
        timeline_id: current.timeline_id.clone(),
        completed_boundary: current.boundary,
        model_identity: current.model_identity.clone(),
        quantum_ns: current.quantum_ns,
    };
    let _ = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.release(
            context,
            ReleaseAuthorityRequest {
                authority_grant: current.grant,
                session_id: current.session_id,
                correlation_id: Vec::new(),
            },
        ),
    )
    .await;
}

pub(crate) async fn reset_simulation(
    route: &PublicRoute,
    request: ResetRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    subscriptions: &Arc<Mutex<BTreeMap<String, CancellationToken>>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<ResetResponse, PublicTransportError> {
    validate_simulation_correlation(&request.correlation_id)?;
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
    authorize_simulation_grant(route, &request.authority_grant, current)?;
    if current.failure.is_some() || current.resetting || current.in_flight.is_some() {
        return Err(simulation_rejected(
            "reset requires a completed transition and no concurrent reset",
        ));
    }
    if Instant::now() >= current.lease_deadline {
        let expired = guard.take();
        drop(guard);
        if let Some(expired) = expired {
            release_backend_authority(expired, simulation_backend).await;
        }
        return Err(simulation_rejected("simulation authority lease expired"));
    }
    if request.execution_id != current.execution_id || request.timeline_id != current.timeline_id {
        return Err(simulation_rejected(
            "reset execution or timeline does not match authority",
        ));
    }
    if request.session_id != current.session_id {
        return Err(simulation_rejected(
            "reset session does not match authority",
        ));
    }
    if request.completed_boundary != current.boundary {
        return Err(simulation_rejected(
            "reset boundary does not match authoritative progress",
        ));
    }
    let mut adapter_guard = adapter.lock().await;
    if !adapter_guard.simulation_session_active(route.principal(), &request.session_id, now_ms) {
        drop(adapter_guard);
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected(
            "simulation session is no longer active",
        ));
    }
    adapter_guard
        .authorize_simulation_session(route, &request.session_id, now_ms)
        .map_err(|error| simulation_adapter_error("reset", error))?;
    let summary = match adapter_guard.simulation_execution(&current.execution_id) {
        Ok(summary) => summary,
        Err(error) => {
            drop(adapter_guard);
            drop(guard);
            revoke_simulation_authority(authority, simulation_backend).await;
            return Err(simulation_adapter_error("reset", error));
        }
    };
    if summary.timeline_id != current.timeline_id {
        drop(adapter_guard);
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected(
            "reset timeline was invalidated by the supervisor",
        ));
    }
    drop(adapter_guard);
    let next_timeline_id = phoxal::identity::TimelineId::mint().to_string();
    let context = PublicSimulationContext {
        principal: current.principal.clone(),
        session_id: current.session_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: request.correlation_id.clone(),
        execution_id: current.execution_id.clone(),
        timeline_id: current.timeline_id.clone(),
        completed_boundary: current.boundary,
        model_identity: current.model_identity.clone(),
        quantum_ns: current.quantum_ns,
    };
    current.resetting = true;
    drop(guard);
    let reset = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.reset(context.clone(), request.clone(), next_timeline_id.clone()),
    )
    .await;
    let reset_error = match reset {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(simulation_backend_error("reset", error)),
        Err(_) => Some(PublicTransportError::Timeout {
            operation: "reset".to_owned(),
        }),
    };
    if let Some(error) = reset_error {
        if let Some(current) = authority.lock().await.as_mut()
            && current.grant == context.authority_grant
        {
            current.failure = Some(bounded_error_detail(&error.to_string()));
        }
        return Err(error);
    }
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority was released during reset"))?;
    if current.grant != context.authority_grant || current.timeline_id != context.timeline_id {
        return Err(simulation_rejected(
            "simulation authority changed during reset",
        ));
    }
    let mut adapter_guard = adapter.lock().await;
    if let Err(error) = adapter_guard.reset_timeline(&current.execution_id, &next_timeline_id) {
        drop(adapter_guard);
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_adapter_error("reset", error));
    }
    drop(adapter_guard);
    let mut next_grant = vec![0_u8; SIMULATION_GRANT_BYTES];
    getrandom::fill(&mut next_grant)
        .map_err(|_| simulation_rejected("cannot obtain reset authority entropy"))?;
    current.grant = next_grant;
    current.resetting = false;
    current.timeline_id = next_timeline_id.clone();
    current.boundary = 0;
    current.in_flight = None;
    current.accepted_sequence_watermark = 0;
    current.lease_deadline = Instant::now()
        .checked_add(SIMULATION_AUTHORITY_LEASE)
        .ok_or_else(|| simulation_rejected("simulation authority lease overflows the clock"))?;
    let response = ResetResponse {
        next_timeline_id,
        boundary: 0,
        session_id: request.session_id.clone(),
        execution_id: current.execution_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: request.correlation_id.clone(),
        previous_timeline_id: request.timeline_id.clone(),
        requested_boundary: request.completed_boundary,
    };
    drop(guard);
    cancel_session_subscriptions(route, &request.session_id, subscriptions).await;
    Ok(response)
}

pub(crate) async fn release_simulation(
    route: &PublicRoute,
    request: ReleaseAuthorityRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<ReleaseAuthorityResponse, PublicTransportError> {
    validate_simulation_correlation(&request.correlation_id)?;
    let mut guard = authority.lock().await;
    let current = guard
        .as_ref()
        .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
    authorize_simulation_grant(route, &request.authority_grant, current)?;
    if request.session_id != current.session_id {
        return Err(simulation_rejected(
            "release session does not match authority",
        ));
    }
    if Instant::now() >= current.lease_deadline {
        let expired = guard.take();
        drop(guard);
        if let Some(expired) = expired {
            release_backend_authority(expired, simulation_backend).await;
        }
        return Err(simulation_rejected("simulation authority lease expired"));
    }
    let session_active = adapter.lock().await.simulation_session_active(
        route.principal(),
        &request.session_id,
        now_ms,
    );
    if !session_active {
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected("release session is no longer active"));
    }
    let context = PublicSimulationContext {
        principal: current.principal.clone(),
        session_id: current.session_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: request.correlation_id.clone(),
        execution_id: current.execution_id.clone(),
        timeline_id: current.timeline_id.clone(),
        completed_boundary: current.boundary,
        model_identity: current.model_identity.clone(),
        quantum_ns: current.quantum_ns,
    };
    let current = guard
        .take()
        .ok_or_else(|| simulation_rejected("simulation authority was released"))?;
    drop(guard);
    let release = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.release(context, request.clone()),
    )
    .await
    .map_err(|_| PublicTransportError::Timeout {
        operation: "release-authority".to_owned(),
    })?;
    release.map_err(|error| simulation_backend_error("release-authority", error))?;
    let session_id = request.session_id.clone();
    let authority_grant = request.authority_grant.clone();
    let correlation_id = request.correlation_id.clone();
    let execution_id = current.execution_id.clone();
    let timeline_id = current.timeline_id.clone();
    let completed_boundary = current.boundary;
    Ok(ReleaseAuthorityResponse {
        session_id,
        authority_grant,
        correlation_id,
        execution_id,
        timeline_id,
        completed_boundary,
    })
}

pub(crate) async fn progress_simulation(
    route: &PublicRoute,
    request: ProgressRequest,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    authority: &Arc<Mutex<Option<SimulationAuthority>>>,
    simulation_backend: &Arc<dyn PublicSimulationBackend>,
    now_ms: u64,
) -> Result<ProgressResponse, PublicTransportError> {
    validate_simulation_correlation(&request.correlation_id)?;
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority is not active"))?;
    authorize_simulation_grant(route, &request.authority_grant, current)?;
    if Instant::now() >= current.lease_deadline {
        let expired = guard.take();
        drop(guard);
        if let Some(expired) = expired {
            release_backend_authority(expired, simulation_backend).await;
        }
        return Err(simulation_rejected("simulation authority lease expired"));
    }
    if request.session_id != current.session_id {
        return Err(simulation_rejected(
            "progress session does not match authority",
        ));
    }
    let mut adapter_guard = adapter.lock().await;
    if !adapter_guard.simulation_session_active(route.principal(), &request.session_id, now_ms) {
        drop(adapter_guard);
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected(
            "simulation session is no longer active",
        ));
    }
    adapter_guard
        .authorize_simulation_session(route, &request.session_id, now_ms)
        .map_err(|error| simulation_adapter_error("progress", error))?;
    let summary = match adapter_guard.simulation_execution(&current.execution_id) {
        Ok(summary) => summary,
        Err(error) => {
            drop(adapter_guard);
            drop(guard);
            revoke_simulation_authority(authority, simulation_backend).await;
            return Err(simulation_adapter_error("progress", error));
        }
    };
    if summary.timeline_id != current.timeline_id {
        drop(adapter_guard);
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected(
            "simulation timeline was invalidated by the supervisor",
        ));
    }
    if let Some(detail) = &current.failure {
        return Ok(ProgressResponse {
            execution_id: current.execution_id.clone(),
            timeline_id: current.timeline_id.clone(),
            completed_boundary: current.boundary,
            failed: true,
            detail: Some(detail.clone()),
            session_id: current.session_id.clone(),
            authority_grant: current.grant.clone(),
            correlation_id: request.correlation_id.clone(),
        });
    }
    let context = PublicSimulationContext {
        principal: current.principal.clone(),
        session_id: current.session_id.clone(),
        authority_grant: current.grant.clone(),
        correlation_id: request.correlation_id.clone(),
        execution_id: current.execution_id.clone(),
        timeline_id: current.timeline_id.clone(),
        completed_boundary: current.boundary,
        model_identity: current.model_identity.clone(),
        quantum_ns: current.quantum_ns,
    };
    drop(adapter_guard);
    drop(guard);
    let response = tokio::time::timeout(
        DEFAULT_PUBLIC_DEADLINE,
        simulation_backend.progress(context.clone(), request.clone()),
    )
    .await
    .map_err(|_| PublicTransportError::Timeout {
        operation: "progress".to_owned(),
    })?
    .map_err(|error| simulation_backend_error("progress", error))?;
    let mut guard = authority.lock().await;
    let current = guard
        .as_mut()
        .ok_or_else(|| simulation_rejected("simulation authority was released during progress"))?;
    if current.grant != context.authority_grant || current.timeline_id != context.timeline_id {
        return Err(simulation_rejected(
            "simulation authority changed during progress",
        ));
    }
    let mut response = response;
    if (!response.execution_id.is_empty() && response.execution_id != current.execution_id)
        || (!response.timeline_id.is_empty() && response.timeline_id != current.timeline_id)
        || (!response.session_id.is_empty() && response.session_id != current.session_id)
    {
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
        return Err(simulation_rejected(
            "simulation backend returned inconsistent progress identity",
        ));
    }
    response.execution_id = current.execution_id.clone();
    response.timeline_id = current.timeline_id.clone();
    response.session_id = current.session_id.clone();
    response.authority_grant = current.grant.clone();
    response.correlation_id = request.correlation_id;
    response.completed_boundary = current.boundary;
    if response.failed {
        drop(guard);
        revoke_simulation_authority(authority, simulation_backend).await;
    } else {
        current.lease_deadline = Instant::now()
            .checked_add(SIMULATION_AUTHORITY_LEASE)
            .ok_or_else(|| simulation_rejected("simulation authority lease overflows the clock"))?;
    }
    Ok(response)
}

/// Check the live session and timeline before completing a phase.
pub(crate) async fn authorize_live_simulation_grant(
    route: &PublicRoute,
    grant: &[u8],
    current: &SimulationAuthority,
    adapter: &Arc<Mutex<SupervisorAdapter>>,
    now_ms: u64,
) -> Result<(), PublicTransportError> {
    authorize_simulation_grant(route, grant, current)?;
    if Instant::now() >= current.lease_deadline {
        return Err(simulation_rejected("simulation authority lease expired"));
    }
    let mut adapter = adapter.lock().await;
    adapter
        .authorize_simulation_session(route, &current.session_id, now_ms)
        .map_err(|error| simulation_adapter_error("simulation phase", error))?;
    let execution = adapter
        .simulation_execution(&current.execution_id)
        .map_err(|error| simulation_adapter_error("simulation phase", error))?;
    if execution.timeline_id != current.timeline_id {
        return Err(simulation_rejected(
            "simulation timeline was invalidated by the supervisor",
        ));
    }
    Ok(())
}

pub(crate) fn authorize_simulation_grant(
    route: &PublicRoute,
    grant: &[u8],
    authority: &SimulationAuthority,
) -> Result<(), PublicTransportError> {
    if authority.principal != route.principal()
        || grant.len() != SIMULATION_GRANT_BYTES
        || !constant_time_eq(grant, &authority.grant)
    {
        return Err(simulation_rejected(
            "simulation authority grant or principal is invalid",
        ));
    }
    Ok(())
}

pub(crate) fn validate_simulation_correlation(
    correlation_id: &[u8],
) -> Result<(), PublicTransportError> {
    if correlation_id.is_empty() || correlation_id.len() > MAX_SIMULATION_CORRELATION_BYTES {
        return Err(simulation_rejected(
            "simulation correlation_id must contain 1..=64 bytes",
        ));
    }
    Ok(())
}

pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub(crate) fn simulation_rejected(detail: &str) -> PublicTransportError {
    PublicTransportError::Rejected {
        operation: "simulation".to_owned(),
        detail: detail.to_owned(),
    }
}

pub(crate) fn simulation_adapter_error(
    operation: &str,
    error: SupervisorAdapterError,
) -> PublicTransportError {
    PublicTransportError::Adapter {
        operation: operation.to_owned(),
        detail: error.to_string(),
    }
}

pub(crate) fn simulation_backend_error(
    operation: &str,
    error: PublicBackendError,
) -> PublicTransportError {
    match error {
        PublicBackendError::RejectedBeforeAdmission(detail) => PublicTransportError::Rejected {
            operation: operation.to_owned(),
            detail: bounded_error_detail(&detail),
        },
        PublicBackendError::Capacity => PublicTransportError::Rejected {
            operation: operation.to_owned(),
            detail: "simulation backend observer capacity is exhausted".to_owned(),
        },
        PublicBackendError::Transport(detail) => PublicTransportError::Transport(detail),
    }
}
