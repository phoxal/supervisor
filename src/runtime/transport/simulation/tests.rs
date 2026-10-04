use super::products::{canonical_membership_digest, observation_memberships};
use super::*;
use crate::runtime::adapter::{
    ExecutionDefinition, ServiceMethods, SimulationDefinition, SimulationProviderDefinition,
};
use phoxal::communication::DeploymentTarget;
use phoxal::communication::session::OpenSessionRequest;
use phoxal::communication::session::{
    ExecutionState, ExecutionSummary, MethodMetadata, MethodShape,
};
use phoxal::communication::simulation::{
    CutReceipt, Observation, ProductDisposition, ProductMembership, ProviderRequirement,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type BackendFuture<T> = Pin<Box<dyn Future<Output = Result<T, PublicBackendError>> + Send>>;
#[derive(Clone, Default)]
struct Backend {
    initial: Arc<AtomicUsize>,
    prepared: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    resets: Arc<AtomicUsize>,
    block: Arc<AtomicBool>,
    omit_receipt: Arc<AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}
impl PublicSimulationBackend for Backend {
    fn acquire(&self, _: PublicSimulationContext, _: AcquireAuthorityRequest) -> BackendFuture<()> {
        Box::pin(async { Ok(()) })
    }
    fn admit_initial_observations(
        &self,
        _: PublicSimulationContext,
        _: AdmitInitialObservationsRequest,
    ) -> BackendFuture<AdmitInitialObservationsResponse> {
        self.initial.fetch_add(1, Ordering::SeqCst);
        let omit_receipt = self.omit_receipt.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(AdmitInitialObservationsResponse {
                receipt: (!omit_receipt).then(CutReceipt::default),
            })
        })
    }
    fn prepare_boundary(
        &self,
        _: PublicSimulationContext,
        _: PrepareBoundaryRequest,
    ) -> BackendFuture<PrepareBoundaryResponse> {
        let backend = self.clone();
        Box::pin(async move {
            backend.prepared.fetch_add(1, Ordering::SeqCst);
            if backend.block.load(Ordering::SeqCst) {
                backend.entered.notify_one();
                backend.resume.notified().await;
            }
            Ok(PrepareBoundaryResponse {
                receipt: Some(CutReceipt::default()),
                actuation: Vec::new(),
            })
        })
    }
    fn admit_observations(
        &self,
        _: PublicSimulationContext,
        _: AdmitObservationsRequest,
    ) -> BackendFuture<AdmitObservationsResponse> {
        self.admitted.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(AdmitObservationsResponse {
                receipt: Some(CutReceipt::default()),
            })
        })
    }
    fn reset(&self, _: PublicSimulationContext, _: ResetRequest, _: String) -> BackendFuture<()> {
        self.resets.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn release(&self, _: PublicSimulationContext, _: ReleaseAuthorityRequest) -> BackendFuture<()> {
        Box::pin(async { Ok(()) })
    }
    fn progress(
        &self,
        context: PublicSimulationContext,
        _: ProgressRequest,
    ) -> BackendFuture<ProgressResponse> {
        Box::pin(async move {
            Ok(ProgressResponse {
                completed_boundary: context.completed_boundary,
                ..Default::default()
            })
        })
    }
}

struct Fixture {
    adapter: Arc<Mutex<SupervisorAdapter>>,
    authority: Arc<Mutex<Option<SimulationAuthority>>>,
    backend: Arc<dyn PublicSimulationBackend>,
    counts: Backend,
    route: PublicRoute,
    key: TransitionKey,
}
impl Fixture {
    async fn new() -> Self {
        let target = DeploymentTarget::new("local", "supervisor").unwrap();
        let mut adapter = SupervisorAdapter::with_defaults(target.clone(), "test", "test").unwrap();
        let definition = SimulationDefinition::new(
            "model",
            1_000_000,
            vec![
                SimulationProviderDefinition::new(
                    "sensor",
                    "sample",
                    MethodShape::Observation,
                    "fixture.Empty",
                    "fixture.Sample",
                    1_000_000_000,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        adapter
            .install_execution(
                ExecutionDefinition::new(
                    ExecutionSummary {
                        execution_id: "execution".into(),
                        timeline_id: "timeline".into(),
                        state: ExecutionState::Ready,
                    },
                    vec![
                        ServiceMethods::new(
                            "sensor",
                            vec![MethodMetadata {
                                endpoint: "sample".into(),
                                shape: MethodShape::Observation,
                                input_fqn: "fixture.Empty".into(),
                                output_fqn: "fixture.Sample".into(),
                                max_message_bytes: 1024,
                                max_buffered_items: 8,
                                retained_latest: false,
                                lease_valid_for_ms: None,
                            }],
                        )
                        .unwrap(),
                    ],
                )
                .unwrap()
                .with_simulation(definition)
                .unwrap(),
            )
            .unwrap();
        let open = PublicRoute::for_operation(&target, "simulator", PublicOperation::Open).unwrap();
        let session_id = adapter
            .open(
                &open,
                &OpenSessionRequest {
                    protocol: phoxal::communication::SESSION_PROTOCOL.into(),
                },
                0,
            )
            .unwrap()
            .session_id;
        let route =
            PublicRoute::for_operation(&target, "simulator", PublicOperation::AcquireAuthority)
                .unwrap();
        let counts = Backend::default();
        let backend: Arc<dyn PublicSimulationBackend> = Arc::new(counts.clone());
        let adapter = Arc::new(Mutex::new(adapter));
        let authority = Arc::new(Mutex::new(None));
        let acquired = acquire_simulation_authority(
            &route,
            AcquireAuthorityRequest {
                execution_id: "execution".into(),
                model_identity: "model".into(),
                quantum_ns: 1_000_000,
                providers: vec![ProviderRequirement {
                    rate_microhertz: 1_000_000_000,
                    service_instance: "sensor".into(),
                    port: "sample".into(),
                    shape: MethodShape::Observation as i32,
                    input_fqn: "fixture.Empty".into(),
                    payload_fqn: "fixture.Sample".into(),
                }],
                session_id: session_id.clone(),
                correlation_id: vec![1],
                ..Default::default()
            },
            &adapter,
            &authority,
            &backend,
            0,
        )
        .await
        .unwrap();
        Self {
            adapter,
            authority,
            backend,
            counts,
            route,
            key: TransitionKey {
                session_id,
                execution_id: "execution".into(),
                timeline_id: "timeline".into(),
                authority_grant: acquired.authority_grant,
                boundary: 0,
                operation_sequence: 1,
            },
        }
    }
    async fn initialize(&self) -> AdmitInitialObservationsResponse {
        admit_initial_observations(
            &self.route,
            self.initial(),
            &self.adapter,
            &self.authority,
            &self.backend,
            0,
        )
        .await
        .unwrap()
    }
    fn initial(&self) -> AdmitInitialObservationsRequest {
        let observations = observations(0);
        AdmitInitialObservationsRequest {
            transition_key: Some(self.key.clone()),
            membership_digest: canonical_membership_digest(
                &observation_memberships(&observations).unwrap(),
            )
            .unwrap()
            .to_vec(),
            observations,
            correlation_id: vec![2],
        }
    }
    fn prepare(&self) -> PrepareBoundaryRequest {
        PrepareBoundaryRequest {
            transition_key: Some(TransitionKey {
                operation_sequence: 2,
                ..self.key.clone()
            }),
            correlation_id: vec![3],
        }
    }
}
fn observations(boundary: u64) -> Vec<Observation> {
    vec![Observation {
        membership: Some(ProductMembership {
            producer: "sensor".into(),
            port: "sample".into(),
            producer_incarnation: vec![1],
            sequence: boundary + 1,
            capture_boundary: boundary,
            capture_time_ns: boundary * 1_000_000,
            disposition: ProductDisposition::Empty,
            item_count: 0,
            encoded_bytes: 0,
            payload_digest: Sha256::digest([]).to_vec(),
        }),
        payload: Vec::new(),
    }]
}

#[tokio::test]
async fn completed_phases_reject_duplicates_and_reset_retires_the_grant() {
    let fixture = Fixture::new().await;
    assert!(
        prepare_boundary(
            &fixture.route,
            fixture.prepare(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err(),
        "initial observations are mandatory"
    );
    fixture.initialize().await;
    assert!(
        admit_initial_observations(
            &fixture.route,
            fixture.initial(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.counts.initial.load(Ordering::SeqCst), 1);
    let foreign = PublicRoute::for_operation(
        &DeploymentTarget::new("local", "supervisor").unwrap(),
        "other",
        PublicOperation::AdmitInitialObservations,
    )
    .unwrap();
    assert!(
        admit_initial_observations(
            &foreign,
            fixture.initial(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    let request = fixture.prepare();
    prepare_boundary(
        &fixture.route,
        request.clone(),
        &fixture.adapter,
        &fixture.authority,
        &fixture.backend,
        0,
    )
    .await
    .unwrap();
    assert!(
        prepare_boundary(
            &fixture.route,
            request.clone(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.counts.prepared.load(Ordering::SeqCst), 1);
    let observations = observations(1);
    let mut admission = AdmitObservationsRequest {
        transition_key: request.transition_key,
        correlation_id: vec![4],
        membership_digest: canonical_membership_digest(
            &observation_memberships(&observations).unwrap(),
        )
        .unwrap()
        .to_vec(),
        observations,
    };
    admission.observations[0]
        .membership
        .as_mut()
        .unwrap()
        .capture_time_ns = 999;
    assert!(
        admit_observations(
            &fixture.route,
            admission.clone(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    admission.observations[0]
        .membership
        .as_mut()
        .unwrap()
        .capture_time_ns = 1_000_000;
    admit_observations(
        &fixture.route,
        admission.clone(),
        &fixture.adapter,
        &fixture.authority,
        &fixture.backend,
        0,
    )
    .await
    .unwrap();
    assert!(
        admit_observations(
            &fixture.route,
            admission,
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.counts.admitted.load(Ordering::SeqCst), 1);
    let reset = ResetRequest {
        authority_grant: fixture.key.authority_grant.clone(),
        session_id: fixture.key.session_id.clone(),
        execution_id: fixture.key.execution_id.clone(),
        timeline_id: fixture.key.timeline_id.clone(),
        completed_boundary: 1,
        correlation_id: vec![5],
    };
    let subscriptions = Arc::new(Mutex::new(BTreeMap::new()));
    let response = reset_simulation(
        &fixture.route,
        reset.clone(),
        &fixture.adapter,
        &subscriptions,
        &fixture.authority,
        &fixture.backend,
        0,
    )
    .await
    .unwrap();
    assert_ne!(response.authority_grant, fixture.key.authority_grant);
    assert!(
        reset_simulation(
            &fixture.route,
            reset,
            &fixture.adapter,
            &subscriptions,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.counts.resets.load(Ordering::SeqCst), 1);
    assert!(
        admit_initial_observations(
            &fixture.route,
            fixture.initial(),
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn blocked_prepare_does_not_block_progress_or_release_and_cannot_commit_after_release() {
    let fixture = Arc::new(Fixture::new().await);
    fixture.initialize().await;
    fixture.counts.block.store(true, Ordering::SeqCst);
    let task_fixture = fixture.clone();
    let task = tokio::spawn(async move {
        prepare_boundary(
            &task_fixture.route,
            task_fixture.prepare(),
            &task_fixture.adapter,
            &task_fixture.authority,
            &task_fixture.backend,
            0,
        )
        .await
    });
    fixture.counts.entered.notified().await;
    let progress = tokio::time::timeout(
        Duration::from_millis(100),
        progress_simulation(
            &fixture.route,
            ProgressRequest {
                authority_grant: fixture.key.authority_grant.clone(),
                session_id: fixture.key.session_id.clone(),
                correlation_id: vec![6],
            },
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(progress.completed_boundary, 0);
    tokio::time::timeout(
        Duration::from_millis(100),
        release_simulation(
            &fixture.route,
            ReleaseAuthorityRequest {
                authority_grant: fixture.key.authority_grant.clone(),
                session_id: fixture.key.session_id.clone(),
                correlation_id: vec![7],
            },
            &fixture.adapter,
            &fixture.authority,
            &fixture.backend,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.counts.resume.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(fixture.authority.lock().await.is_none());
}

#[tokio::test]
async fn not_due_must_match_the_exact_immutable_rational_schedule() {
    let fixture = Fixture::new().await;
    let provider = SimulationProviderDefinition::new(
        "sensor",
        "sample",
        MethodShape::Observation,
        "fixture.Empty",
        "fixture.Sample",
        30_000_000,
    )
    .unwrap();
    assert_eq!(
        (1..=500_000)
            .filter(|boundary| provider.due(*boundary, 2_000_000))
            .count(),
        30_000
    );
    let definition = SimulationDefinition::new("model", 2_000_000, vec![provider]).unwrap();
    for (boundary, disposition, accepted) in [
        (0, ProductDisposition::Empty, true),
        (0, ProductDisposition::NotDue, false),
        (16, ProductDisposition::NotDue, true),
        (16, ProductDisposition::Empty, false),
        (17, ProductDisposition::NotDue, false),
        (17, ProductDisposition::Empty, true),
        (34, ProductDisposition::Empty, true),
    ] {
        let mut cut = observations(boundary);
        let member = cut[0].membership.as_mut().unwrap();
        member.disposition = disposition;
        member.capture_time_ns = boundary * 2_000_000;
        let digest = canonical_membership_digest(&observation_memberships(&cut).unwrap()).unwrap();
        let result = super::products::validate_observation_cut(
            &cut,
            boundary,
            &definition,
            &fixture.adapter,
            "execution",
            (1024, 4096),
            &digest,
        )
        .await;
        assert_eq!(
            result.is_ok(),
            accepted,
            "boundary {boundary}, {disposition:?}: {result:?}"
        );
    }
}

#[tokio::test]
async fn phase_and_progress_require_a_live_session_and_timeline() {
    for close_session in [true, false] {
        let fixture = Fixture::new().await;
        fixture.initialize().await;
        if close_session {
            let route = PublicRoute::for_operation(
                &DeploymentTarget::new("local", "supervisor").unwrap(),
                "simulator",
                PublicOperation::Close,
            )
            .unwrap();
            fixture
                .adapter
                .lock()
                .await
                .close(
                    &route,
                    &phoxal::communication::session::CloseSessionRequest {
                        session_id: fixture.key.session_id.clone(),
                    },
                    0,
                )
                .unwrap();
        } else {
            fixture
                .adapter
                .lock()
                .await
                .reset_timeline("execution", "replacement")
                .unwrap();
        }
        assert!(
            admit_initial_observations(
                &fixture.route,
                fixture.initial(),
                &fixture.adapter,
                &fixture.authority,
                &fixture.backend,
                0
            )
            .await
            .is_err(),
            "phase must not outlive its session or timeline"
        );
        assert!(
            progress_simulation(
                &fixture.route,
                ProgressRequest {
                    session_id: fixture.key.session_id.clone(),
                    authority_grant: fixture.key.authority_grant.clone(),
                    correlation_id: vec![12],
                },
                &fixture.adapter,
                &fixture.authority,
                &fixture.backend,
                0
            )
            .await
            .is_err(),
            "progress must not outlive its session or timeline"
        );
    }
}

#[tokio::test]
async fn invalid_backend_receipt_is_a_terminal_failure() {
    let fixture = Fixture::new().await;
    fixture.counts.omit_receipt.store(true, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(
            admit_initial_observations(
                &fixture.route,
                fixture.initial(),
                &fixture.adapter,
                &fixture.authority,
                &fixture.backend,
                0
            )
            .await
            .is_err()
        );
    }
    assert_eq!(fixture.counts.initial.load(Ordering::SeqCst), 1);
    let progress = progress_simulation(
        &fixture.route,
        ProgressRequest {
            session_id: fixture.key.session_id.clone(),
            authority_grant: fixture.key.authority_grant.clone(),
            correlation_id: vec![12],
        },
        &fixture.adapter,
        &fixture.authority,
        &fixture.backend,
        0,
    )
    .await
    .unwrap();
    assert!(
        progress.failed,
        "backend already mutated; this cannot remain an unknown retryable outcome"
    );
    assert_eq!(progress.completed_boundary, 0);
    assert!(progress.detail.is_some());
}
