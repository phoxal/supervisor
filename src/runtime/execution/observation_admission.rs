//! A simulation cut is admitted only after every connected receiver accepts it.

use super::{
    ExpectedDelivery, MAX_PRODUCT_RECEIPTS, RuntimeExecutionProtocol, decode,
    delivery_ack_candidate, wire,
};
use phoxal::communication::simulation::{Observation, ProductDisposition};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

impl RuntimeExecutionProtocol {
    pub(super) fn observation_deliveries(
        &self,
        observations: &[Observation],
    ) -> Result<BTreeMap<String, BTreeSet<ExpectedDelivery>>, String> {
        let mut deliveries = BTreeMap::<String, BTreeSet<ExpectedDelivery>>::new();
        let mut total = 0usize;
        for observation in observations {
            let membership = observation
                .membership
                .as_ref()
                .ok_or("observation membership missing")?;
            if membership.disposition != ProductDisposition::Present {
                continue;
            }
            if membership.item_count != 1 {
                return Err("simulation observation must encode exactly one declared item".into());
            }
            let route = (
                membership.producer.clone(),
                membership.port.clone(),
                "publish".into(),
            );
            for target in self.inner.delivery_routes.get(&route).into_iter().flatten() {
                total += 1;
                if total > MAX_PRODUCT_RECEIPTS {
                    return Err(
                        "simulation observation fan-out exceeds acknowledgement capacity".into(),
                    );
                }
                deliveries
                    .entry(membership.producer.clone())
                    .or_default()
                    .insert(ExpectedDelivery {
                        source: membership.producer.clone(),
                        target: target.clone(),
                        port: membership.port.clone(),
                        direction: "publish".into(),
                        sequence: membership.sequence,
                        item: 0,
                        bytes: membership.encoded_bytes,
                    });
            }
        }
        Ok(deliveries)
    }

    pub(super) async fn wait_observation_admission(
        &self,
        observations: &[Observation],
        timeline: &str,
        boundary: u64,
    ) -> Result<(), String> {
        let deliveries = self.observation_deliveries(observations)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        for (producer, mut pending) in deliveries {
            let subscriber = self
                .inner
                .observation_acknowledgements
                .get(&producer)
                .ok_or_else(|| format!("no observation admission channel for {producer}"))?;
            while !pending.is_empty() {
                let sample = tokio::select! {
                    _ = self.inner.failed.cancelled() => return Err("execution failed while admitting observations".into()),
                    received = tokio::time::timeout_at(deadline, subscriber.recv_async()) => received
                        .map_err(|_| format!("observation admission timed out for {producer} at boundary {boundary}"))?
                        .map_err(|e| e.to_string())?,
                };
                let ack: wire::DeliveryAck = decode(sample).map_err(|e| e.to_string())?;
                let Some(candidate) = delivery_ack_candidate(
                    &ack,
                    &self.inner.execution_id,
                    timeline,
                    &producer,
                    boundary,
                    &pending,
                ) else {
                    continue;
                };
                if !ack.admitted {
                    return Err(ack.detail.unwrap_or_else(|| {
                        format!("{} rejected observation admission", candidate.target)
                    }));
                }
                pending.remove(&candidate);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::state::ExecutionState;
    use phoxal::communication::simulation::ProductMembership;
    use phoxal::{
        identity::ExecutionId,
        runtime::connection::{ConnectionConfig, ConnectionOwner},
    };
    use std::{path::Path, sync::Arc};

    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observation_admission_waits_for_exact_receiver_ack_and_propagates_refusal() {
        let temporary = tempfile::tempdir().unwrap();
        let endpoint = format!(
            "unixsock-stream/{}",
            temporary.path().join("router.sock").display()
        );
        let execution = ExecutionId::mint();
        let router = crate::runtime::router::start_embedded_router(
            execution,
            endpoint.clone(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        let (owner, bus) = ConnectionOwner::open(ConnectionConfig::for_external(
            execution,
            None,
            vec![endpoint],
        ))
        .await
        .unwrap();
        let mut source = crate::runtime::bundle::RuntimeBundle::for_test_with_connections(
            Path::new("."),
            "observations",
            vec![crate::runtime::bundle::TestExecutable::with_artifact(
                "consumer",
                serde_json::json!({
                    "runtime": { "schema": "phoxal/artifact/v0", "record": "runtime", "period_ms": 20, "timeout_ms": 500,
                        "inputs": [{"name": "value", "delivery": "observation_latest", "port": "value", "max_items": 1, "max_bytes": 8}],
                        "outputs": [] }, "descriptors": []
                }),
            )],
            BTreeMap::from([(
                "consumer.value".to_owned(),
                serde_json::json!("sensor.value"),
            )]),
        );
        source.set_simulation(phoxal::artifact::bundle::BundleSimulation {
            protocol: "phoxal.simulation.v1".into(),
            mode: "controlled".into(),
            model_identity: "fixture".into(),
            quantum_ns: 2_000_000,
            providers: vec![phoxal::artifact::bundle::BundleSimulationProvider {
                rate_microhertz: 100_000_000,
                service_fqn: "fixture.Sensor".into(),
                method: "Sample".into(),
                service_instance: "sensor".into(),
                port: "value".into(),
                shape: phoxal::artifact::MethodShape::Observation,
                retained_latest: false,
                lease_valid_for_ms: None,
                input_fqn: "test.Empty".into(),
                payload_fqn: "test.Value".into(),
                max_message_bytes: 8,
                max_buffered_items: 1,
            }],
            actuation_bindings: vec![],
        });
        let protocol =
            RuntimeExecutionProtocol::open(bus.clone(), &source, ExecutionState::new(), None)
                .await
                .unwrap();
        let observations = vec![Observation {
            membership: Some(ProductMembership {
                producer: "sensor".into(),
                port: "value".into(),
                sequence: 1,
                capture_boundary: 0,
                disposition: ProductDisposition::Present,
                item_count: 1,
                encoded_bytes: 1,
                ..Default::default()
            }),
            payload: vec![7],
        }];
        let pending_protocol = protocol.clone();
        let pending_observations = observations.clone();
        let mut waiting = tokio::spawn(async move {
            pending_protocol
                .wait_observation_admission(&pending_observations, "timeline", 0)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut waiting)
                .await
                .is_err(),
            "publication alone cannot admit the cut"
        );
        let mut ack = wire::DeliveryAck {
            execution_id: execution.to_string(),
            timeline_id: "old-timeline".into(),
            boundary: 0,
            source: "sensor".into(),
            target: "consumer.value".into(),
            port: "value".into(),
            direction: "publish".into(),
            sequence: 1,
            item: 0,
            bytes: 1,
            admitted: true,
            detail: None,
        };
        super::super::send(&bus, "sensor", "delivery-ack", &ack)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut waiting)
                .await
                .is_err(),
            "stale acknowledgement cannot release the barrier"
        );
        ack.timeline_id = "timeline".into();
        super::super::send(&bus, "sensor", "delivery-ack", &ack)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut next = observations.clone();
        next[0].membership.as_mut().unwrap().sequence = 2;
        ack.sequence = 2;
        ack.admitted = false;
        ack.detail = Some("receiver byte budget exhausted".into());
        super::super::send(&bus, "sensor", "delivery-ack", &ack)
            .await
            .unwrap();
        let rejected = protocol
            .wait_observation_admission(&next, "timeline", 0)
            .await
            .unwrap_err();
        assert!(rejected.contains("byte budget exhausted"));
        next[0].membership.as_mut().unwrap().disposition = ProductDisposition::NotDue;
        next[0].membership.as_mut().unwrap().item_count = 0;
        next[0].payload.clear();
        protocol
            .wait_observation_admission(&next, "timeline", 0)
            .await
            .expect("NotDue has no transport record to admit");
        owner.close().await;
        router.close().await.unwrap();
    }
}
