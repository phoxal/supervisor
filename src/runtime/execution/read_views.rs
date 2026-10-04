//! Capture immutable query views before releasing any due invocation.

use super::{DEFAULT_RUNTIME_TIMEOUT, RuntimeExecutionProtocol, decode, send, wire};

impl RuntimeExecutionProtocol {
    pub(super) async fn pin_read_views(&self, timeline: &str, boundary: u64) -> Result<(), String> {
        let request = wire::PinReadViewsRequest {
            execution_id: self.inner.execution_id.clone(),
            timeline_id: timeline.to_owned(),
            boundary,
        };
        let deadline = tokio::time::Instant::now() + DEFAULT_RUNTIME_TIMEOUT;
        for runtime in self
            .inner
            .instances
            .iter()
            .filter(|runtime| runtime.pin_response.is_some())
        {
            send(
                &self.inner.bus,
                &runtime.instance,
                "pin-read-views",
                &request,
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        for runtime in &self.inner.instances {
            let Some(subscriber) = &runtime.pin_response else {
                continue;
            };
            // Capturing a view is a transport barrier, not a service invocation.
            // Repeating this exact request preserves the first pinned snapshot.
            let result = tokio::time::timeout_at(deadline, async {
                let mut retry = tokio::time::interval(std::time::Duration::from_millis(50));
                retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                retry.tick().await;
                loop {
                    let sample = tokio::select! {
                        biased;
                        _ = self.inner.failed.cancelled() => return Err("execution failed while pinning Read views".to_owned()),
                        sample = subscriber.recv_async() => sample.map_err(|e| e.to_string())?,
                        _ = retry.tick() => {
                            send(&self.inner.bus, &runtime.instance, "pin-read-views", &request).await.map_err(|e| e.to_string())?;
                            continue;
                        }
                    };
                    let response: wire::PinReadViewsResponse = decode(sample).map_err(|e| e.to_string())?;
                    if response.execution_id != self.inner.execution_id || response.timeline_id != timeline
                        || response.boundary != boundary || response.runtime_instance != runtime.instance { continue; }
                    return if response.admitted { Ok(()) } else { Err(response.detail.unwrap_or_else(|| "Read pin refused".to_owned())) };
                }
            }).await.map_err(|_| format!("runtime {} timed out pinning Read views", runtime.instance))?;
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        bundle::{RuntimeBundle, TestExecutable},
        state::ExecutionState,
    };
    use phoxal::runtime::connection::{ConnectionConfig, ConnectionOwner};
    use std::{collections::BTreeMap, path::Path, sync::Arc};

    #[serial_test::serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lost_pin_response_retries_the_exact_request_and_rejects_stale_acknowledgements() {
        let temporary = tempfile::tempdir().unwrap();
        let execution = phoxal::identity::ExecutionId::mint();
        let endpoint = format!(
            "unixsock-stream/{}",
            temporary.path().join("router.sock").display()
        );
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
        let source = RuntimeBundle::for_test_with_connections(
            Path::new("."),
            "read-pin",
            vec![TestExecutable::with_artifact(
                "reader",
                serde_json::json!({"runtime": {
                "period_ms": 20, "timeout_ms": 10, "inputs": [], "outputs": [{
                    "name": "view",
                    "port": "window",
                    "signature": {
                        "shape": "call",
                        "retained_latest": false,
                        "lease_valid_for_ms": null
                    },
                    "max_request_bytes": 64,
                    "max_bytes": 64
                }]
            }, "descriptors": []}),
            )],
            BTreeMap::new(),
        );
        let protocol =
            RuntimeExecutionProtocol::open(bus.clone(), &source, ExecutionState::new(), None)
                .await
                .unwrap();
        let requests = super::super::declare(&bus, "reader", "pin-read-views")
            .await
            .unwrap();
        let actor_bus = bus.clone();
        let actor = tokio::spawn(async move {
            let first: wire::PinReadViewsRequest =
                decode(requests.recv_async().await.unwrap()).unwrap();
            let second: wire::PinReadViewsRequest =
                decode(requests.recv_async().await.unwrap()).unwrap();
            assert_eq!(first, second);
            let mut response = wire::PinReadViewsResponse {
                execution_id: first.execution_id,
                timeline_id: "retired".to_owned(),
                boundary: first.boundary,
                runtime_instance: "reader".to_owned(),
                admitted: false,
                detail: Some("stale".to_owned()),
            };
            send(&actor_bus, "reader", "pin-read-views-response", &response)
                .await
                .unwrap();
            response.timeline_id = first.timeline_id;
            response.admitted = true;
            response.detail = None;
            send(&actor_bus, "reader", "pin-read-views-response", &response)
                .await
                .unwrap();
        });
        protocol.pin_read_views("timeline", 1).await.unwrap();
        actor.await.unwrap();
        owner.close().await;
        router.close().await.unwrap();
    }
}
