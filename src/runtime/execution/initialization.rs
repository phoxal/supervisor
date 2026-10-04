//! Closed initialized-State cut after every controlled receiver is ready.

use super::*;

impl RuntimeExecutionProtocol {
    pub(super) async fn initialize_states(&self, timeline: &str) -> Result<(), String> {
        let request = wire::InitializeStateRequest {
            execution_id: self.inner.execution_id.clone(),
            timeline_id: timeline.to_owned(),
        };
        for runtime in &self.inner.instances {
            send(
                &self.inner.bus,
                &runtime.instance,
                "initialize-state",
                &request,
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        for runtime in &self.inner.instances {
            let sample =
                tokio::time::timeout(runtime.timeout, runtime.initialize_response.recv_async())
                    .await
                    .map_err(|_| {
                        format!("runtime {} initialized State timed out", runtime.instance)
                    })?
                    .map_err(|e| e.to_string())?;
            let response: wire::InitializeStateResponse =
                decode(sample).map_err(|e| e.to_string())?;
            if response.execution_id != request.execution_id
                || response.timeline_id != timeline
                || response.runtime_instance != runtime.instance
            {
                return Err("initialized State response has a stale execution or timeline".into());
            }
            let initialized_ports = record_parts(&self.inner.artifacts[&runtime.instance])
                .1
                .iter()
                .filter(|output| {
                    output.bootstrap
                        && output
                            .signature
                            .as_ref()
                            .is_some_and(|signature| signature.retained_latest)
                })
                .filter_map(|output| output.port.as_deref())
                .collect::<BTreeSet<_>>();
            if response.required_products.iter().any(|product| {
                product.items != u32::from(initialized_ports.contains(product.port.as_str()))
                    || product.sequence != 0
            }) {
                return Err(format!(
                    "runtime {} returned an incomplete initialized State cut",
                    runtime.instance
                ));
            }
            // Reuse receipt and fan-out validation, without invoking a runtime
            // or treating initialization as an accepted computation.
            let receipt = wire::InvocationAccepted {
                execution_id: response.execution_id,
                timeline_id: response.timeline_id,
                runtime_instance: response.runtime_instance,
                boundary: 0,
                required_products: response.required_products,
                required_deliveries: response.required_deliveries,
                ..Default::default()
            };
            validate_products(&receipt, &runtime.product_ports)?;
            let expected = expected_deliveries(
                &runtime.instance,
                &receipt,
                &self.inner.delivery_routes,
                &self.inner.request_routes,
            )?;
            wait_delivery_acknowledgements(DeliveryAckWait {
                acknowledgements: &runtime.delivery_ack,
                failures: &runtime.failures,
                execution_id: &self.inner.execution_id,
                timeline_id: timeline,
                instance: &runtime.instance,
                boundary: 0,
                expected: &expected,
                timeout: runtime.timeout,
            })
            .await?;
            trace::initialized(&receipt);
        }
        Ok(())
    }
}
