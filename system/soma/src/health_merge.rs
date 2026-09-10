// SPDX-License-Identifier: MulanPSL-2.0

//! Deterministic composition of leased health primitive frames and runtime facts.

use crate::pb::soma::{
    ActuatorState, ComponentStatus, FaultState, Metric, PowerSourceState, SafetyEndpointState,
    SomaHealthSnapshot,
};
use std::collections::BTreeMap;

/// Combine independent primitive frames without letting the latest provider erase others.
pub(crate) fn merge_primitive_snapshots<'a>(
    body_id: &str,
    snapshots: impl Iterator<Item = &'a SomaHealthSnapshot>,
) -> SomaHealthSnapshot {
    let mut merged = SomaHealthSnapshot {
        schema_version: 1,
        body_id: body_id.to_string(),
        ttl_ms: u32::MAX,
        ..Default::default()
    };
    let mut components: BTreeMap<String, ComponentStatus> = BTreeMap::new();
    let mut actuators: BTreeMap<String, ActuatorState> = BTreeMap::new();
    let mut power_sources: BTreeMap<String, PowerSourceState> = BTreeMap::new();
    let mut safety_endpoints: BTreeMap<String, SafetyEndpointState> = BTreeMap::new();
    let mut faults: BTreeMap<(String, String), FaultState> = BTreeMap::new();
    let mut metrics: BTreeMap<(String, String), Metric> = BTreeMap::new();
    let mut saw_snapshot = false;

    for snapshot in snapshots {
        saw_snapshot = true;
        merged.schema_version = merged.schema_version.max(snapshot.schema_version);
        merged.source_ts_ns = merged.source_ts_ns.max(snapshot.source_ts_ns);
        merged.ttl_ms = merged.ttl_ms.min(snapshot.ttl_ms.max(1));
        merge_safety(&mut merged, snapshot);
        for component in &snapshot.components {
            components
                .entry(component.id.clone())
                .and_modify(|current| {
                    if health_rank(component.health) > health_rank(current.health) {
                        *current = component.clone();
                    }
                })
                .or_insert_with(|| component.clone());
        }
        for actuator in &snapshot.actuators {
            actuators.insert(actuator.component_id.clone(), actuator.clone());
        }
        for power in &snapshot.power_sources {
            power_sources.insert(power.component_id.clone(), power.clone());
        }
        for endpoint in &snapshot.safety_endpoints {
            safety_endpoints.insert(endpoint.name.clone(), endpoint.clone());
        }
        for fault in &snapshot.faults {
            faults.insert(
                (fault.component_id.clone(), fault.fault_id.clone()),
                fault.clone(),
            );
        }
        for metric in &snapshot.metrics {
            metrics.insert(
                (metric.component_id.clone(), metric.name.clone()),
                metric.clone(),
            );
        }
    }

    if !saw_snapshot {
        merged.ttl_ms = 1;
    }
    merged.components = components.into_values().collect();
    merged.actuators = actuators.into_values().collect();
    merged.power_sources = power_sources.into_values().collect();
    merged.safety_endpoints = safety_endpoints.into_values().collect();
    merged.faults = faults.into_values().collect();
    merged.metrics = metrics.into_values().collect();
    merged
}

/// Overlay observed primitive data while retaining unrelated ROS runtime facts.
pub(crate) fn overlay_primitive_snapshot(
    mut runtime: SomaHealthSnapshot,
    primitive: SomaHealthSnapshot,
) -> SomaHealthSnapshot {
    runtime.schema_version = runtime.schema_version.max(primitive.schema_version);
    runtime.source_ts_ns = runtime.source_ts_ns.max(primitive.source_ts_ns);
    runtime.ttl_ms = runtime.ttl_ms.max(1).min(primitive.ttl_ms.max(1));
    merge_safety(&mut runtime, &primitive);

    let mut components: BTreeMap<String, ComponentStatus> = runtime
        .components
        .drain(..)
        .map(|component| (component.id.clone(), component))
        .collect();
    for component in primitive.components {
        if component.id == "body" {
            components
                .entry(component.id.clone())
                .and_modify(|current| {
                    if health_rank(component.health) >= health_rank(current.health) {
                        *current = component.clone();
                    }
                })
                .or_insert(component);
        } else {
            components.insert(component.id.clone(), component);
        }
    }
    runtime.components = components.into_values().collect();
    overlay_by_key(&mut runtime.actuators, primitive.actuators, |item| {
        item.component_id.clone()
    });
    overlay_by_key(
        &mut runtime.power_sources,
        primitive.power_sources,
        |item| item.component_id.clone(),
    );
    overlay_by_key(
        &mut runtime.safety_endpoints,
        primitive.safety_endpoints,
        |item| item.name.clone(),
    );
    overlay_by_key(&mut runtime.faults, primitive.faults, |item| {
        format!("{}\0{}", item.component_id, item.fault_id)
    });
    overlay_by_key(&mut runtime.metrics, primitive.metrics, |item| {
        format!("{}\0{}", item.component_id, item.name)
    });
    runtime
}

/// Overlay keyed values deterministically, with incoming primitive data authoritative.
fn overlay_by_key<T>(target: &mut Vec<T>, incoming: Vec<T>, key: impl Fn(&T) -> String) {
    let mut merged: BTreeMap<String, T> = target.drain(..).map(|item| (key(&item), item)).collect();
    for item in incoming {
        merged.insert(key(&item), item);
    }
    *target = merged.into_values().collect();
}

/// Merge safety conservatively; an emergency stop wins over a generic fault.
fn merge_safety(merged: &mut SomaHealthSnapshot, snapshot: &SomaHealthSnapshot) {
    let Some(incoming) = snapshot.safety.as_ref() else {
        return;
    };
    let Some(current) = merged.safety.as_mut() else {
        merged.safety = Some(incoming.clone());
        return;
    };
    current.motion_allowed &= incoming.motion_allowed;
    current.motor_power_allowed &= incoming.motor_power_allowed;
    current.aggregate_state = match (current.aggregate_state, incoming.aggregate_state) {
        (4, _) | (_, 4) => 4,
        (5, _) | (_, 5) => 5,
        (_, state) => state,
    };
    if !incoming.detail.is_empty() && !current.detail.contains(&incoming.detail) {
        if !current.detail.is_empty() {
            current.detail.push_str("; ");
        }
        current.detail.push_str(&incoming.detail);
    }
}

fn health_rank(health: u32) -> u8 {
    match health {
        2 => 4,
        1 => 3,
        3 => 2,
        4 => 1,
        _ => 0,
    }
}
