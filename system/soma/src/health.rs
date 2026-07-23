// SPDX-License-Identifier: MulanPSL-2.0
//
// Soma health data collector — discovers health primitives via Atlas,
// consumes their gRPC HealthState streams, aggregates data into
// SomaHealthSnapshot, and broadcasts to subscribers (Vitals).

use crate::pb::soma::{
    ActuatorState, ComponentStatus, FaultState, Metric, PowerSourceState, SafetyEndpointState,
    SafetyState, Scalar, SomaHealthSnapshot,
};
use crate::store::{SomaBody, SomaComponent};
use anyhow::{Context, Result};
use robonix_atlas::client::AtlasClient;
use robonix_scribe::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{Duration, interval};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

const SCHEMA_VERSION: u32 = 1;
const QUALITY_VALID: u32 = 0;
const HEALTH_OK: u32 = 0;
const HEALTH_ERROR: u32 = 2;
const HEALTH_UNKNOWN: u32 = 4;
const KIND_UNKNOWN: u32 = 0;
const KIND_BODY: u32 = 1;
const KIND_ARM: u32 = 2;
const KIND_LEG: u32 = 3;
const KIND_JOINT: u32 = 4;
const KIND_WHEEL: u32 = 5;
const KIND_GRIPPER: u32 = 6;
const KIND_BATTERY: u32 = 7;
const KIND_COMPUTER: u32 = 8;
const KIND_SENSOR: u32 = 9;
const KIND_CONTROLLER: u32 = 10;
const KIND_END_EFFECTOR: u32 = 11;
const OP_UNKNOWN: u32 = 0;
const OP_OFFLINE: u32 = 1;
const OP_ACTIVE: u32 = 4;
const OP_FAULT: u32 = 8;
const SAFETY_NORMAL: u32 = 1;
const SAFETY_FAULT: u32 = 5;
const SAFETY_ESTOP: u32 = 4;
const ESTOP_RELEASED: u32 = 0;
const ESTOP_TYPE_HARDWARE: u32 = 1;
const FAULT_ERROR: u32 = 2;

/// How often SOMA pushes a new SomaHealthSnapshot (when no new primitive data arrives).
const DEFAULT_PUSH_INTERVAL_MS: u64 = 500;

/// Start health data collection: discover primitives via Atlas,
/// consume their gRPC HealthState streams, aggregate into
/// SomaHealthSnapshot, and broadcast.
pub async fn start_health_collector(
    mut atlas: AtlasClient,
    body: Arc<SomaBody>,
    tx: broadcast::Sender<SomaHealthSnapshot>,
) -> Result<()> {
    use robonix_atlas::pb as atlas_pb;

    info!("[soma-health] discovering health primitives...");

    // Discover providers implementing robonix/primitive/health/stream.
    let capabilities = atlas
        .flatten_capabilities(
            "robonix/primitive/health/stream",
            "",
            atlas_pb::Transport::Grpc,
        )
        .await
        .context("discover health primitives")?;

    if capabilities.is_empty() {
        info!("[soma-health] no health primitives found — sending empty snapshots");
        // Keep broadcasting empty snapshots so Vitals doesn't stall.
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(DEFAULT_PUSH_INTERVAL_MS));
            let mut seq: u64 = 0;
            loop {
                tick.tick().await;
                seq += 1;
                let snapshot = empty_snapshot(&body, seq);
                let _ = tx.send(snapshot);
            }
        });
        return Ok(());
    }

    // Deduplicate by provider_id.
    let mut seen = std::collections::HashSet::new();
    let providers: Vec<_> = capabilities
        .into_iter()
        .filter(|c| seen.insert(c.provider_id.clone()))
        .collect();

    info!(
        "[soma-health] found {} health primitive(s)",
        providers.len()
    );

    for cap in providers {
        let provider_id = cap.provider_id.clone();
        let tx = tx.clone();
        let body = Arc::clone(&body);
        let mut atlas = atlas.clone();

        tokio::spawn(async move {
            if let Err(e) = consume_primitive_stream(&mut atlas, &provider_id, body, tx).await {
                robonix_scribe::warn!(
                    "[soma-health] primitive '{}' stream ended: {e:#}",
                    provider_id
                );
            }
        });
    }

    Ok(())
}

/// Connect to one health primitive's StreamHealthState gRPC and forward data.
async fn consume_primitive_stream(
    atlas: &mut AtlasClient,
    provider_id: &str,
    body: Arc<SomaBody>,
    tx: broadcast::Sender<SomaHealthSnapshot>,
) -> Result<()> {
    use crate::pb::contracts::robonix_primitive_health_stream_client::RobonixPrimitiveHealthStreamClient;
    use crate::pb::health::StreamHealthStateRequest;
    use robonix_atlas::pb as atlas_pb;

    // Get the endpoint from Atlas.
    let (_channel_id, endpoint_str, _params) = atlas
        .connect_capability(
            "soma",
            provider_id,
            "robonix/primitive/health/stream",
            atlas_pb::Transport::Grpc,
        )
        .await
        .with_context(|| format!("connect to health primitive '{provider_id}'"))?;

    let normalized = if endpoint_str.starts_with("http") {
        endpoint_str.clone()
    } else {
        format!("http://{endpoint_str}")
    };

    info!(
        "[soma-health] connected to health primitive '{}' at {}",
        provider_id, normalized
    );

    let channel = Channel::from_shared(normalized.clone())
        .context("invalid endpoint")?
        .connect()
        .await
        .context("dial health primitive")?;

    let mut client = RobonixPrimitiveHealthStreamClient::new(channel);
    let mut stream = client
        .stream_health_state(StreamHealthStateRequest {})
        .await
        .context("open StreamHealthState")?
        .into_inner();

    let mut seq: u64 = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(health_state) => {
                seq += 1;
                let snapshot = health_state_to_snapshot(&health_state, &body, seq);
                let _ = tx.send(snapshot);
            }
            Err(e) => {
                robonix_scribe::warn!("[soma-health] stream error from '{}': {e:#}", provider_id);
                break;
            }
        }
    }

    Ok(())
}

/// Convert one primitive frame using the Soma-described topology when present.
fn health_state_to_snapshot(
    state: &crate::pb::health::HealthState,
    body: &SomaBody,
    seq: u64,
) -> SomaHealthSnapshot {
    if body.components.is_empty() {
        legacy_piper_health_state_to_snapshot(state, body, seq)
    } else {
        described_health_state_to_snapshot(state, body, seq)
    }
}

/// Project generic HealthState readings onto components declared in Soma YAML.
fn described_health_state_to_snapshot(
    state: &crate::pb::health::HealthState,
    body: &SomaBody,
    seq: u64,
) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();
    let readings: HashMap<&str, &crate::pb::health::SensorReading> = state
        .readings
        .iter()
        .map(|reading| (reading.name.as_str(), reading))
        .collect();
    let safety_state = aggregate_safety_state(&readings);

    let mut components = Vec::with_capacity(body.components.len() + 1);
    components.push(observed_component(
        component(
            "body",
            "",
            KIND_BODY,
            &body.display_name,
            &body.root_link,
            &body.model_name,
        ),
        &readings,
    ));
    components.extend(body.components.iter().map(|body_component| {
        observed_component(
            component(
                &body_component.id,
                &body_component.parent_id,
                component_kind(&body_component.component_type),
                body_component
                    .id
                    .rsplit('/')
                    .next()
                    .unwrap_or(&body_component.id),
                &body_component.frame_id,
                &body_component.component_type,
            ),
            &readings,
        )
    }));

    let actuators: Vec<ActuatorState> = body
        .components
        .iter()
        .filter(|component| {
            matches!(
                component_kind(&component.component_type),
                KIND_JOINT | KIND_WHEEL
            )
        })
        .map(|component| described_actuator_state(component, &readings))
        .collect();
    let power_sources: Vec<PowerSourceState> = body
        .components
        .iter()
        .filter(|component| component_kind(&component.component_type) == KIND_BATTERY)
        .map(|component| described_power_source(component, state, &readings))
        .collect();
    let metrics = described_metrics(body, &readings);
    let faults = described_faults(body, &readings, now_ns);

    if (safety_state != SAFETY_NORMAL || !faults.is_empty())
        && let Some(root) = components.first_mut()
    {
        root.health = HEALTH_ERROR;
        root.operational_state = OP_FAULT;
        root.detail = if safety_state == SAFETY_ESTOP {
            "emergency stop active".to_string()
        } else {
            "health fault active".to_string()
        };
    }

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body.robot_id.clone(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators,
        power_sources,
        safety: Some(SafetyState {
            motion_allowed: safety_state == SAFETY_NORMAL,
            motor_power_allowed: safety_state == SAFETY_NORMAL,
            aggregate_state: safety_state,
            detail: String::new(),
        }),
        safety_endpoints: vec![SafetyEndpointState {
            name: "hardware_estop".to_string(),
            r#type: ESTOP_TYPE_HARDWARE,
            state: if safety_state == SAFETY_ESTOP { 1 } else { 0 },
            detail: String::new(),
        }],
        faults,
        metrics,
    }
}

/// Preserve the original six-joint Piper projection for legacy Soma files.
fn legacy_piper_health_state_to_snapshot(
    state: &crate::pb::health::HealthState,
    body: &SomaBody,
    seq: u64,
) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();
    let body_id = &body.robot_id;
    let arm_model = &body.model_name;

    // Build component tree: root + arm + joints.
    let mut components = vec![
        component("body", "", KIND_BODY, "Piper Robot", "base_link", "piper"),
        component(
            "body/arm",
            "body",
            KIND_ARM,
            "Piper arm",
            "arm_base_link",
            arm_model,
        ),
    ];
    for joint_idx in 1..=6 {
        components.push(component(
            &format!("body/arm/joint_{joint_idx}"),
            "body/arm",
            KIND_JOINT,
            &format!("joint_{joint_idx}"),
            &format!("joint_{joint_idx}"),
            arm_model,
        ));
    }

    // Parse sensor readings into per-joint data.
    let mut joint_temps = [0.0f64; 6];
    let mut joint_errors = [0u32; 6];
    let mut joint_enabled = [true; 6];
    let mut arm_state: u32 = SAFETY_NORMAL;

    for reading in &state.readings {
        let name = &reading.name;
        // Parse joint index from name like "body/arm/joint_N/..."
        if let Some(joint_idx) = parse_joint_index(name) {
            if name.ends_with("/motor_temp") {
                joint_temps[joint_idx] = reading.temp_c as f64;
            } else if name.ends_with("/error") {
                joint_errors[joint_idx] = reading.current_a as u32;
            } else if name.ends_with("/enabled") {
                joint_enabled[joint_idx] = reading.current_a >= 0.5;
            }
        } else if name == "body/arm/state" {
            let raw = reading.current_a as u32;
            arm_state = match raw {
                0 => SAFETY_NORMAL,
                2 => SAFETY_ESTOP,
                _ => SAFETY_FAULT,
            };
        }
    }

    // Build actuators from parsed joint data.
    let actuators: Vec<ActuatorState> = (0..6)
        .map(|i| {
            let joint_idx = (i + 1) as u32;
            ActuatorState {
                component_id: format!("body/arm/joint_{joint_idx}"),
                joint_name: format!("joint_{joint_idx}"),
                position: Some(scalar(0.0, "rad")),
                velocity: Some(scalar(0.0, "rad/s")),
                effort: Some(scalar(0.0, "Nm")),
                current: Some(scalar(0.0, "A")),
                voltage: Some(scalar(24.0, "V")),
                motor_temp: Some(scalar(joint_temps[i], "degC")),
                driver_temp: Some(scalar(joint_temps[i] + 3.0, "degC")),
                torque_enabled: joint_enabled[i],
                brake_engaged: false,
                communication_ok: joint_temps[i] >= 0.0,
                vendor_mode: 0,
                vendor_error_code: joint_errors[i],
                status_flags: joint_errors[i],
            }
        })
        .collect();

    // Build faults from non-zero error codes.
    let mut faults = Vec::new();
    for (i, err) in joint_errors.iter().enumerate() {
        if *err != 0 {
            faults.push(FaultState {
                component_id: format!("body/arm/joint_{}", i + 1),
                fault_id: "piper_foc_fault".to_string(),
                severity: FAULT_ERROR,
                active: true,
                clearable: true,
                onset_ts_ns: now_ns,
                vendor_code: *err,
                vendor_code_text: format!("0x{err:02X}"),
                message: format!("joint_{} foc_status=0x{err:02X}", i + 1),
                attributes: vec![],
                vendor_raw_json: String::new(),
            });
        }
    }

    let motion_allowed = arm_state == SAFETY_NORMAL;
    let motor_power_allowed = arm_state == SAFETY_NORMAL;

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body_id.to_string(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators,
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed,
            motor_power_allowed,
            aggregate_state: arm_state,
            detail: String::new(),
        }),
        safety_endpoints: vec![SafetyEndpointState {
            name: "hardware_estop".to_string(),
            r#type: ESTOP_TYPE_HARDWARE,
            state: ESTOP_RELEASED,
            detail: String::new(),
        }],
        faults,
        metrics: vec![],
    }
}

/// Build an empty snapshot (used when no health primitives are available).
fn empty_snapshot(body: &SomaBody, seq: u64) -> SomaHealthSnapshot {
    if body.components.is_empty() {
        legacy_empty_snapshot(body, seq)
    } else {
        described_empty_snapshot(body, seq)
    }
}

/// Build an unavailable snapshot while retaining the Soma-described topology.
fn described_empty_snapshot(body: &SomaBody, seq: u64) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();
    let mut components = Vec::with_capacity(body.components.len() + 1);
    components.push(offline_component(
        "body",
        "",
        KIND_BODY,
        &body.display_name,
        &body.root_link,
        &body.model_name,
    ));
    components.extend(body.components.iter().map(|component| {
        offline_component(
            &component.id,
            &component.parent_id,
            component_kind(&component.component_type),
            component.id.rsplit('/').next().unwrap_or(&component.id),
            &component.frame_id,
            &component.component_type,
        )
    }));
    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body.robot_id.clone(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators: vec![],
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed: false,
            motor_power_allowed: false,
            aggregate_state: SAFETY_FAULT,
            detail: "no health primitive connected".to_string(),
        }),
        safety_endpoints: vec![],
        faults: vec![],
        metrics: vec![],
    }
}

/// Preserve the original empty Piper snapshot for legacy Soma files.
fn legacy_empty_snapshot(body: &SomaBody, seq: u64) -> SomaHealthSnapshot {
    let now_ns = chrono_now_ns();
    let body_id = &body.robot_id;
    let arm_model = &body.model_name;
    let mut components = vec![
        component("body", "", KIND_BODY, "Piper Robot", "base_link", "piper"),
        component(
            "body/arm",
            "body",
            KIND_ARM,
            "Piper arm",
            "arm_base_link",
            arm_model,
        ),
    ];
    for joint_idx in 1..=6 {
        components.push(component(
            &format!("body/arm/joint_{joint_idx}"),
            "body/arm",
            KIND_JOINT,
            &format!("joint_{joint_idx}"),
            &format!("joint_{joint_idx}"),
            arm_model,
        ));
    }

    SomaHealthSnapshot {
        schema_version: SCHEMA_VERSION,
        body_id: body_id.to_string(),
        seq,
        source_ts_ns: now_ns,
        soma_ts_ns: now_ns,
        ttl_ms: 1500,
        components,
        actuators: vec![],
        power_sources: vec![],
        safety: Some(SafetyState {
            motion_allowed: false,
            motor_power_allowed: false,
            aggregate_state: SAFETY_FAULT,
            detail: "no health primitive connected".to_string(),
        }),
        safety_endpoints: vec![],
        faults: vec![],
        metrics: vec![],
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Apply availability and fault controls from one HealthState frame.
fn observed_component(
    mut status: ComponentStatus,
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
) -> ComponentStatus {
    let observed = component_observed(readings, &status.id);
    let online_control = control_bool(readings, &status.id, "online")
        .or_else(|| control_bool(readings, &status.id, "communication_ok"));
    let online = online_control.unwrap_or(observed);
    let present = control_bool(readings, &status.id, "present").unwrap_or(true);
    let error_code = control_code(readings, &status.id, "error");
    status.present = present;
    status.online = online;
    status.health = if error_code != 0 || !present || !online {
        HEALTH_ERROR
    } else if observed || online_control.is_some() {
        HEALTH_OK
    } else {
        HEALTH_UNKNOWN
    };
    status.operational_state = match status.health {
        HEALTH_OK => OP_ACTIVE,
        HEALTH_ERROR => OP_FAULT,
        _ => OP_UNKNOWN,
    };
    if error_code != 0 {
        status.detail = format!("error_code=0x{error_code:X}");
    } else if !present || !online {
        status.detail = format!("present={present}; online={online}");
    }
    status
}

/// Convert a Soma component type label into the stable wire enum value.
fn component_kind(component_type: &str) -> u32 {
    match component_type.trim().to_ascii_lowercase().as_str() {
        "body" | "mobile_base" => KIND_BODY,
        "arm" => KIND_ARM,
        "leg" => KIND_LEG,
        "joint" => KIND_JOINT,
        "wheel" => KIND_WHEEL,
        "gripper" => KIND_GRIPPER,
        "battery" => KIND_BATTERY,
        "computer" => KIND_COMPUTER,
        "controller" => KIND_CONTROLLER,
        "end_effector" => KIND_END_EFFECTOR,
        "sensor" | "camera" | "rgb_camera" | "depth_camera" | "rgbd_camera" | "lidar"
        | "lidar_2d" | "lidar_3d" | "imu" | "audio_io" => KIND_SENSOR,
        _ => KIND_UNKNOWN,
    }
}

/// Build actuator telemetry for one described joint or wheel.
fn described_actuator_state(
    component: &SomaComponent,
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
) -> ActuatorState {
    let reading = readings.get(component.id.as_str()).copied();
    let motor_temp = reading.or_else(|| child_reading(readings, &component.id, "motor_temp"));
    let driver_temp = child_reading(readings, &component.id, "driver_temp");
    let error_code = control_code(readings, &component.id, "error");
    ActuatorState {
        component_id: component.id.clone(),
        joint_name: component
            .id
            .rsplit('/')
            .next()
            .unwrap_or(&component.id)
            .to_string(),
        position: None,
        velocity: None,
        effort: None,
        current: reading.and_then(|value| observed_scalar(value.current_a, "A")),
        voltage: reading.and_then(|value| observed_scalar(value.voltage, "V")),
        motor_temp: motor_temp.and_then(|value| observed_scalar(value.temp_c, "degC")),
        driver_temp: driver_temp.and_then(|value| observed_scalar(value.temp_c, "degC")),
        torque_enabled: control_bool(readings, &component.id, "enabled").unwrap_or(true),
        brake_engaged: control_bool(readings, &component.id, "brake_engaged").unwrap_or(false),
        communication_ok: control_bool(readings, &component.id, "communication_ok")
            .unwrap_or(motor_temp.is_some()),
        vendor_mode: 0,
        vendor_error_code: error_code,
        status_flags: error_code,
    }
}

/// Build one battery state, preferring component readings over frame totals.
fn described_power_source(
    component: &SomaComponent,
    state: &crate::pb::health::HealthState,
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
) -> PowerSourceState {
    let reading = readings.get(component.id.as_str()).copied();
    let voltage = reading
        .and_then(|value| observed_scalar(value.voltage, "V"))
        .or_else(|| observed_scalar(state.voltage, "V"));
    let current = reading
        .and_then(|value| observed_scalar(value.current_a, "A"))
        .or_else(|| Some(scalar(if state.charging { 1.0 } else { 0.0 }, "A")));
    PowerSourceState {
        component_id: component.id.clone(),
        soc_percent: reading.and_then(|value| observed_scalar(value.battery_percent, "percent")),
        soh_percent: None,
        voltage,
        current,
        temperature: reading.and_then(|value| observed_scalar(value.temp_c, "degC")),
        remaining_s: (state.remaining_s >= 0).then(|| scalar(state.remaining_s as f64, "s")),
        cycle_count: 0,
        cell_voltages: vec![],
        vendor_status_code: control_code(readings, &component.id, "error"),
    }
}

/// Convert non-actuator component readings into thresholdable Soma metrics.
fn described_metrics(
    body: &SomaBody,
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
) -> Vec<Metric> {
    let mut metrics = Vec::new();
    for component in &body.components {
        if matches!(
            component_kind(&component.component_type),
            KIND_JOINT | KIND_WHEEL | KIND_BATTERY
        ) {
            continue;
        }
        let Some(reading) = readings.get(component.id.as_str()).copied() else {
            continue;
        };
        push_metric(
            &mut metrics,
            &component.id,
            "temperature",
            observed_scalar(reading.temp_c, "degC"),
        );
        push_metric(
            &mut metrics,
            &component.id,
            "voltage",
            observed_scalar(reading.voltage, "V"),
        );
        push_metric(
            &mut metrics,
            &component.id,
            "current",
            observed_scalar(reading.current_a, "A"),
        );
    }
    metrics
}

/// Append an available metric while omitting unavailable scalar fields.
fn push_metric(metrics: &mut Vec<Metric>, component_id: &str, name: &str, value: Option<Scalar>) {
    if let Some(value) = value {
        metrics.push(Metric {
            component_id: component_id.to_string(),
            name: name.to_string(),
            value: Some(value),
            source_key: component_id.to_string(),
        });
    }
}

/// Convert non-zero component error controls into active fault records.
fn described_faults(
    body: &SomaBody,
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
    now_ns: i64,
) -> Vec<FaultState> {
    body.components
        .iter()
        .filter_map(|component| {
            let error_code = control_code(readings, &component.id, "error");
            (error_code != 0).then(|| FaultState {
                component_id: component.id.clone(),
                fault_id: "device_fault".to_string(),
                severity: FAULT_ERROR,
                active: true,
                clearable: true,
                onset_ts_ns: now_ns,
                vendor_code: error_code,
                vendor_code_text: format!("0x{error_code:X}"),
                message: format!("{} error_code=0x{error_code:X}", component.id),
                attributes: vec![],
                vendor_raw_json: String::new(),
            })
        })
        .collect()
}

/// Read the whole-body safety code, with the legacy arm key as fallback.
fn aggregate_safety_state(readings: &HashMap<&str, &crate::pb::health::SensorReading>) -> u32 {
    let raw = readings
        .get("body/state")
        .or_else(|| readings.get("body/arm/state"))
        .map(|reading| reading.current_a as u32)
        .unwrap_or(0);
    match raw {
        0 => SAFETY_NORMAL,
        2 => SAFETY_ESTOP,
        _ => SAFETY_FAULT,
    }
}

/// Treat exact and suffixed readings as observations of the same component.
fn component_observed(
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
    component_id: &str,
) -> bool {
    let child_prefix = format!("{component_id}/");
    readings.contains_key(component_id)
        || readings.keys().any(|name| name.starts_with(&child_prefix))
}

/// Look up a control reading encoded as `<component>/<suffix>`.
fn child_reading<'a>(
    readings: &'a HashMap<&str, &'a crate::pb::health::SensorReading>,
    component_id: &str,
    suffix: &str,
) -> Option<&'a crate::pb::health::SensorReading> {
    let key = format!("{component_id}/{suffix}");
    readings.get(key.as_str()).copied()
}

fn control_bool(
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
    component_id: &str,
    suffix: &str,
) -> Option<bool> {
    child_reading(readings, component_id, suffix).map(|reading| reading.current_a >= 0.5)
}

fn control_code(
    readings: &HashMap<&str, &crate::pb::health::SensorReading>,
    component_id: &str,
    suffix: &str,
) -> u32 {
    child_reading(readings, component_id, suffix)
        .map(|reading| reading.current_a.max(0.0) as u32)
        .unwrap_or(0)
}

fn observed_scalar(value: f32, unit: &str) -> Option<Scalar> {
    (value.is_finite() && value >= 0.0).then(|| scalar(value as f64, unit))
}

/// Create one topology entry marked unavailable when no provider exists.
fn offline_component(
    id: &str,
    parent_id: &str,
    kind: u32,
    name: &str,
    frame_id: &str,
    model: &str,
) -> ComponentStatus {
    let mut status = component(id, parent_id, kind, name, frame_id, model);
    status.health = HEALTH_UNKNOWN;
    status.operational_state = OP_OFFLINE;
    status.online = false;
    status.detail = "no health primitive connected".to_string();
    status
}

/// Construct one component record before applying frame-specific health.
fn component(
    id: &str,
    parent_id: &str,
    kind: u32,
    name: &str,
    frame_id: &str,
    model: &str,
) -> ComponentStatus {
    ComponentStatus {
        id: id.to_string(),
        parent_id: parent_id.to_string(),
        kind,
        name: name.to_string(),
        frame_id: frame_id.to_string(),
        model: model.to_string(),
        serial: String::new(),
        health: HEALTH_UNKNOWN,
        operational_state: OP_ACTIVE,
        present: true,
        online: true,
        detail: String::new(),
    }
}

fn scalar(value: f64, unit: &str) -> Scalar {
    Scalar {
        value,
        unit: unit.to_string(),
        quality: QUALITY_VALID,
    }
}

/// Parse "body/arm/joint_N/..." → Some(N-1) (0-indexed), or None.
fn parse_joint_index(name: &str) -> Option<usize> {
    let prefix = "body/arm/joint_";
    let rest = name.strip_prefix(prefix)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let idx: usize = digits.parse().ok()?;
    if !(1..=6).contains(&idx) {
        return None;
    }
    Some(idx - 1)
}

fn chrono_now_ns() -> i64 {
    // Use a simple monotonic approach; avoids pulling in chrono just for this.
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    start.elapsed().as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::health::{HealthState, SensorReading};
    use std::path::PathBuf;

    fn webots_body() -> SomaBody {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("examples/webots/soma.yaml");
        SomaBody::load(&path).expect("load Webots body")
    }

    /// Build one test reading with battery data marked unavailable.
    fn reading(name: &str, temp_c: f32, voltage: f32, current_a: f32) -> SensorReading {
        SensorReading {
            name: name.to_string(),
            temp_c,
            voltage,
            current_a,
            battery_percent: -1.0,
        }
    }

    #[test]
    fn parse_joint_index_valid() {
        assert_eq!(parse_joint_index("body/arm/joint_1/motor_temp"), Some(0));
        assert_eq!(parse_joint_index("body/arm/joint_6/enabled"), Some(5));
        assert_eq!(parse_joint_index("body/arm/joint_3/error"), Some(2));
    }

    #[test]
    fn parse_joint_index_invalid() {
        assert_eq!(parse_joint_index("body/arm/joint_7/motor_temp"), None);
        assert_eq!(parse_joint_index("body/arm/joint_0/motor_temp"), None);
        assert_eq!(parse_joint_index("body/leg/joint_1/motor_temp"), None);
        assert_eq!(parse_joint_index("random_string"), None);
    }

    /// Nominal TIAGo readings retain wheel, battery, and sensor semantics.
    #[test]
    fn projects_described_webots_health() {
        let mut battery = reading("body/base/battery", 32.0, 24.8, 0.0);
        battery.battery_percent = 82.0;
        let state = HealthState {
            voltage: 24.8,
            charging: false,
            remaining_s: 10800,
            readings: vec![
                reading("body", 36.0, -1.0, -1.0),
                reading("body/base", 34.0, -1.0, -1.0),
                reading("body/base/left_wheel", 38.0, 24.8, 0.7),
                reading("body/base/left_wheel/driver_temp", 41.0, -1.0, -1.0),
                reading("body/base/left_wheel/communication_ok", -1.0, -1.0, 1.0),
                reading("body/base/right_wheel", 38.5, 24.8, 0.7),
                reading("body/base/right_wheel/driver_temp", 41.5, -1.0, -1.0),
                reading("body/base/right_wheel/communication_ok", -1.0, -1.0, 1.0),
                battery,
                reading("body/head_camera", 42.0, -1.0, -1.0),
                reading("body/hokuyo_lidar", 39.0, -1.0, -1.0),
                reading("body/audio", 35.0, -1.0, -1.0),
                reading("body/state", -1.0, -1.0, 0.0),
            ],
        };

        let snapshot = health_state_to_snapshot(&state, &webots_body(), 7);
        assert_eq!(snapshot.body_id, "tiago_webots");
        assert_eq!(snapshot.seq, 7);
        assert_eq!(snapshot.actuators.len(), 2);
        assert!(
            snapshot
                .actuators
                .iter()
                .all(|actuator| actuator.communication_ok)
        );
        assert!(snapshot.actuators.iter().all(|actuator| {
            actuator
                .motor_temp
                .as_ref()
                .is_some_and(|temperature| temperature.value < 40.0)
        }));
        assert_eq!(snapshot.power_sources.len(), 1);
        assert_eq!(
            snapshot.power_sources[0]
                .soc_percent
                .as_ref()
                .expect("battery SOC")
                .value,
            82.0
        );
        assert!(snapshot.faults.is_empty());
        assert_eq!(
            snapshot.safety.as_ref().expect("safety").aggregate_state,
            SAFETY_NORMAL
        );
        assert!(snapshot.metrics.iter().any(|metric| {
            metric.component_id == "body/head_camera" && metric.name == "temperature"
        }));
        assert!(snapshot.components.iter().any(|component| {
            component.id == "body/base/left_wheel"
                && component.kind == KIND_WHEEL
                && component.health == HEALTH_OK
        }));
    }

    /// Existing suffix-only actuator producers remain valid with a component tree.
    #[test]
    fn accepts_suffix_only_actuator_readings() {
        let state = HealthState {
            voltage: -1.0,
            charging: false,
            remaining_s: -1,
            readings: vec![
                reading("body/base/left_wheel/motor_temp", 37.0, -1.0, -1.0),
                reading("body/base/left_wheel/enabled", -1.0, -1.0, 1.0),
                reading("body/base/left_wheel/error", -1.0, -1.0, 0.0),
            ],
        };

        let snapshot = health_state_to_snapshot(&state, &webots_body(), 1);
        let wheel = snapshot
            .actuators
            .iter()
            .find(|actuator| actuator.component_id == "body/base/left_wheel")
            .expect("left wheel actuator");
        assert!(wheel.communication_ok);
        assert!(wheel.torque_enabled);
        assert_eq!(wheel.motor_temp.as_ref().expect("motor temp").value, 37.0);
        assert!(snapshot.components.iter().any(|component| {
            component.id == "body/base/left_wheel" && component.health == HEALTH_OK
        }));
    }
}
