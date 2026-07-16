// SPDX-License-Identifier: MulanPSL-2.0
//
// Soma ingestion converts SomaHealthSnapshot facts into the target Vitals v1
// output surface. Vitals keeps ownership of threshold judgement here.

use crate::pb::contracts::robonix_system_soma_health_client::RobonixSystemSomaHealthClient;
use crate::pb::soma::{
    ActuatorState, ComponentStatus, FaultState, Scalar, SomaHealthSnapshot, StreamHealthRequest,
};
use crate::pb::vitals::{BodyComponent, BodyHealth, HealthSignal, PowerState, VitalsSnapshot};
use anyhow::{Context, Result};
use robonix_atlas::client::{self as atlas_client, AtlasClient};
use std::collections::HashMap;
use tonic::transport::{Channel, Endpoint};

/// Component health is nominal.
pub const HEALTH_OK: u32 = 0;
/// Component health is degraded but functional.
pub const HEALTH_WARN: u32 = 1;
/// Component health requires attention.
pub const HEALTH_ERROR: u32 = 2;
/// Component data is stale (sensor / stream timed out).
pub const HEALTH_STALE: u32 = 3;

const QUALITY_VALID: u32 = 0;
const QUALITY_STALE: u32 = 1;
const QUALITY_INVALID: u32 = 3;

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

const SAFETY_ESTOP: u32 = 4;
const SAFETY_FAULT: u32 = 5;

/// Device health is independent from observation quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealthSeverity {
    Ok,
    Warn,
    Error,
}

/// Fault observation quality does not manufacture device-health evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationQuality {
    Valid,
    Unknown,
}

/// One classified fault occurrence before the public v1 compatibility step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FaultAssessment {
    health: Option<HealthSeverity>,
    quality: ObservationQuality,
}

/// Classify a Soma fault severity exactly once. Inactive faults never produce
/// device health, while unknown raw values remain quality uncertainty.
fn classify_fault(active: bool, raw_severity: u32) -> FaultAssessment {
    let recognized = match raw_severity {
        0 => Some(HealthSeverity::Ok),
        1 => Some(HealthSeverity::Warn),
        2 | 3 => Some(HealthSeverity::Error),
        _ => None,
    };
    FaultAssessment {
        health: active.then_some(recognized).flatten(),
        quality: if recognized.is_some() {
            ObservationQuality::Valid
        } else {
            ObservationQuality::Unknown
        },
    }
}

/// One threshold evaluation rule keyed by a component selector + signal name.
#[derive(Debug, Clone, Default)]
pub struct SomaThresholdRule {
    #[allow(dead_code)] // Rule ids are kept for logging/debug output as the pipeline grows.
    pub id: String,
    pub selector: SomaThresholdSelector,
    pub warn_above: Option<f64>,
    pub error_above: Option<f64>,
    pub warn_below: Option<f64>,
    pub error_below: Option<f64>,
    #[allow(dead_code)] // Units document threshold intent; comparisons use normalized Soma units.
    pub unit: String,
}

/// Identifies which components a threshold rule applies to. Priority:
/// exact component_id (3) > component_id_glob (2) > kind (1).
#[derive(Debug, Clone, Default)]
pub struct SomaThresholdSelector {
    pub kind: Option<u32>,
    pub component_id: Option<String>,
    pub component_id_glob: Option<String>,
    pub signal: String,
}

#[derive(Debug, Clone, Copy)]
struct Threshold {
    warn_above: Option<f64>,
    error_above: Option<f64>,
    warn_below: Option<f64>,
    error_below: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct ThresholdBounds {
    warn_above: Option<f64>,
    error_above: Option<f64>,
    warn_below: Option<f64>,
    error_below: Option<f64>,
}

/// Open the Soma health stream, either from an explicit endpoint or via Atlas
/// discovery.  Returns `Ok(None)` when Atlas discovery finds no provider and
/// no explicit endpoint was supplied, signalling that no Soma is available.
pub async fn open_soma_stream(
    atlas: &mut AtlasClient,
    consumer_id: &str,
    endpoint: Option<&str>,
) -> Result<Option<tonic::codec::Streaming<SomaHealthSnapshot>>> {
    let channel = if let Some(endpoint) = endpoint {
        ChannelSource::Direct(connect_direct(endpoint).await?)
    } else {
        match atlas_client::connect_to_capability(atlas, consumer_id, "robonix/system/soma/health")
            .await
        {
            Ok((_channel_id, provider_id, channel)) => {
                log::info!("[vitals] connected to Soma provider '{provider_id}' through Atlas");
                ChannelSource::Discovered(channel)
            }
            Err(e) => {
                log::info!("[vitals] Soma health stream not available: {e:#}");
                return Ok(None);
            }
        }
    };

    let mut client = RobonixSystemSomaHealthClient::new(channel.into_channel());
    let stream = client
        .stream_health(StreamHealthRequest {})
        .await
        .context("open Soma StreamHealth")?
        .into_inner();
    Ok(Some(stream))
}

enum ChannelSource {
    Direct(Channel),
    Discovered(Channel),
}

impl ChannelSource {
    fn into_channel(self) -> Channel {
        match self {
            Self::Direct(c) | Self::Discovered(c) => c,
        }
    }
}

/// Load the selector-style Soma threshold YAML. If the file is an older
/// Vitals threshold file or contains no rules, default demo-safe rules are
/// returned so Soma mock scenarios work out of the box.
pub fn load_soma_thresholds(yaml_str: &str) -> Result<Vec<SomaThresholdRule>> {
    #[derive(serde::Deserialize)]
    struct Doc {
        #[serde(default)]
        rules: Vec<RuleYaml>,
    }

    #[derive(serde::Deserialize)]
    struct RuleYaml {
        id: String,
        selector: SelectorYaml,
        #[serde(default)]
        warn_above: Option<f64>,
        #[serde(default)]
        error_above: Option<f64>,
        #[serde(default)]
        warn_below: Option<f64>,
        #[serde(default)]
        error_below: Option<f64>,
        #[serde(default)]
        unit: String,
    }

    #[derive(serde::Deserialize)]
    struct SelectorYaml {
        #[serde(default)]
        kind: Option<String>,
        #[serde(default)]
        component_id: Option<String>,
        #[serde(default)]
        component_id_glob: Option<String>,
        signal: String,
    }

    let doc: Doc = serde_yaml::from_str(yaml_str)?;
    if doc.rules.is_empty() {
        return Ok(default_thresholds());
    }
    let mut out = Vec::with_capacity(doc.rules.len());
    for rule in doc.rules {
        let kind = match rule.selector.kind.as_deref() {
            Some(raw) => {
                let k = kind_from_name(raw);
                if k.is_none() {
                    log::error!(
                        "[vitals] threshold rule '{}': unrecognized kind '{}' — rule will not fire via kind selector",
                        rule.id,
                        raw
                    );
                }
                k
            }
            None => None,
        };
        out.push(SomaThresholdRule {
            id: rule.id,
            selector: SomaThresholdSelector {
                kind,
                component_id: rule.selector.component_id,
                component_id_glob: rule.selector.component_id_glob,
                signal: rule.selector.signal,
            },
            warn_above: rule.warn_above,
            error_above: rule.error_above,
            warn_below: rule.warn_below,
            error_below: rule.error_below,
            unit: rule.unit,
        });
    }
    Ok(out)
}

/// Return the built-in Soma threshold rules used when no YAML file is found.
pub fn default_thresholds() -> Vec<SomaThresholdRule> {
    vec![
        rule_kind(
            "joint_motor_temp",
            KIND_JOINT,
            "motor_temp",
            ThresholdBounds::above(60.0, 75.0),
            "degC",
        ),
        rule_kind(
            "joint_driver_temp",
            KIND_JOINT,
            "driver_temp",
            ThresholdBounds::above(70.0, 85.0),
            "degC",
        ),
        rule_kind(
            "wheel_driver_temp",
            KIND_WHEEL,
            "driver_temp",
            ThresholdBounds::above(75.0, 90.0),
            "degC",
        ),
        rule_kind(
            "sensor_temperature",
            KIND_SENSOR,
            "temperature",
            ThresholdBounds::above(80.0, 90.0),
            "degC",
        ),
        rule_kind(
            "battery_soc",
            KIND_BATTERY,
            "soc_percent",
            ThresholdBounds::below(20.0, 8.0),
            "percent",
        ),
        rule_kind(
            "battery_voltage",
            KIND_BATTERY,
            "voltage",
            ThresholdBounds::below(22.0, 19.0),
            "V",
        ),
    ]
}

/// Convert one Soma snapshot into the Vitals v1 output contract, deriving
/// sparse health signals from Soma facts, active faults, and thresholds.
///
/// Soma `ttl_ms` is accepted but intentionally not read in Vitals v1; this
/// converter performs no TTL watchdog or freshness synthesis.
pub fn snapshot_to_vitals(
    snapshot: &SomaHealthSnapshot,
    rules: &[SomaThresholdRule],
    ts_ns: u64,
) -> VitalsSnapshot {
    let component_kind: HashMap<&str, u32> = snapshot
        .components
        .iter()
        .map(|c| (c.id.as_str(), c.kind))
        .collect();
    let mut health_signals = Vec::new();

    for actuator in &snapshot.actuators {
        let kind = component_kind
            .get(actuator.component_id.as_str())
            .copied()
            .unwrap_or(KIND_JOINT);
        push_scalar_health(
            &mut health_signals,
            rules,
            &actuator.component_id,
            kind,
            "motor_temp",
            actuator.motor_temp.as_ref(),
        );
        push_scalar_health(
            &mut health_signals,
            rules,
            &actuator.component_id,
            kind,
            "driver_temp",
            actuator.driver_temp.as_ref(),
        );
        if !actuator.communication_ok {
            health_signals.push(HealthSignal {
                key: format!("{}/communication", actuator.component_id),
                status: HEALTH_ERROR,
                detail: format!("{} communication is not OK", actuator.component_id),
                observed_value: 0.0,
                reference_value: 1.0,
            });
        }
        if actuator.vendor_error_code != 0 {
            health_signals.push(HealthSignal {
                key: format!("{}/vendor_error", actuator.component_id),
                status: HEALTH_ERROR,
                detail: format!(
                    "{} vendor_error_code=0x{:X}",
                    actuator.component_id, actuator.vendor_error_code
                ),
                observed_value: actuator.vendor_error_code as f32,
                reference_value: 0.0,
            });
        }
        let (torque_status, torque_value, torque_label) = if actuator.torque_enabled {
            (HEALTH_OK, 1.0, "enabled")
        } else {
            (HEALTH_WARN, 0.0, "disabled")
        };
        health_signals.push(HealthSignal {
            key: format!("{}/torque_enabled", actuator.component_id),
            status: torque_status,
            detail: format!("{} torque is {torque_label}", actuator.component_id),
            observed_value: torque_value,
            reference_value: 1.0,
        });
    }

    for power in &snapshot.power_sources {
        let kind = component_kind
            .get(power.component_id.as_str())
            .copied()
            .unwrap_or(KIND_BATTERY);
        push_scalar_health(
            &mut health_signals,
            rules,
            &power.component_id,
            kind,
            "soc_percent",
            power.soc_percent.as_ref(),
        );
        push_scalar_health(
            &mut health_signals,
            rules,
            &power.component_id,
            kind,
            "voltage",
            power.voltage.as_ref(),
        );
        push_scalar_health(
            &mut health_signals,
            rules,
            &power.component_id,
            kind,
            "temperature",
            power.temperature.as_ref(),
        );
        if power.vendor_status_code != 0 {
            health_signals.push(HealthSignal {
                key: format!("{}/vendor_status", power.component_id),
                status: HEALTH_ERROR,
                detail: format!("vendor_status_code={}", power.vendor_status_code),
                observed_value: power.vendor_status_code as f32,
                reference_value: 0.0,
            });
        }
    }

    for component in &snapshot.components {
        if actuator_by_component_id(snapshot, &component.id).is_none()
            && !(component.present && component.online)
        {
            health_signals.push(HealthSignal {
                key: format!("{}/availability", component.id),
                status: HEALTH_ERROR,
                detail: format!("present={}; online={}", component.present, component.online),
                observed_value: 0.0,
                reference_value: 1.0,
            });
        }
    }

    // Metrics follow the same selector-based threshold pipeline as typed
    // actuator and power scalars: matching rules emit a HealthSignal and
    // unmatched metrics are ignored.
    for metric in &snapshot.metrics {
        let kind = component_kind
            .get(metric.component_id.as_str())
            .copied()
            .unwrap_or(KIND_SENSOR);
        push_scalar_health(
            &mut health_signals,
            rules,
            &metric.component_id,
            kind,
            &metric.name,
            metric.value.as_ref(),
        );
    }

    for fault in &snapshot.faults {
        let Some((health, quality, raw_severity)) = fault_health_projection(fault) else {
            continue;
        };
        if quality == ObservationQuality::Unknown {
            log::warn!(
                "[vitals] unknown fault severity {} for fault '{}' on {} — treating as ERROR",
                raw_severity,
                fault.fault_id,
                fault.component_id
            );
        }
        health_signals.push(HealthSignal {
            key: format!("{}/fault/{}", fault.component_id, fault.fault_id),
            status: health,
            detail: if fault.message.is_empty() {
                format!(
                    "fault_id={}; vendor_code={}",
                    fault.fault_id, fault.vendor_code
                )
            } else {
                format!(
                    "fault_id={}; message={}; vendor_code={}",
                    fault.fault_id, fault.message, fault.vendor_code
                )
            },
            observed_value: -1.0,
            reference_value: -1.0,
        });
    }

    VitalsSnapshot {
        ts_ns,
        power: Some(power_state(snapshot)),
        health_signals,
        bodies: body_healths(snapshot),
    }
}

/// Project one internal fault assessment into Vitals HealthSignal and
/// BodyHealth status values. Unknown active severities remain fail-closed.
fn fault_health_projection(fault: &FaultState) -> Option<(u32, ObservationQuality, u32)> {
    let raw_severity = fault.severity;
    let assessment = classify_fault(fault.active, raw_severity);
    let health = match assessment.health {
        Some(HealthSeverity::Ok) => HEALTH_OK,
        Some(HealthSeverity::Warn) => HEALTH_WARN,
        Some(HealthSeverity::Error) => HEALTH_ERROR,
        None if fault.active => HEALTH_ERROR,
        None => return None,
    };
    Some((health, assessment.quality, raw_severity))
}

fn connect_endpoint(raw: &str) -> Result<Endpoint> {
    Endpoint::new(normalize_grpc_endpoint(raw))
        .with_context(|| format!("invalid Soma endpoint '{raw}'"))
}

async fn connect_direct(endpoint: &str) -> Result<Channel> {
    connect_endpoint(endpoint)?
        .connect()
        .await
        .with_context(|| format!("connect to Soma at '{endpoint}'"))
}

fn normalize_grpc_endpoint(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

fn rule_kind(
    id: &str,
    kind: u32,
    signal: &str,
    bounds: ThresholdBounds,
    unit: &str,
) -> SomaThresholdRule {
    SomaThresholdRule {
        id: id.to_string(),
        selector: SomaThresholdSelector {
            kind: Some(kind),
            component_id: None,
            component_id_glob: None,
            signal: signal.to_string(),
        },
        warn_above: bounds.warn_above,
        error_above: bounds.error_above,
        warn_below: bounds.warn_below,
        error_below: bounds.error_below,
        unit: unit.to_string(),
    }
}

impl ThresholdBounds {
    fn above(warn: f64, error: f64) -> Self {
        Self {
            warn_above: Some(warn),
            error_above: Some(error),
            warn_below: None,
            error_below: None,
        }
    }

    fn below(warn: f64, error: f64) -> Self {
        Self {
            warn_above: None,
            error_above: None,
            warn_below: Some(warn),
            error_below: Some(error),
        }
    }
}

fn push_scalar_health(
    out: &mut Vec<HealthSignal>,
    rules: &[SomaThresholdRule],
    component_id: &str,
    kind: u32,
    signal: &str,
    scalar: Option<&Scalar>,
) {
    let Some(scalar) = scalar else {
        return;
    };
    let key = format!("{component_id}/{signal}");
    if scalar.quality == QUALITY_STALE {
        out.push(HealthSignal {
            key,
            status: HEALTH_STALE,
            detail: format!("{component_id} {signal} is stale"),
            observed_value: scalar.value as f32,
            reference_value: -1.0,
        });
        return;
    }
    if scalar.quality == QUALITY_INVALID {
        out.push(HealthSignal {
            key,
            status: HEALTH_ERROR,
            detail: format!("{component_id} {signal} is invalid"),
            observed_value: scalar.value as f32,
            reference_value: -1.0,
        });
        return;
    }
    if scalar.quality != QUALITY_VALID {
        return;
    }

    let Some(threshold) = select_threshold(rules, component_id, kind, signal) else {
        return;
    };
    let (health, threshold_value, detail) =
        evaluate_scalar(component_id, signal, scalar.value, threshold);
    out.push(HealthSignal {
        key,
        status: health,
        detail,
        observed_value: scalar.value as f32,
        reference_value: threshold_value as f32,
    });
}

fn select_threshold(
    rules: &[SomaThresholdRule],
    component_id: &str,
    kind: u32,
    signal: &str,
) -> Option<Threshold> {
    let effective_rules = if rules.is_empty() {
        default_thresholds()
    } else {
        rules.to_vec()
    };
    let mut selected: Option<(u8, usize, &SomaThresholdRule)> = None;
    for (idx, rule) in effective_rules.iter().enumerate() {
        let Some(priority) = match_rule(rule, component_id, kind, signal) else {
            continue;
        };
        let replace = selected
            .map(|(old_priority, old_idx, _)| {
                priority > old_priority || (priority == old_priority && idx > old_idx)
            })
            .unwrap_or(true);
        if replace {
            selected = Some((priority, idx, rule));
        }
    }
    selected.map(|(_, _, rule)| Threshold {
        warn_above: rule.warn_above,
        error_above: rule.error_above,
        warn_below: rule.warn_below,
        error_below: rule.error_below,
    })
}

fn match_rule(rule: &SomaThresholdRule, component_id: &str, kind: u32, signal: &str) -> Option<u8> {
    if rule.selector.signal != signal {
        return None;
    }
    if let Some(exact) = &rule.selector.component_id {
        return (exact == component_id).then_some(3);
    }
    if let Some(glob) = &rule.selector.component_id_glob {
        return glob_matches(glob, component_id).then_some(2);
    }
    if let Some(rule_kind) = rule.selector.kind {
        return (rule_kind == kind).then_some(1);
    }
    None
}

fn evaluate_scalar(
    component_id: &str,
    signal: &str,
    value: f64,
    threshold: Threshold,
) -> (u32, f64, String) {
    if value.is_nan() {
        return (
            HEALTH_ERROR,
            -1.0,
            format!("{component_id} {signal} value is NaN"),
        );
    }
    if let Some(error) = threshold.error_above
        && value >= error
    {
        return (
            HEALTH_ERROR,
            error,
            format!("{component_id} {signal} {value:.1} exceeds ERROR threshold {error:.1}"),
        );
    }
    if let Some(warn) = threshold.warn_above
        && value >= warn
    {
        return (
            HEALTH_WARN,
            warn,
            format!("{component_id} {signal} {value:.1} exceeds WARN threshold {warn:.1}"),
        );
    }
    if let Some(error) = threshold.error_below
        && value <= error
    {
        return (
            HEALTH_ERROR,
            error,
            format!("{component_id} {signal} {value:.1} below ERROR threshold {error:.1}"),
        );
    }
    if let Some(warn) = threshold.warn_below
        && value <= warn
    {
        return (
            HEALTH_WARN,
            warn,
            format!("{component_id} {signal} {value:.1} below WARN threshold {warn:.1}"),
        );
    }
    (HEALTH_OK, -1.0, String::new())
}

fn power_state(snapshot: &SomaHealthSnapshot) -> PowerState {
    let Some(power) = snapshot.power_sources.first() else {
        return PowerState {
            soc_percent: -1.0,
            voltage: -1.0,
            charging: false,
            remaining_s: -1,
        };
    };
    PowerState {
        soc_percent: scalar_value(power.soc_percent.as_ref()).unwrap_or(-1.0) as f32,
        voltage: scalar_value(power.voltage.as_ref()).unwrap_or(-1.0) as f32,
        charging: scalar_value(power.current.as_ref()).unwrap_or(0.0) > 0.0,
        remaining_s: scalar_value(power.remaining_s.as_ref()).unwrap_or(-1.0) as i64,
    }
}

fn body_healths(snapshot: &SomaHealthSnapshot) -> Vec<BodyHealth> {
    let Some(root) = root_component(snapshot) else {
        return Vec::new();
    };
    let mut bodies: Vec<BodyHealth> = snapshot
        .components
        .iter()
        .filter(|component| component.parent_id == root.id)
        .map(|component| body_health_for_component(snapshot, component))
        .collect();
    if bodies.is_empty() {
        bodies.push(body_health_for_component(snapshot, root));
    }
    bodies
}

fn body_health_for_component(snapshot: &SomaHealthSnapshot, root: &ComponentStatus) -> BodyHealth {
    // NOTE(gap): Only SafetyState.aggregate_state is checked here.
    //   SafetyEndpointState[] (individual hardware/software/remote e-stops)
    //   is present in the snapshot but not consumed per-endpoint.  The design
    //   doc does not mandate per-endpoint health decisions, but an operator
    //   debugging an e-stop trigger currently has to inspect raw snapshot data
    //   rather than seeing which endpoint fired in the Vitals output.
    let mut status = 0;
    if snapshot
        .safety
        .as_ref()
        .map(|s| s.aggregate_state == SAFETY_ESTOP)
        .unwrap_or(false)
    {
        status = 2;
    } else if snapshot
        .safety
        .as_ref()
        .map(|s| s.aggregate_state == SAFETY_FAULT)
        .unwrap_or(false)
        || snapshot.faults.iter().any(|fault| {
            fault_health_projection(fault)
                .map(|(health, _, _)| health == HEALTH_ERROR)
                .unwrap_or(false)
                && component_contains(root, &fault.component_id)
        })
        || snapshot
            .actuators
            .iter()
            .any(|a| !a.communication_ok && component_contains(root, &a.component_id))
    {
        status = 1;
    }

    BodyHealth {
        key: component_type_name(root),
        model: component_display_model(root),
        status,
        components: body_components(snapshot, root),
    }
}

fn root_component(snapshot: &SomaHealthSnapshot) -> Option<&ComponentStatus> {
    snapshot
        .components
        .iter()
        .find(|c| c.parent_id.is_empty())
        .or_else(|| snapshot.components.iter().find(|c| c.kind == KIND_BODY))
}

fn actuator_by_component_id<'a>(
    snapshot: &'a SomaHealthSnapshot,
    component_id: &str,
) -> Option<&'a ActuatorState> {
    snapshot
        .actuators
        .iter()
        .find(|a| a.component_id == component_id)
}

fn body_components(snapshot: &SomaHealthSnapshot, root: &ComponentStatus) -> Vec<BodyComponent> {
    snapshot
        .components
        .iter()
        .filter(|component| component.id != root.id && component_contains(root, &component.id))
        .map(|component| BodyComponent {
            id: component.id.clone(),
            parent_id: component.parent_id.clone(),
            name: first_non_empty(&[&component.name, &component.id]),
            kind: kind_label(component.kind).to_string(),
            model: component.model.clone(),
        })
        .collect()
}

fn component_contains(root: &ComponentStatus, component_id: &str) -> bool {
    component_id == root.id || component_id.starts_with(&format!("{}/", root.id))
}

fn component_type_name(component: &ComponentStatus) -> String {
    let path_name = component.id.rsplit('/').next().unwrap_or("");
    first_non_empty(&[path_name, &component.name, &component.model])
}

fn kind_label(kind: u32) -> &'static str {
    match kind {
        KIND_BODY => "body",
        KIND_ARM => "arm",
        KIND_LEG => "leg",
        KIND_JOINT => "joint",
        KIND_WHEEL => "wheel",
        KIND_GRIPPER => "gripper",
        KIND_BATTERY => "battery",
        KIND_COMPUTER => "computer",
        KIND_SENSOR => "sensor",
        KIND_CONTROLLER => "controller",
        KIND_END_EFFECTOR => "end_effector",
        _ => "unknown",
    }
}

fn component_display_model(component: &ComponentStatus) -> String {
    first_non_empty(&[&component.model])
}

fn first_non_empty(values: &[&str]) -> String {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn scalar_value(scalar: Option<&Scalar>) -> Option<f64> {
    scalar.and_then(|s| (s.quality == QUALITY_VALID).then_some(s.value))
}

fn kind_from_name(raw: &str) -> Option<u32> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "BODY" => Some(KIND_BODY),
        "ARM" => Some(KIND_ARM),
        "LEG" => Some(KIND_LEG),
        "JOINT" => Some(KIND_JOINT),
        "WHEEL" => Some(KIND_WHEEL),
        "GRIPPER" => Some(KIND_GRIPPER),
        "BATTERY" => Some(KIND_BATTERY),
        "COMPUTER" => Some(KIND_COMPUTER),
        "SENSOR" => Some(KIND_SENSOR),
        "CONTROLLER" => Some(KIND_CONTROLLER),
        "END_EFFECTOR" => Some(KIND_END_EFFECTOR),
        _ => None,
    }
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let value_parts: Vec<&str> = value.split('/').collect();
    if pattern_parts.len() != value_parts.len() {
        return false;
    }
    pattern_parts
        .iter()
        .zip(value_parts.iter())
        .all(|(pattern, value)| segment_matches(pattern, value))
}

fn segment_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let Some(star_idx) = pattern.find('*') else {
        return pattern == value;
    };
    let (prefix, suffix_with_star) = pattern.split_at(star_idx);
    let suffix = &suffix_with_star[1..];
    value.starts_with(prefix) && value.ends_with(suffix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_soma::{MockScenario, generate_snapshot};
    use crate::pb::soma::FaultState;

    #[test]
    fn fault_classifier_separates_internal_health_and_quality() {
        let active_cases = [
            (0, Some(HealthSeverity::Ok), ObservationQuality::Valid),
            (1, Some(HealthSeverity::Warn), ObservationQuality::Valid),
            (2, Some(HealthSeverity::Error), ObservationQuality::Valid),
            (3, Some(HealthSeverity::Error), ObservationQuality::Valid),
            (99, None, ObservationQuality::Unknown),
        ];
        for (raw, expected_health, expected_quality) in active_cases {
            let assessment = classify_fault(true, raw);
            assert_eq!(assessment.health, expected_health, "active raw={raw}");
            assert_eq!(assessment.quality, expected_quality, "active raw={raw}");
        }

        let inactive_known = classify_fault(false, 2);
        assert_eq!(inactive_known.health, None);
        assert_eq!(inactive_known.quality, ObservationQuality::Valid);

        let inactive_unknown = classify_fault(false, 99);
        assert_eq!(inactive_unknown.health, None);
        assert_eq!(inactive_unknown.quality, ObservationQuality::Unknown);
    }

    #[test]
    fn ramp_snapshot_crosses_joint_error_threshold() {
        let snapshot = generate_snapshot(MockScenario::Ramp, 24, None);
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let joint = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_1/motor_temp")
            .expect("joint_1 motor temp health");
        assert_eq!(joint.status, HEALTH_ERROR);
    }

    #[test]
    fn body_health_groups_root_children() {
        let snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        assert_eq!(vitals.bodies.len(), 3);

        let computer = vitals
            .bodies
            .iter()
            .find(|body| body.key == "computer_jetson")
            .expect("computer_jetson health");
        assert_eq!(computer.model, "jetson_agx_orin");
        assert!(
            computer
                .components
                .iter()
                .any(|c| c.id == "body/computer_jetson/cpu" && c.kind == "sensor")
        );

        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm health");
        assert_eq!(arm.model, "mock_arm");
        let joint = arm
            .components
            .iter()
            .find(|c| c.id == "body/arm/joint_1")
            .expect("joint_1 component");
        assert_eq!(joint.parent_id, "body/arm");
        assert_eq!(joint.model, "mock_motor");

        let battery = vitals
            .bodies
            .iter()
            .find(|body| body.key == "battery_main")
            .expect("battery_main health");
        assert_eq!(battery.model, "mock_bms");
        assert!(battery.components.is_empty());
    }

    #[test]
    fn selector_yaml_overrides_kind_rule() {
        let rules = load_soma_thresholds(
            r#"
rules:
  - id: loose
    selector:
      kind: "JOINT"
      signal: "motor_temp"
    warn_above: 90.0
    error_above: 95.0
  - id: exact
    selector:
      component_id: "body/arm/joint_1"
      signal: "motor_temp"
    warn_above: 36.0
    error_above: 50.0
"#,
        )
        .unwrap();
        let snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        let vitals = snapshot_to_vitals(&snapshot, &rules, 123);
        let joint = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_1/motor_temp")
            .expect("joint_1 motor temp health");
        assert_eq!(joint.status, HEALTH_WARN);
    }

    #[test]
    fn legacy_power_projection_uses_first_input_source() {
        let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        let first_power = snapshot
            .power_sources
            .first_mut()
            .expect("normal scenario power source");
        first_power
            .soc_percent
            .as_mut()
            .expect("normal scenario state of charge")
            .value = 12.0;
        let mut second_power = first_power.clone();
        second_power.component_id = "body/battery_backup".to_string();
        second_power
            .soc_percent
            .as_mut()
            .expect("cloned state of charge")
            .value = 88.0;
        snapshot.power_sources.push(second_power);

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        assert_eq!(vitals.power.expect("power projection").soc_percent, 12.0);
    }

    #[test]
    fn unknown_fault_severity_maps_to_error() {
        let snapshot = SomaHealthSnapshot {
            faults: vec![FaultState {
                component_id: "body/arm/joint_1".to_string(),
                fault_id: "future_critical".to_string(),
                severity: 99, // unknown severity from a newer Soma version
                active: true,
                clearable: false,
                onset_ts_ns: 0,
                vendor_code: 99,
                vendor_code_text: String::new(),
                message: "future severity".to_string(),
                attributes: vec![],
                vendor_raw_json: String::new(),
            }],
            ..generate_snapshot(MockScenario::Normal, 1, None)
        };
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let fault = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_1/fault/future_critical")
            .expect("fault signal");
        assert_eq!(
            fault.status, HEALTH_ERROR,
            "unknown fault severity must be treated as ERROR, not OK"
        );
        assert_eq!(
            fault.detail,
            "fault_id=future_critical; message=future severity; vendor_code=99"
        );
        assert_eq!(fault.observed_value, -1.0);
        assert_eq!(fault.reference_value, -1.0);
        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm body projection");
        assert_eq!(arm.status, 1, "unknown severity stays fail-closed in v1");
    }

    #[test]
    fn info_fault_is_the_only_intentional_v1_output_change() {
        let snapshot = SomaHealthSnapshot {
            faults: vec![FaultState {
                component_id: "body/arm/joint_1".to_string(),
                fault_id: "maintenance_notice".to_string(),
                severity: 0,
                active: true,
                clearable: true,
                onset_ts_ns: 0,
                vendor_code: 7,
                vendor_code_text: String::new(),
                message: "maintenance recommended".to_string(),
                attributes: vec![],
                vendor_raw_json: String::new(),
            }],
            ..generate_snapshot(MockScenario::Normal, 1, None)
        };
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let fault = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_1/fault/maintenance_notice")
            .expect("INFO fault signal");
        assert_eq!(
            fault.status, HEALTH_OK,
            "INFO must not be reported as ERROR"
        );
        assert_eq!(
            fault.detail,
            "fault_id=maintenance_notice; message=maintenance recommended; vendor_code=7"
        );
        assert_eq!(fault.observed_value, -1.0);
        assert_eq!(fault.reference_value, -1.0);
        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm body projection");
        assert_eq!(arm.status, 0, "INFO must leave BodyHealth NORMAL");
    }

    #[test]
    fn duplicate_fault_projection_preserves_input_order_and_values() {
        let first = FaultState {
            component_id: "body/arm/joint_1".to_string(),
            fault_id: "duplicate".to_string(),
            severity: 2,
            active: true,
            clearable: true,
            onset_ts_ns: 0,
            vendor_code: 22,
            vendor_code_text: String::new(),
            message: "first error".to_string(),
            attributes: vec![],
            vendor_raw_json: String::new(),
        };
        let mut second = first.clone();
        second.severity = 1;
        second.vendor_code = 11;
        second.message = "second warning".to_string();
        let snapshot = SomaHealthSnapshot {
            faults: vec![first, second],
            ..generate_snapshot(MockScenario::Normal, 1, None)
        };
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let faults: Vec<_> = vitals
            .health_signals
            .iter()
            .filter(|signal| signal.key == "body/arm/joint_1/fault/duplicate")
            .collect();
        assert_eq!(faults.len(), 2);
        assert_eq!(
            (faults[0].status, faults[0].detail.as_str()),
            (
                HEALTH_ERROR,
                "fault_id=duplicate; message=first error; vendor_code=22"
            )
        );
        assert_eq!(
            (faults[1].status, faults[1].detail.as_str()),
            (
                HEALTH_WARN,
                "fault_id=duplicate; message=second warning; vendor_code=11"
            )
        );
        assert!(
            faults
                .iter()
                .all(|fault| fault.observed_value == -1.0 && fault.reference_value == -1.0)
        );
        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm body projection");
        assert_eq!(arm.status, 1, "duplicate ERROR keeps BodyHealth FAULT");
    }

    #[test]
    fn target_body_projection_is_topology_only_and_uses_unknown_model() {
        let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        snapshot
            .components
            .iter_mut()
            .find(|component| component.id == "body/arm")
            .expect("arm component")
            .model
            .clear();

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm body");
        assert_eq!(arm.model, "unknown");
        assert_eq!(arm.status, 0);

        let joint = arm
            .components
            .iter()
            .find(|component| component.id == "body/arm/joint_1")
            .expect("joint topology");
        assert_eq!(joint.parent_id, "body/arm");
        assert_eq!(joint.name, "joint_1");
        assert_eq!(joint.kind, "joint");
        assert_eq!(joint.model, "mock_motor");
    }

    #[test]
    fn target_root_fallback_uses_path_key_and_unknown_model() {
        let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        snapshot.body_id = "must-not-become-model".to_string();
        let mut root = snapshot
            .components
            .iter()
            .find(|component| component.parent_id.is_empty())
            .expect("root component")
            .clone();
        root.id = "fleet/root_body".to_string();
        root.name = "must-not-become-model-either".to_string();
        root.model.clear();
        snapshot.components = vec![root];

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        assert_eq!(vitals.bodies.len(), 1);
        let body = &vitals.bodies[0];
        assert_eq!(body.key, "root_body");
        assert_eq!(body.model, "unknown");
        assert_eq!(body.status, 0);
        assert!(body.components.is_empty());
    }

    #[test]
    fn target_fault_signals_use_deterministic_detail_and_sentinel_values() {
        let snapshot = SomaHealthSnapshot {
            faults: vec![
                FaultState {
                    component_id: "body/arm/joint_1".to_string(),
                    fault_id: "with_message".to_string(),
                    severity: 2,
                    active: true,
                    clearable: true,
                    onset_ts_ns: 0,
                    vendor_code: 64,
                    vendor_code_text: String::new(),
                    message: "driver fault".to_string(),
                    attributes: vec![],
                    vendor_raw_json: String::new(),
                },
                FaultState {
                    component_id: "body/arm/joint_2".to_string(),
                    fault_id: "without_message".to_string(),
                    severity: 0,
                    active: true,
                    clearable: true,
                    onset_ts_ns: 0,
                    vendor_code: 7,
                    vendor_code_text: String::new(),
                    message: String::new(),
                    attributes: vec![],
                    vendor_raw_json: String::new(),
                },
            ],
            ..generate_snapshot(MockScenario::Normal, 1, None)
        };

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let with_message = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_1/fault/with_message")
            .expect("fault signal with message");
        assert_eq!(with_message.status, HEALTH_ERROR);
        assert_eq!(
            with_message.detail,
            "fault_id=with_message; message=driver fault; vendor_code=64"
        );
        assert_eq!(with_message.observed_value, -1.0);
        assert_eq!(with_message.reference_value, -1.0);

        let without_message = vitals
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/arm/joint_2/fault/without_message")
            .expect("fault signal without message");
        assert_eq!(without_message.status, HEALTH_OK);
        assert_eq!(
            without_message.detail,
            "fault_id=without_message; vendor_code=7"
        );
        assert_eq!(without_message.observed_value, -1.0);
        assert_eq!(without_message.reference_value, -1.0);
    }

    #[test]
    fn target_power_vendor_status_signal_appears_and_recovers() {
        let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        snapshot.power_sources[0].vendor_status_code = 7;

        let unhealthy = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let signal = unhealthy
            .health_signals
            .iter()
            .find(|signal| signal.key == "body/battery_main/vendor_status")
            .expect("power vendor status signal");
        assert_eq!(signal.status, HEALTH_ERROR);
        assert_eq!(signal.detail, "vendor_status_code=7");
        assert_eq!(signal.observed_value, 7.0);
        assert_eq!(signal.reference_value, 0.0);

        snapshot.power_sources[0].vendor_status_code = 0;
        let recovered = snapshot_to_vitals(&snapshot, &default_thresholds(), 124);
        assert!(
            recovered
                .health_signals
                .iter()
                .all(|signal| signal.key != "body/battery_main/vendor_status")
        );
    }

    #[test]
    fn target_non_actuator_availability_covers_all_boolean_combinations() {
        for (present, online) in [(true, true), (true, false), (false, true), (false, false)] {
            let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
            let component = snapshot
                .components
                .iter_mut()
                .find(|component| component.id == "body/computer_jetson/cpu")
                .expect("non-actuator component");
            component.present = present;
            component.online = online;

            let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
            let signal = vitals
                .health_signals
                .iter()
                .find(|signal| signal.key == "body/computer_jetson/cpu/availability");
            if present && online {
                assert!(signal.is_none(), "healthy availability must stay sparse");
            } else {
                let signal = signal.expect("unavailable component signal");
                assert_eq!(signal.status, HEALTH_ERROR);
                assert_eq!(signal.detail, format!("present={present}; online={online}"));
                assert_eq!(signal.observed_value, 0.0);
                assert_eq!(signal.reference_value, 1.0);
            }
        }
    }

    #[test]
    fn target_actuator_always_projects_torque_status_without_availability_duplicate() {
        for (enabled, expected_status, expected_value, expected_detail) in [
            (true, HEALTH_OK, 1.0, "body/arm/joint_1 torque is enabled"),
            (
                false,
                HEALTH_WARN,
                0.0,
                "body/arm/joint_1 torque is disabled",
            ),
        ] {
            let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
            let component = snapshot
                .components
                .iter_mut()
                .find(|component| component.id == "body/arm/joint_1")
                .expect("actuator component");
            component.present = false;
            component.online = false;
            snapshot
                .actuators
                .iter_mut()
                .find(|actuator| actuator.component_id == component.id)
                .expect("matching actuator")
                .torque_enabled = enabled;

            let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
            let torque_signals: Vec<_> = vitals
                .health_signals
                .iter()
                .filter(|signal| signal.key == "body/arm/joint_1/torque_enabled")
                .collect();
            assert_eq!(torque_signals.len(), 1, "enabled={enabled}");
            let signal = torque_signals[0];
            assert_eq!(signal.status, expected_status, "enabled={enabled}");
            assert_eq!(signal.observed_value, expected_value, "enabled={enabled}");
            assert_eq!(signal.reference_value, 1.0, "enabled={enabled}");
            assert_eq!(signal.detail, expected_detail, "enabled={enabled}");
            assert!(
                vitals
                    .health_signals
                    .iter()
                    .all(|signal| signal.key != "body/arm/joint_1/availability"),
                "actuator availability must not duplicate torque status"
            );
        }
    }

    #[test]
    fn target_normal_snapshot_projects_six_enabled_torque_signals() {
        let snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let torque_signals: Vec<_> = vitals
            .health_signals
            .iter()
            .filter(|signal| signal.key.ends_with("/torque_enabled"))
            .collect();

        assert_eq!(torque_signals.len(), 6);
        for signal in torque_signals {
            assert_eq!(signal.status, HEALTH_OK, "{}", signal.key);
            assert_eq!(signal.observed_value, 1.0, "{}", signal.key);
            assert_eq!(signal.reference_value, 1.0, "{}", signal.key);
            assert_eq!(
                signal.detail,
                format!(
                    "{} torque is enabled",
                    signal.key.trim_end_matches("/torque_enabled")
                )
            );
        }
    }

    #[test]
    fn target_power_summary_uses_first_source_and_soc_name() {
        let mut snapshot = generate_snapshot(MockScenario::Normal, 1, None);
        snapshot.power_sources[0]
            .soc_percent
            .as_mut()
            .expect("first state of charge")
            .value = 12.0;
        let mut second = snapshot.power_sources[0].clone();
        second.component_id = "body/battery_backup".to_string();
        second
            .soc_percent
            .as_mut()
            .expect("second state of charge")
            .value = 88.0;
        snapshot.power_sources.push(second);

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        assert_eq!(vitals.power.expect("power summary").soc_percent, 12.0);
    }

    #[test]
    fn target_duplicate_fault_signals_preserve_input_order_and_count() {
        let first = FaultState {
            component_id: "body/arm/joint_1".to_string(),
            fault_id: "duplicate".to_string(),
            severity: 2,
            active: true,
            clearable: true,
            onset_ts_ns: 0,
            vendor_code: 22,
            vendor_code_text: String::new(),
            message: "first error".to_string(),
            attributes: vec![],
            vendor_raw_json: String::new(),
        };
        let mut second = first.clone();
        second.severity = 1;
        second.vendor_code = 11;
        second.message = "second warning".to_string();
        let snapshot = SomaHealthSnapshot {
            faults: vec![first, second],
            ..generate_snapshot(MockScenario::Normal, 1, None)
        };

        let vitals = snapshot_to_vitals(&snapshot, &default_thresholds(), 123);
        let faults: Vec<_> = vitals
            .health_signals
            .iter()
            .filter(|signal| signal.key == "body/arm/joint_1/fault/duplicate")
            .collect();
        assert_eq!(faults.len(), 2);
        assert_eq!(
            (faults[0].status, faults[0].detail.as_str()),
            (
                HEALTH_ERROR,
                "fault_id=duplicate; message=first error; vendor_code=22"
            )
        );
        assert_eq!(
            (faults[1].status, faults[1].detail.as_str()),
            (
                HEALTH_WARN,
                "fault_id=duplicate; message=second warning; vendor_code=11"
            )
        );
        assert!(
            faults
                .iter()
                .all(|fault| { fault.observed_value == -1.0 && fault.reference_value == -1.0 })
        );
        let arm = vitals
            .bodies
            .iter()
            .find(|body| body.key == "arm")
            .expect("arm body projection");
        assert_eq!(arm.status, 1, "duplicate ERROR keeps BodyHealth FAULT");
    }
    #[test]
    fn kind_from_name_unknown_returns_none() {
        assert_eq!(kind_from_name("UNICORN"), None);
        assert_eq!(kind_from_name(""), None);
    }

    #[test]
    fn kind_from_name_known_returns_value() {
        assert_eq!(kind_from_name("JOINT"), Some(KIND_JOINT));
        assert_eq!(kind_from_name("  joint  "), Some(KIND_JOINT));
        assert_eq!(kind_from_name("BATTERY"), Some(KIND_BATTERY));
    }

    #[test]
    fn glob_matches_exact_and_wildcard() {
        assert!(glob_matches("body/arm/*", "body/arm/joint_1"));
        assert!(!glob_matches("body/leg/*", "body/arm/joint_1"));
        assert!(glob_matches("body/*/joint_1", "body/arm/joint_1"));
        assert!(!glob_matches("body/*/joint_1", "body/arm/joint_2"));
    }
}
