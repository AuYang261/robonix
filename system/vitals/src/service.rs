// SPDX-License-Identifier: MulanPSL-2.0
//
// gRPC contract handlers:
//   RobonixSystemVitalsGet.GetVitals(GetVitalsRequest) → GetVitalsResponse  (rpc)
//   RobonixSystemVitalsStream.StreamVitals(StreamVitalsRequest) → stream VitalsSnapshot  (server_stream)
//
// Serves the latest projected Vitals v1 snapshot and state-transition stream.

use crate::config::{ExpectedModuleConfig, ExpectedModulePolicy};
use crate::module_health::{
    HEALTH_ERROR, HEALTH_OK, HEALTH_WARN, MODULE_HEALTH_SCHEMA_VERSION, ModuleHealthError,
    ModuleHealthStore,
};
use crate::pb::contracts::robonix_system_vitals_get_server::RobonixSystemVitalsGet;
use crate::pb::contracts::robonix_system_vitals_modules_get_server::RobonixSystemVitalsModulesGet;
use crate::pb::contracts::robonix_system_vitals_stream_server::RobonixSystemVitalsStream;
use crate::pb::module_health::{
    GetModuleHealthSnapshotRequest, GetModuleHealthSnapshotResponse, ModuleHealth,
    ModuleHealthEvent, ModuleHealthReport, ModuleHealthSnapshot,
};
use crate::pb::vitals::{
    GetVitalsRequest, GetVitalsResponse, PowerState, StreamVitalsRequest, VitalsSnapshot,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Shared state: latest snapshot + broadcast channel for StreamVitals subscribers.
/// Tracks health signals to only push on state transitions (OK↔WARN↔ERROR).
struct VitalsState {
    latest: VitalsSnapshot,
    /// Sender for StreamVitals. Each subscriber gets a new Receiver.
    broadcast_tx: tokio::sync::broadcast::Sender<VitalsSnapshot>,
    /// Previous status per health-signal key.
    prev_health: HashMap<String, u32>,
    /// Previous body-level status, keyed by "{key}/{model}".
    prev_body_state: HashMap<String, u32>,
    /// Previous power state for voltage / state-of-charge change detection.
    prev_power: Option<PowerState>,
    /// True until the first collected snapshot has been processed.
    first_snapshot: bool,
    /// True until the first body reading — suppresses ALERTs on startup.
    first_body: bool,
}

/// VitalsServiceImpl — cheap to clone (Arc).
#[derive(Clone)]
pub struct VitalsServiceImpl {
    state: Arc<RwLock<VitalsState>>,
    module_health: Arc<RwLock<ModuleHealthStore>>,
    #[allow(dead_code)] // Phase 3 will use for uptime in snapshot metadata
    start_time: Instant,
}

impl VitalsServiceImpl {
    /// Create a new Vitals service with empty state and no subscribers.
    pub fn new() -> Self {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(64);
        let now_ns = monotonic_ns();
        Self {
            state: Arc::new(RwLock::new(VitalsState {
                latest: VitalsSnapshot {
                    ts_ns: now_ns,
                    power: Some(PowerState {
                        soc_percent: -1.0,
                        voltage: -1.0,
                        charging: false,
                        remaining_s: -1,
                    }),
                    health_signals: vec![],
                    bodies: vec![],
                },
                broadcast_tx,
                prev_health: HashMap::new(),
                prev_body_state: HashMap::new(),
                prev_power: None,
                first_snapshot: true,
                first_body: true,
            })),
            module_health: Arc::new(RwLock::new(ModuleHealthStore::new())),
            start_time: Instant::now(),
        }
    }

    /// Update the cached snapshot. Broadcasts to StreamVitals subscribers only
    /// when at least one health signal transitions (OK→WARN, WARN→ERROR, etc.)
    /// or when the snapshot is the first one collected.
    pub async fn update_snapshot(&self, snapshot: VitalsSnapshot) {
        let mut state = self.state.write().await;

        // Check for health-signal status transitions.
        let mut changed = state.first_snapshot
            || (state.prev_health.is_empty() && !snapshot.health_signals.is_empty());
        state.first_snapshot = false;
        for signal in &snapshot.health_signals {
            match state.prev_health.get(&signal.key).copied() {
                None => {
                    log::info!(
                        "[vitals] health signal added: key={} status={} observed_value={} reference_value={}",
                        signal.key,
                        health_label(signal.status),
                        signal.observed_value,
                        signal.reference_value
                    );
                    if signal.status == crate::soma_ingest::HEALTH_WARN
                        || signal.status == crate::soma_ingest::HEALTH_ERROR
                    {
                        log::warn!("[vitals] ALERT: {} — {}", signal.key, signal.detail);
                    }
                    changed = true;
                }
                Some(prev_h) if signal.status != prev_h => {
                    log::info!(
                        "[vitals] health signal status changed: key={} previous_status={} status={} observed_value={} reference_value={}",
                        signal.key,
                        health_label(prev_h),
                        health_label(signal.status),
                        signal.observed_value,
                        signal.reference_value
                    );
                    if signal.status == crate::soma_ingest::HEALTH_WARN
                        || signal.status == crate::soma_ingest::HEALTH_ERROR
                    {
                        log::warn!("[vitals] ALERT: {} — {}", signal.key, signal.detail);
                    }
                    changed = true;
                }
                Some(_) => {}
            }
        }
        for (name, previous_health) in &state.prev_health {
            if !snapshot
                .health_signals
                .iter()
                .any(|signal| signal.key == *name)
            {
                log::info!(
                    "[vitals] health signal removed: key={} previous_status={}",
                    name,
                    health_label(*previous_health)
                );
                changed = true;
            }
        }

        // Always cache the latest.
        state.latest = snapshot.clone();

        // Track current health for next diff.
        state.prev_health.clear();
        for signal in &snapshot.health_signals {
            state.prev_health.insert(signal.key.clone(), signal.status);
        }

        // ── Body health transition detection ──────────────────────────
        // Per-body keys track status transitions; body removal is not detected.

        for body in &snapshot.bodies {
            let body_key = format!("{}/{}", body.key, body.model);

            if state.first_body {
                // First body reading — always log baseline at info level.
                log::info!(
                    "[vitals] body: {} ({}/{})",
                    body_state_label(body.status),
                    body.key,
                    body.model
                );
                for comp in &body.components {
                    log::info!(
                        "[vitals] {} ({}) model={}",
                        first_non_empty(&[comp.id.as_str(), comp.name.as_str()]),
                        comp.kind,
                        first_non_empty(&[comp.model.as_str()])
                    );
                }
                changed = true;
            } else {
                // Subsequent readings — log only on transition.

                // Body-level state.
                let prev_body_state = state.prev_body_state.get(&body_key).copied().unwrap_or(0);
                if body.status != prev_body_state {
                    log::info!(
                        "[vitals] {} body status: {} → {}",
                        body_key,
                        body_state_label(prev_body_state),
                        body_state_label(body.status)
                    );
                    if body.status != 0 {
                        log::warn!(
                            "[vitals] ALERT: body {} status={}",
                            body_key,
                            body_state_label(body.status)
                        );
                    }
                    changed = true;
                }
            }

            // Persist body state for next diff.
            state.prev_body_state.insert(body_key, body.status);
        }

        if !snapshot.bodies.is_empty() {
            state.first_body = false;
        }

        // ── Power state change detection ──────────────────────────────
        // Voltage and state-of-charge changes don't go through HealthSignal, so we
        // track them separately.  Without this, power-only changes (e.g. voltage
        // sag) produce no log output and no StreamVitals push.
        let power = snapshot.power.as_ref();
        let prev_power = state.prev_power.as_ref();
        let power_changed = match (power, prev_power) {
            (Some(cur), Some(prev)) => {
                (cur.voltage - prev.voltage).abs() > 0.05
                    || cur.charging != prev.charging
                    || (cur.soc_percent - prev.soc_percent).abs() > 0.5
            }
            (Some(_), None) => true, // first snapshot, force log
            _ => false,
        };
        if power_changed {
            if let (Some(cur), None) = (power, prev_power) {
                log::info!(
                    "[vitals] power baseline: soc_percent={:.0}% voltage={:.2}V charging={} remaining_s={}",
                    cur.soc_percent,
                    cur.voltage,
                    cur.charging,
                    cur.remaining_s
                );
            }
            if let (Some(cur), Some(prev)) = (power, prev_power) {
                if (cur.voltage - prev.voltage).abs() > 0.05 {
                    log::info!(
                        "[vitals] voltage: {:.2}V → {:.2}V",
                        prev.voltage,
                        cur.voltage
                    );
                }
                if cur.charging != prev.charging {
                    log::info!("[vitals] charging: {} → {}", prev.charging, cur.charging);
                }
                if (cur.soc_percent - prev.soc_percent).abs() > 0.5 {
                    log::info!(
                        "[vitals] soc_percent: {:.0}% → {:.0}%",
                        prev.soc_percent,
                        cur.soc_percent
                    );
                }
            }
            changed = true;
        }
        state.prev_power = power.cloned();

        // Only broadcast on state transitions to avoid flooding subscribers.
        if changed {
            // One-line summary — only show non-OK health signals.
            let non_ok: Vec<String> = snapshot
                .health_signals
                .iter()
                .filter(|signal| signal.status != 0)
                .map(|signal| {
                    format!(
                        "{}:{}({:.0})",
                        signal.key,
                        health_label(signal.status),
                        signal.observed_value
                    )
                })
                .collect();
            if !non_ok.is_empty() {
                let voltage_str = snapshot
                    .power
                    .as_ref()
                    .map(|p| format!("{:.1}V", p.voltage))
                    .unwrap_or_else(|| "?V".to_string());
                log::info!("[vitals] {} | {}", voltage_str, non_ok.join(" "));
            }
            let _ = state.broadcast_tx.send(snapshot);
        }
    }

    /// Return the latest cached snapshot.
    pub async fn latest_snapshot(&self) -> VitalsSnapshot {
        self.state.read().await.latest.clone()
    }

    /// Ingest one self-reported module health frame into Vitals' aggregate view.
    pub async fn ingest_module_health_report(
        &self,
        report: ModuleHealthReport,
    ) -> Result<Option<ModuleHealthEvent>, ModuleHealthError> {
        self.module_health
            .write()
            .await
            .ingest_report(report, monotonic_ns())
    }

    /// Publish Vitals' own module health into the aggregate module snapshot.
    pub async fn update_self_module_health(
        &self,
        provider_id: &str,
    ) -> Result<Option<ModuleHealthEvent>, ModuleHealthError> {
        self.ingest_module_health_report(vitals_self_health_report(provider_id))
            .await
    }

    /// Publish disabled expected modules into the aggregate module snapshot.
    pub async fn apply_expected_module_config(&self, modules: &[ExpectedModuleConfig]) {
        for module in modules {
            if module.policy == ExpectedModulePolicy::Disabled {
                self.module_health.write().await.synthesize_config_disabled(
                    &module.module_id,
                    module.provider_id_or_empty(),
                    monotonic_ns(),
                );
            }
        }
    }

    /// Mark a known module as stale if its last self-reported frame exceeded ttl_ms.
    pub async fn synthesize_stale_module_if_expired(
        &self,
        module_key: &str,
        policy: ExpectedModulePolicy,
    ) -> Option<ModuleHealthEvent> {
        self.module_health
            .write()
            .await
            .synthesize_stale_if_expired(module_key, stale_health_for(policy), monotonic_ns())
    }

    /// Mark an expected module as unavailable before it has ever reported health.
    pub async fn synthesize_expected_module_unavailable(
        &self,
        module: &ExpectedModuleConfig,
    ) -> Option<ModuleHealthEvent> {
        if module.policy == ExpectedModulePolicy::Disabled {
            return None;
        }

        self.module_health
            .write()
            .await
            .synthesize_expected_unavailable(
                &module.module_id,
                module.provider_id_or_empty(),
                stale_health_for(module.policy),
                module.ttl_ms,
                monotonic_ns(),
            )
    }

    /// Return the latest module health aggregate snapshot.
    pub async fn module_health_snapshot(&self, ts_ns: u64) -> ModuleHealthSnapshot {
        self.module_health.write().await.snapshot(ts_ns)
    }

    #[allow(dead_code)] // Phase 3 will expose uptime through snapshot metadata
    pub fn start_instant(&self) -> Instant {
        self.start_time
    }
}

fn stale_health_for(policy: ExpectedModulePolicy) -> u32 {
    match policy {
        ExpectedModulePolicy::Required => HEALTH_ERROR,
        ExpectedModulePolicy::Optional => HEALTH_WARN,
        ExpectedModulePolicy::Disabled => HEALTH_OK,
    }
}

fn vitals_self_health_report(provider_id: &str) -> ModuleHealthReport {
    ModuleHealthReport {
        schema_version: MODULE_HEALTH_SCHEMA_VERSION,
        module: Some(ModuleHealth {
            module_key: String::new(),
            module_id: "vitals".to_string(),
            provider_id: provider_id.to_string(),
            health: HEALTH_OK,
            state: "active".to_string(),
            reason_code: "OK".to_string(),
            detail: "vitals serving".to_string(),
            source: String::new(),
            received_ts_ns: 0,
            ttl_ms: 0,
        }),
    }
}

fn health_label(h: u32) -> &'static str {
    match h {
        0 => "OK",
        1 => "WARN",
        2 => "ERROR",
        3 => "STALE",
        _ => "UNKNOWN",
    }
}

fn body_state_label(s: u32) -> &'static str {
    match s {
        0 => "NORMAL",
        1 => "FAULT",
        2 => "ESTOP",
        _ => "UNKNOWN",
    }
}

fn first_non_empty<'a>(values: &[&'a str]) -> &'a str {
    values
        .iter()
        .copied()
        .find(|value| !value.trim().is_empty())
        .unwrap_or("unknown")
}

fn monotonic_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

#[tonic::async_trait]
impl RobonixSystemVitalsGet for VitalsServiceImpl {
    async fn get_vitals(
        &self,
        _request: Request<GetVitalsRequest>,
    ) -> Result<Response<GetVitalsResponse>, Status> {
        let snapshot = self.latest_snapshot().await;
        Ok(Response::new(GetVitalsResponse {
            snapshot: Some(snapshot),
        }))
    }
}

#[tonic::async_trait]
impl RobonixSystemVitalsModulesGet for VitalsServiceImpl {
    async fn get_module_health_snapshot(
        &self,
        _request: Request<GetModuleHealthSnapshotRequest>,
    ) -> Result<Response<GetModuleHealthSnapshotResponse>, Status> {
        let snapshot = self.module_health_snapshot(monotonic_ns()).await;
        Ok(Response::new(GetModuleHealthSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }
}

#[tonic::async_trait]
impl RobonixSystemVitalsStream for VitalsServiceImpl {
    type StreamVitalsStream = ReceiverStream<Result<VitalsSnapshot, Status>>;

    async fn stream_vitals(
        &self,
        _request: Request<StreamVitalsRequest>,
    ) -> Result<Response<Self::StreamVitalsStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let mut broadcast_rx = {
            let state = self.state.read().await;
            // Send current snapshot immediately so the subscriber has initial state.
            let current = state.latest.clone();
            if tx.send(Ok(current)).await.is_err() {
                log::warn!("[vitals] StreamVitals initial send failed (subscriber disconnected)");
            }
            state.broadcast_tx.subscribe()
        };

        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(snapshot) => {
                        if tx.send(Ok(snapshot)).await.is_err() {
                            break; // subscriber disconnected
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("[vitals] StreamVitals subscriber lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::vitals::{BodyHealth, HealthSignal};
    use std::sync::{Mutex, Once};
    use tokio::time::{Duration, timeout};
    use tokio_stream::StreamExt;

    struct TestLogger {
        records: Mutex<Vec<(std::thread::ThreadId, String)>>,
    }

    impl log::Log for TestLogger {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.level() <= log::Level::Info
        }

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                self.records
                    .lock()
                    .expect("test log lock")
                    .push((std::thread::current().id(), record.args().to_string()));
            }
        }

        fn flush(&self) {}
    }

    static TEST_LOGGER: TestLogger = TestLogger {
        records: Mutex::new(Vec::new()),
    };
    static TEST_LOGGER_INIT: Once = Once::new();

    fn reset_current_test_logs() {
        TEST_LOGGER_INIT.call_once(|| {
            log::set_logger(&TEST_LOGGER).expect("install test logger");
            log::set_max_level(log::LevelFilter::Info);
        });
        let current = std::thread::current().id();
        TEST_LOGGER
            .records
            .lock()
            .expect("test log lock")
            .retain(|(thread, _)| *thread != current);
    }

    fn current_test_logs() -> Vec<String> {
        let current = std::thread::current().id();
        TEST_LOGGER
            .records
            .lock()
            .expect("test log lock")
            .iter()
            .filter(|(thread, _)| *thread == current)
            .map(|(_, message)| message.clone())
            .collect()
    }

    fn health_signal(key: &str, status: u32) -> HealthSignal {
        HealthSignal {
            key: key.to_string(),
            status,
            detail: String::new(),
            observed_value: 0.0,
            reference_value: -1.0,
        }
    }

    fn snapshot(ts_ns: u64, health_signals: Vec<HealthSignal>) -> VitalsSnapshot {
        VitalsSnapshot {
            ts_ns,
            power: Some(PowerState {
                soc_percent: -1.0,
                voltage: -1.0,
                charging: false,
                remaining_s: -1,
            }),
            health_signals,
            bodies: vec![],
        }
    }

    async fn subscribe_after_baseline(
        service: &VitalsServiceImpl,
        baseline: VitalsSnapshot,
    ) -> ReceiverStream<Result<VitalsSnapshot, Status>> {
        service.update_snapshot(baseline).await;
        let mut stream = service
            .stream_vitals(Request::new(StreamVitalsRequest {}))
            .await
            .expect("stream response")
            .into_inner();
        timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("initial snapshot timeout")
            .expect("initial stream ended")
            .expect("initial snapshot status");
        stream
    }

    #[tokio::test]
    async fn modules_get_returns_empty_snapshot_initially() {
        let svc = VitalsServiceImpl::new();
        let response = svc
            .get_module_health_snapshot(Request::new(GetModuleHealthSnapshotRequest {}))
            .await
            .expect("module health snapshot response")
            .into_inner();

        let snapshot = response.snapshot.expect("module health snapshot");
        assert_eq!(snapshot.schema_version, MODULE_HEALTH_SCHEMA_VERSION);
        assert_eq!(snapshot.seq, 1);
        assert!(snapshot.modules.is_empty());
    }

    #[tokio::test]
    async fn modules_get_returns_ingested_reports() {
        let svc = VitalsServiceImpl::new();
        svc.ingest_module_health_report(ModuleHealthReport {
            schema_version: MODULE_HEALTH_SCHEMA_VERSION,
            module: Some(ModuleHealth {
                module_id: "executor".to_string(),
                provider_id: "executor".to_string(),
                health: HEALTH_OK,
                state: "active".to_string(),
                reason_code: "OK".to_string(),
                detail: "executor serving".to_string(),
                ttl_ms: 5000,
                ..Default::default()
            }),
        })
        .await
        .expect("ingest module health report");

        let snapshot = svc.module_health_snapshot(1000).await;
        assert_eq!(snapshot.modules.len(), 1);
        assert_eq!(snapshot.modules[0].module_key, "executor");
        assert_eq!(snapshot.modules[0].source, "SELF_REPORTED");
    }

    #[tokio::test]
    async fn self_module_health_enters_snapshot() {
        let svc = VitalsServiceImpl::new();
        let event = svc
            .update_self_module_health("vitals")
            .await
            .expect("self health report");
        assert!(event.is_none());

        let snapshot = svc.module_health_snapshot(1000).await;
        assert_eq!(snapshot.modules.len(), 1);
        assert_eq!(snapshot.modules[0].module_key, "vitals");
        assert_eq!(snapshot.modules[0].module_id, "vitals");
        assert_eq!(snapshot.modules[0].provider_id, "vitals");
        assert_eq!(snapshot.modules[0].health, HEALTH_OK);
        assert_eq!(snapshot.modules[0].state, "active");
        assert_eq!(snapshot.modules[0].reason_code, "OK");
        assert_eq!(snapshot.modules[0].detail, "vitals serving");
        assert_eq!(snapshot.modules[0].source, "SELF_REPORTED");
        assert_eq!(snapshot.modules[0].ttl_ms, 0);
    }

    #[tokio::test]
    async fn stale_synthesis_updates_module_snapshot() {
        let svc = VitalsServiceImpl::new();
        svc.ingest_module_health_report(ModuleHealthReport {
            schema_version: MODULE_HEALTH_SCHEMA_VERSION,
            module: Some(ModuleHealth {
                module_id: "pilot".to_string(),
                provider_id: "pilot".to_string(),
                health: HEALTH_OK,
                state: "active".to_string(),
                reason_code: "OK".to_string(),
                detail: "pilot serving".to_string(),
                ttl_ms: 1,
                ..Default::default()
            }),
        })
        .await
        .expect("ingest module health report");

        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let event = svc
            .synthesize_stale_module_if_expired("pilot", ExpectedModulePolicy::Required)
            .await
            .expect("stale event");
        assert_eq!(event.previous_health, HEALTH_OK);
        assert_eq!(event.current_health, crate::module_health::HEALTH_ERROR);

        let snapshot = svc.module_health_snapshot(1000).await;
        assert_eq!(
            snapshot.modules[0].health,
            crate::module_health::HEALTH_ERROR
        );
        assert_eq!(snapshot.modules[0].reason_code, "STALE");
        assert_eq!(
            snapshot.modules[0].source,
            crate::module_health::SOURCE_VITALS_SYNTHESIZED_STALE
        );
    }

    #[tokio::test]
    async fn disabled_expected_module_enters_snapshot() {
        let svc = VitalsServiceImpl::new();
        svc.apply_expected_module_config(&[ExpectedModuleConfig {
            module_id: "speech".to_string(),
            provider_id: None,
            capability: None,
            policy: ExpectedModulePolicy::Disabled,
            ttl_ms: 0,
        }])
        .await;

        let snapshot = svc.module_health_snapshot(1000).await;
        assert_eq!(snapshot.modules.len(), 1);
        assert_eq!(snapshot.modules[0].module_key, "speech");
        assert_eq!(snapshot.modules[0].health, HEALTH_OK);
        assert_eq!(snapshot.modules[0].state, "disabled");
        assert_eq!(snapshot.modules[0].reason_code, "DISABLED");
        assert_eq!(
            snapshot.modules[0].source,
            crate::module_health::SOURCE_CONFIG_DISABLED
        );
    }

    #[tokio::test]
    async fn required_expected_module_can_be_marked_unavailable() {
        let svc = VitalsServiceImpl::new();
        let event = svc
            .synthesize_expected_module_unavailable(&ExpectedModuleConfig {
                module_id: "executor".to_string(),
                provider_id: Some("executor".to_string()),
                capability: None,
                policy: ExpectedModulePolicy::Required,
                ttl_ms: 5000,
            })
            .await
            .expect("missing event");

        assert_eq!(event.previous_health, HEALTH_OK);
        assert_eq!(event.current_health, crate::module_health::HEALTH_ERROR);

        let snapshot = svc.module_health_snapshot(1000).await;
        assert_eq!(snapshot.modules[0].module_key, "executor");
        assert_eq!(
            snapshot.modules[0].health,
            crate::module_health::HEALTH_ERROR
        );
        assert_eq!(snapshot.modules[0].source, "VITALS_SYNTHESIZED_STALE");
    }

    #[tokio::test]
    async fn stream_pushes_when_fault_error_signal_disappears() {
        let service = VitalsServiceImpl::new();
        let fault_key = "body/arm/joint_3/fault/overcurrent";
        let mut stream = subscribe_after_baseline(
            &service,
            snapshot(
                1,
                vec![health_signal(fault_key, crate::soma_ingest::HEALTH_ERROR)],
            ),
        )
        .await;

        service.update_snapshot(snapshot(2, vec![])).await;

        let recovered = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("fault removal must push a recovery snapshot")
            .expect("stream ended after fault removal")
            .expect("fault recovery status");
        assert_eq!(recovered.ts_ns, 2);
        assert!(recovered.health_signals.is_empty());

        service.update_snapshot(snapshot(3, vec![])).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable empty health signals must not push a second recovery snapshot"
        );
    }

    #[tokio::test]
    async fn stream_pushes_when_torque_warn_signal_disappears() {
        let service = VitalsServiceImpl::new();
        let torque_key = "body/arm/joint_6/torque_enabled";
        let mut stream = subscribe_after_baseline(
            &service,
            snapshot(
                1,
                vec![health_signal(torque_key, crate::soma_ingest::HEALTH_WARN)],
            ),
        )
        .await;

        service.update_snapshot(snapshot(2, vec![])).await;

        let recovered = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("torque warning removal must push a recovery snapshot")
            .expect("stream ended after torque warning removal")
            .expect("torque recovery status");
        assert_eq!(recovered.ts_ns, 2);
        assert!(recovered.health_signals.is_empty());

        service.update_snapshot(snapshot(3, vec![])).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable empty components must not push a second torque recovery snapshot"
        );
    }

    #[tokio::test]
    async fn stream_does_not_push_for_stable_health_or_duplicate_snapshot() {
        let service = VitalsServiceImpl::new();
        let key = "body/arm/joint_1/motor_temp";
        let mut baseline_signal = health_signal(key, crate::soma_ingest::HEALTH_WARN);
        baseline_signal.detail = "temperature high: 61".to_string();
        baseline_signal.observed_value = 61.0;
        baseline_signal.reference_value = 60.0;
        let mut stream =
            subscribe_after_baseline(&service, snapshot(1, vec![baseline_signal])).await;

        let mut same_health = health_signal(key, crate::soma_ingest::HEALTH_WARN);
        same_health.detail = "temperature high: 62".to_string();
        same_health.observed_value = 62.0;
        same_health.reference_value = 61.0;
        let stable = snapshot(2, vec![same_health]);
        service.update_snapshot(stable.clone()).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "same key and health must ignore value/detail/threshold changes"
        );

        service.update_snapshot(stable).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "duplicate stable snapshot must not flood the stream"
        );
    }

    #[tokio::test]
    async fn stream_pushes_once_when_info_ok_key_is_added_to_nonempty_set() {
        let service = VitalsServiceImpl::new();
        let existing = health_signal(
            "body/arm/joint_6/torque_enabled",
            crate::soma_ingest::HEALTH_WARN,
        );
        let mut stream =
            subscribe_after_baseline(&service, snapshot(1, vec![existing.clone()])).await;
        let info = health_signal(
            "body/arm/joint_1/fault/maintenance_notice",
            crate::soma_ingest::HEALTH_OK,
        );

        service
            .update_snapshot(snapshot(2, vec![existing.clone(), info.clone()]))
            .await;
        let added = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("new INFO/OK key must push when the previous set is nonempty")
            .expect("stream ended after INFO/OK key addition")
            .expect("INFO/OK key addition status");
        assert_eq!(added.ts_ns, 2);
        assert_eq!(added.health_signals.len(), 2);

        service
            .update_snapshot(snapshot(3, vec![existing, info]))
            .await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable INFO/OK key set must not push a second snapshot"
        );
    }

    #[tokio::test]
    async fn stream_pushes_once_when_warn_or_error_key_is_added() {
        for (status, suffix) in [
            (crate::soma_ingest::HEALTH_WARN, "warn"),
            (crate::soma_ingest::HEALTH_ERROR, "error"),
        ] {
            let service = VitalsServiceImpl::new();
            let existing = health_signal("body/computer/cpu/temp", HEALTH_OK);
            let mut stream =
                subscribe_after_baseline(&service, snapshot(1, vec![existing.clone()])).await;
            let added_signal = health_signal(&format!("body/test/{suffix}"), status);

            service
                .update_snapshot(snapshot(2, vec![existing.clone(), added_signal.clone()]))
                .await;
            let added = timeout(Duration::from_millis(200), stream.next())
                .await
                .unwrap_or_else(|_| panic!("new {suffix} key must push"))
                .expect("stream ended after non-OK key addition")
                .expect("non-OK key addition status");
            assert_eq!(added.ts_ns, 2);
            assert_eq!(added.health_signals.len(), 2);

            service
                .update_snapshot(snapshot(3, vec![existing, added_signal]))
                .await;
            assert!(
                timeout(Duration::from_millis(50), stream.next())
                    .await
                    .is_err(),
                "stable {suffix} key set must not push twice"
            );
        }
    }

    #[tokio::test]
    async fn stream_pushes_once_when_existing_key_status_changes() {
        let service = VitalsServiceImpl::new();
        let key = "body/arm/joint_1/motor_temp";
        let mut stream = subscribe_after_baseline(
            &service,
            snapshot(1, vec![health_signal(key, crate::soma_ingest::HEALTH_WARN)]),
        )
        .await;

        let error = health_signal(key, crate::soma_ingest::HEALTH_ERROR);
        service
            .update_snapshot(snapshot(2, vec![error.clone()]))
            .await;
        let changed = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("existing key status change must push")
            .expect("stream ended after status change")
            .expect("status change result");
        assert_eq!(changed.ts_ns, 2);
        assert_eq!(
            changed.health_signals[0].status,
            crate::soma_ingest::HEALTH_ERROR
        );

        service.update_snapshot(snapshot(3, vec![error])).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable changed status must not push twice"
        );
    }

    #[tokio::test]
    async fn stream_serves_latest_snapshot_immediately_to_new_subscriber() {
        let service = VitalsServiceImpl::new();
        service.update_snapshot(snapshot(7, vec![])).await;

        let mut stream = service
            .stream_vitals(Request::new(StreamVitalsRequest {}))
            .await
            .expect("stream response")
            .into_inner();
        let initial = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("initial snapshot timeout")
            .expect("initial stream ended")
            .expect("initial snapshot status");
        assert_eq!(initial.ts_ns, 7);
    }

    #[tokio::test]
    async fn stream_pushes_once_when_body_status_changes() {
        let service = VitalsServiceImpl::new();
        let mut baseline = snapshot(1, vec![]);
        baseline.bodies = vec![BodyHealth {
            key: "arm".to_string(),
            model: "piper".to_string(),
            status: 0,
            components: vec![],
        }];
        let mut stream = subscribe_after_baseline(&service, baseline).await;

        let mut changed = snapshot(2, vec![]);
        changed.bodies = vec![BodyHealth {
            key: "arm".to_string(),
            model: "piper".to_string(),
            status: 1,
            components: vec![],
        }];
        service.update_snapshot(changed.clone()).await;
        let pushed = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("body status change must push")
            .expect("stream ended after body status change")
            .expect("body status change result");
        assert_eq!(pushed.ts_ns, 2);
        assert_eq!(pushed.bodies[0].status, 1);

        changed.ts_ns = 3;
        service.update_snapshot(changed).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable body status must not push twice"
        );
    }

    #[tokio::test]
    async fn stream_pushes_once_when_power_changes() {
        let service = VitalsServiceImpl::new();
        let mut stream = subscribe_after_baseline(&service, snapshot(1, vec![])).await;

        let mut changed = snapshot(2, vec![]);
        changed.power = Some(PowerState {
            soc_percent: 75.0,
            voltage: 24.0,
            charging: true,
            remaining_s: 3600,
        });
        service.update_snapshot(changed.clone()).await;
        let pushed = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("power change must push")
            .expect("stream ended after power change")
            .expect("power change result");
        assert_eq!(pushed.ts_ns, 2);
        assert_eq!(pushed.power.expect("power summary").soc_percent, 75.0);

        changed.ts_ns = 3;
        service.update_snapshot(changed).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable power must not push twice"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn first_snapshot_logs_torque_status_and_complete_power_baseline_once() {
        reset_current_test_logs();
        let service = VitalsServiceImpl::new();
        let mut baseline = snapshot(
            1,
            vec![HealthSignal {
                key: "body/arm/joint_1/torque_enabled".to_string(),
                status: crate::soma_ingest::HEALTH_OK,
                detail: "body/arm/joint_1 torque is enabled".to_string(),
                observed_value: 1.0,
                reference_value: 1.0,
            }],
        );
        baseline.power = Some(PowerState {
            soc_percent: 82.0,
            voltage: 24.75,
            charging: true,
            remaining_s: 3600,
        });

        service.update_snapshot(baseline.clone()).await;
        let first_logs = current_test_logs();
        assert!(first_logs.iter().any(|message| {
            message.contains(
                "health signal added: key=body/arm/joint_1/torque_enabled status=OK observed_value=1 reference_value=1",
            )
        }));
        assert!(first_logs.iter().any(|message| {
            message.contains(
                "power baseline: soc_percent=82% voltage=24.75V charging=true remaining_s=3600",
            )
        }));

        reset_current_test_logs();
        baseline.ts_ns = 2;
        service.update_snapshot(baseline.clone()).await;
        assert!(
            current_test_logs().is_empty(),
            "stable snapshot must not repeat baseline logs"
        );

        reset_current_test_logs();
        baseline.ts_ns = 3;
        baseline.health_signals[0].status = crate::soma_ingest::HEALTH_WARN;
        baseline.health_signals[0].detail = "body/arm/joint_1 torque is disabled".to_string();
        baseline.health_signals[0].observed_value = 0.0;
        service.update_snapshot(baseline.clone()).await;
        let transition_logs = current_test_logs();
        assert!(transition_logs.iter().any(|message| {
            message.contains(
                "health signal status changed: key=body/arm/joint_1/torque_enabled previous_status=OK status=WARN observed_value=0 reference_value=1",
            )
        }));

        reset_current_test_logs();
        baseline.ts_ns = 4;
        service.update_snapshot(baseline).await;
        assert!(
            current_test_logs().is_empty(),
            "stable torque status must not repeat transition logs"
        );
    }

    #[tokio::test]
    async fn target_stream_pushes_once_when_info_health_signal_disappears() {
        let target_snapshot = |ts_ns, health_signals| VitalsSnapshot {
            ts_ns,
            power: Some(PowerState {
                soc_percent: -1.0,
                voltage: -1.0,
                charging: false,
                remaining_s: -1,
            }),
            health_signals,
            bodies: vec![],
        };
        let service = VitalsServiceImpl::new();
        let info = HealthSignal {
            key: "body/arm/joint_1/fault/maintenance_notice".to_string(),
            status: crate::soma_ingest::HEALTH_OK,
            detail: "fault_id=maintenance_notice; vendor_code=0".to_string(),
            observed_value: -1.0,
            reference_value: -1.0,
        };
        let mut stream = subscribe_after_baseline(&service, target_snapshot(1, vec![info])).await;

        service.update_snapshot(target_snapshot(2, vec![])).await;
        let recovered = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("INFO signal removal must push a recovery snapshot")
            .expect("stream ended after INFO signal removal")
            .expect("INFO recovery status");
        assert_eq!(recovered.ts_ns, 2);
        assert!(recovered.health_signals.is_empty());

        service.update_snapshot(target_snapshot(3, vec![])).await;
        assert!(
            timeout(Duration::from_millis(50), stream.next())
                .await
                .is_err(),
            "stable empty health signals must not push a second recovery snapshot"
        );
    }
}
