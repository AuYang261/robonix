# robonix-vitals

Monitors robot onboard health (CPU/GPU/NVMe temperature, voltage) and body (joint motors), evaluates thresholds, and reports via gRPC.

Vitals consumes the unified health stream (`SomaHealthSnapshot`) from Soma (real or mock). Without Soma, Vitals exits with an error.

## Architecture

Two parallel data paths coexist:

| Path | Use case | Flow |
|---|---|---|
| **Primitive → SOMA** (production) | Real SOMA manages the deployment | `health_piper` primitive → SOMA (aggregates + streams `SomaHealthSnapshot`) → Vitals |
| **Mock SOMA** (development / testing) | No real SOMA needed | `--mock-soma` in-process gRPC server → Vitals |

### Primitive → SOMA path (real hardware)

The health data pipeline when using real SOMA:

```
Piper CAN0 → health_piper (Python Primitive, robonix_api)
  → gRPC: robonix/primitive/health/stream (HealthState frames)
    → SOMA (discovers via Atlas, maps HealthState → SomaHealthSnapshot)
      → gRPC: robonix/system/soma/health (SomaHealthSnapshot stream)
        → Vitals (soma_ingest.rs — threshold evaluation + broadcasting)
```

The `health_piper` primitive implements `robonix/primitive/health/stream` and
wraps `piper_sdk`. It is **not** part of the Robonix source tree — create it in your
deployment's `primitives/` directory following the [vendor onboarding guide](https://robonix.syswonder.org/integration-guide/vendor-onboarding.html).

> **TODO — primitive bring-up failure semantics:** Soma currently exits when any
> primitive fails bring-up. The target behavior is for a primitive failure not to
> block Soma registration or service startup. Hardware unavailability (for example,
> an unavailable CAN port) must be distinguished from process/bootstrap failure: a
> health primitive should remain registered, report the unavailable condition through
> its health stream, and support recovery instead of using lifecycle `CMD_INIT` failure
> as the health state. For a generic `CMD_INIT` error, whether to retain an `ERROR`
> provider or terminate it remains an open decision.

### Mock SOMA path (no SOMA needed)

The `--mock-soma` flag starts an embedded gRPC server inside the Vitals process that
serves `robonix/system/soma/health` and `robonix/system/soma/get_health`. Vitals
consumes it through the same `soma_ingest.rs` pipeline as production.

Optionally, `--mock-soma-arm piper` spawns `scripts/piper_bridge.py` as a subprocess
(stdin/stdout JSON) to merge real joint data into synthetic snapshots.

## Port convention

| Role | Port |
|------|------|
| Atlas | `50051` |
| SOMA / mock SOMA | `50091` |
| Vitals | `50092` |

Mock SOMA replaces real SOMA, so they share the same port.

> **Note:** SOMA and Vitals both default to `50091`.  When running Vitals alongside
> SOMA (or mock SOMA), always pass `--listen 127.0.0.1:50092`.

## Running

For a screen-recording-friendly end-to-end health demo, see
[`VITALS_HEALTH_DEMO.md`](VITALS_HEALTH_DEMO.md).

### With real SOMA (production)

```bash
# Terminal 1: Atlas
robonix-atlas --listen 127.0.0.1:50051 --capabilities capabilities

# Terminal 2: SOMA (spawns health_piper primitive, starts health collection)
robonix-soma --atlas 127.0.0.1:50051 \
  --listen 127.0.0.1:50091 \
  --robot-yaml <deploy>/soma.yaml

# Terminal 3: Vitals (auto-discovers SOMA health stream via Atlas)
robonix-vitals --atlas 127.0.0.1:50051 \
  --listen 127.0.0.1:50092 \
  --thresholds-path system/vitals/thresholds/example_thresholds.yaml \
  --log info
```

Verify:

```bash
# Check registrations
rbnx caps -v    # Should show: soma [ACTIVE], vitals [ACTIVE], health_piper [ACTIVE]

# SOMA health snapshot (includes Piper joint data)
grpcurl -plaintext -d '{}' 127.0.0.1:50091 \
  robonix.contracts.RobonixSystemSomaGetHealth/GetHealth

# Vitals normalized snapshot
grpcurl -plaintext -d '{}' 127.0.0.1:50092 \
  robonix.contracts.RobonixSystemVitalsGet/GetVitals
```

### With mock SOMA (development, no SOMA binary needed)

```bash
# Terminal 1: Atlas (required — mock SOMA registers with it)
robonix-atlas --listen 127.0.0.1:50051 --log info

# Terminal 2: mock Soma (scenarios: normal / ramp / fault / toggle / mixed)
robonix-vitals --atlas 127.0.0.1:50051 \
  --log info \
  --mock-soma \
  --mock-soma-listen 127.0.0.1:50091 \
  --mock-soma-scenario ramp \
  --mock-soma-interval-ms 1000

# Terminal 3: Vitals consuming the Soma health stream
robonix-vitals --atlas 127.0.0.1:50051 \
  --log info \
  --listen 127.0.0.1:50092
```

### Mock Soma with real hardware (bridge subprocess)

```bash
# Terminal 1: Atlas (required — mock SOMA registers with it)
robonix-atlas --listen 127.0.0.1:50051 --log info

# Terminal 2: mock Soma with Piper arm bridge
robonix-vitals --atlas 127.0.0.1:50051 \
  --log info \
  --mock-soma \
  --mock-soma-listen 127.0.0.1:50091 \
  --mock-soma-arm piper \
  --mock-soma-piper-can can0 \
  --mock-soma-bridge-python ~/roboarm/.venv/bin/python3 \
  --mock-soma-interval-ms 1000

# Terminal 3: Vitals consumer
robonix-vitals --atlas 127.0.0.1:50051 \
  --log info \
  --listen 127.0.0.1:50092
```

## CLI flags

| Flag | Env var | Default |
|------|---------|---------|
| `--atlas` | `ROBONIX_ATLAS_ENDPOINT` | `127.0.0.1:50051` |
| `--listen` | `ROBONIX_VITALS_LISTEN` | `127.0.0.1:50091` |
| `--id` | `ROBONIX_VITALS_PROVIDER_ID` | `vitals` |
| `--thresholds-path` | `ROBONIX_VITALS_THRESHOLDS_PATH` | `thresholds/example_thresholds.yaml` |
| `--soma-endpoint` | `ROBONIX_SOMA_ENDPOINT` | — |
| `--mock-soma` | `ROBONIX_VITALS_MOCK_SOMA` | `false` |
| `--mock-soma-listen` | `ROBONIX_VITALS_MOCK_SOMA_LISTEN` | `127.0.0.1:50092` |
| `--mock-soma-scenario` | `ROBONIX_VITALS_MOCK_SOMA_SCENARIO` | `normal` |
| `--mock-soma-interval-ms` | `ROBONIX_VITALS_MOCK_SOMA_INTERVAL_MS` | `10000` |
| `--mock-soma-arm` | `ROBONIX_VITALS_MOCK_SOMA_ARM` | `synthetic` |
| `--mock-soma-piper-can` | `ROBONIX_VITALS_MOCK_SOMA_PIPER_CAN` | `can0` (when arm=piper) |
| `--mock-soma-koch-port` | `ROBONIX_VITALS_MOCK_SOMA_KOCH_PORT` | `/dev/ttyUSB0` (when arm=koch) |
| `--mock-soma-bridge-python` | `ROBONIX_VITALS_MOCK_SOMA_BRIDGE_PYTHON` | `python3` |
| `--mock-soma-piper-script` | `ROBONIX_VITALS_MOCK_SOMA_PIPER_SCRIPT` | `<crate>/scripts/piper_bridge.py` |
| `--mock-soma-koch-script` | `ROBONIX_VITALS_MOCK_SOMA_KOCH_SCRIPT` | `<crate>/scripts/koch_bridge.py` |
| `--config` | `ROBONIX_CONFIG_PATH` | — |
| `--log` | `RUST_LOG` | `robonix_vitals=info` |

## Threshold format

Soma selector format:

```yaml
rules:
  - id: "joint_motor_temp"
    selector:
      kind: "JOINT"
      signal: "motor_temp"
    warn_above: 60.0
    error_above: 75.0
    unit: "degC"
```

## Testing

### Unit tests

```bash
# Vitals crate tests (mock scenarios, threshold evaluation, etc.)
cargo test -p robonix-vitals

# Full workspace tests
cargo test --workspace --all-targets
```

### End-to-end test with real Piper arm (CAN0)

Prerequisites: Piper arm connected via CAN0, `~/roboarm/.venv` with `piper-sdk>=0.6.1`,
and a `health_piper` primitive package created in your deployment per the
[vendor onboarding guide](https://robonix.syswonder.org/integration-guide/vendor-onboarding.html).

**Step 1: Create the primitive** in your deployment directory (e.g. `~/roboarm/robonix/primitives/health_piper/`), then run `rbnx codegen -p .` inside it.

**Step 2: Edit `system/vitals/tests/fixtures/robonix_manifest.yaml`** — add your primitive to the `primitive:` list:

```yaml
primitive:
  - name: health_piper
    path: /home/xjy/roboarm/robonix/primitives/health_piper   # absolute path (no ~)
    config:
      can_port: can0
```

**Step 3: Start the stack** (three terminals, order doesn't matter — Vitals waits for SOMA)

```bash
# Terminal 1: Atlas
cargo run --release -p robonix-atlas -- \
  --listen 127.0.0.1:50051 --capabilities capabilities

# Terminal 2: Vitals (starts gRPC server immediately, retries SOMA in background)
cargo run --release -p robonix-vitals -- \
  --atlas 127.0.0.1:50051 \
  --listen 127.0.0.1:50092 \
  --thresholds-path system/vitals/thresholds/example_thresholds.yaml \
  --log info

# Terminal 3: SOMA (spawns health_piper, starts health streaming)
cargo run --release -p robonix-soma -- \
  --atlas 127.0.0.1:50051 \
  --listen 127.0.0.1:50091 \
  --robot-yaml system/vitals/tests/fixtures/soma.yaml
```

**Step 4: Verify**

```bash
# SOMA health snapshot — should contain Piper joint actuator data
grpcurl -plaintext -d '{}' 127.0.0.1:50091 \
  robonix.contracts.RobonixSystemSomaGetHealth/GetHealth

# Vitals normalized snapshot — should contain Piper arm topology and health signals
grpcurl -plaintext -d '{}' 127.0.0.1:50092 \
  robonix.contracts.RobonixSystemVitalsGet/GetVitals
```

Expected: `GetVitals` returns a `VitalsSnapshot` with an arm `BodyHealth` whose
`components` contain 6 topology-only Piper joints. Motor temperatures matching
threshold rules, active faults, and communication failures are projected when
applicable; every actuator's torque enabled/disabled state is always projected.
These values appear in `health_signals` (`healthSignals` in grpcurl JSON) with
`key`, `status`, `detail`, `observedValue`, and `referenceValue` fields.

### gRPC verification (mock SOMA)

```bash
# Vitals side (port 50092)
grpcurl -plaintext -d '{}' 127.0.0.1:50092 \
  robonix.contracts.RobonixSystemVitalsGet/GetVitals

# Mock Soma side (port 50091 — same as real SOMA)
grpcurl -plaintext -d '{}' 127.0.0.1:50091 \
  robonix.contracts.RobonixSystemSomaGetHealth/GetHealth
```
