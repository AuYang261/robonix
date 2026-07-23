# TIAGo simulated health primitive

This Webots package provides `robonix/primitive/health/state` and
`robonix/primitive/health/stream`. It publishes deterministic nominal values
for the TIAGo base, wheels, battery, camera, lidar, and audio component paths
declared in `examples/webots/soma.yaml`.

Configuration is delivered through the primitive entry in
`robonix_manifest.yaml`:

```yaml
config:
  scenario: normal
  interval_s: 0.5
  battery_percent: 82.0
  voltage: 24.8
  remaining_s: 10800
```

`scenario` is the stable fault-injection entry point. Only `normal` is
implemented now; unsupported values fail lifecycle initialization instead of
silently reporting a healthy robot.
