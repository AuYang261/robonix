uint64 ts_ns                 # Vitals process CLOCK_MONOTONIC timestamp (ns)
PowerState power             # first power source summary; sentinel values when unavailable
HealthSignal[] health_signals # sparse health-signal entries
BodyHealth[] bodies          # root-child subtree entries; root fallback when it has no direct children
