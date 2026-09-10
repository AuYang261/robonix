uint64 ts_ns                 # chronos canonical timestamp (CLOCK_MONOTONIC ns)
# TODO: Replace aggregate power with per-power-source data keyed by component_id.
# Migrate callers and power change notifications before removing this field.
PowerState power
ComponentHealth[] components
BodyHealth[] bodies          # zero or more body health entries
