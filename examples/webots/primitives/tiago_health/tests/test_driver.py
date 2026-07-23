#!/usr/bin/env python3
"""Focused tests for the deterministic Webots health profile."""

import unittest

from tiago_health.driver import HealthSettings, build_health_state


class HealthDriverTests(unittest.TestCase):
    def test_normal_profile_contains_nominal_component_values(self):
        """The default frame covers every Soma component without fault codes."""
        state = build_health_state(HealthSettings())
        readings = {reading.name: reading for reading in state.readings}
        self.assertAlmostEqual(state.voltage, 24.8, places=5)
        self.assertEqual(readings["body/state"].current_a, 0.0)
        self.assertEqual(readings["body/base/left_wheel/error"].current_a, 0.0)
        self.assertEqual(readings["body/base/right_wheel/communication_ok"].current_a, 1.0)
        self.assertAlmostEqual(
            readings["body/base/battery"].battery_percent, 82.0, places=5
        )

    def test_unknown_scenario_is_rejected(self):
        """Reserved scenarios fail initialization until their data is implemented."""
        with self.assertRaisesRegex(ValueError, "only 'normal' is implemented"):
            HealthSettings.from_config({"scenario": "wheel_fault"})


if __name__ == "__main__":
    unittest.main()
