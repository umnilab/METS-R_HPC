"""Shared stop-sign mode: configuration, ingress timing, ownership and cleanup."""
import importlib.util
import math
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scenic_exp.runtime import testing


class StopConfigurationTests(unittest.TestCase):
    def test_stop_parameters_are_typed_and_bounded(self):
        stop = testing.resolve_test_parameters()["stop_sign"]
        self.assertEqual(stop, dict(index=0, enabled=True, max_distance_m=90., minimum_red=90,
                                    red_dominance=1.25, roi_scale=1.7, min_patch_pixels=25))
        for key, value in (("index", -1), ("index", 1.5), ("index", True),
                           ("enabled", 1), ("enabled", "false"),
                           ("max_distance_m", 0), ("max_distance_m", math.inf),
                           ("minimum_red", -1), ("minimum_red", 256), ("minimum_red", 90.5),
                           ("red_dominance", 1), ("roi_scale", 0), ("roi_scale", math.nan),
                           ("min_patch_pixels", 0), ("min_patch_pixels", 2.5)):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                testing.resolve_test_parameters({"stop_sign": {key: value}})

    def test_controller_ownership_rules_allow_only_external_stop_mode(self):
        self.assertIn("stop_sign_patch", testing.SECURITY_SCENARIOS)
        testing.validate_test_modes("none", "stop_sign_patch", "external")
        testing.validate_test_modes("merge", "stop_sign_patch", "pcla")
        for safety, security, controller in (("merge", "stop_sign_patch", "external"),
                                             ("none", "sensor_spoofing", "external"),
                                             ("none", "stop_sign_patch", "autopilot")):
            with self.subTest(safety=safety, security=security, controller=controller), self.assertRaises(ValueError):
                testing.validate_test_modes(safety, security, controller)


@unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy required for stop-sign frames")
class StopRuntimeTests(unittest.TestCase):
    def make_runtime(self, *, enabled=True, rgb=True, index=0):
        from scenic_exp.runtime.stop_sign import StopSignCandidate
        origin = NS(x=0., y=0., z=0.)
        target = NS(x=10., y=0., z=0.)
        transform = NS(location=origin, rotation=NS(roll=0., pitch=0., yaw=0.))
        actor = NS(get_location=lambda: origin, get_transform=lambda: transform)
        candidate = StopSignCandidate(index=index, actor_id="123", route_location=target,
                                     visual_location=target, visual_extent_m=.65, source="fixture",
                                     approach_transform=None, approach_distance_m=35., same_road_distance_m=35.,
                                     road_id=1, route_is_junction=False, visual_id="mesh-123")
        ego = NS(name="ego", carlaActor=actor)
        simulation = NS(currentTime=0, timestep=.1, objects=[ego], destroy=Mock(), carla_world=object())
        config = testing.resolve_test_parameters({"trigger": {"start_s": .2},
            "event": {"duration_s": .2, "repeat_s": .4}, "stop_sign": {"enabled": enabled, "index": index}})
        runtime = testing.ScenarioRuntime(simulation, ego, "none", "stop_sign_patch", config)
        self.addCleanup(runtime.close)
        delivered = []
        interface = NS(update_sensor=lambda tag, data, timestamp: delivered.append((tag, data, timestamp)))
        sensors = [{"id": "front", "type": "sensor.camera.rgb", "fov": 90.}] if rgb else []
        pcla = NS(agent_instance=NS(sensor_interface=interface, sensors=lambda: sensors))
        return runtime, simulation, pcla, interface, delivered, candidate

    def test_scheduled_ingress_recolors_only_active_frames_and_reports_target(self):
        import numpy as np
        runtime, simulation, pcla, interface, delivered, candidate = self.make_runtime()
        original_update = interface.update_sensor
        tap = runtime.bind_pcla(pcla, stop_candidate=candidate)
        self.assertIs(tap, runtime.stop_sign_tap)
        source = np.full((20, 20, 4), [0, 0, 200, 77], dtype=np.uint8)
        for step in range(8):
            simulation.currentTime = step
            runtime.before_step()
            interface.update_sensor("front", source, step + 100)
        self.assertEqual([stamp for _, _, stamp in delivered], list(range(100, 108)))
        changed_steps = [step for step, (_, image, _) in enumerate(delivered) if image is not source]
        self.assertEqual(changed_steps, [2, 3, 6, 7])
        for step in changed_steps:
            image = delivered[step][1]
            self.assertEqual(image[10, 10].tolist(), [200, 0, 0, 77])
            self.assertTrue((image[..., 3] == 77).all())
        self.assertTrue((source[..., 2] == 200).all())
        record = runtime.snapshot()
        self.assertEqual(record["sensor_frames_modified"], 4)
        self.assertEqual(record["stop_sign"]["frames_patched"], 4)
        self.assertGreater(record["stop_sign"]["pixels_patched"], 0)
        self.assertEqual(record["stop_sign"]["target"], dict(x=10., y=0., z=0.))
        self.assertEqual(record["stop_sign"]["candidate_id"], "123")
        simulation.destroy()
        self.assertTrue(runtime.closed)
        self.assertIs(interface.update_sensor, original_update)
        self.assertEqual(runtime.snapshot()["stop_sign"]["frames_patched"], 4)

    def test_native_binding_discovers_configured_candidate_and_applies_settings(self):
        from scenic_exp.runtime import stop_sign
        runtime, simulation, pcla, _, _, candidate = self.make_runtime(index=4)
        runtime.config["stop_sign"].update(max_distance_m=50., minimum_red=120,
                                         red_dominance=1.5, roi_scale=2., min_patch_pixels=9)
        carla = ModuleType("carla")
        with patch.dict(sys.modules, {"carla": carla}), patch.object(stop_sign, "discover_stop_signs", return_value=[candidate]) as discover:
            tap = runtime.bind_pcla(pcla)
        discover.assert_called_once_with(simulation.carla_world, carla, approach_distance_m=35.0)
        self.assertIs(runtime.stop_candidate, candidate)
        self.assertEqual(tap._args, stop_sign.StopSignPatchSettings(50., 120, 1.5, 2., 9))

    def test_unavailable_candidate_and_missing_rgb_fail_before_installing_hook(self):
        from scenic_exp.runtime import stop_sign
        runtime, _, pcla, interface, _, candidate = self.make_runtime(rgb=False)
        original_update = interface.update_sensor
        with patch.dict(sys.modules, {"carla": ModuleType("carla")}), patch.object(stop_sign, "discover_stop_signs", return_value=[]):
            with self.assertRaisesRegex(ValueError, "available indices"):
                runtime.bind_pcla(pcla)
        self.assertIsNone(runtime.stop_sign_tap)
        self.assertIs(interface.update_sensor, original_update)
        with self.assertRaisesRegex(RuntimeError, "RGB camera"):
            runtime.bind_pcla(pcla, stop_candidate=candidate)
        self.assertIs(interface.update_sensor, original_update)

    def test_disabled_baseline_keeps_input_unmodified_and_allows_no_rgb(self):
        import numpy as np
        for rgb in (False, True):
            with self.subTest(rgb=rgb):
                runtime, simulation, pcla, interface, delivered, candidate = self.make_runtime(enabled=False, rgb=rgb)
                runtime.bind_pcla(pcla, stop_candidate=candidate)
                simulation.currentTime = 2
                runtime.before_step()
                source = np.full((20, 20, 4), [0, 0, 200, 77], dtype=np.uint8)
                interface.update_sensor("front", source, 15)
                self.assertIs(delivered[0][1], source)
                self.assertFalse(runtime.active)
                self.assertIsNone(runtime.onset)
                self.assertEqual(runtime.snapshot()["stop_sign"]["frames_patched"], 0)

    def test_double_binding_is_rejected_and_does_not_leak_sensor_wrappers(self):
        runtime, _, pcla, interface, _, candidate = self.make_runtime()
        original_update = interface.update_sensor
        runtime.bind_pcla(pcla, stop_candidate=candidate)
        wrapped_update = interface.update_sensor
        with self.assertRaisesRegex(RuntimeError, "already.*bound"):
            runtime.bind_pcla(pcla, stop_candidate=candidate)
        self.assertIs(interface.update_sensor, wrapped_update)
        runtime.close()
        runtime.close()
        self.assertIs(interface.update_sensor, original_update)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            runtime.bind_pcla(pcla, stop_candidate=candidate)


if __name__ == "__main__":
    unittest.main()
