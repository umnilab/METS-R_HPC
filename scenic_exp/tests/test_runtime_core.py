"""Offline contracts for the shared runtime and its compatibility imports."""
import importlib.util
import math
from pathlib import Path
import sys
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scenic_exp.runtime import testing
from scenic_exp.runtime.lifecycle import register_pre_actor_teardown
from utils import scenic_testing as legacy


class ConfigurationTests(unittest.TestCase):
    def test_legacy_imports_resolve_to_shared_implementation(self):
        for name in ("ScenarioRuntime", "resolve_test_parameters", "safety_sites",
                     "merge_path", "path_control", "trigger_reached", "event_active",
                     "perturb_sensor_batch", "scenario_test_records", "load_test_config",
                     "write_test_report", "_line", "_pose"):
            self.assertIs(getattr(legacy, name), getattr(testing, name), name)

    def test_resolved_profiles_are_independent_and_reject_invalid_parameters(self):
        overrides = {"sensor": {"imu_offset": [1, 2, 3, 4, 5, 6, 7]}}
        resolved = testing.resolve_test_parameters(overrides)
        resolved["sensor"]["imu_offset"][0] = 99
        self.assertEqual(overrides["sensor"]["imu_offset"][0], 1)
        self.assertEqual(testing.resolve_test_parameters()["sensor"]["imu_offset"][0], 0)
        for invalid in ({"sensor": {"speed_bias": 2}},
                        {"trigger": {"start_s": math.nan}},
                        {"event": {"duration_s": 2, "repeat_s": 1}},
                        {"perception": {"roi": [.9, 0, .2, .5]}}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                testing.resolve_test_parameters(invalid)

    def test_scheduled_effect_uses_simulation_time_and_half_open_windows(self):
        cfg = testing.resolve_test_parameters({"event": {"duration_s": .2, "repeat_s": .4}})
        self.assertEqual([testing.event_active(step * .1, .2, cfg) for step in range(10)],
                         [False, False, True, True, False, False, True, True, False, False])


@unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy required for sensor arrays")
class SensorTests(unittest.TestCase):
    def test_sensor_bias_preserves_source_batch_timestamp_and_unselected_sensor(self):
        import numpy as np
        for kind, payload, offset in (("speedometer", {"speed": 1.}, None),
                                      ("gnss", np.array([40., -86., 2.]), [1, 2, 3]),
                                      ("imu", np.zeros(7), [1, 2, 3, 4, 5, 6, 7])):
            with self.subTest(kind=kind):
                values = {"kind": kind, "speed_bias_mps": -5}
                if offset is not None:
                    values[kind + "_offset"] = offset
                cfg = testing.resolve_test_parameters({"sensor": values})
                original = payload.copy()
                batch = {"selected": (42, payload), "other": (43, object())}
                modified = testing.perturb_sensor_batch(batch, "selected", "sensor_spoofing", cfg)
                self.assertIsNot(modified, batch)
                self.assertIs(modified["other"], batch["other"])
                self.assertEqual(modified["selected"][0], 42)
                self.assertIsNot(modified["selected"][1], payload)
                if kind == "speedometer":
                    self.assertEqual(payload, original)
                    self.assertEqual(modified["selected"][1]["speed"], 0)
                else:
                    np.testing.assert_array_equal(payload, original)
                    np.testing.assert_allclose(modified["selected"][1], original + offset)

    def test_camera_patch_and_blackout_preserve_bgra_contract(self):
        import numpy as np
        source = np.full((10, 20, 4), 77, dtype=np.uint8)
        cfg = testing.resolve_test_parameters({"perception": {
            "roi": [.25, .2, .5, .5], "color_rgb": [255, 12, 3]}})
        batch = {"camera": (19, source)}
        stamp, image = testing.perturb_sensor_batch(batch, "camera", "adversarial_perception", cfg)["camera"]
        self.assertEqual(stamp, 19)
        self.assertEqual(image.dtype, source.dtype)
        np.testing.assert_array_equal(image[2:7, 5:15, :3], np.broadcast_to([3, 12, 255], (5, 10, 3)))
        self.assertTrue((image[..., 3] == 77).all())
        self.assertTrue((source == 77).all())
        cfg["perception"]["mode"] = "blackout"
        _, black = testing.perturb_sensor_batch(batch, "camera", "adversarial_perception", cfg)["camera"]
        self.assertTrue((black[..., :3] == 0).all())
        self.assertTrue((black[..., 3] == 77).all())


class LifecycleTests(unittest.TestCase):
    def test_native_callback_is_preferred_and_releasable_without_destroy_patch(self):
        callbacks = []
        simulation = NS(add_pre_actor_teardown_callback=callbacks.append, destroy=Mock())
        original = simulation.destroy
        cleanup = Mock()
        release = register_pre_actor_teardown(simulation, cleanup)
        self.assertIs(simulation.destroy, original)
        self.assertEqual(len(callbacks), 1)
        callbacks[0]()
        callbacks[0]()
        release()
        cleanup.assert_called_once_with()
        original.assert_not_called()

        cancelled = Mock()
        register_pre_actor_teardown(simulation, cancelled)()
        callbacks[1]()
        cancelled.assert_not_called()

    def test_fallback_runs_destroy_after_cleanup_failure_and_restores_override(self):
        events = []
        original = Mock(side_effect=lambda *args, **kwargs: events.append("destroy actors"))
        simulation = NS(destroy=original)
        def cleanup():
            events.append("close sensors")
            raise RuntimeError("cleanup failure")
        register_pre_actor_teardown(simulation, cleanup)
        with self.assertRaisesRegex(RuntimeError, "cleanup failure"):
            simulation.destroy("argument", selected=True)
        self.assertEqual(events, ["close sensors", "destroy actors"])
        original.assert_called_once_with("argument", selected=True)
        self.assertIs(simulation.destroy, original)

    def test_fallback_preserves_later_hooks_and_inherited_destroy_method(self):
        events = []
        class Simulation:
            def destroy(self):
                events.append("destroy actors")
        simulation = Simulation()
        first_release = register_pre_actor_teardown(simulation, lambda: events.append("first"))
        register_pre_actor_teardown(simulation, lambda: events.append("second"))
        first_release()  # Must not remove the second hook.
        simulation.destroy()
        self.assertEqual(events, ["second", "destroy actors"])
        self.assertNotIn("destroy", vars(simulation))

    @unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy required for sensor arrays")
    def test_runtime_restores_sensor_before_native_actor_teardown(self):
        events = []
        callbacks = []
        class Interface:
            def get_data(self):
                return {"speed": (3, {"speed": 2.})}
        interface = Interface()
        ego = NS(name="ego")
        simulation = NS(currentTime=0, timestep=.1, objects=[ego],
                        add_pre_actor_teardown_callback=callbacks.append)
        def destroy():
            for callback in callbacks:
                callback()
            events.append(("destroy actors", interface.get_data()["speed"][1]["speed"]))
        simulation.destroy = destroy
        runtime = testing.ScenarioRuntime(simulation, ego, "none", "sensor_spoofing",
                                          testing.resolve_test_parameters({"trigger": {"start_s": 0}}))
        self.addCleanup(runtime.close)
        runtime.bind_pcla(NS(agent_instance=NS(sensor_interface=interface, sensors=lambda: [
            {"id": "speed", "type": "sensor.speedometer"}])))
        runtime.before_step()
        self.assertEqual(interface.get_data()["speed"][1]["speed"], 7.)
        self.assertIs(simulation.destroy, destroy)
        simulation.destroy()
        runtime.close()
        self.assertTrue(runtime.closed)
        self.assertEqual(events, [("destroy actors", 2.)])
        self.assertNotIn("get_data", vars(interface))
        self.assertIs(simulation.destroy, destroy)


if __name__ == "__main__":
    unittest.main()
