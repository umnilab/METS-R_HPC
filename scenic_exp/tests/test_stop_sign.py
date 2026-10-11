"""Stop-sign effects can run without importing a dashboard or simulator SDK."""

import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from scenic_exp.runtime.stop_sign import (
    StopSignCandidate,
    StopSignColorPatchTap,
    StopSignPatchSettings,
    discover_stop_signs,
    project_stop_sign,
)


def location(x=0.0, y=0.0, z=0.0):
    return SimpleNamespace(x=x, y=y, z=z)


def candidate(x=10.0):
    return StopSignCandidate(
        index=0,
        actor_id="stop",
        route_location=location(x),
        visual_location=location(x),
        visual_extent_m=0.65,
        source="test",
        approach_transform=None,
        approach_distance_m=10.0,
        same_road_distance_m=10.0,
        road_id=1,
        route_is_junction=False,
    )


class SensorInterface:
    def __init__(self):
        self.received = []

    def update_sensor(self, tag, data, timestamp):
        self.received.append((tag, data, timestamp))
        return "queued"


class StopDiscoveryTests(unittest.TestCase):
    def discover(self, actors, signs):
        world = SimpleNamespace(
            get_actors=lambda: SimpleNamespace(filter=lambda pattern: actors),
            get_environment_objects=lambda label: signs,
            get_map=lambda: SimpleNamespace(get_waypoint=lambda *args, **kwargs: None),
        )
        carla = SimpleNamespace(CityObjectLabel=SimpleNamespace(TrafficSigns=1))
        return discover_stop_signs(world, carla)

    def sign(self, sign_id, x, *, name="BP_Stop", height=.65):
        return SimpleNamespace(
            id=sign_id, name=name,
            bounding_box=SimpleNamespace(location=location(x, 2, 2), extent=location(.65, .05, height)),
        )

    def test_actor_targets_require_distinct_nearby_upright_meshes(self):
        actors = [SimpleNamespace(id=index, get_location=lambda x=x: location(x))
                  for index, x in ((2, 30), (1, 0), (3, 100))]
        signs = [self.sign("right", 31), self.sign("left", 1),
                 self.sign("paint", 100, name="Stencil_STOP", height=.01)]

        found = self.discover(actors, signs)

        self.assertEqual([(item.index, item.actor_id, item.visual_id) for item in found],
                         [(0, "1", "left"), (1, "2", "right")])
        self.assertEqual([item.route_location.x for item in found], [0, 30])
        self.assertTrue(all(item.source.startswith("traffic.stop + upright") for item in found))
        self.assertTrue(all(item.approach_transform is None for item in found))

    def test_environment_only_targets_keep_ids_extents_and_coordinate_order(self):
        signs = [self.sign("right", 30), self.sign("left", 0),
                 self.sign("flat", 10, height=.1)]

        found = self.discover([], signs)

        self.assertEqual([(item.index, item.actor_id, item.visual_id) for item in found],
                         [(0, "left", "left"), (1, "right", "right")])
        self.assertTrue(all(item.route_location is item.visual_location for item in found))
        self.assertTrue(all(item.visual_extent_m == .65 for item in found))
        self.assertTrue(all(item.source.startswith("upright CARLA environment") for item in found))

    def test_unmatched_actors_do_not_enable_environment_fallback(self):
        actors = [SimpleNamespace(id=1, get_location=lambda: location(100))]
        self.assertEqual(self.discover(actors, [self.sign("distant", 0)]), [])


class StopSignRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.interface = SensorInterface()
        self.original_update = self.interface.update_sensor
        self.spec = {"type": "sensor.camera.rgb", "id": "front", "fov": 90.0}
        self.pcla = SimpleNamespace(agent_instance=SimpleNamespace(
            sensor_interface=self.interface,
            sensors=lambda: [self.spec, {"type": "sensor.other.imu", "id": "imu"}],
        ))
        self.ego = SimpleNamespace(
            get_location=lambda: location(),
            get_transform=lambda: SimpleNamespace(
                location=location(), rotation=SimpleNamespace(roll=0, pitch=0, yaw=0),
            ),
        )
        self.settings = replace(StopSignPatchSettings(), attack_min_patch_pixels=1)

    def make_tap(self, **kwargs):
        tap = StopSignColorPatchTap(
            self.pcla, self.ego, kwargs.pop("candidate", candidate()),
            kwargs.pop("enabled", True), self.settings, **kwargs,
        )
        self.addCleanup(tap.close)
        return tap

    def frame(self):
        data = np.zeros((100, 100, 4), dtype=np.uint8)
        data[:, :, 3] = 255
        data[50, 50, :3] = [10, 20, 200]
        data[1, 1, :3] = [10, 20, 200]  # Red outside the projected target.
        data[50, 51, :3] = [30, 200, 20]  # Green inside the projected target.
        return data

    def test_import_is_independent_of_demo_simulator_and_imaging_packages(self):
        script = """
import importlib.abc
import os
import sys

class NoIntegrationImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split('.')[0] in {'tutorials', 'carla', 'scenic', 'PCLA', 'PIL', 'utils'}:
            raise AssertionError('Unexpected integration import: ' + fullname)

sys.meta_path.insert(0, NoIntegrationImports())
before = os.getcwd()
from scenic_exp.runtime.stop_sign import StopSignColorPatchTap
assert os.getcwd() == before
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_patch_reaches_queue_without_mutating_source_or_other_pixels(self):
        tap = self.make_tap()
        frame = self.frame()
        before = frame.copy()
        result = self.interface.update_sensor("front", frame, 42)
        self.assertEqual(result, "queued")
        tag, delivered, timestamp = self.interface.received[-1]
        self.assertEqual((tag, timestamp), ("front", 42))
        self.assertIsNot(delivered, frame)
        np.testing.assert_array_equal(frame, before)
        np.testing.assert_array_equal(delivered[50, 50], [200, 20, 10, 255])
        np.testing.assert_array_equal(delivered[1, 1], before[1, 1])
        np.testing.assert_array_equal(delivered[50, 51], before[50, 51])
        self.assertEqual(tap.frames_patched, 1)
        self.assertEqual(tap.pixels_patched, 1)
        self.assertEqual(tap.meaningful_patched_frames, 1)
        self.assertEqual(tap.closest_patched_distance_m, 10.0)

    def test_baseline_forwards_same_frame_and_records_no_attack(self):
        tap = self.make_tap(enabled=False)
        frame = self.frame()
        self.interface.update_sensor("front", frame, 3)
        self.assertIs(self.interface.received[-1][1], frame)
        self.assertEqual(tap.projected_frames, 0)
        self.assertEqual(tap.frames_patched, 0)
        self.assertEqual(set(tap._latest), {"front"})

    def test_event_callback_gates_patch_at_sensor_ingress(self):
        event = {"active": False}
        tap = self.make_tap(active=lambda: event["active"])
        frame = self.frame()
        self.interface.update_sensor("front", frame, 1)
        self.assertIs(self.interface.received[-1][1], frame)
        event["active"] = True
        self.interface.update_sensor("front", frame, 2)
        self.assertIsNot(self.interface.received[-1][1], frame)
        event["active"] = False
        self.interface.update_sensor("front", frame, 3)
        self.assertIs(self.interface.received[-1][1], frame)
        self.assertEqual(tap.frames_patched, 1)

    def test_enabled_attack_without_rgb_camera_fails_before_wrapping_interface(self):
        self.pcla.agent_instance.sensors = lambda: [{"type": "sensor.other.imu", "id": "imu"}]
        with self.assertRaisesRegex(RuntimeError, "RGB camera"):
            self.make_tap()
        self.assertEqual(self.interface.update_sensor, self.original_update)

    def test_other_sensors_and_invalid_camera_shapes_pass_through(self):
        tap = self.make_tap()
        for tag, data in (("imu", object()), ("front", np.zeros((10, 10)))):
            self.interface.update_sensor(tag, data, 4)
            self.assertIs(self.interface.received[-1][1], data)
        self.assertEqual(tap.frames_patched, 0)

    def test_out_of_range_or_behind_camera_is_unmodified(self):
        for x in (100.0, -10.0):
            with self.subTest(target_x=x):
                tap = self.make_tap(candidate=candidate(x))
                frame = self.frame()
                self.interface.update_sensor("front", frame, 7)
                self.assertIs(self.interface.received[-1][1], frame)
                self.assertEqual(tap.frames_patched, 0)
                tap.close()
        self.assertIsNone(project_stop_sign(
            self.ego, self.spec, location(-10), (100, 100, 4), 0.65, 1.7,
        ))

    def test_snapshots_keep_original_and_delivered_frame_and_cache_encoder(self):
        encoded = []

        def encode(rgb):
            encoded.append(rgb.copy())
            return bytes(rgb[50, 50])

        tap = self.make_tap(image_encoder=encode)
        self.interface.update_sensor("front", self.frame(), 9)
        snapshots = tap.snapshots()
        self.assertEqual(snapshots["front"][1:], (bytes([10, 20, 200]), 9))
        self.assertEqual(snapshots["front__reference"][1:], (bytes([200, 20, 10]), 9))
        self.assertEqual(tap.snapshots(), snapshots)
        self.assertEqual(len(encoded), 2)

    def test_close_restores_callback_and_is_idempotent(self):
        tap = self.make_tap()
        tap.close()
        tap.close()
        self.assertEqual(self.interface.update_sensor, self.original_update)

    def test_close_does_not_remove_a_callback_installed_by_another_owner(self):
        tap = self.make_tap()
        replacement = lambda *args: None
        self.interface.update_sensor = replacement
        tap.close()
        self.assertIs(self.interface.update_sensor, replacement)

    def test_recolor_respects_threshold_dominance_and_alpha(self):
        region = np.array([[
            [0, 0, 89, 1], [0, 80, 90, 2], [100, 0, 90, 3], [0, 0, 90, 4],
        ]], dtype=np.uint8)
        original = region.copy()
        self.assertEqual(StopSignColorPatchTap.recolor_red_bgra(region), 1)
        np.testing.assert_array_equal(region[:, :3], original[:, :3])
        np.testing.assert_array_equal(region[0, 3], [128, 0, 0, 4])


if __name__ == "__main__":
    unittest.main()
