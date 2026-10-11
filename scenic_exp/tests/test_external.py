"""External stop-sign trials exercise the same effects as native Scenic control."""

from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

import numpy as np

from scenic_exp.runtime import external
from scenic_exp.runtime.controllers import PCLAController
from scenic_exp.runtime.integration import install_runtime_hook
from scenic_exp.runtime.stop_sign import StopSignCandidate


def location(x=0.0, y=0.0, z=0.0):
    return NS(x=x, y=y, z=z)


class FakeSimulation:
    def __init__(self, ego, interface, events):
        self.ego = ego
        self.objects = [ego]
        self.carla_client = object()
        self.currentTime = 0
        self.timestep = 0.05
        self.callbacks = []
        self.interface = interface
        self.events = events
        self.destroyed = False
        self.frames = []

    def add_pre_actor_teardown_callback(self, callback):
        self.callbacks.append(callback)

    def step(self):
        self.events.append(("cosim", self.ego.test_runtime.active))
        data = np.zeros((100, 100, 4), dtype=np.uint8)
        data[:, :, 3] = 255
        data[50, 50, :3] = [10, 20, 200]
        self.frames.append(data)
        self.interface.update_sensor("front", data, self.currentTime)
        self.currentTime += 1
        return "advanced"

    def destroy(self):
        for callback in list(self.callbacks):
            callback()
        self.events.append("destroy actor")
        self.destroyed = True


class ExternalRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.args = NS(
            pcla_dir=None, pcla_route="route.xml", pcla_agent="test",
            route_output_dir="routes", route_min_distance_m=250,
            length=5, timestep=0.05, attack_max_distance_m=90,
            attack_min_red=90, attack_red_dominance=1.25,
            attack_roi_scale=1.7, attack_min_patch_pixels=1,
        )
        transform = NS(location=location(), rotation=NS(roll=0, pitch=0, yaw=0))
        self.actor = NS(
            get_location=lambda: location(), get_transform=lambda: transform,
            disable_constant_velocity=Mock(),
            apply_control=Mock(side_effect=lambda _: self.events.append("control")),
        )
        self.ego = NS(carlaActor=self.actor, name="ego")
        self.interface = NS(update_sensor=Mock())
        self.original_update = self.interface.update_sensor
        agent = NS(sensor_interface=self.interface, sensors=lambda: [
            {"id": "front", "type": "sensor.camera.rgb", "fov": 90.0},
        ])
        self.pcla = NS(
            agent_instance=agent,
            get_action=Mock(side_effect=self.infer),
            cleanup=Mock(side_effect=self.cleanup_sensors),
        )
        self.controller = object.__new__(PCLAController)
        self.controller.pcla = self.pcla
        self.controller.ego_actor = self.actor
        self.controller.tap = None
        self.controller.last_control = None
        self.controller._last_tick = None
        self.target = StopSignCandidate(
            index=2, actor_id="stop", route_location=location(10),
            visual_location=location(10), visual_extent_m=0.65,
            source="test", approach_transform=transform, approach_distance_m=10,
            same_road_distance_m=10, road_id=1, route_is_junction=False,
        )
        self.spec = NS(candidate=self.target, run_number=1, phase="attack", attack_enabled=True)
        self.simulation = FakeSimulation(self.ego, self.interface, self.events)
        self.factory = Mock(return_value=self.controller)
        constructor = patch.object(external, "PCLAController", self.factory)
        constructor.start()
        self.addCleanup(constructor.stop)
        sdk = patch.object(external.StopSignControllerRuntime, "_carla_module", return_value=object())
        sdk.start()
        self.addCleanup(sdk.stop)

    def infer(self):
        self.events.append("inference")
        return NS(throttle=0.5, brake=0, steer=0)

    def cleanup_sensors(self, **kwargs):
        self.assertFalse(self.simulation.destroyed)
        self.assertIs(self.interface.update_sensor, self.original_update)
        self.events.append("cleanup sensors")

    def install(self, runtime=None):
        runtime = runtime or external.StopSignControllerRuntime(self.args, self.spec)
        simulator = NS(createSimulation=lambda scene: self.simulation)
        uninstall = install_runtime_hook(simulator, NS(current=runtime))
        self.addCleanup(uninstall)
        self.addCleanup(runtime.close)
        simulator.createSimulation(object())
        return runtime

    def test_controller_setup_once_and_effect_precedes_cosim_and_inference(self):
        runtime = self.install()
        self.assertEqual(self.simulation.step(), "advanced")
        self.simulation.step()
        self.factory.assert_called_once()
        self.assertEqual(self.events, [
            ("cosim", True), "inference", "control",
            ("cosim", True), "inference", "control",
        ])
        self.assertEqual(self.pcla.get_action.call_count, 2)
        self.assertEqual(self.actor.apply_control.call_count, 2)
        self.assertEqual(runtime.tap.frames_patched, 2)
        _, delivered, timestamp = self.original_update.call_args.args
        self.assertEqual(timestamp, 1)
        np.testing.assert_array_equal(delivered[50, 50], [200, 20, 10, 255])
        np.testing.assert_array_equal(self.simulation.frames[-1][50, 50], [10, 20, 200, 255])
        self.assertIs(self.ego.test_runtime, runtime.effects)

    def test_cleanup_restores_input_then_sensors_before_vehicle_destruction(self):
        runtime = self.install()
        self.simulation.step()
        self.simulation.destroy()
        runtime.close()
        self.assertEqual(self.events[-2:], ["cleanup sensors", "destroy actor"])
        self.pcla.cleanup.assert_called_once_with(destroy_vehicle=False)
        self.assertIs(self.interface.update_sensor, self.original_update)
        self.assertNotIn("step", vars(self.simulation))

    def test_baseline_forwards_original_camera_data(self):
        self.spec.attack_enabled = False
        self.spec.phase = "baseline"
        runtime = self.install()
        self.simulation.step()
        self.assertEqual(self.events[0], ("cosim", False))
        self.assertEqual(runtime.tap.frames_patched, 0)
        self.assertIs(self.original_update.call_args.args[1], self.simulation.frames[0])
        self.assertFalse(runtime.effects.snapshot()["security_active"])
        self.actor.apply_control.assert_called_once()

    def test_supplied_scenic_config_controls_effect_instead_of_cli_defaults(self):
        config = external.stop_sign_test_parameters(self.args, index=self.target.index)
        config["stop_sign"]["minimum_red"] = 240  # The image's 200 red now stays unchanged.
        config["trigger"]["start_s"] = 0.05
        runtime = self.install(external.StopSignControllerRuntime(
            self.args, self.spec, test_parameters=config,
        ))
        self.simulation.step()
        self.simulation.step()
        self.assertEqual(runtime.effects.config, config)
        self.assertEqual([event for event in self.events if isinstance(event, tuple)],
                         [("cosim", False), ("cosim", True)])
        self.assertEqual(runtime.tap.frames_patched, 0)
        self.assertEqual(runtime.tap.projected_frames, 1)

    def test_trial_configuration_mismatch_fails_before_loading_controller(self):
        config = external.stop_sign_test_parameters(self.args, index=999)
        runtime = self.install(external.StopSignControllerRuntime(
            self.args, self.spec, test_parameters=config,
        ))
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.simulation.step()
        self.assertTrue(runtime.closed)
        self.assertNotIn("step", vars(self.simulation))
        self.factory.assert_not_called()
        self.pcla.cleanup.assert_not_called()
        self.pcla.get_action.assert_not_called()
        self.actor.apply_control.assert_not_called()

    def test_hazard_trigger_fails_before_loading_pcla_or_generating_route(self):
        self.args.pcla_route = None
        config = external.stop_sign_test_parameters(self.args, index=self.target.index)
        config["trigger"]["mode"] = "distance"
        runtime = self.install(external.StopSignControllerRuntime(
            self.args, self.spec, test_parameters=config,
        ))
        with patch.object(external, "_import_pcla") as importer:
            with patch.object(external, "generate_route_via_stop") as route_maker:
                with self.assertRaisesRegex(ValueError, "use a time trigger"):
                    self.simulation.step()
        importer.assert_not_called()
        route_maker.assert_not_called()
        self.factory.assert_not_called()
        self.assertTrue(runtime.closed)
        self.assertNotIn("step", vars(self.simulation))

    def test_failed_effect_bind_cleans_controller_before_reraising(self):
        runtime = self.install()
        with patch.object(external.ScenarioRuntime, "bind_pcla", side_effect=RuntimeError("missing RGB sensor")):
            with self.assertRaisesRegex(RuntimeError, "missing RGB sensor"):
                self.simulation.step()
        self.assertTrue(runtime.closed)
        self.assertTrue(runtime.effects.closed)
        self.pcla.cleanup.assert_called_once_with(destroy_vehicle=False)
        self.pcla.get_action.assert_not_called()
        self.assertNotIn("step", vars(self.simulation))

    def test_generated_route_targets_selected_stop_once(self):
        self.args.pcla_route = None
        runtime = self.install()
        pcla_module = object()
        with patch.object(external, "_import_pcla", return_value=pcla_module):
            with patch.object(external, "generate_route_via_stop", return_value=Path("generated.xml")) as route_maker:
                self.simulation.step()
                self.simulation.step()
        route_maker.assert_called_once_with(
            pcla_module, self.simulation.carla_client, self.actor, self.target,
            Path("routes/run_01_stop_2_attack.xml"), 250,
        )
        self.assertEqual(runtime.route_path, Path("generated.xml"))
        controller_args = self.factory.call_args.args[0]
        self.assertEqual(controller_args.pcla_route, "generated.xml")

    def test_demo_compilation_and_runtime_share_one_resolved_configuration(self):
        from tutorials.dashboard_demos import demo4

        args = demo4.parse_args(["--attack-min-red", "150", "--attack-roi-scale", "2.5"])
        scenic = NS(scenarioFromFile=Mock())
        demo4._compile_scenario(args, scenic, Mock(), Mock(), {
            "attack_stop_index": self.target.index, "attack_enabled": True,
        })
        params = scenic.scenarioFromFile.call_args.kwargs["params"]
        self.assertEqual(params["ego_controller"], "external")
        self.assertEqual(params["security_scenario"], "stop_sign_patch")
        self.assertEqual(params["test_parameters"], external.stop_sign_test_parameters(
            args, index=self.target.index, enabled=True,
        ))
        dashboard = Mock()
        runtime = demo4.RunRuntime(
            self.args, self.spec, dashboard, lambda: "pending",
            test_parameters=params["test_parameters"],
        )
        self.addCleanup(runtime.close)
        runtime.attach(self.simulation)
        runtime.before_step()
        runtime.before_step()
        dashboard.configure_camera_views.assert_called_once_with(runtime.tap.camera_specs)
        self.assertEqual(runtime.effects.config, params["test_parameters"])


if __name__ == "__main__":
    unittest.main()
