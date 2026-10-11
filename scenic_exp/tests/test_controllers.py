"""Offline checks for the reusable PCLA adapter's ownership contract."""

from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scenic_exp.runtime import controllers


class PCLAControllerTests(unittest.TestCase):
    def make_controller(self, *, tap_factory=None):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        route = Path(temporary.name) / "route.xml"
        route.write_text("<routes/>", encoding="utf-8")
        actor = Mock()
        pcla = SimpleNamespace(get_action=Mock(return_value=object()), cleanup=Mock())
        args = SimpleNamespace(pcla_dir=None, pcla_route=str(route), pcla_agent="test")
        module = SimpleNamespace(PCLA=Mock(return_value=pcla))
        with patch.object(controllers, "_import_pcla", return_value=module):
            controller = controllers.PCLAController(
                args,
                SimpleNamespace(VehicleControl=Mock(return_value=object())),
                object(),
                actor,
                tap_factory=tap_factory,
            )
        self.addCleanup(controller.close)
        actor.apply_control.reset_mock()  # Ignore the setup brake command.
        return controller, actor, pcla

    def test_module_import_needs_no_optional_dependencies_or_dashboard(self):
        root = Path(__file__).resolve().parents[2]
        script = """
import os
import sys
before = os.getcwd()
from scenic_exp.runtime.controllers import PCLAController
assert os.getcwd() == before
for module in ('PCLA', 'carla', 'numpy', 'omegaconf'):
    assert module not in sys.modules, module
assert not any(name.startswith('tutorials.') for name in sys.modules)
"""
        result = subprocess.run(
            [sys.executable, "-S", "-c", script], cwd=root,
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_repeated_tick_only_infers_and_applies_once(self):
        controller, actor, pcla = self.make_controller()
        first = controller.step(tick=0)
        self.assertIs(controller.step(tick=0), first)
        pcla.get_action.assert_called_once_with()
        actor.apply_control.assert_called_once_with(first)
        controller.step(tick=1)
        self.assertEqual(pcla.get_action.call_count, 2)
        self.assertEqual(actor.apply_control.call_count, 2)

    def test_scenic_get_action_does_not_apply_control(self):
        controller, actor, pcla = self.make_controller()
        control = controller.get_action()
        pcla.get_action.assert_called_once_with()
        self.assertIs(control, pcla.get_action.return_value)
        actor.apply_control.assert_not_called()
        self.assertEqual(controller.camera_snapshots(), {})

    def test_default_cleanup_preserves_vehicle_and_is_idempotent(self):
        events = []
        tap = SimpleNamespace(close=Mock(side_effect=lambda: events.append("tap")))
        controller, actor, pcla = self.make_controller(tap_factory=lambda _: tap)
        pcla.cleanup.side_effect = lambda **kwargs: events.append("sensors")
        controller.close()
        controller.close()
        self.assertEqual(events, ["tap", "sensors"])
        pcla.cleanup.assert_called_once_with(destroy_vehicle=False)
        actor.destroy.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            controller.step(tick=0)

    def test_tap_cleanup_failure_still_cleans_pcla_sensors(self):
        tap = SimpleNamespace(close=Mock(side_effect=RuntimeError("tap failure")))
        controller, _, pcla = self.make_controller(tap_factory=lambda _: tap)
        with patch("builtins.print"):
            controller.close()
        pcla.cleanup.assert_called_once_with(destroy_vehicle=False)

    def test_tap_install_failure_cleans_sensors_before_reraising(self):
        with tempfile.TemporaryDirectory() as directory:
            route = Path(directory) / "route.xml"
            route.write_text("<routes/>", encoding="utf-8")
            pcla = SimpleNamespace(cleanup=Mock())
            args = SimpleNamespace(pcla_dir=None, pcla_route=str(route), pcla_agent="test")
            with patch.object(controllers, "_import_pcla", return_value=SimpleNamespace(PCLA=Mock(return_value=pcla))):
                with self.assertRaisesRegex(RuntimeError, "cannot install"):
                    controllers.PCLAController(
                        args, SimpleNamespace(VehicleControl=Mock()), object(), Mock(),
                        tap_factory=Mock(side_effect=RuntimeError("cannot install")),
                    )
            pcla.cleanup.assert_called_once_with(destroy_vehicle=False)

    def test_pcla_loader_restores_callers_utils_modules(self):
        repo_utils = ModuleType("utils")
        repo_helper = ModuleType("utils.helper")
        alien_utils = ModuleType("utils")

        def load_pcla(*_):
            sys.modules.pop("utils.helper", None)
            sys.modules["utils"] = alien_utils
            sys.modules["utils.pcla_only"] = ModuleType("utils.pcla_only")
            return SimpleNamespace(cleanup=Mock())

        with tempfile.TemporaryDirectory() as directory:
            route = Path(directory) / "route.xml"
            route.write_text("<routes/>", encoding="utf-8")
            args = SimpleNamespace(pcla_dir=None, pcla_route=str(route), pcla_agent="test")
            with patch.dict(sys.modules, {"utils": repo_utils, "utils.helper": repo_helper}):
                with patch.object(controllers, "_import_pcla", return_value=SimpleNamespace(PCLA=load_pcla)):
                    controller = controllers.PCLAController(
                        args, SimpleNamespace(VehicleControl=Mock()), object(), Mock()
                    )
                self.addCleanup(controller.close)
                self.assertIs(sys.modules["utils"], repo_utils)
                self.assertIs(sys.modules["utils.helper"], repo_helper)
                self.assertNotIn("utils.pcla_only", sys.modules)


class NativeScenicControllerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.route_output = Path(temporary.name) / "routes" / "native.xml"
        self.actor = Mock()
        self.ego = SimpleNamespace(carlaActor=self.actor, route=["road-a", "road-b"])
        self.simulation = SimpleNamespace(
            carla_client=object(),
            generate_carla_trajectory=Mock(),
            map=SimpleNamespace(get_waypoint=Mock()),
            step=Mock(),
        )
        self.policy = object()
        self.construct = Mock(return_value=self.policy)
        self.route_maker = Mock()
        pcla = ModuleType("PCLA")
        pcla.PCLA = self.construct
        functions = ModuleType("pcla_functions")
        functions.__path__ = []
        route_maker = ModuleType("pcla_functions.route_maker")
        route_maker.route_maker = self.route_maker
        modules = patch.dict(sys.modules, {
            "PCLA": pcla,
            "pcla_functions": functions,
            "pcla_functions.route_maker": route_maker,
        })
        modules.start()
        self.addCleanup(modules.stop)
        environment = patch.dict(controllers.os.environ, {"PCLA_HOME": ""})
        environment.start()
        self.addCleanup(environment.stop)

    def create(self, route=None):
        return controllers.create_scenic_pcla(
            self.simulation, self.ego, "test-agent", route, self.route_output,
        )

    def test_explicit_route_constructs_policy_without_changing_actor_or_stepping(self):
        route = Path("provided-route.xml")
        self.assertIs(self.create(route), self.policy)
        self.construct.assert_called_once_with(
            "test-agent", self.actor, str(route), self.simulation.carla_client,
        )
        self.route_maker.assert_not_called()
        self.simulation.generate_carla_trajectory.assert_not_called()
        self.assertEqual(self.actor.method_calls, [])
        self.simulation.step.assert_not_called()

    def test_generated_route_uses_authoritative_metsr_path_and_filters_unmapped_points(self):
        locations = [object(), object(), object()]
        first, last = object(), object()
        self.simulation.generate_carla_trajectory.return_value = locations
        self.simulation.map.get_waypoint.side_effect = [first, None, last]
        self.assertIs(self.create(), self.policy)
        self.simulation.generate_carla_trajectory.assert_called_once_with(
            route=self.ego.route, obj=self.ego,
        )
        self.assertEqual([call.args[0] for call in self.simulation.map.get_waypoint.call_args_list], locations)
        self.route_maker.assert_called_once_with([first, last], savePath=str(self.route_output))
        self.assertTrue(self.route_output.parent.is_dir())
        self.construct.assert_called_once_with(
            "test-agent", self.actor, str(self.route_output), self.simulation.carla_client,
        )
        self.assertEqual(self.actor.method_calls, [])
        self.simulation.step.assert_not_called()

    def test_missing_authoritative_route_fails_without_creating_policy(self):
        self.ego.route = None
        with self.assertRaisesRegex(RuntimeError, "Missing authoritative METS-R ego route"):
            self.create()
        self.simulation.generate_carla_trajectory.assert_not_called()
        self.route_maker.assert_not_called()
        self.construct.assert_not_called()

    def test_insufficient_mapped_waypoints_fails_without_writing_route(self):
        self.simulation.generate_carla_trajectory.return_value = [object(), object()]
        self.simulation.map.get_waypoint.side_effect = [None, object()]
        with self.assertRaisesRegex(RuntimeError, "at least two road waypoints"):
            self.create()
        self.route_maker.assert_not_called()
        self.construct.assert_not_called()

    def test_missing_carla_actor_fails_without_creating_policy(self):
        self.ego.carlaActor = None
        with self.assertRaisesRegex(RuntimeError, "CARLA actor"):
            self.create("provided-route.xml")
        self.construct.assert_not_called()

    def test_constructor_failure_restores_callers_utils_modules(self):
        repo_utils = ModuleType("utils")
        helper = ModuleType("utils.helper")

        def fail_load(*_):
            sys.modules.pop("utils.helper", None)
            sys.modules["utils"] = ModuleType("utils")
            sys.modules["utils.pcla_only"] = ModuleType("utils.pcla_only")
            raise RuntimeError("model failed to load")

        self.construct.side_effect = fail_load
        with patch.dict(sys.modules, {"utils": repo_utils, "utils.helper": helper}):
            with self.assertRaisesRegex(RuntimeError, "model failed to load"):
                self.create("provided-route.xml")
            self.assertIs(sys.modules["utils"], repo_utils)
            self.assertIs(sys.modules["utils.helper"], helper)
            self.assertNotIn("utils.pcla_only", sys.modules)


if __name__ == "__main__":
    unittest.main()
