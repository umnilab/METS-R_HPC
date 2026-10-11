"""Optional real Scenic compilation/behavior checks; no simulator servers needed.

Run in the PCLA environment with CARLA's Python API and the METSRSim Scenic
fork installed. These checks deliberately skip when those dependencies are absent.
"""
import importlib.util
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ROOT / "scenic_exp" / "scenarios"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scenic_exp.runtime.integration import install_runtime_hook


def _scenic_dependencies_available():
    try:
        return all(importlib.util.find_spec(name) is not None for name in
                   ("carla", "scenic", "scenic.simulators.cosim"))
    except (ImportError, ValueError):
        return False


@unittest.skipUnless(_scenic_dependencies_available(),
                     "Requires CARLA Python API and the METSRSim Scenic co-simulation fork")
class ScenicAdapterTests(unittest.TestCase):
    def setUp(self):
        original_cwd = os.getcwd()
        self.addCleanup(os.chdir, original_cwd)

    def compile(self, filename="traffic_template.scenic", **overrides):
        import scenic
        from scenic.core.utils import setSeed

        params = dict(num_commuters=0, length=.3, timestep=.1, spawn_interval_s=.1,
                      allow_bubble_spawns=True)
        params.update(overrides)
        setSeed(33)
        return scenic.scenarioFromFile(str(SCENARIOS / filename), mode2D=True, params=params)

    def scene(self, scenario):
        from scenic.core.utils import setSeed

        setSeed(33)
        scene, _ = scenario.generate(maxIterations=200)
        return scene

    def test_real_external_behavior_attaches_before_steps_and_controls_afterward(self):
        from scenic.core.simulators import DummySimulation, DummySimulator

        events = []

        class Runtime:
            simulation = None
            closed = False

            def attach(self, simulation):
                self.simulation = simulation
                events.append("attach")

            def before_step(self):
                if self.closed:
                    raise RuntimeError("Runtime closed before stepping")
                events.append("before")

            def apply_control(self):
                events.append("control")

            def after_step(self):
                events.append("after")

            def close(self):
                if not self.closed:
                    self.closed = True
                    events.append("close")

        class ObservedSimulation(DummySimulation):
            def step(self):
                events.append("simulation start")
                result = super().step()
                events.append("simulation end")
                return result

            def destroy(self):
                events.append("destroy actors")
                return super().destroy()

        class ObservedSimulator(DummySimulator):
            def createSimulation(self, scene, **kwargs):
                return ObservedSimulation(scene, **kwargs)

        scenario = self.compile(ego_controller="external", security_scenario="none")
        runtime = Runtime()
        simulator = ObservedSimulator()
        uninstall = install_runtime_hook(simulator, SimpleNamespace(current=runtime))
        self.addCleanup(uninstall)
        simulation = simulator.simulate(self.scene(scenario), maxSteps=10,
                                        timestep=scenario.params["timestep"], verbosity=0)
        self.assertIsNotNone(simulation)
        self.assertIs(runtime.simulation, simulation)
        steps = events.count("simulation start")
        self.assertGreater(steps, 0)
        self.assertEqual(events, ["attach"] +
                         ["before", "simulation start", "simulation end", "control", "after"] * steps +
                         ["close", "destroy actors"])
        self.assertTrue(runtime.closed)
        self.assertNotIn("step", vars(simulation))

    def test_real_external_behavior_reports_missing_runtime_provider(self):
        from scenic.core.simulators import DummySimulator

        scenario = self.compile(ego_controller="external", security_scenario="none")
        with self.assertRaisesRegex(RuntimeError, "requires an attached experiment runtime"):
            DummySimulator().simulate(self.scene(scenario), maxSteps=10,
                                      timestep=scenario.params["timestep"], verbosity=0)

    def test_stop_preset_carries_selected_index_flag_and_schedule_without_pcla(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), patch.dict(sys.modules, {"PCLA": None}):
                scenario = self.compile("town05_stop_sign_patch.scenic",
                                        attack_stop_index=4, attack_enabled=enabled)
                self.assertEqual(scenario.params["ego_controller"], "external")
                self.assertEqual(scenario.params["security_scenario"], "stop_sign_patch")
                config = scenario.params["test_parameters"]
                self.assertEqual(config["stop_sign"]["index"], 4)
                self.assertIs(config["stop_sign"]["enabled"], enabled)
                self.assertEqual(config["trigger"]["start_s"], 0)
                self.assertAlmostEqual(config["event"]["duration_s"], .4)

    def test_native_merge_and_stop_profile_generates_without_loading_pcla(self):
        with patch.dict(sys.modules, {"PCLA": None}):
            scenario = self.compile(town="Town05", ego_controller="pcla",
                                    safety_scenario="merge", security_scenario="stop_sign_patch",
                                    test_parameters={"stop_sign": {"index": 2, "enabled": True},
                                                     "trigger": {"mode": "distance"}})
            scene = self.scene(scenario)
        self.assertEqual(scenario.params["safety_scenario"], "merge")
        self.assertEqual(scenario.params["security_scenario"], "stop_sign_patch")
        self.assertEqual(scenario.params["test_parameters"]["stop_sign"]["index"], 2)
        self.assertEqual({obj.name for obj in scene.objects}, {"ego", "hazard_0"})
        self.assertIsNotNone(scene.objects[0].test_site_id)


if __name__ == "__main__":
    unittest.main()
