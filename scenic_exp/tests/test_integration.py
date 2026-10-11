"""Exercise the Scenic/controller boundary without a Scenic or CARLA install."""

import unittest
from types import SimpleNamespace

from scenic_exp.runtime.integration import (
    attach_runtime,
    install_runtime_hook,
    require_external_runtime,
)


class FakeRuntime:
    def __init__(self, events, fail_at=None):
        self.events = events
        self.fail_at = fail_at
        self.simulation = None
        self.closed = False

    def attach(self, simulation):
        self.simulation = simulation
        self.events.append("attach")
        if self.fail_at == "attach":
            raise RuntimeError("setup failed")

    def before_step(self):
        if self.closed:
            raise RuntimeError("runtime was closed before stepping")
        self.events.append("before")
        if self.fail_at == "before":
            raise RuntimeError("trigger failed")

    def apply_control(self):
        self.events.append("control")
        if self.fail_at == "control":
            raise RuntimeError("inference failed")

    def after_step(self):
        self.events.append("after")

    def close(self):
        if not self.closed:
            self.closed = True
            self.events.append("close")


class FakeSimulation:
    def __init__(self, events, *, run_in_constructor=True, destroy_on_failure=True):
        self.events = events
        self.destroy_on_failure = destroy_on_failure
        self.results = []
        if run_in_constructor:
            self.run()

    def run(self):
        try:
            # This is the call made by ExternalControlBehavior before its yield.
            require_external_runtime(self)
            self.results = [self.step(1, marker="first"), self.step(2, marker="second")]
        except BaseException:
            if self.destroy_on_failure:
                self.destroy()
            raise
        else:
            self.destroy()

    def step(self, tick, *, marker):
        self.events.append("cosim")
        return tick, marker

    def destroy(self):
        self.events.append("destroy actors")


class FakeSimulator:
    def __init__(self, events, **options):
        self.events = events
        self.options = options
        self.last_simulation = None

    def createSimulation(self, scene, **kwargs):
        # Retain the object even if its constructor throws, for cleanup checks.
        simulation = FakeSimulation.__new__(FakeSimulation)
        self.last_simulation = simulation
        simulation.__init__(self.events, **self.options, **kwargs)
        return simulation


class RuntimeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.runtime = FakeRuntime(self.events)
        self.holder = SimpleNamespace(current=self.runtime)

    def install(self, simulator):
        uninstall = install_runtime_hook(simulator, self.holder)
        self.addCleanup(uninstall)
        return uninstall

    def assert_no_pending_runtime(self):
        other = FakeSimulation([], run_in_constructor=False)
        with self.assertRaisesRegex(RuntimeError, "requires an attached experiment runtime"):
            require_external_runtime(other)

    def test_constructor_run_attaches_before_first_step_and_cleans_before_actors(self):
        simulator = FakeSimulator(self.events)
        self.install(simulator)
        simulation = simulator.createSimulation(object())
        self.assertEqual(self.events, [
            "attach", "before", "cosim", "control", "after",
            "before", "cosim", "control", "after", "close", "destroy actors",
        ])
        self.assertEqual(simulation.results, [(1, "first"), (2, "second")])
        self.assertNotIn("step", vars(simulation))
        self.assertNotIn("destroy", vars(simulation))
        self.assert_no_pending_runtime()

    def test_provider_changes_only_the_selected_simulator_instance(self):
        simulator = FakeSimulator(self.events)
        other = FakeSimulator([])
        original_factory = FakeSimulator.createSimulation
        original_step = FakeSimulation.step
        uninstall = self.install(simulator)
        self.assertIs(FakeSimulator.createSimulation, original_factory)
        self.assertIs(FakeSimulation.step, original_step)
        self.assertNotIn("createSimulation", vars(other))
        simulator.createSimulation(object())
        self.assertIs(FakeSimulation.step, original_step)
        uninstall()
        uninstall()
        self.assertNotIn("createSimulation", vars(simulator))
        self.assertNotIn("_metsr_runtime_installer", vars(simulator))

    def test_uninstall_restores_an_existing_instance_factory(self):
        simulator = FakeSimulator(self.events)
        factory = simulator.createSimulation
        simulator.createSimulation = factory
        uninstall = self.install(simulator)
        uninstall()
        self.assertIs(simulator.createSimulation, factory)

    def test_uninstall_preserves_a_new_factory_installed_by_another_owner(self):
        simulator = FakeSimulator(self.events)
        uninstall = self.install(simulator)
        replacement = lambda *args: None
        simulator.createSimulation = replacement
        uninstall()
        self.assertIs(simulator.createSimulation, replacement)

    def test_context_is_cleared_between_runs_with_new_runtimes(self):
        simulator = FakeSimulator(self.events)
        self.install(simulator)
        first = simulator.createSimulation(object())
        self.assert_no_pending_runtime()
        second_runtime = FakeRuntime(self.events)
        self.holder.current = second_runtime
        second = simulator.createSimulation(object())
        self.assertIsNot(first, second)
        self.assertIs(self.runtime.simulation, first)
        self.assertIs(second_runtime.simulation, second)
        self.assertEqual(self.events.count("close"), 2)
        self.assert_no_pending_runtime()

    def test_missing_provider_fails_instead_of_leaving_external_ego_idle(self):
        self.assert_no_pending_runtime()
        simulator = FakeSimulator(self.events)
        self.install(simulator)
        self.holder.current = None
        with self.assertRaisesRegex(RuntimeError, "holder.current"):
            simulator.createSimulation(object())
        self.assertIsNone(simulator.last_simulation)
        self.assertEqual(self.events, [])

    def test_duplicate_provider_is_rejected(self):
        simulator = FakeSimulator(self.events)
        self.install(simulator)
        with self.assertRaisesRegex(RuntimeError, "already installed"):
            install_runtime_hook(simulator, self.holder)

    def test_setup_failure_closes_runtime_and_does_not_leave_context_or_step_hook(self):
        self.runtime.fail_at = "attach"
        simulator = FakeSimulator(self.events, destroy_on_failure=False)
        self.install(simulator)
        with self.assertRaisesRegex(RuntimeError, "setup failed"):
            simulator.createSimulation(object())
        self.assertEqual(self.events, ["attach", "close"])
        self.assertNotIn("step", vars(simulator.last_simulation))
        self.assertNotIn("_metsr_experiment_runtime", vars(simulator.last_simulation))
        self.assert_no_pending_runtime()

    def test_factory_failure_before_attachment_still_closes_runtime_and_context(self):
        simulator = FakeSimulator(self.events)

        def fail_factory(scene):
            raise RuntimeError("CARLA setup failed")

        simulator.createSimulation = fail_factory
        self.install(simulator)
        with self.assertRaisesRegex(RuntimeError, "CARLA setup failed"):
            simulator.createSimulation(object())
        self.assertTrue(self.runtime.closed)
        self.assertIsNone(self.runtime.simulation)
        self.assert_no_pending_runtime()

    def test_cleanup_error_preserves_original_inference_failure(self):
        simulation = FakeSimulation(self.events, run_in_constructor=False)
        self.runtime.fail_at = "control"

        def fail_close():
            raise RuntimeError("cleanup failed")

        self.runtime.close = fail_close
        attach_runtime(simulation, self.runtime)
        with self.assertRaisesRegex(RuntimeError, "inference failed"):
            simulation.step(1, marker="first")
        self.assertNotIn("step", vars(simulation))
        self.assertNotIn("_metsr_experiment_runtime", vars(simulation))

    def test_inference_failure_cleans_before_normal_simulator_teardown(self):
        self.runtime.fail_at = "control"
        simulator = FakeSimulator(self.events)
        self.install(simulator)
        with self.assertRaisesRegex(RuntimeError, "inference failed"):
            simulator.createSimulation(object())
        self.assertEqual(self.events, [
            "attach", "before", "cosim", "control", "close", "destroy actors",
        ])
        self.assertNotIn("step", vars(simulator.last_simulation))
        self.assert_no_pending_runtime()

    def test_constructor_failure_without_teardown_restores_step_hook(self):
        self.runtime.fail_at = "control"
        simulator = FakeSimulator(self.events, destroy_on_failure=False)
        self.install(simulator)
        with self.assertRaisesRegex(RuntimeError, "inference failed"):
            simulator.createSimulation(object())
        self.assertTrue(self.runtime.closed)
        self.assertNotIn("step", vars(simulator.last_simulation))
        self.assert_no_pending_runtime()

    def test_direct_attachment_failure_cleans_runtime_without_factory_help(self):
        simulation = FakeSimulation(self.events, run_in_constructor=False)
        self.runtime.fail_at = "before"
        attach_runtime(simulation, self.runtime)
        with self.assertRaisesRegex(RuntimeError, "trigger failed"):
            simulation.step(1, marker="first")
        self.assertTrue(self.runtime.closed)
        self.assertNotIn("step", vars(simulation))
        simulation.destroy()
        self.assertEqual(self.events[-2:], ["close", "destroy actors"])

    def test_returned_simulation_can_start_after_factory_returns(self):
        simulator = FakeSimulator(self.events, run_in_constructor=False)
        self.install(simulator)
        simulation = simulator.createSimulation(object())
        self.assertFalse(self.runtime.closed)
        self.assert_no_pending_runtime()
        simulation.run()
        self.assertEqual(simulation.results, [(1, "first"), (2, "second")])
        self.assertTrue(self.runtime.closed)

    def test_direct_attachment_is_idempotent_and_restores_existing_step_override(self):
        simulation = FakeSimulation(self.events, run_in_constructor=False)
        previous_step = simulation.step
        simulation.step = previous_step
        attach_runtime(simulation, self.runtime)
        wrapped = simulation.step
        self.assertIs(require_external_runtime(simulation), self.runtime)
        attach_runtime(simulation, self.runtime)
        self.assertIs(simulation.step, wrapped)
        simulation.step(1, marker="only")
        simulation.destroy()
        self.assertIs(simulation.step, previous_step)
        self.assertEqual(self.events.count("attach"), 1)
        self.assertEqual(self.events.count("control"), 1)

    def test_same_runtime_cannot_be_reused_for_another_simulation(self):
        first = FakeSimulation(self.events, run_in_constructor=False)
        second = FakeSimulation(self.events, run_in_constructor=False)
        attach_runtime(first, self.runtime)
        self.addCleanup(first.destroy)
        with self.assertRaisesRegex(RuntimeError, "fresh experiment runtime"):
            attach_runtime(second, self.runtime)
        self.assertNotIn("step", vars(second))


if __name__ == "__main__":
    unittest.main()
