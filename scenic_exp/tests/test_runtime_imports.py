"""Compatibility and dependency boundaries after separating runtime responsibilities."""
from pathlib import Path
import subprocess
import sys
import unittest

from scenic_exp.runtime import config, reporting, safety, scenario, sensors, testing, timing


class RuntimeImportTests(unittest.TestCase):
    def test_facade_keeps_the_canonical_objects_for_existing_callers_and_patches(self):
        exports = {
            config: ("DEFAULT_TEST_PARAMETERS", "SAFETY_SCENARIOS", "SECURITY_SCENARIOS",
                     "resolve_test_parameters", "validate_test_modes", "load_test_config"),
            safety: ("_line", "_pose", "safety_sites", "merge_path", "path_control"),
            timing: ("encounter", "trigger_reached", "event_active"),
            sensors: ("perturb_sensor_batch",),
            scenario: ("ScenarioRuntime", "scenario_test_records"),
            reporting: ("write_test_report",),
        }
        for module, names in exports.items():
            for name in names:
                with self.subTest(module=module.__name__, name=name):
                    self.assertIs(getattr(testing, name), getattr(module, name))

    def test_configuration_and_runtime_imports_need_no_optional_dependencies(self):
        script = """
import importlib.abc
import os
import sys

class NoOptionalImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split('.')[0] in {'numpy', 'shapely', 'scenic', 'carla', 'PCLA', 'PIL', 'tutorials'}:
            raise AssertionError('Unexpected optional import: ' + fullname)

sys.meta_path.insert(0, NoOptionalImports())
before = os.getcwd()
from scenic_exp.runtime import config, reporting, safety, scenario, sensors, testing, timing
from utils import scenic_testing
assert config.resolve_test_parameters()['stop_sign']['enabled'] is True
assert scenario.ScenarioRuntime is testing.ScenarioRuntime is scenic_testing.ScenarioRuntime
assert os.getcwd() == before
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
