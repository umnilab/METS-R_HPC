"""Compatibility facade for the Scenic experiment runtime.

Use the responsibility modules for new code: config, safety, timing, sensors,
scenario, and reporting. These exports preserve existing scenarios and runners.
Importing this facade does not load CARLA, PCLA, NumPy, or Shapely.
"""
from .config import (
    DEFAULT_TEST_PARAMETERS,
    SAFETY_SCENARIOS,
    SECURITY_SCENARIOS,
    load_test_config,
    resolve_test_parameters,
    validate_test_modes,
)
from .lifecycle import register_pre_actor_teardown
from .reporting import write_test_report
from .safety import _line, _pose, merge_path, path_control, safety_sites
from .scenario import ScenarioRuntime, scenario_test_records
from .sensors import perturb_sensor_batch
from .timing import encounter, event_active, trigger_reached

__all__ = [
    "DEFAULT_TEST_PARAMETERS",
    "SAFETY_SCENARIOS",
    "SECURITY_SCENARIOS",
    "ScenarioRuntime",
    "encounter",
    "event_active",
    "load_test_config",
    "merge_path",
    "path_control",
    "perturb_sensor_batch",
    "register_pre_actor_teardown",
    "resolve_test_parameters",
    "safety_sites",
    "scenario_test_records",
    "trigger_reached",
    "validate_test_modes",
    "write_test_report",
    "_line",
    "_pose",
]
