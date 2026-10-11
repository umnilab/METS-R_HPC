"""Configuration defaults, validation, and JSON profiles for Scenic experiments."""
from __future__ import annotations

from copy import deepcopy
import json
import math
from pathlib import Path
from typing import Any

TestParameters = dict[str, dict[str, Any]]

SAFETY_SCENARIOS = (
    "none", "vehicle_interception", "traffic_blockage", "sudden_braking",
    "cut_in", "merge", "collision_avoidance",
)
SECURITY_SCENARIOS = (
    "none", "sensor_spoofing", "traffic_control_spoofing",
    "adversarial_perception", "adversarial_driving", "stop_sign_patch",
)

DEFAULT_TEST_PARAMETERS: TestParameters = {
    "placement": {
        "gap_m": 25.0, "road_id": None, "lane_id": None, "site_index": None,
    },
    "trigger": {
        "mode": "time", "start_s": 3.0, "distance_m": 20.0, "ttc_s": 2.0,
    },
    "event": {"duration_s": 2.0, "repeat_s": 0.0},
    "hazard": {"speed_mps": 8.0, "merge_distance_m": 20.0, "lookahead_m": 5.0},
    "sensor": {
        "id": None, "kind": "speedometer", "speed_bias_mps": 5.0,
        "gnss_offset": [0.0001, 0.0001, 0.0],
        "imu_offset": [0, 0, 0, 0, 0, 0.1, 0],
    },
    "perception": {
        "camera_id": None, "mode": "patch", "roi": [0.4, 0.4, 0.2, 0.2],
        "color_rgb": [255, 0, 0],
    },
    "traffic_control": {"actor_id": None, "state": "Green", "radius_m": 80.0},
    "driving": {
        "mode": "weave", "steer_bias": 0.15, "weave_amplitude": 0.3,
        "weave_period_s": 2.0, "throttle": 0.75,
    },
    "stop_sign": {
        "index": 0, "enabled": True, "max_distance_m": 90.0,
        "minimum_red": 90, "red_dominance": 1.25, "roi_scale": 1.7,
        "min_patch_pixels": 25,
    },
}

_CHOICES = {
    ("trigger", "mode"): ("time", "distance", "ttc"),
    ("sensor", "kind"): ("speedometer", "gnss", "imu"),
    ("perception", "mode"): ("patch", "blackout"),
    ("traffic_control", "state"): ("Red", "Yellow", "Green", "Off"),
    ("driving", "mode"): ("weave", "steering_bias", "brake_check", "throttle_burst"),
}

# Bounds are inclusive. None leaves the corresponding side unbounded.
_NUMBER_BOUNDS = (
    ("placement", "gap_m", 8, None),
    ("trigger", "start_s", 0, None),
    ("trigger", "distance_m", 0.01, None),
    ("trigger", "ttc_s", 0.01, None),
    ("event", "duration_s", 0.01, None),
    ("event", "repeat_s", 0, None),
    ("hazard", "speed_mps", 0, None),
    ("hazard", "merge_distance_m", 5, None),
    ("hazard", "lookahead_m", 1, None),
    ("traffic_control", "radius_m", 1, None),
    ("sensor", "speed_bias_mps", None, None),
    ("driving", "steer_bias", -1, 1),
    ("driving", "weave_amplitude", 0, 1),
    ("driving", "weave_period_s", 0.01, None),
    ("driving", "throttle", 0, 1),
    ("stop_sign", "max_distance_m", 0, None),
    ("stop_sign", "red_dominance", 1, None),
    ("stop_sign", "roi_scale", 0, None),
)


def _is_finite_number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def resolve_test_parameters(overrides: TestParameters | None = None) -> TestParameters:
    """Return independent defaults merged with validated partial parameter groups."""
    config = deepcopy(DEFAULT_TEST_PARAMETERS)
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError("test_parameters must be a dictionary")
    for group, values in overrides.items():
        if group not in config or not isinstance(values, dict):
            raise ValueError(f"Unknown or invalid test_parameters group: {group}")
        for key, value in values.items():
            if key not in config[group]:
                raise ValueError(f"Unknown test parameter: {group}.{key}")
            config[group][key] = deepcopy(value)

    for (group, key), allowed in _CHOICES.items():
        if config[group][key] not in allowed:
            raise ValueError(f"{group}.{key} must be one of {allowed}")
    for group, key, minimum, maximum in _NUMBER_BOUNDS:
        value = config[group][key]
        if not _is_finite_number(value):
            raise ValueError(f"{group}.{key} must be a finite number")
        if (
            (minimum is not None and value < minimum)
            or (maximum is not None and value > maximum)
        ):
            raise ValueError(f"{group}.{key} is out of range [{minimum}, {maximum}]")
    if 0 < config["event"]["repeat_s"] < config["event"]["duration_s"]:
        raise ValueError("event.repeat_s must be zero (one shot) or at least event.duration_s")

    for group, key, size in (
        ("sensor", "gnss_offset", 3),
        ("sensor", "imu_offset", 7),
        ("perception", "roi", 4),
        ("perception", "color_rgb", 3),
    ):
        values = config[group][key]
        if (
            not isinstance(values, (list, tuple))
            or len(values) != size
            or not all(_is_finite_number(value) for value in values)
        ):
            raise ValueError(f"{group}.{key} must contain {size} finite numbers")
    left, top, width, height = config["perception"]["roi"]
    if (
        min(left, top) < 0 or min(width, height) <= 0
        or left + width > 1 or top + height > 1
    ):
        raise ValueError("perception.roi must be an in-image [x, y, width, height] fraction")
    if any(not 0 <= value <= 255 for value in config["perception"]["color_rgb"]):
        raise ValueError("perception.color_rgb components must be in [0, 255]")
    for group, key in (("placement", "site_index"), ("traffic_control", "actor_id")):
        value = config[group][key]
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise ValueError(f"{group}.{key} must be a nonnegative integer or null")
    stop = config["stop_sign"]
    if not isinstance(stop["enabled"], bool):
        raise ValueError("stop_sign.enabled must be a boolean")
    for key, minimum, maximum in (
        ("index", 0, None),
        ("minimum_red", 0, 255),
        ("min_patch_pixels", 1, None),
    ):
        value = stop[key]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < minimum
            or (maximum is not None and value > maximum)
        ):
            raise ValueError(f"stop_sign.{key} must be an integer in [{minimum}, {maximum}]")
    if stop["max_distance_m"] <= 0 or stop["roi_scale"] <= 0:
        raise ValueError("stop_sign.max_distance_m and stop_sign.roi_scale must be positive")
    if stop["red_dominance"] <= 1:
        raise ValueError("stop_sign.red_dominance must be greater than one")
    return config


def validate_test_modes(safety: str, security: str, controller: str) -> None:
    """Reject unknown modes and combinations with conflicting controller ownership."""
    if safety not in SAFETY_SCENARIOS:
        raise ValueError(f"safety_scenario must be one of {SAFETY_SCENARIOS}")
    if security not in SECURITY_SCENARIOS:
        raise ValueError(f"security_scenario must be one of {SECURITY_SCENARIOS}")
    if controller == "external" and (
        safety != "none" or security not in ("none", "stop_sign_patch")
    ):
        raise ValueError(
            "External controllers support only stop_sign_patch without a safety encounter; "
            "use the PCLA/autopilot template for other profiles"
        )
    requires_native_pcla = security in (
        "sensor_spoofing", "adversarial_perception", "adversarial_driving",
    )
    if requires_native_pcla and controller != "pcla":
        raise ValueError(f"{security} requires ego_controller='pcla'")
    if security == "stop_sign_patch" and controller not in ("pcla", "external"):
        raise ValueError("stop_sign_patch requires ego_controller='pcla' or an external PCLA runtime")


def load_test_config(path: str | Path) -> dict[str, Any]:
    """Read and validate a JSON profile without overriding later CLI selectors."""
    with Path(path).open(encoding="utf-8") as stream:
        config = json.load(stream)
    allowed = {"safety_scenario", "security_scenario", "test_parameters"}
    if not isinstance(config, dict) or set(config) - allowed:
        raise ValueError(f"Test config must contain only {sorted(allowed)}")
    for name, choices in (
        ("safety_scenario", SAFETY_SCENARIOS),
        ("security_scenario", SECURITY_SCENARIOS),
    ):
        if name in config and config[name] not in choices:
            raise ValueError(f"{name} must be one of {choices}")
    if "test_parameters" in config:
        config["test_parameters"] = resolve_test_parameters(config["test_parameters"])
    return config
