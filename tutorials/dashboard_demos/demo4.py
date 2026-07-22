"""Scenic search over Town05 stop-sign color patches with a PCLA ego.

Each selected stop sign is evaluated with the same Scenic seed in a baseline run
and an attack run. The attack changes red pixels belonging to that one projected
stop-sign region to blue before PCLA receives its CARLA RGB tensors. The
dashboard shows those exact tensors together with live congestion measurements.
"""

from __future__ import annotations

import argparse
import csv
import inspect
import math
import os
import re
import sys
import threading
import types
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
_SCENARIO = _REPO_ROOT / "scenic_exp" / "scenarios" / "town05_stop_sign_patch.scenic"
_MAP_ROOT = _REPO_ROOT / "data" / "CARLA"
_DASHBOARD_DIR = _REPO_ROOT / "output" / "tracr_demo4_dashboard"
_EXPORT_DIR = _REPO_ROOT / "scenic_exp" / "data_logs" / "CARLA_05" / "stop_sign_patch"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
os.chdir(_REPO_ROOT)

from tutorials.dashboard_demos import demo2 as scenic_demo  # noqa: E402
from tutorials.dashboard_demos import demo3 as pcla_demo  # noqa: E402
from utils.cosim_support import (  # noqa: E402
    CarlaSensorPanel,
    _start_viz_with_port_fallback,
    image_array_to_png,
)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Use Scenic to compare Town05 stop-sign color-patch locations with "
            "a PCLA-controlled ego and a live TRACR dashboard."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("scenic_file_arg", nargs="?", help="Scenic search program.")
    parser.add_argument("--scenic-file", default=str(_SCENARIO))
    parser.add_argument("--scenic-model", default="scenic.simulators.cosim.model")
    parser.add_argument("--verbosity", type=int, default=2)
    parser.add_argument("--seed", type=int, default=33)
    parser.add_argument("--address", default="127.0.0.1")
    parser.add_argument(
        "--carla-host",
        default=None,
        help=(
            "CARLA server host override. In WSL NAT mode the Windows-host "
            "gateway is detected automatically when this is omitted."
        ),
    )
    parser.add_argument("--town", default="Town05")
    parser.add_argument("--map-locations", default=str(_MAP_ROOT))
    parser.add_argument("--num-commuters", type=_positive_int, default=100)
    parser.add_argument("--length", type=_positive_int, default=60)
    parser.add_argument("--timestep", type=float, default=0.05)
    parser.add_argument("--metsr-client-timestep", type=float, default=0.05)
    parser.add_argument("--bubble-size", type=_positive_int, default=100)
    parser.add_argument("--allow-bubble-spawns", action="store_true")

    parser.add_argument(
        "--stop-sign-indices",
        default="",
        help="Comma-separated indices from the printed, coordinate-sorted stop signs.",
    )
    parser.add_argument("--candidate-offset", type=int, default=0)
    parser.add_argument("--candidate-limit", type=_positive_int, default=3)
    parser.add_argument(
        "--approach-distance-m",
        type=float,
        default=35.0,
        help="Place the Scenic/PCLA ego this far upstream of each selected stop.",
    )
    parser.add_argument("--attack-only", action="store_true")
    parser.add_argument("--attack-max-distance-m", type=float, default=90.0)
    parser.add_argument("--attack-min-red", type=int, default=90)
    parser.add_argument("--attack-red-dominance", type=float, default=1.25)
    parser.add_argument("--attack-roi-scale", type=float, default=1.7)
    parser.add_argument("--free-flow-speed-mps", type=float, default=13.4)

    parser.add_argument("--pcla-dir", default=os.environ.get("PCLA_HOME"))
    parser.add_argument("--pcla-agent", default="tfv6_visiononly")
    parser.add_argument("--pcla-route", default=None)
    parser.add_argument("--route-output-dir", default=str(_DASHBOARD_DIR / "routes"))
    parser.add_argument("--route-min-distance-m", type=float, default=250.0)

    parser.add_argument("--metsr-host", default="localhost")
    parser.add_argument("--metsr-port", type=int, default=4000)
    parser.add_argument("--carla-port", type=int, default=2000)
    parser.add_argument("--carla-timeout-s", type=float, default=60.0)
    parser.add_argument("--metsr-sim-dir", default=None)
    parser.add_argument("--metsr-viz-port", type=int, default=8080)
    parser.add_argument("--viz-stream-host", default="0.0.0.0")
    parser.add_argument("--viz-stream-port", type=int, default=8768)
    parser.add_argument("--viz-url", default="https://engineering.purdue.edu/HSEES/METSRVis/")
    parser.add_argument("--dashboard-dir", default=str(_DASHBOARD_DIR))
    parser.add_argument("--dashboard-port", type=int, default=8896)
    parser.add_argument("--export-folder", default=str(_EXPORT_DIR))
    parser.add_argument("--carla-camera-z", type=float, default=85.0)
    parser.add_argument("--render-every", type=_positive_int, default=2)
    parser.add_argument("--dashboard-every", type=_positive_int, default=2)
    parser.add_argument("--camera-every", type=_positive_int, default=2)
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--no-hold-dashboard", dest="hold_dashboard", action="store_false")
    parser.set_defaults(hold_dashboard=True)

    args = parser.parse_args(argv)
    if args.scenic_file_arg:
        args.scenic_file = args.scenic_file_arg
    configured_host = str(args.carla_host or args.address)
    if args.carla_host:
        args.address = configured_host
    elif configured_host.lower() in {"127.0.0.1", "localhost", "::1"}:
        detected_host = scenic_demo.wsl_windows_host()
        if detected_host:
            args.address = detected_host
            print(
                f"WSL detected: using Windows CARLA host {detected_host} "
                f"instead of {configured_host}."
            )
    if args.candidate_offset < 0:
        parser.error("--candidate-offset cannot be negative")
    if args.timestep <= 0 or args.metsr_client_timestep <= 0:
        parser.error("simulation timesteps must be greater than zero")
    ratio = args.metsr_client_timestep / args.timestep
    if args.timestep > args.metsr_client_timestep or not math.isclose(
        ratio, round(ratio), rel_tol=0.0, abs_tol=1e-9
    ):
        parser.error("--metsr-client-timestep must be an integer multiple of --timestep")
    if not 0 <= args.attack_min_red <= 255:
        parser.error("--attack-min-red must be in [0, 255]")
    if args.attack_red_dominance <= 1.0:
        parser.error("--attack-red-dominance must be greater than 1")
    positive = (
        args.carla_timeout_s,
        args.attack_max_distance_m,
        args.attack_roi_scale,
        args.free_flow_speed_mps,
        args.route_min_distance_m,
        args.approach_distance_m,
    )
    if any(value <= 0 for value in positive):
        parser.error("attack, speed, and route distance values must be positive")

    for name in (
        "scenic_file",
        "map_locations",
        "route_output_dir",
        "dashboard_dir",
        "export_folder",
    ):
        value = Path(getattr(args, name)).expanduser()
        if not value.is_absolute():
            value = _REPO_ROOT / value
        setattr(args, name, str(value.resolve()))
    if args.pcla_route:
        route = Path(args.pcla_route).expanduser()
        if not route.is_absolute():
            route = _REPO_ROOT / route
        args.pcla_route = str(route.resolve())
    return args


@dataclass(frozen=True)
class StopSignCandidate:
    index: int
    actor_id: str
    route_location: Any
    visual_location: Any
    visual_extent_m: float
    source: str
    approach_transform: Any
    approach_distance_m: float
    same_road_distance_m: float
    road_id: Optional[int]
    route_is_junction: bool

    @property
    def location_text(self) -> str:
        loc = self.route_location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def initial_location_text(self) -> str:
        if self.approach_transform is None:
            return "unavailable"
        loc = self.approach_transform.location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def label(self) -> str:
        road = "?" if self.road_id is None else str(self.road_id)
        return (
            f"stop[{self.index}] road={road} id={self.actor_id} "
            f"{self.location_text} start={self.approach_distance_m:.1f}m upstream"
        )


@dataclass(frozen=True)
class RunSpec:
    run_number: int
    seed: int
    candidate: StopSignCandidate
    attack_enabled: bool

    @property
    def phase(self) -> str:
        return "attack" if self.attack_enabled else "baseline"


def _distance(a: Any, b: Any) -> float:
    return math.sqrt(
        (float(a.x) - float(b.x)) ** 2
        + (float(a.y) - float(b.y)) ** 2
        + (float(a.z) - float(b.z)) ** 2
    )


def _environment_location(obj: Any) -> Optional[Any]:
    location = getattr(getattr(obj, "bounding_box", None), "location", None)
    if location is not None:
        return location
    return getattr(getattr(obj, "transform", None), "location", None)


def _environment_extent(obj: Any) -> float:
    extent = getattr(getattr(obj, "bounding_box", None), "extent", None)
    values = [
        abs(float(getattr(extent, axis, 0.0) or 0.0))
        for axis in ("x", "y", "z")
    ]
    return max((value for value in values if 0.05 <= value <= 5.0), default=0.65)


def _copy_location(location: Any, carla_module: Any) -> Any:
    return carla_module.Location(
        x=float(location.x), y=float(location.y), z=float(location.z)
    )


def _stop_trigger_location(actor: Any, carla_module: Any) -> Any:
    """Return the world-space center of a stop actor's lane trigger."""
    actor_location = actor.get_location()
    volume_location = getattr(getattr(actor, "trigger_volume", None), "location", None)
    transform = getattr(actor, "get_transform", lambda: None)()
    if volume_location is None or transform is None:
        return actor_location
    route = _copy_location(volume_location, carla_module)
    try:
        transformed = transform.transform(route)
        return route if transformed is None else transformed
    except Exception:
        return actor_location


def _stop_waypoints(world_map: Any, actor: Any, route: Any) -> List[Any]:
    waypoints: List[Any] = []
    affected = getattr(actor, "get_affected_lane_waypoints", None)
    if callable(affected):
        try:
            waypoints.extend(list(affected() or []))
        except Exception:
            pass
    try:
        projected = world_map.get_waypoint(route, project_to_road=True)
    except Exception:
        projected = None
    if projected is not None:
        waypoints.append(projected)
    unique: Dict[Tuple[Any, ...], Any] = {}
    for waypoint in waypoints:
        transform = getattr(waypoint, "transform", None)
        location = getattr(transform, "location", None)
        if location is None:
            continue
        key = (
            getattr(waypoint, "road_id", None),
            getattr(waypoint, "section_id", None),
            getattr(waypoint, "lane_id", None),
            round(float(location.x), 2),
            round(float(location.y), 2),
        )
        unique[key] = waypoint
    return list(unique.values())


def _find_stop_approach(
    world_map: Any,
    actor: Any,
    route: Any,
    requested_distance_m: float,
) -> Tuple[Any, float, float, Optional[int], bool]:
    """Find a driving waypoint upstream, favoring a long same-road lead-in."""
    waypoints = _stop_waypoints(world_map, actor, route)
    if not waypoints:
        return None, 0.0, 0.0, None, False
    route_waypoint = min(
        waypoints,
        key=lambda waypoint: _distance(waypoint.transform.location, route),
    )
    road_id = getattr(route_waypoint, "road_id", None)
    is_junction = bool(getattr(route_waypoint, "is_junction", False))
    distances: List[float] = []
    distance = float(requested_distance_m)
    while distance >= 5.0:
        distances.append(distance)
        distance -= 5.0
    if not distances or not math.isclose(distances[-1], 5.0):
        distances.append(5.0)

    options: List[Tuple[float, float, float, Any]] = []
    same_road_distance = 0.0
    for waypoint in waypoints:
        for requested in distances:
            try:
                previous = list(waypoint.previous(requested) or [])
            except Exception:
                previous = []
            for upstream in previous:
                transform = getattr(upstream, "transform", None)
                location = getattr(transform, "location", None)
                if location is None:
                    continue
                actual = _distance(route, location)
                same_road = (
                    actual if getattr(upstream, "road_id", None) == road_id else 0.0
                )
                same_road_distance = max(same_road_distance, same_road)
                options.append((requested, same_road, actual, transform))
    if not options:
        return None, 0.0, same_road_distance, road_id, is_junction
    _, _, actual, transform = max(
        options,
        key=lambda option: (option[0], option[1], option[2]),
    )
    return transform, actual, same_road_distance, road_id, is_junction


def discover_stop_signs(
    world: Any,
    carla_module: Any,
    approach_distance_m: float = 35.0,
) -> List[StopSignCandidate]:
    try:
        actors = list(world.get_actors().filter("traffic.stop*"))
    except Exception:
        actors = []
    unique = {
        str(getattr(actor, "id", id(actor))): actor
        for actor in actors
        if actor is not None
    }

    sign_objects: List[Any] = []
    label = getattr(getattr(carla_module, "CityObjectLabel", None), "TrafficSigns", None)
    if label is not None and callable(getattr(world, "get_environment_objects", None)):
        try:
            sign_objects = list(world.get_environment_objects(label))
        except Exception:
            pass
    named_stops = [
        obj for obj in sign_objects
        if "stop" in str(getattr(obj, "name", "")).lower()
    ]
    visual_pool = named_stops or sign_objects

    world_map = world.get_map()
    raw: List[Dict[str, Any]] = []
    for actor_id, actor in unique.items():
        route = _stop_trigger_location(actor, carla_module)
        nearby = [
            (_distance(route, location), obj, location)
            for obj in visual_pool
            if (location := _environment_location(obj)) is not None
        ]
        nearest = min(nearby, default=None, key=lambda item: item[0])
        if nearest is not None and nearest[0] <= 25.0:
            distance, obj, visual = nearest
            extent = _environment_extent(obj)
            source = f"traffic.stop + environment sign ({distance:.1f} m)"
        else:
            actor_location = actor.get_location()
            visual = carla_module.Location(
                x=float(actor_location.x),
                y=float(actor_location.y),
                z=float(actor_location.z) + 2.25,
            )
            extent = 0.65
            source = "traffic.stop actor"
        approach, approach_distance, same_road_distance, road_id, is_junction = (
            _find_stop_approach(world_map, actor, route, approach_distance_m)
        )
        raw.append(
            {
                "actor_id": actor_id,
                "route": route,
                "visual": visual,
                "extent": extent,
                "source": source,
                "approach": approach,
                "approach_distance": approach_distance,
                "same_road_distance": same_road_distance,
                "road_id": road_id,
                "is_junction": is_junction,
            }
        )

    if not raw:
        for obj in named_stops:
            location = _environment_location(obj)
            if location is None:
                continue
            approach, approach_distance, same_road_distance, road_id, is_junction = (
                _find_stop_approach(world_map, obj, location, approach_distance_m)
            )
            raw.append(
                {
                    "actor_id": str(getattr(obj, "id", len(raw))),
                    "route": location,
                    "visual": location,
                    "extent": _environment_extent(obj),
                    "source": "named CARLA environment stop sign",
                    "approach": approach,
                    "approach_distance": approach_distance,
                    "same_road_distance": same_road_distance,
                    "road_id": road_id,
                    "is_junction": is_junction,
                }
            )
    raw.sort(
        key=lambda row: (
            float(row["route"].x),
            float(row["route"].y),
            row["actor_id"],
        )
    )
    return [
        StopSignCandidate(
            index=index,
            actor_id=row["actor_id"],
            route_location=row["route"],
            visual_location=row["visual"],
            visual_extent_m=row["extent"],
            source=row["source"],
            approach_transform=row["approach"],
            approach_distance_m=row["approach_distance"],
            same_road_distance_m=row["same_road_distance"],
            road_id=row["road_id"],
            route_is_junction=row["is_junction"],
        )
        for index, row in enumerate(raw)
    ]


def select_candidates(
    candidates: Sequence[StopSignCandidate],
    indices_text: str,
    offset: int,
    limit: int,
) -> List[StopSignCandidate]:
    if not candidates:
        raise RuntimeError("The CARLA map exposed no modeled stop signs")
    if indices_text.strip():
        try:
            indices = [int(value.strip()) for value in indices_text.split(",") if value.strip()]
        except ValueError as exc:
            raise ValueError("--stop-sign-indices must be comma-separated integers") from exc
        invalid = [index for index in indices if index < 0 or index >= len(candidates)]
        if not indices or invalid:
            raise ValueError(
                f"invalid stop-sign indices {invalid or indices}; valid range is "
                f"[0, {len(candidates) - 1}]"
            )
        selected = [candidates[index] for index in indices]
    else:
        ranked = sorted(
            candidates,
            key=lambda candidate: (
                candidate.approach_transform is None,
                candidate.route_is_junction,
                -candidate.same_road_distance_m,
                -candidate.approach_distance_m,
                candidate.index,
            ),
        )
        selected = list(ranked[offset : offset + limit])
    if not selected:
        raise ValueError("the candidate offset/limit selected no stop signs")
    missing = [
        candidate.index
        for candidate in selected
        if candidate.approach_transform is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not find an upstream driving waypoint for stop signs {missing}"
        )
    return selected


def _rotation_matrix(roll: float, pitch: float, yaw: float) -> np.ndarray:
    roll, pitch, yaw = np.radians([roll, pitch, yaw])
    cr, sr = math.cos(roll), math.sin(roll)
    cp, sp = math.cos(pitch), math.sin(pitch)
    cy, sy = math.cos(yaw), math.sin(yaw)
    return np.array(
        [
            [cp * cy, cy * sp * sr - sy * cr, -cy * sp * cr - sy * sr],
            [cp * sy, sy * sp * sr + cy * cr, -sy * sp * cr + cy * sr],
            [sp, -cp * sr, cp * cr],
        ],
        dtype=float,
    )


def _transform_matrix(x: float, y: float, z: float, roll: float, pitch: float, yaw: float) -> np.ndarray:
    matrix = np.eye(4, dtype=float)
    matrix[:3, :3] = _rotation_matrix(roll, pitch, yaw)
    matrix[:3, 3] = [x, y, z]
    return matrix


def _actor_transform_matrix(actor: Any) -> np.ndarray:
    transform = actor.get_transform()
    loc, rot = transform.location, transform.rotation
    return _transform_matrix(
        float(loc.x), float(loc.y), float(loc.z),
        float(rot.roll), float(rot.pitch), float(rot.yaw),
    )


def annotate_overhead_target(
    rgb: np.ndarray,
    camera_actor: Any,
    target: Any,
    fov: float = 80.0,
) -> np.ndarray:
    """Draw a cyan ring over the selected stop in the dashboard camera only."""
    marked = np.array(rgb, copy=True)
    if marked.ndim != 3 or marked.shape[2] < 3 or camera_actor is None:
        return marked
    try:
        camera_from_world = np.linalg.inv(_actor_transform_matrix(camera_actor))
        local = camera_from_world @ np.array(
            [float(target.x), float(target.y), float(target.z), 1.0]
        )
    except Exception:
        return marked
    depth = float(local[0])
    if depth <= 0.1:
        return marked
    height, width = marked.shape[:2]
    focal = width / (2.0 * math.tan(math.radians(float(fov)) / 2.0))
    center_x = int(round(width / 2.0 + focal * float(local[1]) / depth))
    center_y = int(round(height / 2.0 - focal * float(local[2]) / depth))
    if not (0 <= center_x < width and 0 <= center_y < height):
        return marked

    radius = 14
    yy, xx = np.ogrid[:height, :width]
    distance_sq = (xx - center_x) ** 2 + (yy - center_y) ** 2
    ring = (distance_sq >= (radius - 2) ** 2) & (
        distance_sq <= (radius + 2) ** 2
    )
    marked[ring, :3] = (0, 255, 255)
    x0, x1 = max(0, center_x - radius - 6), min(width, center_x + radius + 7)
    y0, y1 = max(0, center_y - radius - 6), min(height, center_y + radius + 7)
    marked[max(0, center_y - 1) : min(height, center_y + 2), x0:x1, :3] = (
        0,
        255,
        255,
    )
    marked[y0:y1, max(0, center_x - 1) : min(width, center_x + 2), :3] = (
        0,
        255,
        255,
    )
    return marked


def project_stop_sign(
    ego_actor: Any,
    spec: Mapping[str, Any],
    target: Any,
    image_shape: Sequence[int],
    extent_m: float,
    roi_scale: float,
) -> Optional[Tuple[int, int, int, int, float]]:
    height, width = int(image_shape[0]), int(image_shape[1])
    relative = _transform_matrix(
        *(float(spec.get(key, 0.0) or 0.0) for key in ("x", "y", "z", "roll", "pitch", "yaw"))
    )
    camera_from_world = np.linalg.inv(_actor_transform_matrix(ego_actor) @ relative)
    local = camera_from_world @ np.array(
        [float(target.x), float(target.y), float(target.z), 1.0]
    )
    depth = float(local[0])
    if depth <= 0.1:
        return None
    fov = float(spec.get("fov", 90.0) or 90.0)
    focal = width / (2.0 * math.tan(math.radians(fov) / 2.0))
    u = width / 2.0 + focal * float(local[1]) / depth
    v = height / 2.0 - focal * float(local[2]) / depth
    radius = max(5, int(round(focal * extent_m * roi_scale / depth)))
    x0, x1 = max(0, round(u) - radius), min(width, round(u) + radius + 1)
    y0, y1 = max(0, round(v) - radius), min(height, round(v) + radius + 1)
    if x0 >= x1 or y0 >= y1:
        return None
    return x0, y0, x1, y1, depth


class StopSignColorPatchTap:
    """Patch one projected stop sign before PCLA consumes CARLA BGRA frames."""

    def __init__(
        self,
        pcla: Any,
        ego_actor: Any,
        candidate: StopSignCandidate,
        enabled: bool,
        args: argparse.Namespace,
    ) -> None:
        agent = getattr(pcla, "agent_instance", None)
        interface = getattr(agent, "sensor_interface", None)
        if interface is None or not callable(getattr(interface, "update_sensor", None)):
            raise RuntimeError("PCLA agent did not expose sensor_interface.update_sensor")
        specs = [
            dict(spec) for spec in agent.sensors()
            if str(spec.get("type", "")).startswith("sensor.camera.rgb")
        ]
        specs.sort(key=lambda spec: str(spec.get("id", "")))
        self.camera_specs = specs
        self._specs = {str(spec.get("id")): spec for spec in specs}
        phase = "ATTACKED red->blue" if enabled else "BASELINE unmodified"
        self._labels = {
            sensor_id: f"{phase} | {pcla_demo._camera_label(spec, index)}"
            for index, (sensor_id, spec) in enumerate(self._specs.items())
        }
        if enabled:
            for index, (sensor_id, spec) in enumerate(self._specs.items()):
                self._labels[f"{sensor_id}__reference"] = (
                    f"REFERENCE red original | {pcla_demo._camera_label(spec, index)}"
                )
        self._interface = interface
        self._original_update = interface.update_sensor
        self._ego_actor = ego_actor
        self._candidate = candidate
        self._enabled = enabled
        self._args = args
        self._lock = threading.Lock()
        self._latest: Dict[str, Tuple[Any, np.ndarray]] = {}
        self._png_cache: Dict[str, Tuple[Any, bytes]] = {}
        self.frames_patched = 0
        self.pixels_patched = 0
        self.last_target_distance_m: Optional[float] = None
        self.minimum_target_distance_m: Optional[float] = None
        original_update = self._original_update

        def update_sensor(tag: Any, data: Any, timestamp: Any) -> Any:
            sensor_id = str(tag)
            delivered = data
            if sensor_id in self._specs:
                array = np.asarray(data)
                if array.ndim == 3 and array.shape[2] >= 3:
                    original_rgb = array[:, :, :3][:, :, ::-1].copy()
                    distance = _distance(ego_actor.get_location(), candidate.route_location)
                    self.last_target_distance_m = distance
                    self.minimum_target_distance_m = min(
                        distance,
                        self.minimum_target_distance_m
                        if self.minimum_target_distance_m is not None else distance,
                    )
                    if enabled and distance <= args.attack_max_distance_m:
                        projection = project_stop_sign(
                            ego_actor, self._specs[sensor_id], candidate.visual_location,
                            array.shape, candidate.visual_extent_m, args.attack_roi_scale,
                        )
                        if projection is not None:
                            x0, y0, x1, y1, _ = projection
                            working = np.array(array, copy=True)
                            count = self.recolor_red_bgra(
                                working[y0:y1, x0:x1],
                                args.attack_min_red,
                                args.attack_red_dominance,
                            )
                            if count:
                                delivered = working
                                self.frames_patched += 1
                                self.pixels_patched += count
                    shown = np.asarray(delivered)
                    rgb = shown[:, :, :3][:, :, ::-1].copy()
                    with self._lock:
                        if enabled:
                            self._latest[f"{sensor_id}__reference"] = (timestamp, original_rgb)
                        self._latest[sensor_id] = (timestamp, rgb)
            return original_update(tag, delivered, timestamp)

        self._wrapped_update = update_sensor
        interface.update_sensor = update_sensor

    @staticmethod
    def recolor_red_bgra(region: np.ndarray, minimum_red: int = 90, dominance: float = 1.25) -> int:
        if region.ndim != 3 or region.shape[2] < 3 or region.size == 0:
            return 0
        blue = region[:, :, 0].astype(np.float32)
        green = region[:, :, 1].astype(np.float32)
        red = region[:, :, 2].astype(np.float32)
        mask = (
            (red >= minimum_red)
            & (red >= green * dominance)
            & (red >= blue * dominance)
        )
        count = int(np.count_nonzero(mask))
        if count:
            old_red = region[:, :, 2][mask].copy()
            old_blue = region[:, :, 0][mask].copy()
            region[:, :, 0][mask] = np.maximum(old_red, 128)
            region[:, :, 2][mask] = np.minimum(old_blue, 48)
        return count

    def snapshots(self) -> Dict[str, Tuple[str, bytes, Any]]:
        result: Dict[str, Tuple[str, bytes, Any]] = {}
        with self._lock:
            latest = dict(self._latest)
        for sensor_id, (frame, rgb) in latest.items():
            cached = self._png_cache.get(sensor_id)
            if cached is None or cached[0] != frame:
                cached = (frame, image_array_to_png(rgb))
                self._png_cache[sensor_id] = cached
            result[sensor_id] = (self._labels.get(sensor_id, sensor_id), cached[1], frame)
        return result

    def close(self) -> None:
        if getattr(self._interface, "update_sensor", None) is self._wrapped_update:
            self._interface.update_sensor = self._original_update


class Demo4Dashboard(pcla_demo.PCLADashboard):
    """PCLA dashboard with a live Scenic multi-run comparison table."""

    _RUN_COLUMNS = (
        ("run", "run"),
        ("seed", "seed"),
        ("phase", "phase"),
        ("candidate_index", "stop"),
        ("road_id", "road"),
        ("status", "status"),
        ("congestion_score_s", "score (s)"),
        ("attack_delta_s", "delta (s)"),
        ("rank", "rank"),
        ("patched_frames", "patched"),
    )

    def __init__(
        self,
        directory: Path,
        port: int,
        viz_url: str,
        stream_url: str,
        open_browser: bool,
        town: str,
    ) -> None:
        super().__init__(directory, port, viz_url, stream_url, open_browser)
        self.town = str(town)
        self.metsr_viz_map = scenic_demo.metsr_vis_map_for_town(self.town)
        self._overhead_png = pcla_demo.blank_png(
            f"Waiting for CARLA {self.town} traffic"
        )
        self._run_rows: List[Dict[str, Any]] = []

    @staticmethod
    def _run_value(key: str, value: Any) -> str:
        if value in (None, ""):
            return "n/a"
        if key in {"congestion_score_s", "attack_delta_s"}:
            try:
                return f"{float(value):.1f}"
            except (TypeError, ValueError):
                pass
        return str(value)

    def _runs_table_html_locked(self) -> str:
        if not self._run_rows:
            return "<div class='runs-empty'>Waiting for Scenic run definitions...</div>"
        header = "".join(
            f"<th>{escape(label)}</th>" for _, label in self._RUN_COLUMNS
        )
        body = "".join(
            "<tr class='scenic-run scenic-run--"
            + re.sub(r"[^a-z0-9_-]", "-", str(row.get("status", "queued")).lower())
            + "'>"
            + "".join(
                f"<td>{escape(self._run_value(key, row.get(key)))}</td>"
                for key, _ in self._RUN_COLUMNS
            )
            + "</tr>"
            for row in self._run_rows
        )
        return (
            "<table class='scenic-runs-table'><thead><tr>"
            f"{header}</tr></thead><tbody>{body}</tbody></table>"
        )

    def _update_run_locked(self, run_number: int, values: Mapping[str, Any]) -> None:
        for row in self._run_rows:
            if int(row.get("run", -1)) == int(run_number):
                row.update(dict(values))
                return
        self._run_rows.append({"run": int(run_number), **dict(values)})

    def configure_runs(self, specs: Sequence[RunSpec]) -> None:
        with self._pcla_lock:
            self._run_rows = [
                {
                    "run": spec.run_number,
                    "seed": spec.seed,
                    "phase": spec.phase,
                    "candidate_index": spec.candidate.index,
                    "road_id": spec.candidate.road_id,
                    "status": "queued",
                    "congestion_score_s": None,
                    "attack_delta_s": None,
                    "rank": None,
                    "patched_frames": 0,
                }
                for spec in specs
            ]
        self._refresh_external_state(force=True)

    def begin_run(self, spec: RunSpec) -> None:
        with self._pcla_lock:
            self._update_run_locked(spec.run_number, {"status": "running"})
        self._refresh_external_state(force=True)

    def update_results(self, rows: Sequence[Mapping[str, Any]]) -> None:
        with self._pcla_lock:
            for row in rows:
                run_number = int(row.get("run", 0) or 0)
                if run_number > 0:
                    self._update_run_locked(run_number, row)
        self._refresh_external_state(force=True)

    def _external_css(self) -> str:
        return super()._external_css() + """
          .pcla-sensors-panel {grid-column: span 8;}
          .runs-panel {grid-column: span 4;}
          #camera-grid {
            flex: 1 1 auto;
            min-height: 0;
            padding: 6px;
            overflow: auto;
            grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
          }
          #camera-grid .camera-panel {min-height: 145px;}
          .camera-placeholder, .runs-empty {
            height: 100%;
            min-height: 80px;
            display: grid;
            place-items: center;
            padding: 12px;
            color: #94a3b8;
            text-align: center;
          }
          #runs-table {flex: 1 1 auto; min-height: 0; overflow: auto;}
          .scenic-runs-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 9.5px;
            font-variant-numeric: tabular-nums;
          }
          .scenic-runs-table th, .scenic-runs-table td {
            padding: 4px 5px;
            border-bottom: 1px solid #303641;
            text-align: left;
            white-space: nowrap;
          }
          .scenic-runs-table th {
            position: sticky;
            top: 0;
            z-index: 2;
            color: #bfdbfe;
            background: #171b22;
          }
          .scenic-run--running td {background: #172554; color: #dbeafe;}
          .scenic-run--finished td {background: #052e16; color: #dcfce7;}
          .scenic-run--failed td, .scenic-run--interrupted td {
            background: #450a0a;
            color: #fee2e2;
          }
          @media (max-width: 900px) {
            .pcla-sensors-panel, .runs-panel {grid-column: auto;}
            #camera-grid {display: grid;}
          }
        """

    def _external_page_html(self) -> str:
        page = super()._external_page_html()
        page = re.sub(
            r"<title>.*?</title>",
            f"<title>TRACR {escape(self.town)} Scenic + PCLA stop-sign attack</title>",
            page,
            count=1,
        )
        page = re.sub(
            r"<h1>.*?</h1>",
            f"<h1>TRACR {escape(self.town)} | Scenic stop-sign search | PCLA red-to-blue patch</h1>",
            page,
            count=1,
        )
        with self._pcla_lock:
            runs_html = self._runs_table_html_locked()
            overhead_uri = self._png_uri(self._overhead_png)
        page = page.replace(
            '<img id="overhead-camera" alt="CARLA ego tracking camera">',
            '<img id="overhead-camera" alt="CARLA ego tracking camera" '
            f'src="{overhead_uri}">',
        )
        page = page.replace(
            "<h2>CARLA ego tracking camera</h2>",
            "<h2>CARLA ego tracking camera (cyan marker = selected stop)</h2>",
        )
        page = page.replace(
            '<div id="camera-grid"></div>',
            """<section class="panel pcla-sensors-panel">
        <h2>PCLA RGB sensor inputs</h2>
        <div id="camera-grid"><div class="camera-placeholder" data-dashboard-placeholder>
          Waiting for PCLA camera frames...
        </div></div>
      </section>
      <section class="panel runs-panel">
        <h2>Scenic multi-run congestion table</h2>
        <div id="runs-table">""" + runs_html + """</div>
      </section>""",
        )
        page = page.replace(
            "const root = document.getElementById('camera-grid');",
            "const root = document.getElementById('camera-grid');\n"
            "      for (const node of root.querySelectorAll('[data-dashboard-placeholder]')) node.remove();",
        )
        page = page.replace(
            "updateCameras(state.camera_views || []);",
            "updateCameras(state.camera_views || []);\n"
            "        const runsTable = document.getElementById('runs-table');\n"
            "        if (runsTable && state.runs_table_html !== undefined) "
            "runsTable.innerHTML = state.runs_table_html;",
        )
        page = page.replace(
            "console.debug('TRACR PCLA dashboard refresh failed', error);",
            "document.getElementById('status').textContent = "
            "'Dashboard state unavailable: ' + String(error);\n"
            "        console.debug('TRACR demo4 dashboard refresh failed', error);",
        )
        return page

    def _external_state(self) -> Dict[str, Any]:
        state = super()._external_state()
        with self._pcla_lock:
            state["runs_table_html"] = self._runs_table_html_locked()
        return state


def _call_route_maker(pcla_module: Any, waypoints: Sequence[Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        pcla_module.route_maker(waypoints, str(path))
    except TypeError:
        pcla_module.route_maker(waypoints, savePath=str(path))
    if not path.exists():
        raise RuntimeError(f"PCLA route_maker did not create {path}")


def generate_route_via_stop(
    pcla_module: Any,
    client: Any,
    ego_actor: Any,
    candidate: StopSignCandidate,
    output_path: Path,
    min_distance_m: float,
) -> Path:
    world_map = client.get_world().get_map()
    start = ego_actor.get_location()
    waypoint = world_map.get_waypoint(candidate.route_location, project_to_road=True)
    target = waypoint.transform.location if waypoint is not None else candidate.route_location
    first = list(pcla_module.location_to_waypoint(client, start, target))
    if len(first) <= 1:
        raise RuntimeError(f"Could not route the PCLA ego to {candidate.label}")

    destinations = sorted(
        world_map.get_spawn_points(),
        key=lambda transform: _distance(target, transform.location),
        reverse=True,
    )
    preferred = [
        transform for transform in destinations
        if _distance(target, transform.location) >= min_distance_m
    ]
    last_error: Optional[BaseException] = None
    for destination in (preferred or destinations)[:30]:
        try:
            second = list(
                pcla_module.location_to_waypoint(client, target, destination.location)
            )
            if len(second) <= 1:
                continue
            _call_route_maker(pcla_module, first + second[1:], output_path)
            return output_path
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"Could not generate a PCLA route through {candidate.label}"
    ) from last_error


@dataclass
class RunStats:
    steps: int = 0
    network_delay_s: float = 0.0
    queue_vehicle_s: float = 0.0
    ego_stopped_s: float = 0.0
    speed_sum_mps: float = 0.0
    speed_samples: int = 0
    max_bubble_queue: int = 0
    max_carla_actors: int = 0

    @property
    def avg_network_speed_mps(self) -> Optional[float]:
        return (
            None if self.speed_samples <= 0
            else self.speed_sum_mps / self.speed_samples
        )

    @property
    def congestion_score_s(self) -> float:
        return self.network_delay_s + self.queue_vehicle_s


class RunRuntime:
    def __init__(
        self,
        args: argparse.Namespace,
        spec: RunSpec,
        dashboard: Demo4Dashboard,
        best_provider: Callable[[], str],
    ) -> None:
        self.args = args
        self.spec = spec
        self.dashboard = dashboard
        self.best_provider = best_provider
        self.simulation: Any = None
        self.controller: Optional[pcla_demo.PCLAController] = None
        self.tap: Optional[StopSignColorPatchTap] = None
        self.panel: Optional[CarlaSensorPanel] = None
        self.stats = RunStats()
        self.last_control: Any = None
        self.render_error = ""
        self.overhead_png: Optional[bytes] = None
        self.camera_views: Dict[str, Tuple[str, bytes, Any]] = {}
        self.route_path: Optional[Path] = None
        self.ego_vehicle_id: Optional[Any] = None
        self.closed = False
        self.summary: Optional[Dict[str, Any]] = None

    def attach(self, simulation: Any) -> None:
        self.simulation = simulation

    def _ego(self) -> Tuple[Any, Any]:
        ego = getattr(self.simulation, "ego", None)
        if ego is None:
            objects = list(getattr(self.simulation, "objects", []) or [])
            ego = objects[0] if objects else None
        actor = getattr(ego, "carlaActor", None)
        if ego is None or actor is None:
            raise RuntimeError("Scenic did not expose the CARLA ego actor for PCLA")
        return ego, actor

    @staticmethod
    def _carla_module() -> Any:
        try:
            import carla
        except ImportError as exc:
            raise RuntimeError("The matching CARLA Python API is required") from exc
        return carla

    def _ensure_controller(self) -> None:
        if self.controller is not None:
            return
        ego, actor = self._ego()
        if self.args.pcla_route:
            route_path = Path(self.args.pcla_route)
        else:
            pcla_module = pcla_demo._import_pcla(self.args.pcla_dir)
            route_path = (
                Path(self.args.route_output_dir)
                / f"run_{self.spec.run_number:02d}_stop_{self.spec.candidate.index}_{self.spec.phase}.xml"
            )
            route_path = generate_route_via_stop(
                pcla_module,
                self.simulation.carla_client,
                actor,
                self.spec.candidate,
                route_path,
                self.args.route_min_distance_m,
            )
        self.route_path = route_path
        controller_args = types.SimpleNamespace(
            pcla_dir=self.args.pcla_dir,
            pcla_agent=self.args.pcla_agent,
            pcla_route=str(route_path),
            route_output=str(route_path),
            route_end_spawn_index=None,
            route_min_distance_m=self.args.route_min_distance_m,
        )
        self.controller = pcla_demo.PCLAController(
            controller_args,
            self._carla_module(),
            self.simulation.carla_client,
            actor,
        )
        if self.controller.tap is not None:
            self.controller.tap.close()
        self.tap = StopSignColorPatchTap(
            self.controller.pcla,
            actor,
            self.spec.candidate,
            self.spec.attack_enabled,
            self.args,
        )
        self.controller.tap = self.tap
        self.dashboard.configure_camera_views(self.tap.camera_specs)
        try:
            self.ego_vehicle_id = self.simulation.getMetsrPrivateVehId(ego)
        except Exception:
            self.ego_vehicle_id = getattr(ego, "name", 0)
        self.dashboard.ego_vehicle_id = self.ego_vehicle_id
        print(f"PCLA route for {self.spec.phase}: {route_path}")

    def before_step(self) -> None:
        self._ensure_controller()
        assert self.controller is not None
        self.last_control = self.controller.step()

    def _world_vehicle_speeds(self) -> List[float]:
        speeds: List[float] = []
        try:
            actors = self.simulation.carla_world.get_actors().filter("vehicle.*")
        except Exception:
            actors = []
        for actor in actors:
            try:
                velocity = actor.get_velocity()
                speeds.append(
                    math.sqrt(
                        float(velocity.x) ** 2
                        + float(velocity.y) ** 2
                        + float(velocity.z) ** 2
                    )
                )
            except Exception:
                pass
        return speeds

    def _metsr_vehicle_speeds(self) -> List[float]:
        query = getattr(
            self.simulation.metsr_client,
            "_query_viz_stream_vehicle_records",
            None,
        )
        if not callable(query):
            return []
        try:
            records = query(
                transform_coords=False,
                include_public=True,
                include_private=True,
                batch_size=1000,
            )
        except Exception:
            return []
        speeds: List[float] = []
        for record in records or []:
            if not isinstance(record, Mapping):
                continue
            value = scenic_demo.first_float(
                record.get("speed"),
                record.get("speed_mps"),
                record.get("velocity"),
            )
            if value is not None and value >= 0:
                speeds.append(float(value))
        return speeds

    def _sample_congestion(self) -> None:
        interval_s = self.args.timestep * self.args.dashboard_every
        speeds = self._metsr_vehicle_speeds() or self._world_vehicle_speeds()
        if not speeds:
            return
        free_flow = self.args.free_flow_speed_mps
        self.stats.network_delay_s += sum(
            max(0.0, 1.0 - speed / free_flow) * interval_s
            for speed in speeds
        )
        self.stats.speed_sum_mps += sum(speeds)
        self.stats.speed_samples += len(speeds)

    def _update_step_stats(self, actor: Any) -> None:
        self.stats.steps += 1
        queue = len(getattr(self.simulation, "bubble_spawn_queue", []) or [])
        actors = len(getattr(self.simulation, "carla_actors", []) or [])
        self.stats.max_bubble_queue = max(self.stats.max_bubble_queue, queue)
        self.stats.max_carla_actors = max(self.stats.max_carla_actors, actors)
        self.stats.queue_vehicle_s += queue * self.args.timestep
        if pcla_demo._speed_kmh(actor) < 1.0:
            self.stats.ego_stopped_s += self.args.timestep
        if self.stats.steps % self.args.dashboard_every == 0:
            self._sample_congestion()

    def _render_metsr(self) -> None:
        try:
            render = self.simulation.metsr_client.render
            try:
                render(client_wait_timeout=0.0)
            except TypeError:
                render()
            self.render_error = ""
        except Exception as exc:
            self.render_error = str(exc).splitlines()[0]

    def _update_camera(self, actor: Any) -> None:
        if self.panel is None:
            def destroy(target: Any) -> None:
                try:
                    if target is not None and getattr(target, "is_alive", True):
                        target.destroy()
                except Exception:
                    pass

            self.panel = CarlaSensorPanel(
                self.simulation.carla_world,
                self._carla_module(),
                destroy,
                vehicle_camera_enabled=False,
                lidar_enabled=False,
            )
            self.panel.spawn_overhead_camera(z=self.args.carla_camera_z)
        vehicles: Dict[str, Any] = {}
        try:
            for vehicle in self.simulation.carla_world.get_actors().filter("vehicle.*"):
                vehicles[str(vehicle.id)] = vehicle
        except Exception:
            pass
        state = scenic_demo._CarlaSensorState(vehicles)
        self.panel.ensure_sensors(
            state,
            preferred_vehicle_ids=[str(getattr(actor, "id", ""))],
        )
        latest = self.panel.latest_camera
        camera_actor = self.panel.camera_actor
        if latest is None:
            self.overhead_png = self.panel.camera_png()
        else:
            marked = annotate_overhead_target(
                latest, camera_actor, self.spec.candidate.visual_location
            )
            self.overhead_png = image_array_to_png(marked)
        if self.tap is not None:
            self.camera_views = self.tap.snapshots()

    def _telemetry(self, actor: Any) -> Dict[str, Any]:
        tap = self.tap
        avg_speed = self.stats.avg_network_speed_mps
        return {
            "run": self.spec.run_number,
            "seed": self.spec.seed,
            "phase": self.spec.phase,
            "target_stop": self.spec.candidate.index,
            "target_id": self.spec.candidate.actor_id,
            "target_xyz": self.spec.candidate.location_text,
            "target_road": self.spec.candidate.road_id,
            "initial_xyz": self.spec.candidate.initial_location_text,
            "target_distance_m": (
                "n/a" if tap is None or tap.last_target_distance_m is None
                else f"{tap.last_target_distance_m:.1f}"
            ),
            "patched_frames": 0 if tap is None else tap.frames_patched,
            "patched_pixels": 0 if tap is None else tap.pixels_patched,
            "ego_speed_kmh": f"{pcla_demo._speed_kmh(actor):.1f}",
            "network_speed_kmh": "n/a" if avg_speed is None else f"{avg_speed * 3.6:.1f}",
            "network_delay_s": f"{self.stats.network_delay_s:.1f}",
            "queue_vehicle_s": f"{self.stats.queue_vehicle_s:.1f}",
            "congestion_score_s": f"{self.stats.congestion_score_s:.1f}",
            "max_bubble_queue": self.stats.max_bubble_queue,
            "best_so_far": self.best_provider(),
        }

    def after_step(self) -> None:
        _, actor = self._ego()
        self._update_step_stats(actor)
        if self.stats.steps % self.args.render_every == 0:
            self._render_metsr()
        if self.stats.steps % self.args.camera_every == 0:
            self._update_camera(actor)
        if self.stats.steps % self.args.dashboard_every == 0:
            status = (
                f"Scenic run {self.spec.run_number} | {self.spec.phase} | "
                f"{self.spec.candidate.label}"
            )
            if self.render_error:
                status += f" | METS-R Viz waiting: {self.render_error}"
            self.dashboard.publish(
                status=status,
                telemetry=self._telemetry(actor),
                ego_vehicle_id=self.ego_vehicle_id,
                overhead_png=self.overhead_png,
                camera_views=self.camera_views,
            )

    def finish(self, status: str = "finished", error: str = "") -> Dict[str, Any]:
        if self.summary is not None:
            return self.summary
        tap = self.tap
        candidate = self.spec.candidate
        initial_transform = candidate.approach_transform
        initial_location = initial_transform.location
        initial_rotation = initial_transform.rotation
        self.summary = {
            "run": self.spec.run_number,
            "seed": self.spec.seed,
            "status": status,
            "candidate_index": candidate.index,
            "candidate_id": candidate.actor_id,
            "road_id": candidate.road_id,
            "approach_distance_m": candidate.approach_distance_m,
            "same_road_distance_m": candidate.same_road_distance_m,
            "initial_x": float(initial_location.x),
            "initial_y": float(initial_location.y),
            "initial_z": float(initial_location.z),
            "initial_carla_yaw": float(initial_rotation.yaw),
            "route_is_junction": candidate.route_is_junction,
            "candidate_source": candidate.source,
            "target_x": float(candidate.route_location.x),
            "target_y": float(candidate.route_location.y),
            "target_z": float(candidate.route_location.z),
            "phase": self.spec.phase,
            "attack_enabled": self.spec.attack_enabled,
            "patched_frames": 0 if tap is None else tap.frames_patched,
            "patched_pixels": 0 if tap is None else tap.pixels_patched,
            "minimum_target_distance_m": (
                None if tap is None else tap.minimum_target_distance_m
            ),
            "avg_network_speed_mps": self.stats.avg_network_speed_mps,
            "network_delay_s": self.stats.network_delay_s,
            "queue_vehicle_s": self.stats.queue_vehicle_s,
            "ego_stopped_s": self.stats.ego_stopped_s,
            "congestion_score_s": self.stats.congestion_score_s,
            "max_bubble_queue": self.stats.max_bubble_queue,
            "max_carla_actors": self.stats.max_carla_actors,
            "route": "" if self.route_path is None else str(self.route_path),
            "attack_delta_s": None,
            "rank": None,
            "error": error,
        }
        return self.summary

    def before_destroy(self) -> None:
        self.finish()
        self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.panel is not None:
            try:
                self.panel.close()
            except Exception:
                pass
            self.panel = None
        if self.controller is not None:
            self.controller.close()
            self.controller = None


class RuntimeHolder:
    def __init__(self) -> None:
        self.current: Optional[RunRuntime] = None


def install_runtime_hook(simulator: Any, holder: RuntimeHolder) -> None:
    """Install demo4 hooks before Scenic constructs and runs CosimSimulation."""
    try:
        from scenic.simulators.cosim import simulator as cosim_module
        simulation_cls = getattr(cosim_module, "CosimSimulation", None)
    except Exception:
        simulation_cls = None
    if simulation_cls is None:
        raise RuntimeError("Could not find Scenic CosimSimulation for demo4 hooks")

    patch_version = 1
    if getattr(simulation_cls, "_tracr_demo4_patch_version", 0) < patch_version:
        original_step = getattr(
            simulation_cls, "_tracr_demo4_original_step", simulation_cls.step
        )
        original_destroy = getattr(
            simulation_cls, "_tracr_demo4_original_destroy", simulation_cls.destroy
        )
        simulation_cls._tracr_demo4_original_step = original_step
        simulation_cls._tracr_demo4_original_destroy = original_destroy

        def runtime_for(simulation: Any) -> Optional[RunRuntime]:
            return getattr(simulation, "_tracr_demo4_runtime", None) or getattr(
                type(simulation), "_tracr_demo4_pending_runtime", None
            )

        def step_with_demo4(self: Any, *args: Any, **kwargs: Any) -> Any:
            runtime = runtime_for(self)
            if runtime is None:
                return original_step(self, *args, **kwargs)
            if runtime.simulation is None:
                runtime.attach(self)
            runtime.before_step()
            result = original_step(self, *args, **kwargs)
            runtime.after_step()
            return result

        def destroy_with_demo4(self: Any, *args: Any, **kwargs: Any) -> Any:
            runtime = runtime_for(self)
            try:
                return original_destroy(self, *args, **kwargs)
            finally:
                if runtime is not None:
                    runtime.close()

        simulation_cls.step = step_with_demo4
        simulation_cls.destroy = destroy_with_demo4
        simulation_cls._tracr_demo4_patch_version = patch_version

    original_create = simulator.createSimulation

    def create_with_demo4(scene: Any, *args: Any, **kwargs: Any) -> Any:
        runtime = holder.current
        if runtime is None:
            raise RuntimeError("demo4 runtime was not configured before Scenic simulation")
        simulation_cls._tracr_demo4_pending_runtime = runtime
        try:
            simulation = original_create(scene, *args, **kwargs)
            simulation._tracr_demo4_runtime = runtime
            if runtime.simulation is None:
                runtime.attach(simulation)
            return simulation
        finally:
            if getattr(simulation_cls, "_tracr_demo4_pending_runtime", None) is runtime:
                simulation_cls._tracr_demo4_pending_runtime = None

    simulator.createSimulation = create_with_demo4


_RESULT_FIELDS = [
    "run", "seed", "status", "rank", "candidate_index", "candidate_id",
    "road_id", "route_is_junction", "target_x", "target_y", "target_z",
    "initial_x", "initial_y", "initial_z", "initial_carla_yaw",
    "approach_distance_m", "same_road_distance_m", "candidate_source", "phase",
    "attack_enabled", "patched_frames", "patched_pixels",
    "minimum_target_distance_m", "avg_network_speed_mps", "network_delay_s",
    "queue_vehicle_s", "ego_stopped_s", "congestion_score_s",
    "attack_delta_s", "max_bubble_queue", "max_carla_actors", "route",
    "artifact_base", "error",
]


def update_rankings(rows: List[Dict[str, Any]], paired: bool) -> None:
    by_candidate: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for row in rows:
        if row.get("status") == "finished":
            by_candidate.setdefault(int(row["candidate_index"]), {})[
                str(row["phase"])
            ] = row
    ranked: List[Tuple[float, int]] = []
    for candidate_index, phases in by_candidate.items():
        attack = phases.get("attack")
        baseline = phases.get("baseline")
        if attack is None or (paired and baseline is None):
            continue
        value = float(attack["congestion_score_s"])
        if baseline is not None and paired:
            value -= float(baseline["congestion_score_s"])
        attack["attack_delta_s"] = value
        ranked.append((value, candidate_index))
    ranked.sort(reverse=True)
    for rank, (_, candidate_index) in enumerate(ranked, start=1):
        phases = by_candidate[candidate_index]
        value = phases["attack"]["attack_delta_s"]
        for row in phases.values():
            row["rank"] = rank
            row["attack_delta_s"] = value


def write_results(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=_RESULT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def best_result_text(rows: Sequence[Mapping[str, Any]], paired: bool) -> str:
    attacks = [
        row for row in rows
        if row.get("phase") == "attack"
        and row.get("status") == "finished"
        and row.get("attack_delta_s") is not None
    ]
    if not attacks:
        return "pending"
    best = max(attacks, key=lambda row: float(row["attack_delta_s"]))
    label = "delta" if paired else "score"
    return f"stop[{best['candidate_index']}] {label}={float(best['attack_delta_s']):.1f}s"


def _simulator_kwargs(args: argparse.Namespace, cls: Any, run_name: Path) -> Dict[str, Any]:
    map_dir = Path(args.map_locations) / args.town / "facility" / "road"
    kwargs: Dict[str, Any] = {
        "metsr_host": args.metsr_host,
        "metsr_port": args.metsr_port,
        "address": args.address,
        "carla_port": args.carla_port,
        "timeout": args.carla_timeout_s,
        "carla_map": args.town,
        "xml_map": str(map_dir / f"{args.town}.net.xml"),
        "map_path": str(map_dir / f"{args.town}.xodr"),
        "timestep": args.timestep,
        "bubble_size": args.bubble_size,
        "run_name": str(run_name),
        "metsr_sim_dir": args.metsr_sim_dir,
        "render": False,
        "metsr_viz_port": args.metsr_viz_port,
        "metsr_render_freq": args.render_every,
    }
    if "sim_timestep" in inspect.signature(cls).parameters:
        kwargs["sim_timestep"] = args.metsr_client_timestep
    return kwargs


def _scenic_initial_pose(candidate: StopSignCandidate) -> Dict[str, float]:
    transform = candidate.approach_transform
    if transform is None:
        raise RuntimeError(f"No upstream initial pose is available for {candidate.label}")
    location = transform.location
    rotation = transform.rotation
    heading = -math.radians(float(rotation.yaw) + 90.0)
    heading = (heading + math.pi) % (2.0 * math.pi) - math.pi
    return {
        "initial_x": float(location.x),
        "initial_y": -float(location.y),
        "initial_heading": heading,
    }


def _compile_scenario(
    args: argparse.Namespace,
    scenic: Any,
    set_debugging_options: Any,
    set_seed: Any,
    run_params: Optional[Mapping[str, Any]] = None,
) -> Any:
    set_debugging_options(verbosity=args.verbosity, fullBacktrace=False)
    set_seed(args.seed)
    map_dir = Path(args.map_locations) / args.town / "facility" / "road"
    params = {
        "address": args.address,
        "town": args.town,
        "map": str(map_dir / f"{args.town}.xodr"),
        "xml_map": str(map_dir / f"{args.town}.net.xml"),
        "num_commuters": args.num_commuters,
        "length": args.length,
        "timestep": args.timestep,
        "bubble_size": args.bubble_size,
        "seed": args.seed,
        "export_folder": args.export_folder,
        "allow_bubble_spawns": args.allow_bubble_spawns,
        "attack_stop_index": 0,
        "attack_enabled": False,
        "initial_x": 0.0,
        "initial_y": 0.0,
        "initial_heading": 0.0,
    }
    if run_params:
        params.update(dict(run_params))
    return scenic.scenarioFromFile(
        path=args.scenic_file,
        model=args.scenic_model,
        mode2D=True,
        params=params,
    )


def _start_stream(client: Any, args: argparse.Namespace) -> str:
    scenic_demo.patch_metsr_client_viz_compat(client)
    info = _start_viz_with_port_fallback(
        client,
        {
            "server_port": args.viz_stream_port,
            "host": args.viz_stream_host,
            "tick_interval": 1,
            "transform_coords": False,
            "include_public": True,
            "include_private": True,
            "include_links": False,
        },
    )
    return str(info.get("browser_url") or info.get("url") or "")


def _run_specs(
    candidates: Sequence[StopSignCandidate],
    seed: int,
    attack_only: bool,
) -> List[RunSpec]:
    specs: List[RunSpec] = []
    for candidate_number, candidate in enumerate(candidates):
        for enabled in ((True,) if attack_only else (False, True)):
            specs.append(
                RunSpec(
                    len(specs) + 1,
                    seed + candidate_number,
                    candidate,
                    enabled,
                )
            )
    return specs


def run(args: argparse.Namespace) -> int:
    try:
        import scenic
        from scenic import setDebuggingOptions
        from scenic.core.utils import setSeed
        from scenic.simulators.cosim import CosimSimulator
    except ImportError as exc:
        raise SystemExit(
            "Install Kv139/Scenic branch METSRSim in the PCLA environment "
            "before running demo4.py."
        ) from exc

    scenario_path = Path(args.scenic_file)
    if not scenario_path.exists():
        raise FileNotFoundError(f"Scenic scenario does not exist: {scenario_path}")
    export_dir = Path(args.export_folder)
    result_path = export_dir / "tracr_demo4_stop_sign_results.csv"
    paired = not args.attack_only
    rows: List[Dict[str, Any]] = []
    holder = RuntimeHolder()
    simulator: Any = None
    dashboard: Optional[Demo4Dashboard] = None
    exit_code = 0
    try:
        simulator = CosimSimulator(
            **_simulator_kwargs(args, CosimSimulator, export_dir / "demo4_pending")
        )
        discovered = discover_stop_signs(
            simulator.world,
            RunRuntime._carla_module(),
            args.approach_distance_m,
        )
        candidates = select_candidates(
            discovered,
            args.stop_sign_indices,
            args.candidate_offset,
            args.candidate_limit,
        )
        print(f"Discovered {len(discovered)} {args.town} stop signs.")
        for candidate in candidates:
            print(f"  selected {candidate.label} via {candidate.source}")

        specs = _run_specs(candidates, args.seed, args.attack_only)
        dashboard = Demo4Dashboard(
            Path(args.dashboard_dir),
            args.dashboard_port,
            args.viz_url,
            _start_stream(simulator.metsr_client, args),
            args.open_browser,
            args.town,
        )
        dashboard.configure_runs(specs)
        print(f"TRACR Scenic + PCLA stop-sign dashboard: {dashboard.start()}")
        install_runtime_hook(simulator, holder)

        def best_provider() -> str:
            return best_result_text(rows, paired)

        for spec in specs:
            run_base = (
                export_dir
                / f"stop_{spec.candidate.index}_{spec.phase}_seed_{spec.seed}_run_{spec.run_number}"
            )
            run_params: Dict[str, Any] = _scenic_initial_pose(spec.candidate)
            run_params.update(
                {
                    "seed": spec.seed,
                    "attack_stop_index": spec.candidate.index,
                    "attack_enabled": spec.attack_enabled,
                    "run_name": str(run_base),
                }
            )
            scenario = _compile_scenario(
                args, scenic, setDebuggingOptions, setSeed, run_params
            )
            setSeed(spec.seed)
            scene, _ = scenario.generate()
            simulator.run_name = str(run_base)
            runtime = RunRuntime(args, spec, dashboard, best_provider)
            holder.current = runtime
            print(
                f"Starting run {spec.run_number}/{len(specs)}: {spec.phase}, "
                f"{spec.candidate.label}, seed={spec.seed}"
            )
            dashboard.begin_run(spec)
            dashboard.publish(
                status=(
                    f"Starting Scenic run {spec.run_number}/{len(specs)} | "
                    f"{spec.phase} | {spec.candidate.label}"
                ),
                telemetry={
                    "run": f"{spec.run_number}/{len(specs)}",
                    "seed": spec.seed,
                    "phase": spec.phase,
                    "target_stop": spec.candidate.index,
                    "target_xyz": spec.candidate.location_text,
                    "best_so_far": best_provider(),
                },
            )
            try:
                simulation = simulator.simulate(scene)
                if not simulation:
                    raise RuntimeError("Scenic returned no simulation result")
                records = getattr(simulation.result, "records", {}) or {}
                scenic_demo.write_scenic_records(
                    records,
                    run_base.with_name(run_base.name + "_trajectory.csv"),
                )
                row = runtime.finish()
                row["artifact_base"] = str(run_base)
                rows.append(row)
                update_rankings(rows, paired)
                write_results(result_path, rows)
                print(
                    f"Finished run {spec.run_number}: "
                    f"congestion={row['congestion_score_s']:.1f}s, "
                    f"patched_pixels={row['patched_pixels']}"
                )
            except KeyboardInterrupt:
                exit_code = 130
                row = runtime.finish("interrupted", "interrupted by user")
                row["artifact_base"] = str(run_base)
                rows.append(row)
                break
            except Exception as exc:
                exit_code = 1
                message = str(exc).splitlines()[0]
                row = runtime.finish("failed", message)
                row["artifact_base"] = str(run_base)
                rows.append(row)
                dashboard.publish(
                    status=f"Run {spec.run_number} failed: {message}",
                    telemetry={
                        "state": "failed",
                        "run": spec.run_number,
                        "error": message,
                        "best_so_far": best_provider(),
                    },
                )
                print(f"Demo4 run {spec.run_number} failed: {message}")
                break
            finally:
                runtime.close()
                holder.current = None
                update_rankings(rows, paired)
                write_results(result_path, rows)
                dashboard.update_results(rows)

        attacks = [
            row for row in rows
            if row.get("phase") == "attack"
            and row.get("status") == "finished"
            and row.get("attack_delta_s") is not None
        ]
        if attacks:
            best = max(attacks, key=lambda row: float(row["attack_delta_s"]))
            effect = "attack minus baseline" if paired else "attack score"
            dashboard.publish(
                status=(
                    f"Worst congestion: stop[{best['candidate_index']}] | "
                    f"{effect}={float(best['attack_delta_s']):.1f}s"
                ),
                telemetry={
                    "state": "complete" if exit_code == 0 else "stopped",
                    "worst_stop": best["candidate_index"],
                    "target_xyz": (
                        f"({best['target_x']:.1f}, {best['target_y']:.1f}, "
                        f"{best['target_z']:.1f})"
                    ),
                    "effect": effect,
                    "effect_s": f"{best['attack_delta_s']:.1f}",
                    "attack_score_s": f"{best['congestion_score_s']:.1f}",
                    "patched_pixels": best["patched_pixels"],
                    "results_csv": str(result_path),
                },
            )
            print(
                f"Worst location: stop[{best['candidate_index']}] at "
                f"({best['target_x']:.1f}, {best['target_y']:.1f}, "
                f"{best['target_z']:.1f}); {effect}={best['attack_delta_s']:.1f}s"
            )
        else:
            dashboard.publish(
                status="Stopped before a comparable attack result completed",
                telemetry={"state": "stopped", "results_csv": str(result_path)},
            )
        print(f"Demo4 result table: {result_path}")
        if args.hold_dashboard and exit_code != 130:
            input("Runs complete. Press Enter to close the dashboard.")
        return exit_code
    finally:
        if holder.current is not None:
            holder.current.close()
        if simulator is not None:
            try:
                simulator.destroy()
            except Exception:
                pass
        if dashboard is not None:
            dashboard.stop_external()


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
