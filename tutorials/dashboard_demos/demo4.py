"""Scenic search over Town06 stop-sign color patches with a PCLA ego.

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
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
_SCENARIO = _REPO_ROOT / "scenic_exp" / "scenarios" / "town06_stop_sign_patch.scenic"
_MAP_ROOT = _REPO_ROOT / "data" / "CARLA"
_DASHBOARD_DIR = _REPO_ROOT / "output" / "tracr_demo4_dashboard"
_EXPORT_DIR = _REPO_ROOT / "scenic_exp" / "data_logs" / "CARLA_06" / "stop_sign_patch"

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
            "Use Scenic to compare Town06 stop-sign color-patch locations with "
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
    parser.add_argument("--town", default="Town06")
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
        help="Comma-separated indices from coordinate-sorted Town06 stop signs.",
    )
    parser.add_argument("--candidate-offset", type=int, default=0)
    parser.add_argument("--candidate-limit", type=_positive_int, default=3)
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
    if args.town != "Town06":
        parser.error("demo4 currently requires --town Town06")
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
        args.attack_max_distance_m,
        args.attack_roi_scale,
        args.free_flow_speed_mps,
        args.route_min_distance_m,
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

    @property
    def location_text(self) -> str:
        loc = self.route_location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def label(self) -> str:
        return f"stop[{self.index}] id={self.actor_id} {self.location_text}"


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


def discover_stop_signs(world: Any, carla_module: Any) -> List[StopSignCandidate]:
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

    raw: List[Tuple[str, Any, Any, float, str]] = []
    for actor_id, actor in unique.items():
        route = actor.get_location()
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
            visual = carla_module.Location(
                x=float(route.x), y=float(route.y), z=float(route.z) + 2.25
            )
            extent = 0.65
            source = "traffic.stop trigger"
        raw.append((actor_id, route, visual, extent, source))

    if not raw:
        for obj in named_stops:
            location = _environment_location(obj)
            if location is not None:
                raw.append(
                    (
                        str(getattr(obj, "id", len(raw))),
                        location,
                        location,
                        _environment_extent(obj),
                        "named CARLA environment stop sign",
                    )
                )
    raw.sort(key=lambda row: (float(row[1].x), float(row[1].y), row[0]))
    return [
        StopSignCandidate(index, actor_id, route, visual, extent, source)
        for index, (actor_id, route, visual, extent, source) in enumerate(raw)
    ]


def select_candidates(
    candidates: Sequence[StopSignCandidate],
    indices_text: str,
    offset: int,
    limit: int,
) -> List[StopSignCandidate]:
    if not candidates:
        raise RuntimeError(
            "Town06 exposed no traffic.stop actors or named stop-sign objects"
        )
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
        return [candidates[index] for index in indices]
    selected = list(candidates[offset : offset + limit])
    if not selected:
        raise ValueError("the candidate offset/limit selected no stop signs")
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
    def _external_page_html(self) -> str:
        page = super()._external_page_html()
        page = re.sub(
            r"<title>.*?</title>",
            "<title>TRACR Town06 Scenic ? PCLA stop-sign attack</title>",
            page,
            count=1,
        )
        return re.sub(
            r"<h1>.*?</h1>",
            "<h1>TRACR Town06 ? Scenic stop-sign search ? PCLA red?blue patch</h1>",
            page,
            count=1,
        )


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
        self.overhead_png = self.panel.camera_png()
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
            "target_distance_m": (
                "?" if tap is None or tap.last_target_distance_m is None
                else f"{tap.last_target_distance_m:.1f}"
            ),
            "patched_frames": 0 if tap is None else tap.frames_patched,
            "patched_pixels": 0 if tap is None else tap.pixels_patched,
            "ego_speed_kmh": f"{pcla_demo._speed_kmh(actor):.1f}",
            "network_speed_kmh": "?" if avg_speed is None else f"{avg_speed * 3.6:.1f}",
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
                f"Scenic run {self.spec.run_number} ? {self.spec.phase} ? "
                f"{self.spec.candidate.label}"
            )
            if self.render_error:
                status += f" ? METS-R Viz waiting: {self.render_error}"
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
        self.summary = {
            "run": self.spec.run_number,
            "seed": self.spec.seed,
            "status": status,
            "candidate_index": candidate.index,
            "candidate_id": candidate.actor_id,
            "target_x": float(candidate.route_location.x),
            "target_y": float(candidate.route_location.y),
            "target_z": float(candidate.route_location.z),
            "candidate_source": candidate.source,
            "phase": self.spec.phase,
            "attack_enabled": self.spec.attack_enabled,
            "patched_frames": 0 if tap is None else tap.frames_patched,
            "patched_pixels": 0 if tap is None else tap.pixels_patched,
            "minimum_target_distance_m": (
                None if tap is None else tap.last_target_distance_m
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
    original_create = simulator.createSimulation

    def create_with_demo4(scene: Any, *args: Any, **kwargs: Any) -> Any:
        simulation = original_create(scene, *args, **kwargs)
        runtime = holder.current
        if runtime is None:
            raise RuntimeError("demo4 runtime was not configured before Scenic simulation")
        runtime.attach(simulation)
        original_step = simulation.step
        original_destroy = simulation.destroy

        def step_with_demo4(self: Any, *step_args: Any, **step_kwargs: Any) -> Any:
            runtime.before_step()
            result = original_step(*step_args, **step_kwargs)
            runtime.after_step()
            return result

        def destroy_with_demo4(self: Any, *destroy_args: Any, **destroy_kwargs: Any) -> Any:
            try:
                runtime.before_destroy()
            finally:
                return original_destroy(*destroy_args, **destroy_kwargs)

        simulation.step = types.MethodType(step_with_demo4, simulation)
        simulation.destroy = types.MethodType(destroy_with_demo4, simulation)
        return simulation

    simulator.createSimulation = create_with_demo4


_RESULT_FIELDS = [
    "run", "seed", "status", "rank", "candidate_index", "candidate_id",
    "target_x", "target_y", "target_z", "candidate_source", "phase",
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
    label = "?" if paired else "score"
    return f"stop[{best['candidate_index']}] {label}={float(best['attack_delta_s']):.1f}s"


def _simulator_kwargs(args: argparse.Namespace, cls: Any, run_name: Path) -> Dict[str, Any]:
    map_dir = Path(args.map_locations) / args.town / "facility" / "road"
    kwargs: Dict[str, Any] = {
        "metsr_host": args.metsr_host,
        "metsr_port": args.metsr_port,
        "address": args.address,
        "carla_port": args.carla_port,
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


def _compile_scenario(
    args: argparse.Namespace,
    scenic: Any,
    set_debugging_options: Any,
    set_seed: Any,
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
    }
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
    scenario = _compile_scenario(
        args, scenic, setDebuggingOptions, setSeed
    )

    try:
        simulator = CosimSimulator(
            **_simulator_kwargs(args, CosimSimulator, export_dir / "demo4_pending")
        )
        discovered = discover_stop_signs(
            simulator.world,
            RunRuntime._carla_module(),
        )
        candidates = select_candidates(
            discovered,
            args.stop_sign_indices,
            args.candidate_offset,
            args.candidate_limit,
        )
        print(f"Discovered {len(discovered)} Town06 stop signs.")
        for candidate in candidates:
            print(f"  selected {candidate.label} via {candidate.source}")

        dashboard = Demo4Dashboard(
            Path(args.dashboard_dir),
            args.dashboard_port,
            args.viz_url,
            _start_stream(simulator.metsr_client, args),
            args.open_browser,
        )
        print(f"TRACR Scenic ? PCLA stop-sign dashboard: {dashboard.start()}")
        install_runtime_hook(simulator, holder)
        specs = _run_specs(candidates, args.seed, args.attack_only)

        def best_provider() -> str:
            return best_result_text(rows, paired)

        for spec in specs:
            run_base = (
                export_dir
                / f"stop_{spec.candidate.index}_{spec.phase}_seed_{spec.seed}_run_{spec.run_number}"
            )
            params = getattr(scenario, "params", None)
            if isinstance(params, dict):
                params.update(
                    {
                        "seed": spec.seed,
                        "attack_stop_index": spec.candidate.index,
                        "attack_enabled": spec.attack_enabled,
                        "run_name": str(run_base),
                    }
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
            dashboard.publish(
                status=(
                    f"Starting Scenic run {spec.run_number}/{len(specs)} ? "
                    f"{spec.phase} ? {spec.candidate.label}"
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
                    f"Worst congestion: stop[{best['candidate_index']}] ? "
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
