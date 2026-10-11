"""Scenic search over Town05 stop-sign color patches with a PCLA ego.

Each selected stop sign is evaluated with the same Scenic seed in a baseline run
and an attack run. The attack changes red pixels belonging to that one projected
stop-sign region to blue before PCLA receives its CARLA RGB tensors. The
dashboard shows those exact tensors together with live congestion measurements.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import json
import math
import os
import re
import random
import sys
import traceback
import types
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

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
from scenic_exp.runtime.external import StopSignControllerRuntime, stop_sign_test_parameters
from scenic_exp.runtime.integration import install_runtime_hook
from utils.cosim_support import (  # noqa: E402
    CarlaSensorPanel,
    _start_viz_with_port_fallback,
    image_array_to_png,
)


# Compatibility exports for existing demo callers; implementation lives in the runtime.
from scenic_exp.runtime.stop_sign import (  # noqa: E402, F401
    StopSignCandidate,
    _distance,
    _horizontal_distance,
    _environment_location,
    _match_stop_actors_to_signs,
    _stop_trigger_location,
    _find_stop_approach,
    discover_stop_signs,
    select_candidates,
    _transform_matrix,
    _actor_transform_matrix,
    annotate_overhead_target,
    project_stop_sign,
    StopSignColorPatchTap,
    _waypoint_location,
    _route_through_stop_quality,
    generate_route_via_stop,
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
    parser.add_argument(
        "--opendrive-map",
        default=None,
        help="Exact XODR loaded by both Scenic and CosimSimulator.",
    )
    parser.add_argument(
        "--sumo-map",
        default=None,
        help="Exact SUMO/METS-R network paired with --opendrive-map.",
    )
    parser.add_argument("--num-commuters", type=_positive_int, default=100)
    parser.add_argument(
        "--local-commuters", type=int, default=8,
        help="Commuters initially near the stop on its road layer, included in --num-commuters; 0 keeps citywide-only demand.",
    )
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
        help=(
            "Desired upstream lead-in; prefer a shorter approach outside a junction "
            "on the stop sign's road when available."
        ),
    )
    parser.add_argument("--attack-only", action="store_true")
    parser.add_argument("--attack-max-distance-m", type=float, default=90.0)
    parser.add_argument("--attack-min-red", type=int, default=90)
    parser.add_argument("--attack-red-dominance", type=float, default=1.25)
    parser.add_argument("--attack-roi-scale", type=float, default=1.7)
    parser.add_argument(
        "--attack-min-patch-pixels",
        type=_positive_int,
        default=25,
        help="Minimum recolored pixels in one camera frame for a meaningful patch.",
    )
    parser.add_argument(
        "--attack-min-valid-frames",
        type=_positive_int,
        default=3,
        help="Meaningful patched camera frames required for a valid attack run.",
    )
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
    if args.local_commuters < 0:
        parser.error("--local-commuters must be nonnegative")
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
    map_root = Path(args.map_locations)
    town_road_dir = map_root / args.town / "facility" / "road"
    map_dir = town_road_dir if town_road_dir.is_dir() else map_root
    for name, default_name in (
        ("opendrive_map", f"{args.town}.xodr"),
        ("sumo_map", f"{args.town}.net.xml"),
    ):
        configured = getattr(args, name)
        value = Path(configured).expanduser() if configured else map_dir / default_name
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
class RunSpec:
    run_number: int
    seed: int
    candidate: StopSignCandidate
    attack_enabled: bool

    @property
    def phase(self) -> str:
        return "attack" if self.attack_enabled else "baseline"


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

    def _runs_table_html_locked(self) -> str:
        if not self._run_rows:
            return "<div class='runs-empty'>Waiting for Scenic run definitions...</div>"
        header = "".join(
            f"<th>{escape(label)}</th>" for _, label in self._RUN_COLUMNS
        )
        rows = []
        for row in self._run_rows:
            status = re.sub(r"[^a-z0-9_-]", "-", str(row.get("status", "queued")).lower())
            cells = []
            for key, _ in self._RUN_COLUMNS:
                value = row.get(key)
                if value in (None, ""):
                    value = "n/a"
                elif key in {"congestion_score_s", "attack_delta_s"}:
                    try:
                        value = f"{float(value):.1f}"
                    except (TypeError, ValueError):
                        pass
                cells.append(f"<td>{escape(str(value))}</td>")
            rows.append(f"<tr class='scenic-run scenic-run--{status}'>" + "".join(cells) + "</tr>")
        body = "".join(rows)
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
    target_seen: bool = False
    stopped_at_target: bool = False
    resumed_after_stop: bool = False
    crossed_stop: bool = False
    stop_zone_stopped_s: float = 0.0
    current_stop_dwell_s: float = 0.0
    max_stop_dwell_s: float = 0.0
    signed_stop_progress_m: Optional[float] = None
    stop_lateral_m: Optional[float] = None

    @property
    def avg_network_speed_mps(self) -> Optional[float]:
        return (
            None if self.speed_samples <= 0
            else self.speed_sum_mps / self.speed_samples
        )

    @property
    def congestion_score_s(self) -> float:
        return self.network_delay_s + self.queue_vehicle_s


class RunRuntime(StopSignControllerRuntime):
    def __init__(
        self,
        args: argparse.Namespace,
        spec: RunSpec,
        dashboard: Demo4Dashboard,
        best_provider: Callable[[], str],
        *,
        test_parameters=None,
    ) -> None:
        super().__init__(args, spec, test_parameters=test_parameters)
        self.dashboard = dashboard
        self.best_provider = best_provider
        self.panel: Optional[CarlaSensorPanel] = None
        self.stats = RunStats()
        self.render_error = ""
        self.overhead_png: Optional[bytes] = None
        self.camera_views: Dict[str, Tuple[str, bytes, Any]] = {}
        self.ego_vehicle_id: Optional[Any] = None
        self.summary: Optional[Dict[str, Any]] = None

    def _ensure_controller(self) -> None:
        ready = self.controller is not None
        super()._ensure_controller()
        if ready:
            return
        ego, _ = self._ego()
        self.dashboard.configure_camera_views(self.tap.camera_specs)
        try:
            self.ego_vehicle_id = self.simulation.getMetsrPrivateVehId(ego)
        except Exception:
            self.ego_vehicle_id = getattr(ego, "name", 0)
        self.dashboard.ego_vehicle_id = self.ego_vehicle_id
        print(f"PCLA route for {self.spec.phase}: {self.route_path}")

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
        requested = self.last_control
        try:
            applied = actor.get_control()
        except Exception:
            applied = None
        return {
            "run": self.spec.run_number,
            "seed": self.spec.seed,
            "phase": self.spec.phase,
            "target_stop": self.spec.candidate.index,
            "target_id": self.spec.candidate.actor_id,
            "target_xyz": self.spec.candidate.location_text,
            "target_route_xyz": self.spec.candidate.location_text,
            "target_visual_xyz": self.spec.candidate.visual_location_text,
            "target_visual_id": self.spec.candidate.visual_id,
            "target_visual_name": self.spec.candidate.visual_name,
            "target_road": self.spec.candidate.road_id,
            "initial_xyz": self.spec.candidate.initial_location_text,
            "target_distance_m": (
                "n/a" if tap is None or tap.last_target_distance_m is None
                else f"{tap.last_target_distance_m:.1f}"
            ),
            "patched_frames": 0 if tap is None else tap.frames_patched,
            "patched_pixels": 0 if tap is None else tap.pixels_patched,
            "ego_speed_kmh": f"{pcla_demo._speed_kmh(actor):.1f}",
            "pcla_throttle": (
                "n/a" if requested is None
                else f"{float(getattr(requested, 'throttle', 0.0)):.3f}"
            ),
            "pcla_brake": (
                "n/a" if requested is None
                else f"{float(getattr(requested, 'brake', 0.0)):.3f}"
            ),
            "applied_throttle": (
                "n/a" if applied is None
                else f"{float(getattr(applied, 'throttle', 0.0)):.3f}"
            ),
            "applied_brake": (
                "n/a" if applied is None
                else f"{float(getattr(applied, 'brake', 0.0)):.3f}"
            ),
            "stop_progress_m": (
                "n/a" if self.stats.signed_stop_progress_m is None
                else f"{self.stats.signed_stop_progress_m:.1f}"
            ),
            "stop_zone_stopped_s": f"{self.stats.stop_zone_stopped_s:.1f}",
            "max_stop_dwell_s": f"{self.stats.max_stop_dwell_s:.1f}",
            "stop_outcome": self._outcome(),
            "network_speed_kmh": "n/a" if avg_speed is None else f"{avg_speed * 3.6:.1f}",
            "network_delay_s": f"{self.stats.network_delay_s:.1f}",
            "queue_vehicle_s": f"{self.stats.queue_vehicle_s:.1f}",
            "congestion_score_s": f"{self.stats.congestion_score_s:.1f}",
            "max_bubble_queue": self.stats.max_bubble_queue,
            "best_so_far": self.best_provider(),
        }

    def _outcome(self) -> str:
        tap = self.tap
        entered_attack_range = (
            tap is not None
            and tap.minimum_target_distance_m is not None
            and tap.minimum_target_distance_m <= self.args.attack_max_distance_m
        )
        if (
            self.spec.attack_enabled
            and entered_attack_range
            and tap is not None
            and tap.frames_patched == 0
        ):
            return "unpatchable"
        if self.stats.crossed_stop:
            return (
                "stopped_then_resumed"
                if self.stats.stopped_at_target
                else "passed_without_stop"
            )
        if self.stats.stopped_at_target and self.stats.max_stop_dwell_s >= 5.0:
            return "stalled_at_line"
        if self.stats.target_seen:
            return "approached_no_crossing"
        return "missed_target"

    def after_step(self) -> None:
        _, actor = self._ego()
        self.stats.steps += 1
        queue = len(getattr(self.simulation, "bubble_spawn_queue", []) or [])
        actors = len(getattr(self.simulation, "carla_actors", []) or [])
        self.stats.max_bubble_queue = max(self.stats.max_bubble_queue, queue)
        self.stats.max_carla_actors = max(self.stats.max_carla_actors, actors)
        self.stats.queue_vehicle_s += queue * self.args.timestep
        speed_kmh = pcla_demo._speed_kmh(actor)
        if speed_kmh < 1.0:
            self.stats.ego_stopped_s += self.args.timestep
        actor_location = actor.get_location()
        target_waypoint = self.spec.candidate.route_waypoint
        target_transform = getattr(target_waypoint, "transform", None)
        target_location = getattr(target_transform, "location", None)
        if target_location is None:
            target_location = self.spec.candidate.route_location
        target_distance = _horizontal_distance(actor_location, target_location)
        if target_distance <= 8.0:
            self.stats.target_seen = True
            if speed_kmh < 1.0:
                self.stats.stopped_at_target = True
                self.stats.stop_zone_stopped_s += self.args.timestep
                self.stats.current_stop_dwell_s += self.args.timestep
                self.stats.max_stop_dwell_s = max(
                    self.stats.max_stop_dwell_s,
                    self.stats.current_stop_dwell_s,
                )
            else:
                self.stats.current_stop_dwell_s = 0.0
        else:
            self.stats.current_stop_dwell_s = 0.0
        if self.stats.stopped_at_target and speed_kmh >= 3.6:
            self.stats.resumed_after_stop = True

        rotation = getattr(target_transform, "rotation", None)
        if rotation is not None:
            yaw = math.radians(float(getattr(rotation, "yaw", 0.0)))
            dx = float(actor_location.x) - float(target_location.x)
            dy = float(actor_location.y) - float(target_location.y)
            progress = dx * math.cos(yaw) + dy * math.sin(yaw)
            lateral = -dx * math.sin(yaw) + dy * math.cos(yaw)
            previous_progress = self.stats.signed_stop_progress_m
            self.stats.signed_stop_progress_m = progress
            self.stats.stop_lateral_m = lateral
            lane_width = float(getattr(target_waypoint, "lane_width", 3.5) or 3.5)
            if (
                self.stats.target_seen
                and abs(lateral) <= max(4.0, lane_width)
                and previous_progress is not None
                and previous_progress < 0.0 <= progress
            ):
                self.stats.crossed_stop = True

        if self.stats.steps % self.args.dashboard_every == 0:
            # Prefer the complete METS-R population; fall back to CARLA actors.
            client = self.simulation.metsr_client
            query = getattr(client, "query_visualization_vehicles", None)
            if not callable(query):
                # Independently installed Scenic clients still use the older name.
                query = getattr(client, "_query_viz_stream_vehicle_records", None)
            records = []
            if callable(query):
                try:
                    records = query(
                        transform_coords=False, include_public=True,
                        include_private=True, batch_size=1000,
                    )
                except Exception:
                    pass
            speeds = []
            for record in records or []:
                if not isinstance(record, Mapping):
                    continue
                value = scenic_demo.first_float(
                    record.get("speed"), record.get("speed_mps"), record.get("velocity"),
                )
                if value is not None and value >= 0:
                    speeds.append(float(value))
            if not speeds:
                try:
                    actors = self.simulation.carla_world.get_actors().filter("vehicle.*")
                except Exception:
                    actors = []
                for vehicle in actors:
                    try:
                        velocity = vehicle.get_velocity()
                        speeds.append(math.sqrt(
                            float(velocity.x) ** 2 + float(velocity.y) ** 2 + float(velocity.z) ** 2
                        ))
                    except Exception:
                        pass
            if speeds:
                interval_s = self.args.timestep * self.args.dashboard_every
                self.stats.network_delay_s += sum(
                    max(0.0, 1.0 - speed / self.args.free_flow_speed_mps) * interval_s
                    for speed in speeds
                )
                self.stats.speed_sum_mps += sum(speeds)
                self.stats.speed_samples += len(speeds)
        if self.stats.steps % self.args.render_every == 0:
            try:
                render = self.simulation.metsr_client.render
                try:
                    render(client_wait_timeout=0.0)
                except TypeError:
                    render()
                self.render_error = ""
            except Exception as exc:
                self.render_error = str(exc).splitlines()[0]
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
        outcome = self._outcome()
        if status == "finished" and outcome == "unpatchable":
            status = "unpatchable"
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
            "visual_id": candidate.visual_id,
            "visual_name": candidate.visual_name,
            "visual_x": float(candidate.visual_location.x),
            "visual_y": float(candidate.visual_location.y),
            "visual_z": float(candidate.visual_location.z),
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
            "stop_zone_stopped_s": self.stats.stop_zone_stopped_s,
            "max_stop_dwell_s": self.stats.max_stop_dwell_s,
            "stop_progress_m": self.stats.signed_stop_progress_m,
            "stop_lateral_m": self.stats.stop_lateral_m,
            "target_seen": self.stats.target_seen,
            "stopped_at_target": self.stats.stopped_at_target,
            "resumed_after_stop": self.stats.resumed_after_stop,
            "crossed_stop": self.stats.crossed_stop,
            "stop_outcome": outcome,
            "congestion_score_s": self.stats.congestion_score_s,
            "max_bubble_queue": self.stats.max_bubble_queue,
            "max_carla_actors": self.stats.max_carla_actors,
            "route": "" if self.route_path is None else str(self.route_path),
            "pcla_throttle": (
                None if self.last_control is None
                else float(getattr(self.last_control, "throttle", 0.0))
            ),
            "pcla_brake": (
                None if self.last_control is None
                else float(getattr(self.last_control, "brake", 0.0))
            ),
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
        if self.panel is not None:
            try:
                self.panel.close()
            except Exception:
                pass
            self.panel = None
        super().close()


class RuntimeHolder:
    def __init__(self) -> None:
        self.current: Optional[RunRuntime] = None


_RESULT_FIELDS = [
    "run", "seed", "status", "rank", "candidate_index", "candidate_id",
    "road_id", "route_is_junction", "target_x", "target_y", "target_z",
    "visual_id", "visual_name", "visual_x", "visual_y", "visual_z",
    "initial_x", "initial_y", "initial_z", "initial_carla_yaw",
    "approach_distance_m", "same_road_distance_m", "candidate_source", "phase",
    "attack_enabled", "patched_frames", "patched_pixels",
    "minimum_target_distance_m", "avg_network_speed_mps", "network_delay_s",
    "queue_vehicle_s", "ego_stopped_s", "congestion_score_s",
    "stop_zone_stopped_s", "max_stop_dwell_s", "stop_progress_m",
    "stop_lateral_m", "target_seen",
    "stopped_at_target", "resumed_after_stop", "crossed_stop", "stop_outcome",
    "pcla_throttle", "pcla_brake",
    "attack_delta_s", "max_bubble_queue", "max_carla_actors", "route",
    "artifact_base", "error",
]


def update_rankings(rows: List[Dict[str, Any]], paired: bool) -> None:
    by_candidate: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for row in rows:
        attack_is_valid = (
            row.get("phase") != "attack"
            or int(row.get("patched_frames") or 0) > 0
        )
        if row.get("status") == "finished" and attack_is_valid:
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
    kwargs: Dict[str, Any] = {
        "metsr_host": args.metsr_host,
        "metsr_port": args.metsr_port,
        "address": args.address,
        "carla_port": args.carla_port,
        "timeout": args.carla_timeout_s,
        "carla_map": args.town,
        "xml_map": args.sumo_map,
        "map_path": args.opendrive_map,
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


def _local_commuter_poses(
    world_map: Any, helper: Any, candidate: StopSignCandidate,
    count: int, seed: int, bubble_size: float,
) -> Tuple[Tuple[float, ...], ...]:
    """Choose initial local demand on the ego's connected ordinary road layer."""
    if count == 0:
        return ()
    from scenic.core.regions import CircularRegion

    start = candidate.approach_transform.location
    anchor = world_map.get_waypoint(start)
    radius = min(80.0, float(bubble_size) * 0.8)
    region = CircularRegion(center=(start.x, -start.y), radius=radius)
    roads = helper._get_bubble_roads(region, anchor_road_id=anchor.road_id)
    road_ids = {str(road.id) for road in roads}
    network = helper.workspace.network
    candidates = []
    for waypoint in world_map.generate_waypoints(4.0):
        if waypoint.is_junction or str(waypoint.road_id) not in road_ids:
            continue
        key = f"{waypoint.road_id}_{waypoint.lane_id}"
        if not helper.scenic_to_metsr_map_lanes.get(key):
            continue
        point = waypoint.transform.location
        distance = math.hypot(point.x - start.x, point.y - start.y)
        if not 15.0 <= distance <= radius:
            continue
        projected = world_map.get_waypoint(point)
        lane = network.laneAt((point.x, -point.y))
        if (projected is None or projected.is_junction or lane is None
                or (projected.road_id, projected.lane_id)
                != (waypoint.road_id, waypoint.lane_id)
                or f"{lane.road.id}_{lane.id}" != key):
            # Scenic's 2D lane lookup must agree at this initial pose, including
            # where an overpass overlaps the local ground road component.
            continue
        def ordinary_neighbor(neighbors):
            return any(not neighbor.is_junction
                       and (neighbor.road_id, neighbor.lane_id)
                       == (waypoint.road_id, waypoint.lane_id)
                       for neighbor in neighbors)
        if not (ordinary_neighbor(waypoint.previous(6.0))
                and ordinary_neighbor(waypoint.next(6.0))):
            continue
        candidates.append(waypoint)

    # A separate random stream makes the initial cohort independent of PCLA,
    # rendering, or traffic evolution. Stable ordering removes map iteration
    # order from the seed's meaning.
    candidates.sort(key=lambda waypoint: (
        waypoint.road_id, waypoint.section_id, waypoint.lane_id, waypoint.s))
    random.Random(seed).shuffle(candidates)
    yaw = math.radians(candidate.approach_transform.rotation.yaw)
    def behind_ego(waypoint):
        point = waypoint.transform.location
        dx, dy = point.x - start.x, point.y - start.y
        return (dx * math.cos(yaw) + dy * math.sin(yaw) < -10.0
                and abs(-dx * math.sin(yaw) + dy * math.cos(yaw)) < 5.0
                and math.cos(math.radians(waypoint.transform.rotation.yaw) - yaw) > .9)
    candidates.sort(key=lambda waypoint: not behind_ego(waypoint))
    selected = []
    for waypoint in candidates:
        point = waypoint.transform.location
        if any(math.hypot(point.x - pose[0], -point.y - pose[1]) < 10.0
               for pose in selected):
            continue
        heading = -math.radians(waypoint.transform.rotation.yaw + 90.0)
        heading = (heading + math.pi) % (2.0 * math.pi) - math.pi
        selected.append((float(point.x), -float(point.y), float(point.z),
                         heading, int(waypoint.road_id), int(waypoint.lane_id)))
        if len(selected) == count:
            return tuple(selected)
    raise RuntimeError(
        f"Only {len(selected)} spaced local commuter poses fit near {candidate.label}; "
        f"requested {count}. Reduce --local-commuters or use 0 for citywide-only demand."
    )


def _local_traffic_plan(args: argparse.Namespace, world_map: Any,
                        candidate: StopSignCandidate, seed: int) -> Tuple[Tuple[float, ...], ...]:
    count = min(args.local_commuters, args.num_commuters)
    if not count:
        return ()
    from scenic.domains.driving.roads import Network
    from scenic.simulators.cosim.utils.network_helper import network_cache
    from scenic.simulators.cosim.utils.utils import generate_map

    network = Network.fromFile(args.opendrive_map, writeCache=False)
    mapping = generate_map(args.sumo_map)
    helper = network_cache(types.SimpleNamespace(network=network), mapping, ())
    return _local_commuter_poses(world_map, helper, candidate, count, seed, args.bubble_size)


def _compile_scenario(
    args: argparse.Namespace,
    scenic: Any,
    set_debugging_options: Any,
    set_seed: Any,
    run_params: Optional[Mapping[str, Any]] = None,
) -> Any:
    set_debugging_options(verbosity=args.verbosity, fullBacktrace=False)
    set_seed(args.seed)
    params = {
        "address": args.address,
        "town": args.town,
        "map": args.opendrive_map,
        "xml_map": args.sumo_map,
        "num_commuters": args.num_commuters,
        "length": args.length,
        "timestep": args.timestep,
        "bubble_size": args.bubble_size,
        "seed": args.seed,
        "export_folder": args.export_folder,
        "allow_bubble_spawns": args.allow_bubble_spawns,
        "local_commuter_poses": (),
        "attack_stop_index": 0,
        "attack_enabled": False,
        "initial_x": 0.0,
        "initial_y": 0.0,
        "initial_heading": 0.0,
    }
    if run_params:
        params.update(dict(run_params))
    params["ego_controller"] = "external"
    params["security_scenario"] = "stop_sign_patch"
    params["test_parameters"] = stop_sign_test_parameters(
        args, index=params["attack_stop_index"], enabled=params["attack_enabled"],
    )
    return scenic.scenarioFromFile(
        path=args.scenic_file,
        model=args.scenic_model,
        mode2D=True,
        params=params,
    )


def _verify_carla_opendrive(args: argparse.Namespace, world: Any) -> str:
    """Fail before Scenic compilation if CARLA and the selected XODR differ."""
    path = Path(args.opendrive_map)
    if not path.is_file():
        raise FileNotFoundError(f"OpenDRIVE map does not exist: {path}")
    sumo_path = Path(args.sumo_map)
    if not sumo_path.is_file():
        raise FileNotFoundError(f"SUMO/METS-R map does not exist: {sumo_path}")
    world_map = world.get_map()
    live_opendrive = world_map.to_opendrive()
    selected_bytes = path.read_bytes()
    live_bytes = live_opendrive.encode("utf-8")
    selected_digest = hashlib.sha256(selected_bytes).hexdigest()
    live_digest = hashlib.sha256(live_bytes).hexdigest()
    if selected_digest != live_digest:
        raise RuntimeError(
            f"OpenDRIVE mismatch: Scenic/Cosim selected {path} "
            f"(sha256 {selected_digest[:12]}), but CARLA {world_map.name} exposes "
            f"sha256 {live_digest[:12]}. Pass --opendrive-map for the CARLA map "
            "actually loaded by the server."
        )
    print(
        f"Verified Scenic/CARLA OpenDRIVE: {path} "
        f"(sha256 {selected_digest[:12]})."
    )
    return selected_digest


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
    uninstall_runtime = None
    dashboard: Optional[Demo4Dashboard] = None
    exit_code = 0
    try:
        simulator = CosimSimulator(
            **_simulator_kwargs(args, CosimSimulator, export_dir / "demo4_pending")
        )
        _verify_carla_opendrive(args, simulator.world)
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
        uninstall_runtime = install_runtime_hook(simulator, holder)

        def best_provider() -> str:
            return best_result_text(rows, paired)

        local_plans: Dict[Tuple[int, int], Tuple[Tuple[float, ...], ...]] = {}
        for spec in specs:
            run_base = (
                export_dir
                / f"stop_{spec.candidate.index}_{spec.phase}_seed_{spec.seed}_run_{spec.run_number}"
            )
            plan_key = (spec.candidate.index, spec.seed)
            if plan_key not in local_plans:
                local_plans[plan_key] = _local_traffic_plan(
                    args, simulator.world.get_map(), spec.candidate, spec.seed)
            local_poses = local_plans[plan_key]
            run_base.parent.mkdir(parents=True, exist_ok=True)
            run_base.with_name(run_base.name + "_local_traffic.json").write_text(
                json.dumps({"seed": spec.seed, "local_commuters": len(local_poses),
                            "citywide_commuters": args.num_commuters - len(local_poses),
                            "pose_fields": ["scenic_x", "scenic_y", "z", "heading", "carla_road", "carla_lane"],
                            "poses": local_poses}, indent=2), encoding="utf-8")
            print(f"Demand: {len(local_poses)} local commuters and "
                  f"{args.num_commuters - len(local_poses)} citywide commuters, seed={spec.seed}")
            run_params: Dict[str, Any] = _scenic_initial_pose(spec.candidate)
            run_params.update(
                {
                    "seed": spec.seed,
                    "attack_stop_index": spec.candidate.index,
                    "attack_enabled": spec.attack_enabled,
                    "run_name": str(run_base),
                    "local_commuter_poses": local_poses,
                }
            )
            scenario = _compile_scenario(
                args, scenic, setDebuggingOptions, setSeed, run_params
            )
            setSeed(spec.seed)
            scene, _ = scenario.generate()
            simulator.run_name = str(run_base)
            runtime = RunRuntime(args, spec, dashboard, best_provider,
                                 test_parameters=scenario.params["test_parameters"])
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
                traceback_text = traceback.format_exc()
                traceback_path = run_base.with_name(run_base.name + "_traceback.txt")
                traceback_path.write_text(traceback_text, encoding="utf-8")
                print(traceback_text, file=sys.stderr, end="")
                print(f"Demo4 traceback: {traceback_path}")
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
        if uninstall_runtime is not None:
            uninstall_runtime()
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
