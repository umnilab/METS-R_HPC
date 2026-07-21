"""Scenic-driven TRACR dashboard demo.

Run this script with a Scenic file and open the printed dashboard URL.
The dashboard can render METS-R Vis from Scenic step boundaries using the single
METS-R client. Pass --wait-for-space to pause before Scenic starts.

Scenic requirement:
    Install and use the METSRSim branch from
    https://github.com/Kv139/Scenic/tree/METSRSim
"""

from __future__ import annotations

import argparse
import csv
import functools
import json
import math
import os
import sys
import threading
import time
import types
from dataclasses import dataclass, fields
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_THIS_DIR = Path(__file__).resolve().parent
_TUTORIALS_DIR = _THIS_DIR.parent
_REPO_ROOT = _TUTORIALS_DIR.parent
_SCENIC_EXP_DIR = _REPO_ROOT / "scenic_exp"
_SCENIC_SOURCE_URL = "https://github.com/Kv139/Scenic/tree/METSRSim"
_DEFAULT_SCENARIO = _SCENIC_EXP_DIR / "scenarios" / "constant_flow_n_bubble_n_attack.scenic"
_DEFAULT_MAP_DIR = _REPO_ROOT / "data" / "CARLA"
_DEFAULT_EXPORT_DIR = _SCENIC_EXP_DIR / "data_logs" / "CARLA_06" / "constant_flow"
_DEFAULT_DASHBOARD_DIR = _REPO_ROOT / "output" / "tracr_demo2_dashboard"

for path in (_REPO_ROOT, _THIS_DIR):
    text_path = str(path)
    if text_path not in sys.path:
        sys.path.insert(0, text_path)

from utils.cosim_support import (
    CarlaSensorPanel,
    METS_R_VIS_PRIVATE_VEHICLE_TYPE,
    TRACRDashboard,
    blank_png,
    metsr_vis_vehicle_type_for_record,
)


@dataclass
class Args:
    scenic_file: str = str(_DEFAULT_SCENARIO)
    scenic_model: str = "scenic.simulators.cosim.model"
    verbosity: int = 2
    seed: int = 33
    increment_seed: bool = True
    total_simulations: int = 1
    address: str = "127.0.0.1"
    carla_host: Optional[str] = None
    town: str = "Town06"
    map_locations: str = str(_DEFAULT_MAP_DIR)
    num_commuters: int = 5
    timestep: float = 0.1
    length: int = 10
    bubble_size: int = 100
    metsr_host: str = "localhost"
    metsr_port: int = 4000
    carla_port: int = 2000
    carla_timeout_s: float = 60.0
    output_root: str = str(_REPO_ROOT / "output")
    metsr_sim_dir: Optional[str] = None
    export_folder: str = str(_DEFAULT_EXPORT_DIR)
    dashboard_dir: str = str(_DEFAULT_DASHBOARD_DIR)
    dashboard_port: int = 8898
    viz_url: str = "https://engineering.purdue.edu/HSEES/METSRVis/"
    metsr_viz_map: Optional[int] = None
    metsr_viz_vehicle_type: int = METS_R_VIS_PRIVATE_VEHICLE_TYPE
    viz_stream_host: str = "0.0.0.0"
    viz_stream_port: int = 8766
    viz_initial_x: Optional[float] = None
    viz_initial_y: Optional[float] = None
    open_browser: bool = False
    wait_for_space: bool = False
    hold_dashboard: bool = True
    render_interval_s: float = 0.25
    viz_stream_warmup_s: float = 1.0
    client_retry_interval_s: float = 2.0
    client_connect_wait_s: float = 6.0
    require_viz_client: bool = False
    render_client_wait_timeout_s: float = 0.0
    viz_render_thread: bool = False
    viz_render_on_step: bool = True
    viz_render_step_interval: int = 1
    viz_include_public: bool = False
    viz_include_private: bool = True
    viz_include_links: bool = False
    viz_transform_coords: bool = False
    carla_sensor_panels: bool = True
    speedy_mode: bool = False
    carla_camera_z: float = 205.8
    carla_autopilot_compat: bool = False
    carla_autopilot_speed_kmh: float = 30.0
    carla_tm_ignore_lights_percent: float = 100.0
    carla_tm_ignore_signs_percent: float = 100.0
    highlight_ego_vehicle_id: int = 0
    metsr_tick_seconds: float = 0.1
    opendrive_map: Optional[str] = None
    sumo_map: Optional[str] = None


def parse_args(argv: Optional[Sequence[str]] = None) -> Args:
    defaults = Args()
    parser = argparse.ArgumentParser(
        description="Run a Scenic scenario with a TRACR-style live dashboard.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=f"Required Scenic fork: {_SCENIC_SOURCE_URL}",
    )
    parser.add_argument("scenic_file_arg", nargs="?", help="Scenic program to run.")
    for field in fields(Args):
        name = field.name
        default = getattr(defaults, name)
        option = "--" + name.replace("_", "-")
        if isinstance(default, bool):
            group = parser.add_mutually_exclusive_group()
            group.add_argument(option, dest=name, action="store_true")
            group.add_argument("--no-" + name.replace("_", "-"), dest=name, action="store_false")
            parser.set_defaults(**{name: default})
        elif default is None:
            parser.add_argument(option, dest=name, default=default)
        else:
            parser.add_argument(option, dest=name, default=default, type=type(default))
    namespace = parser.parse_args(argv)
    args = Args()
    for field in fields(Args):
        setattr(args, field.name, getattr(namespace, field.name))
    if namespace.scenic_file_arg:
        args.scenic_file = namespace.scenic_file_arg
    return normalize_args(args)



def metsr_vis_map_for_town(town: str) -> int:
    text = str(town or "").strip().lower()
    if text.startswith("town"):
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return 3 + int(digits)
    return 12


def resolve_metsr_sim_folder(args: Args) -> Optional[str]:
    if not args.metsr_sim_dir:
        return None
    raw_path = Path(str(args.metsr_sim_dir)).expanduser()
    if raw_path.is_absolute():
        return str(raw_path.resolve())

    output_root = Path(args.output_root).expanduser().resolve()
    if raw_path.parts and raw_path.parts[0].lower() == output_root.name.lower():
        return str((_REPO_ROOT / raw_path).resolve())
    return str((output_root / raw_path).resolve())


def wsl_windows_host() -> Optional[str]:
    """Return the Windows host address when running under WSL NAT."""
    if os.name != "posix":
        return None
    try:
        release = Path("/proc/sys/kernel/osrelease").read_text(encoding="utf-8")
    except OSError:
        release = ""
    if "microsoft" not in release.lower() and "WSL_DISTRO_NAME" not in os.environ:
        return None

    try:
        route_lines = Path("/proc/net/route").read_text(encoding="utf-8").splitlines()
    except OSError:
        route_lines = []
    for line in route_lines[1:]:
        columns = line.split()
        if len(columns) < 4 or columns[1] != "00000000":
            continue
        try:
            gateway = int(columns[2], 16)
            flags = int(columns[3], 16)
        except ValueError:
            continue
        if flags & 0x2:
            address = ".".join(
                str((gateway >> (8 * index)) & 0xFF) for index in range(4)
            )
            if address != "0.0.0.0":
                return address

    try:
        resolv_lines = Path("/etc/resolv.conf").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in resolv_lines:
        columns = line.split()
        if len(columns) == 2 and columns[0] == "nameserver":
            return columns[1]
    return None


def normalize_args(args: Args) -> Args:
    args.scenic_file = str(Path(args.scenic_file).expanduser().resolve())
    map_root = Path(args.map_locations).expanduser().resolve()
    args.map_locations = str(map_root)
    args.export_folder = str(Path(args.export_folder).expanduser().resolve())
    args.output_root = str(Path(args.output_root).expanduser().resolve())
    args.dashboard_dir = str(Path(args.dashboard_dir).expanduser().resolve())
    args.metsr_viz_map = metsr_vis_map_for_town(args.town) if args.metsr_viz_map in (None, "") else int(args.metsr_viz_map)
    args.metsr_viz_vehicle_type = int(args.metsr_viz_vehicle_type)
    town_road_dir = map_root / args.town / "facility" / "road"
    map_dir = town_road_dir if town_road_dir.is_dir() else map_root
    args.opendrive_map = str(Path(args.opendrive_map).expanduser().resolve()) if args.opendrive_map else str(map_dir / f"{args.town}.xodr")
    args.sumo_map = str(Path(args.sumo_map).expanduser().resolve()) if args.sumo_map else str(map_dir / f"{args.town}.net.xml")
    configured_host = str(args.carla_host or args.address)
    if args.carla_host:
        args.address = configured_host
    elif configured_host.lower() in {"127.0.0.1", "localhost", "::1"}:
        detected_host = wsl_windows_host()
        if detected_host:
            args.address = detected_host
            print(
                f"WSL detected: using Windows CARLA host {detected_host} "
                f"instead of {configured_host}."
            )
    if float(args.carla_timeout_s) <= 0:
        raise ValueError("--carla-timeout-s must be greater than zero")
    if args.metsr_sim_dir:
        args.metsr_sim_dir = resolve_metsr_sim_folder(args)
    return args


def first_float(*values: Any) -> Optional[float]:
    for value in values:
        if value is None or value == "":
            continue
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            return parsed
    return None


def mean(values: Sequence[float]) -> Optional[float]:
    valid = [float(value) for value in values if math.isfinite(float(value))]
    return sum(valid) / len(valid) if valid else None


def fmt(value: Optional[float], digits: int = 1, suffix: str = "") -> str:
    if value is None or not math.isfinite(float(value)):
        return "—"
    return f"{float(value):.{digits}f}{suffix}"

class ScenicTRACRDashboard(TRACRDashboard):
    def __init__(self, directory: str, port: int, viz_url: str, open_browser_flag: bool = False, metsr_viz_map: int = 12, metsr_viz_vehicle_type: int = METS_R_VIS_PRIVATE_VEHICLE_TYPE, speedy_mode: bool = False):
        self.bsm_panel_title = "Scenic multi-round experiment"
        self.bsm_show_map = False
        super().__init__(
            viz_url=viz_url,
            stream_url=None,
            fullscreen=True,
            local_viz_patch=False,
            bsm_stream_label="Scenic metrics",
            metsr_viz_map=metsr_viz_map,
            metsr_viz_vehicle_type=metsr_viz_vehicle_type,
            external_speedy_mode=speedy_mode,
        )
        self.directory = Path(directory)
        self.port = int(port)
        self.open_browser_flag = bool(open_browser_flag)
        self.speedy_mode = bool(speedy_mode)
        self._scenic_lock = threading.Lock()
        self.scenario_text = ""
        self.run_state = "idle"
        self.speed: Dict[str, Any] = {"tick": None, "vehicle_count": 0, "avg_mps": None, "avg_mph": None, "updated_at": None}
        self.results: List[Dict[str, Any]] = []
        self.current_run: Optional[int] = None
        self.expected_ticks = 0
        self._run_telemetry: Dict[str, Any] = {}
        self._camera_png = blank_png("Scenic co-simulation is running in CARLA")
        self._vehicle_camera_png = blank_png("No separate vehicle camera panel in Scenic demo")
        self._lidar_png = blank_png("No LiDAR panel in Scenic demo")
        self._update_metrics_html_locked()

    def start(self) -> str:
        return self.display_external(
            directory=str(self.directory),
            port=int(self.port),
            open_browser=bool(self.open_browser_flag),
            speedy_mode=bool(self.speedy_mode),
        )

    def stop(self) -> None:
        self.stop_external()

    def set_status(self, text: str, run_state: Optional[str] = None) -> None:
        with self._scenic_lock:
            if run_state is not None:
                self.run_state = str(run_state)
            self._status_text = self._compose_status(str(text))
            self._update_metrics_html_locked()
        self.update_status(self._status_text, force_external=True)

    def set_scenario(self, text: str) -> None:
        with self._scenic_lock:
            self.scenario_text = str(text)
            self._status_text = self._compose_status(self.run_state)
            self._update_metrics_html_locked()
        self.update_status(self._status_text, force_external=True)

    def set_stream(self, stream_url: str, probe: str = "") -> None:
        self.stream_url = str(stream_url or "")
        self.stream_probe = {"ok": bool(stream_url), "url": self.stream_url, "error": ""}
        if probe and not stream_url:
            self.stream_probe = {"ok": False, "url": self.stream_url, "error": str(probe)}
        self._sync_metsr_vis_frame_url(force_external=True)

    def configure_runs(self, run_seeds: Sequence[int], expected_ticks: int) -> None:
        with self._scenic_lock:
            self.expected_ticks = max(0, int(expected_ticks))
            self.results = [
                {
                    "run": run_number,
                    "seed": int(seed),
                    "status": "queued",
                    "progress": "",
                }
                for run_number, seed in enumerate(run_seeds, start=1)
            ]
            self._update_metrics_html_locked()
        self._refresh_external_state(force=True)

    def begin_run(self, run_number: int, run_seed: int) -> None:
        with self._scenic_lock:
            self.current_run = int(run_number)
            self.run_state = "running"
            self.speed = {"tick": None, "vehicle_count": 0, "avg_mps": None, "avg_mph": None, "updated_at": None}
            self._run_telemetry = {
                "avg_mph_sum": 0.0,
                "avg_mph_samples": 0,
                "max_vehicle_count": 0,
                "ego_spawned_link": None,
                "last_tick": None,
            }
            self._update_run_locked(
                run_number,
                {
                    "seed": int(run_seed),
                    "status": "running",
                    "progress": self._progress_text(None),
                    "error": "",
                },
            )
            self._update_metrics_html_locked()
        self._refresh_external_state(force=True)

    def update_run_status(self, run_number: int, status: str, **updates: Any) -> None:
        with self._scenic_lock:
            self.run_state = str(status)
            self._update_run_locked(run_number, {"status": str(status), **updates})
            self._update_metrics_html_locked()
        self._refresh_external_state(force=True)

    def set_run_result(self, result: Dict[str, Any]) -> None:
        run_number = int(result.get("run", self.current_run or 0))
        with self._scenic_lock:
            values = dict(result or {})
            values.setdefault("status", "finished")
            values.setdefault("progress", self._progress_text(self._run_telemetry.get("last_tick")))
            self._update_run_locked(run_number, values)
            self._update_metrics_html_locked()
        self._refresh_external_state(force=True)

    def update_speed(self, speed: Dict[str, Any]) -> None:
        with self._scenic_lock:
            self.speed = dict(speed or {})
            selected_vehicle_id = self.speed.get("selected_vehicle_id")
            selected_vehicle_type = self.speed.get("selected_vehicle_type")
            if selected_vehicle_id is not None:
                self.ego_vehicle_id = selected_vehicle_id
            if selected_vehicle_type is not None:
                self.metsr_viz_vehicle_type = selected_vehicle_type
            if self.current_run is not None:
                tick = first_float(self.speed.get("tick"))
                avg_mph = first_float(self.speed.get("avg_mph"))
                vehicle_count = first_float(self.speed.get("vehicle_count"))
                selected_vehicle_road = self.speed.get("selected_vehicle_road")
                if tick is not None:
                    self._run_telemetry["last_tick"] = int(tick)
                if avg_mph is not None:
                    self._run_telemetry["avg_mph_sum"] += avg_mph
                    self._run_telemetry["avg_mph_samples"] += 1
                if vehicle_count is not None:
                    self._run_telemetry["max_vehicle_count"] = max(
                        int(self._run_telemetry.get("max_vehicle_count", 0)),
                        int(vehicle_count),
                    )
                if self._run_telemetry.get("ego_spawned_link") in (None, "") and selected_vehicle_road not in (None, ""):
                    self._run_telemetry["ego_spawned_link"] = selected_vehicle_road
                self._update_run_locked(
                    self.current_run,
                    {"progress": self._progress_text(self._run_telemetry.get("last_tick"))},
                )
            self._status_text = self._compose_status(self.run_state or "running")
            self._update_metrics_html_locked()
        self._sync_metsr_vis_frame_url(force_external=True)

    def add_result(self, result: Dict[str, Any]) -> None:
        self.set_run_result(result)

    def latest_speed(self) -> Dict[str, Any]:
        with self._scenic_lock:
            return dict(self.speed or {})

    def current_run_telemetry(self) -> Dict[str, Any]:
        with self._scenic_lock:
            sample_count = int(self._run_telemetry.get("avg_mph_samples", 0) or 0)
            avg_mph = None
            if sample_count > 0:
                avg_mph = float(self._run_telemetry.get("avg_mph_sum", 0.0)) / sample_count
            return {
                "avg_mph": avg_mph,
                "max_vehicle_count": int(self._run_telemetry.get("max_vehicle_count", 0) or 0),
                "ego_spawned_link": self._run_telemetry.get("ego_spawned_link"),
                "last_tick": self._run_telemetry.get("last_tick"),
            }

    def update_sensor_images(self, camera_png: bytes, vehicle_camera_png: bytes, lidar_png: bytes) -> None:
        with self._scenic_lock:
            if camera_png:
                self._camera_png = camera_png
            if vehicle_camera_png:
                self._vehicle_camera_png = vehicle_camera_png
            if lidar_png:
                self._lidar_png = lidar_png
        self._refresh_external_state(force=False)

    def _compose_status(self, text: str) -> str:
        parts = [str(text or "Ready")]
        if self.scenario_text:
            parts.append(self.scenario_text)
        if self.run_state:
            parts.append(f"state={self.run_state}")
        tick = self.speed.get("tick") if isinstance(self.speed, dict) else None
        avg_mph = self.speed.get("avg_mph") if isinstance(self.speed, dict) else None
        if tick is not None:
            parts.append(f"tick={tick}")
        if avg_mph is not None:
            parts.append(f"avg={fmt(avg_mph, 1, ' mph')}")
        return " | ".join(parts)

    def _progress_text(self, tick: Optional[Any]) -> str:
        parsed_tick = first_float(tick)
        if parsed_tick is None:
            return f"0/{self.expected_ticks}" if self.expected_ticks > 0 else ""
        current = max(0, int(parsed_tick))
        if self.expected_ticks > 0:
            return f"{min(current, self.expected_ticks)}/{self.expected_ticks}"
        return str(current)

    def _update_run_locked(self, run_number: int, updates: Dict[str, Any]) -> None:
        for row in self.results:
            if int(row.get("run", -1)) == int(run_number):
                row.update(updates)
                return
        self.results.append({"run": int(run_number), **updates})

    @staticmethod
    def _display_value(key: str, value: Any) -> str:
        if value in (None, ""):
            return "—"
        if key == "avg_speed_mph":
            return fmt(first_float(value), 1, " mph")
        if key in ("completed_trips", "completed_routes", "max_active_vehicles"):
            parsed = first_float(value)
            return "—" if parsed is None else str(int(round(parsed)))
        return str(value)

    def _update_metrics_html_locked(self) -> None:
        sp = self.speed or {}
        current_row = next((row for row in self.results if int(row.get("run", -1)) == int(self.current_run or -1)), {})
        live_items = [
            ("run", f"{self.current_run}/{len(self.results)}" if self.current_run is not None else "—"),
            ("seed", current_row.get("seed")),
            ("state", self.run_state or "idle"),
            ("progress", self._progress_text(sp.get("tick"))),
            ("vehicles", sp.get("vehicle_count", 0)),
            ("avg", fmt(sp.get("avg_mph"), 1, " mph")),
            ("ego link", self._run_telemetry.get("ego_spawned_link") or sp.get("selected_vehicle_road")),
        ]
        live_html = "".join(
            "<span class='scenic-live-item'>"
            f"<b>{escape(str(key))}</b> {escape(self._display_value(str(key), value))}"
            "</span>"
            for key, value in live_items
        )
        result_columns = [
            ("run", "run"),
            ("seed", "seed"),
            ("status", "status"),
            ("progress", "progress"),
            ("ego_spawned_link", "ego link"),
            ("completed_trips", "trips"),
            ("completed_routes", "routes"),
            ("avg_speed_mph", "avg speed"),
            ("max_active_vehicles", "max vehicles"),
        ]
        if self.results:
            header = "".join(f"<th>{escape(label)}</th>" for _, label in result_columns)
            body = "".join(
                f"<tr class='scenic-run scenic-run--{escape(str(row.get('status', 'queued')))}'>"
                + "".join(
                    f"<td>{escape(self._display_value(key, row.get(key)))}</td>"
                    for key, _ in result_columns
                )
                + "</tr>"
                for row in self.results
            )
            results_html = f"<table class='tracr-table scenic-runs-table'><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
        else:
            results_html = "<div class='tracr-empty'>Waiting for Scenic run summaries...</div>"
        self._bsm_table_html = (
            "<div class='scenic-metrics-layout'>"
            f"<div class='scenic-live-strip'>{live_html}</div>"
            f"<div class='scenic-runs-wrap'>{results_html}</div>"
            "</div>"
        )

    def _external_css(self):
        return super()._external_css() + """
          #bsm-table {overflow: hidden;}
          .scenic-metrics-layout {
            height: 100%;
            min-height: 0;
            display: flex;
            flex-direction: column;
            gap: 6px;
          }
          .scenic-live-strip {
            flex: 0 0 auto;
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 5px 7px;
            overflow-x: auto;
            white-space: nowrap;
            border: 1px solid #475569;
            border-radius: 5px;
            background: #111827;
            color: #e2e8f0;
            font-size: 11px;
          }
          .scenic-live-item b {color: #93c5fd;}
          .scenic-runs-wrap {
            flex: 1 1 auto;
            min-height: 0;
            overflow: auto;
          }
          .scenic-runs-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 10px;
          }
          .scenic-runs-table th {top: 0; z-index: 2;}
          .scenic-run--running td {background: #dbeafe;}
          .scenic-run--finalizing td {background: #fef3c7;}
          .scenic-run--finished td {background: #dcfce7;}
          .scenic-run--failed td, .scenic-run--interrupted td {background: #fee2e2; color: #991b1b;}
        """

    def _bsm_panel_title(self):
        return self.bsm_panel_title

    def _bsm_panel_show_map(self):
        return self.bsm_show_map


def patch_metsr_client_viz_compat(client: Any) -> None:
    if getattr(client, "_tracr_viz_compat_patched", False):
        return

    def wait_for_viz_stream_server(self: Any, host: str, server_port: int, startup_timeout: float = 3) -> None:
        from websockets.sync.client import connect

        connect_host = host if host not in ("", "0.0.0.0", "::") else "127.0.0.1"
        uri = f"ws://{connect_host}:{int(server_port)}"
        deadline = time.time() + max(0.1, float(startup_timeout or 0.1))
        last_error = None
        while time.time() <= deadline:
            try:
                try:
                    websocket = connect(uri, open_timeout=0.5)
                except TypeError:
                    websocket = connect(uri)
                with websocket:
                    return
            except Exception as exc:
                last_error = exc
                time.sleep(0.1)
        raise RuntimeError(f"METS-R Vis stream server did not become reachable at {uri}") from last_error

    client._wait_for_viz_stream_server = types.MethodType(wait_for_viz_stream_server, client)
    client._tracr_viz_compat_patched = True


def install_metsr_client_lock(client: Any, lock: Any) -> None:
    if client is None or getattr(client, "_tracr_lock_patched", False):
        return
    method_names = [
        "send_msg",
        "receive_msg",
        "send_receive_msg",
        "tick",
        "query_tick",
        "render",
        "start_viz",
        "stop_viz",
        "stop_viz_stream",
    ]
    for name in method_names:
        method = getattr(client, name, None)
        if not callable(method):
            continue

        def locked_method(*method_args: Any, __method=method, **method_kwargs: Any) -> Any:
            with lock:
                return __method(*method_args, **method_kwargs)

        setattr(client, name, locked_method)
    client._tracr_lock_patched = True


def patch_cosim_carla_motion_compat(args: Args) -> None:
    if bool(getattr(args, "carla_autopilot_compat", False)):
        print("Using Scenic's native CARLA motion path; TRACR Traffic Manager override is disabled.")


class VizRenderWorker:
    def __init__(self, args: Args, dashboard: ScenicTRACRDashboard, client: Any, client_lock: Any, owns_client: bool = False):
        self.args = args
        self.dashboard = dashboard
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="tracr-demo2-viz", daemon=True)
        self.client = client
        self.client_lock = client_lock
        self.owns_client = bool(owns_client)
        self.viz_started = False
        self.thread_started = False
        self.last_error = ""

    def start(self) -> None:
        if not self.thread_started:
            self.thread.start()
            self.thread_started = True

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread_started:
            self.thread.join(timeout=2.0)
        self._stop_viz_stream()
        if self.owns_client and self.client is not None:
            close = getattr(self.client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            self.client = None

    def wait_until_ready(self, timeout_s: float) -> bool:
        return self.ready_event.wait(timeout=max(0.0, float(timeout_s)))

    def start_stream(self) -> str:
        if not self.viz_started:
            self._connect_client()
        return str(self.dashboard.stream_url or "")

    def render_once(self) -> str:
        if not self.viz_started:
            self._connect_client()
        render_error, render_info = self._render_frame()
        sampled_speed = self._sample_speed()
        if render_info:
            self.dashboard.update_speed(self._speed_from_render_info(render_info, sampled_speed))
        else:
            self.dashboard.update_speed(sampled_speed)
        if render_error:
            self.dashboard.set_status(f"METS-R Vis waiting: {render_error}")
        return render_error

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.render_once()
                self.stop_event.wait(max(0.05, float(self.args.render_interval_s)))
            except Exception as exc:
                self.last_error = str(exc).splitlines()[0]
                self.dashboard.set_status(f"METS-R Vis worker waiting: {self.last_error}", run_state="waiting")
                self.stop_event.wait(max(0.2, float(self.args.client_retry_interval_s)))

    def _connect_client(self) -> None:
        from utils.cosim_support import _start_viz_with_port_fallback

        if self.client is None:
            raise RuntimeError("CosimSimulator did not expose a METS-R client for visualization.")
        with self.client_lock:
            viz_info = _start_viz_with_port_fallback(self.client, {
                "server_port": int(self.args.viz_stream_port),
                "host": self.args.viz_stream_host,
                "tick_interval": 1,
                "transform_coords": bool(self.args.viz_transform_coords),
                "include_public": bool(self.args.viz_include_public),
                "include_private": bool(self.args.viz_include_private),
                "include_links": bool(self.args.viz_include_links),
                "link_snapshot_interval": 1,
                "initial_x": self.args.viz_initial_x,
                "initial_y": self.args.viz_initial_y,
            })
        stream_url = viz_info.get("browser_url") or viz_info.get("url") or ""
        self.viz_started = True
        self.ready_event.set()
        self.dashboard.set_stream(stream_url, "METS-R Vis stream connected")
        self.dashboard.set_status("METS-R Vis stream connected")

    def _stop_viz_stream(self) -> None:
        self.ready_event.clear()
        if not self.viz_started or self.client is None:
            return
        self.viz_started = False
        with self.client_lock:
            for method_name in ("stop_viz", "stop_viz_stream"):
                method = getattr(self.client, method_name, None)
                if callable(method):
                    try:
                        method()
                    except Exception:
                        pass
                    break

    def _sample_speed(self) -> Dict[str, Any]:
        client = self.client
        if client is None:
            return {"tick": None, "vehicle_count": 0, "avg_mps": None, "avg_mph": None, "updated_at": time.time()}
        records = []
        with self.client_lock:
            try:
                tick = int(client.query_tick())
            except Exception:
                tick = getattr(client, "current_tick", None)
            query = getattr(client, "_query_viz_stream_vehicle_records", None)
            if callable(query):
                try:
                    records = query(
                        transform_coords=bool(self.args.viz_transform_coords),
                        include_public=bool(self.args.viz_include_public),
                        include_private=bool(self.args.viz_include_private),
                        batch_size=1000,
                    )
                except Exception as exc:
                    self.last_error = str(exc).splitlines()[0]
        speeds = []
        selected_vehicle_id = None
        selected_vehicle_type = None
        selected_vehicle_road = None
        preferred_vehicle_id = str(int(self.args.highlight_ego_vehicle_id))
        fallback_record = None
        preferred_record = None

        def record_vehicle_id(record: Dict[str, Any]) -> Any:
            return next((record.get(key) for key in ("ID", "id", "vehID", "vehicle_id", "vid") if record.get(key) is not None), None)

        for record in records or []:
            if not isinstance(record, dict):
                continue
            vehicle_id = record_vehicle_id(record)
            if fallback_record is None and vehicle_id is not None:
                fallback_record = record
            if vehicle_id is not None and str(vehicle_id) == preferred_vehicle_id:
                preferred_record = record
            speed = first_float(record.get("speed"), record.get("speed_mps"), record.get("velocity"))
            if speed is not None and speed >= 0.0:
                speeds.append(speed)

        selected_record = preferred_record or fallback_record
        if selected_record is not None:
            selected_vehicle_id = record_vehicle_id(selected_record)
            selected_vehicle_type = metsr_vis_vehicle_type_for_record(selected_record, default=self.args.metsr_viz_vehicle_type)
            selected_vehicle_road = next((selected_record.get(key) for key in ("road", "roadID", "road_id", "link", "edge") if selected_record.get(key) not in (None, "")), None)
        avg_mps = mean(speeds)
        return {
            "tick": tick,
            "vehicle_count": len(speeds),
            "avg_mps": avg_mps,
            "avg_mph": None if avg_mps is None else avg_mps * 2.2369362921,
            "updated_at": time.time(),
            "selected_vehicle_id": selected_vehicle_id,
            "selected_vehicle_type": selected_vehicle_type,
            "selected_vehicle_road": selected_vehicle_road,
        }

    def _speed_from_render_info(self, render_info: Dict[str, Any], sampled_speed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        speed = dict(sampled_speed or {})
        vehicle_count = first_float(render_info.get("vehicle_count"), render_info.get("vehicleCount"))
        if speed.get("tick") is None:
            speed["tick"] = render_info.get("tick")
        if speed.get("vehicle_count") in (None, 0) and vehicle_count is not None:
            speed["vehicle_count"] = int(vehicle_count)
        speed.setdefault("avg_mps", None)
        speed.setdefault("avg_mph", None)
        speed.setdefault("updated_at", time.time())
        speed.setdefault("selected_vehicle_id", self.args.highlight_ego_vehicle_id)
        speed.setdefault("selected_vehicle_type", self.args.metsr_viz_vehicle_type)
        speed.setdefault("selected_vehicle_road", None)
        return speed

    def _render_frame(self) -> Tuple[str, Optional[Dict[str, Any]]]:
        if self.client is None:
            return "METS-R client not available", None
        try:
            with self.client_lock:
                render_info = self.client.render(client_wait_timeout=float(self.args.render_client_wait_timeout_s))
            return "", render_info if isinstance(render_info, dict) else None
        except Exception as exc:
            message = str(exc).splitlines()[0]
            if "Call start_viz() before render()" in message:
                self.viz_started = False
                self.ready_event.clear()
            return message, None


def install_scenic_step_viz_hook(simulator: Any, worker: VizRenderWorker, args: Args, sensor_worker: Optional["CarlaSensorWorker"] = None) -> None:
    """Patch CosimSimulation.step before Scenic constructs/runs the simulation."""
    if bool(args.viz_render_thread) or not bool(args.viz_render_on_step):
        return
    if getattr(simulator, "_tracr_step_viz_hook_installed", False):
        return

    try:
        from scenic.simulators.cosim import simulator as cosim_module
        simulation_cls = getattr(cosim_module, "CosimSimulation", None)
    except Exception:
        simulation_cls = None

    if simulation_cls is None:
        raise RuntimeError("Could not find Scenic CosimSimulation class for TRACR step visualization hook.")

    patch_version = 2
    if getattr(simulation_cls, "_tracr_step_viz_patch_version", 0) < patch_version:
        original_step = getattr(simulation_cls, "_tracr_original_step", simulation_cls.step)
        simulation_cls._tracr_original_step = original_step

        def step_with_dashboard(self: Any) -> Any:
            result = original_step(self)
            sensor = getattr(self, "_tracr_sensor_worker", None) or getattr(type(self), "_tracr_pending_sensor_worker", None)
            if sensor is not None:
                try:
                    sensor.update_once()
                except Exception as exc:
                    sensor.last_error = str(exc).splitlines()[0]
                    sensor.dashboard.set_status(f"CARLA sensors waiting: {sensor.last_error}")
            viz_worker = getattr(self, "_tracr_viz_worker", None) or getattr(type(self), "_tracr_pending_viz_worker", None)
            if viz_worker is not None:
                step_count = int(getattr(self, "_tracr_step_count", 0) or 0) + 1
                self._tracr_step_count = step_count
                step_interval = getattr(self, "_tracr_render_step_interval", None)
                if step_interval is None:
                    step_interval = getattr(type(self), "_tracr_pending_render_step_interval", 1)
                step_interval = max(1, int(step_interval or 1))
                if step_count % step_interval == 0:
                    try:
                        viz_worker.render_once()
                    except Exception as exc:
                        viz_worker.last_error = str(exc).splitlines()[0]
                        viz_worker.dashboard.set_status(f"METS-R Vis step render waiting: {viz_worker.last_error}")
            return result

        simulation_cls.step = step_with_dashboard
        simulation_cls._tracr_step_viz_patched = True
        simulation_cls._tracr_step_viz_patch_version = patch_version

    original_create_simulation = simulator.createSimulation

    def create_simulation_with_dashboard(scene: Any, *method_args: Any, **method_kwargs: Any) -> Any:
        step_interval = max(1, int(getattr(args, "viz_render_step_interval", 1) or 1))
        simulation_cls._tracr_pending_viz_worker = worker
        simulation_cls._tracr_pending_sensor_worker = sensor_worker
        simulation_cls._tracr_pending_render_step_interval = step_interval
        simulation = original_create_simulation(scene, *method_args, **method_kwargs)
        simulation._tracr_viz_worker = worker
        simulation._tracr_sensor_worker = sensor_worker
        simulation._tracr_render_step_interval = step_interval
        if not hasattr(simulation, "_tracr_step_count"):
            simulation._tracr_step_count = 0
        return simulation

    simulator.createSimulation = create_simulation_with_dashboard
    simulator._tracr_step_viz_hook_installed = True

class _CarlaSensorState:
    def __init__(self, vehicles: Dict[str, Any]):
        self.active_vehicles = vehicles
        self.display_vehicles: Dict[str, Any] = {}


class CarlaSensorWorker:
    def __init__(self, args: Args, dashboard: ScenicTRACRDashboard, simulator: Any):
        self.args = args
        self.dashboard = dashboard
        self.simulator = simulator
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="tracr-demo2-carla-sensors", daemon=True)
        self.panel = None
        self.lock = threading.RLock()
        self.last_error = ""

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2.0)
        with self.lock:
            if self.panel is not None:
                try:
                    self.panel.close()
                except Exception:
                    pass
                self.panel = None

    def _destroy_actor(self, actor: Any) -> None:
        try:
            if actor is not None and getattr(actor, "is_alive", True):
                actor.destroy()
        except Exception:
            pass

    def _world(self) -> Any:
        return getattr(self.simulator, "world", None) or getattr(self.simulator, "carla_world", None)

    def _carla_module(self) -> Any:
        try:
            import carla
            return carla
        except ImportError:
            return sys.modules.get("carla")

    def _vehicle_state(self, world: Any) -> _CarlaSensorState:
        vehicles: Dict[str, Any] = {}
        try:
            actors = world.get_actors().filter("vehicle.*")
        except Exception:
            actors = []
        for actor in actors:
            try:
                if actor is not None and getattr(actor, "is_alive", True):
                    vehicles[str(getattr(actor, "id", len(vehicles)))] = actor
            except Exception:
                continue
        return _CarlaSensorState(vehicles)

    def update_once(self) -> None:
        with self.lock:
            world = self._world()
            carla_module = self._carla_module()
            if world is None or carla_module is None:
                raise RuntimeError("CARLA world/module is not available for dashboard sensors")
            if self.panel is None:
                speedy_mode = bool(getattr(self.args, "speedy_mode", False))
                self.panel = CarlaSensorPanel(
                    world,
                    carla_module,
                    self._destroy_actor,
                    vehicle_camera_enabled=not speedy_mode,
                    lidar_enabled=not speedy_mode,
                )
                self.panel.spawn_overhead_camera(z=float(self.args.carla_camera_z))
            state = self._vehicle_state(world)
            self.panel.ensure_sensors(state)
            speedy_mode = bool(getattr(self.args, "speedy_mode", False))
            self.dashboard.update_sensor_images(
                self.panel.camera_png(),
                b"" if speedy_mode else self.panel.vehicle_camera_png(),
                b"" if speedy_mode else self.panel.lidar_png(),
            )

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.update_once()
                self.stop_event.wait(max(0.1, float(self.args.render_interval_s)))
            except Exception as exc:
                self.last_error = str(exc).splitlines()[0]
                self.dashboard.set_status(f"CARLA sensors waiting: {self.last_error}")
                self.stop_event.wait(max(0.5, float(self.args.client_retry_interval_s)))

def wait_for_space() -> None:
    print("Press SPACE in this terminal to start the Scenic simulation.")
    if os.name == "nt":
        import msvcrt

        while True:
            char = msvcrt.getwch()
            if char == " ":
                print("Starting simulation...")
                return
            if char == "\x03":
                raise KeyboardInterrupt
    else:
        import termios
        import tty

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while True:
                char = sys.stdin.read(1)
                if char == " ":
                    print("Starting simulation...")
                    return
                if char == "\x03":
                    raise KeyboardInterrupt
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def find_evlog_files(output_root: str) -> List[Path]:
    root = Path(output_root)
    if not root.exists():
        return []
    return sorted(root.glob("*/agg_output/**/*EVLog*.csv"), key=lambda path: path.stat().st_mtime)


def snapshot_evlogs(output_root: str) -> Dict[str, Tuple[float, int]]:
    snapshot = {}
    for path in find_evlog_files(output_root):
        try:
            stat = path.stat()
        except OSError:
            continue
        snapshot[str(path)] = (stat.st_mtime, stat.st_size)
    return snapshot


def changed_evlogs(before: Dict[str, Tuple[float, int]], output_root: str, run_start: float) -> List[Path]:
    changed = []
    for path in find_evlog_files(output_root):
        key = str(path)
        try:
            stat = path.stat()
        except OSError:
            continue
        old = before.get(key)
        if old is None or stat.st_mtime > old[0] + 1e-6 or stat.st_size != old[1]:
            changed.append(path)
    if changed:
        return changed
    recent = [path for path in find_evlog_files(output_root) if path.stat().st_mtime >= run_start - 2.0]
    return recent[-1:] if recent else []


def summarize_evlogs(paths: Iterable[Path], tick_seconds: float) -> Dict[str, Any]:
    travel_times_s: List[float] = []
    distances_m: List[float] = []
    delay_s: List[float] = []
    energies: List[float] = []
    row_count = 0
    path_list = list(paths)
    for path in path_list:
        try:
            with path.open("r", encoding="utf-8", newline="") as input_file:
                reader = csv.DictReader(input_file)
                for row in reader:
                    row_count += 1
                    arrival_tick = first_float(row.get("tick"))
                    departure_tick = first_float(row.get("departureTime"), row.get("departure_time"))
                    if arrival_tick is not None and departure_tick is not None and arrival_tick >= departure_tick:
                        travel_times_s.append((arrival_tick - departure_tick) * float(tick_seconds))
                    distance = first_float(row.get("distance"), row.get("distance_m"))
                    if distance is not None:
                        distances_m.append(distance)
                    energy = first_float(row.get("tripEnergy"), row.get("trip_energy"))
                    if energy is not None:
                        energies.append(energy)
                    delay = first_float(
                        row.get("delay"),
                        row.get("delay_s"),
                        row.get("travelDelay"),
                        row.get("travel_delay"),
                        row.get("timeLoss"),
                        row.get("time_loss"),
                        row.get("waitingTime"),
                        row.get("waiting_time"),
                    )
                    if delay is not None:
                        delay_s.append(delay)
        except OSError:
            continue
    avg_travel_s = mean(travel_times_s)
    avg_distance_m = mean(distances_m)
    avg_speed_mps = None
    if avg_travel_s is not None and avg_travel_s > 0 and avg_distance_m is not None:
        avg_speed_mps = avg_distance_m / avg_travel_s
    return {
        "completed_trips_raw": len(travel_times_s),
        "rows": row_count,
        "avg_travel_s_raw": avg_travel_s,
        "avg_distance_m_raw": avg_distance_m,
        "avg_speed_mps_raw": avg_speed_mps,
        "energy_kwh_raw": sum(energies) if energies else None,
        "total_delay_s_raw": sum(delay_s) if delay_s else None,
        "evlog_paths": [str(path) for path in path_list],
    }


def summarize_veh_data(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "veh_data_rows": 0,
            "max_active_vehicles": None,
            "max_bubble_actors": None,
            "max_bubble_queue": None,
            "completed_routes": None,
        }
    max_active = None
    max_bubble = None
    max_queue = None
    completed_routes = None
    rows = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                rows += 1
                active = first_float(row.get("active_vehicles"))
                bubble = first_float(row.get("bubble_actors"))
                queue = first_float(row.get("bubble_queue"))
                completed = first_float(row.get("completed_routes"))
                if active is not None:
                    max_active = active if max_active is None else max(max_active, active)
                if bubble is not None:
                    max_bubble = bubble if max_bubble is None else max(max_bubble, bubble)
                if queue is not None:
                    max_queue = queue if max_queue is None else max(max_queue, queue)
                if completed is not None:
                    completed_routes = completed
    except OSError:
        pass
    return {
        "veh_data_rows": rows,
        "max_active_vehicles": max_active,
        "max_bubble_actors": max_bubble,
        "max_bubble_queue": max_queue,
        "completed_routes": completed_routes,
    }


def summarize_simulation(simulation: Any) -> Dict[str, Any]:
    active_values = [
        value
        for value in (first_float(item) for item in (getattr(simulation, "total_active_vehicles", None) or []))
        if value is not None
    ]
    bubble_values = [
        value
        for value in (first_float(item) for item in (getattr(simulation, "bubble_sizes", None) or []))
        if value is not None
    ]
    completed_route = getattr(simulation, "completed_route", None)
    completed_routes = len(completed_route) if completed_route is not None else 0
    return {
        "max_active_vehicles": max(active_values, default=0),
        "max_bubble_actors": max(bubble_values, default=0),
        "completed_routes": completed_routes,
    }


def run_data_base_path(
    args: Args,
    seed: Optional[int] = None,
    run_number: Optional[int] = None,
    session_id: Optional[str] = None,
) -> Path:
    run_seed = int(args.seed if seed is None else seed)
    name = f"vehs_{args.num_commuters}_simtime_{args.length}_seed_{run_seed}"
    if session_id:
        name += f"_session_{session_id}"
    if run_number is not None:
        name += f"_run_{int(run_number)}"
    return Path(args.export_folder) / name


def summary_for_dashboard(
    run_index: int,
    run_seed: int,
    evlog_summary: Dict[str, Any],
    veh_summary: Dict[str, Any],
    live_summary: Dict[str, Any],
) -> Dict[str, Any]:
    evlog_avg_speed_mps = first_float(evlog_summary.get("avg_speed_mps_raw"))
    evlog_avg_speed_mph = None if evlog_avg_speed_mps is None else evlog_avg_speed_mps * 2.2369362921
    avg_speed_mph = first_float(live_summary.get("avg_mph"), evlog_avg_speed_mph)
    completed_routes = first_float(veh_summary.get("completed_routes"))
    max_active_vehicles = first_float(
        veh_summary.get("max_active_vehicles"),
        live_summary.get("max_vehicle_count"),
    )
    paths = evlog_summary.get("evlog_paths") or []
    return {
        "run": run_index,
        "seed": run_seed,
        "status": "finished",
        "completed_trips": int(evlog_summary.get("completed_trips_raw", 0) or 0),
        "completed_routes": int(completed_routes or 0),
        "ego_spawned_link": live_summary.get("ego_spawned_link") or "not observed",
        "total_delay_s": first_float(evlog_summary.get("total_delay_s_raw")),
        "avg_travel_time_s": first_float(evlog_summary.get("avg_travel_s_raw")),
        "avg_speed_mph": avg_speed_mph,
        "max_active_vehicles": int(max_active_vehicles or 0),
        "max_bubble_actors": int(first_float(veh_summary.get("max_bubble_actors")) or 0),
        "max_bubble_queue": int(first_float(veh_summary.get("max_bubble_queue")) or 0),
        "veh_data_rows": veh_summary.get("veh_data_rows", 0),
        "evlog": Path(paths[-1]).name if paths else "",
    }


def write_summary_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run",
        "seed",
        "status",
        "progress",
        "completed_trips",
        "completed_routes",
        "ego_spawned_link",
        "total_delay_s",
        "avg_travel_time_s",
        "avg_speed_mph",
        "max_active_vehicles",
        "max_bubble_actors",
        "max_bubble_queue",
        "veh_data_rows",
        "evlog",
        "artifact_base",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_scenic_records(records: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required to export Scenic result records, matching run_test.py") from exc
    dataframe = pd.DataFrame({key: pd.Series(value) for key, value in records.items()})
    dataframe.to_csv(path, index=False)


def summarize_scenic_records(records: Dict[str, Any]) -> Dict[str, Any]:
    speeds_mps: List[float] = []
    for sample in (records.get("all_velocities") or []):
        payload = sample
        if isinstance(sample, (tuple, list)) and len(sample) >= 2 and isinstance(sample[1], dict):
            payload = sample[1]
        if not isinstance(payload, dict):
            continue
        for velocity in payload.values():
            x = first_float(getattr(velocity, "x", None))
            y = first_float(getattr(velocity, "y", None))
            if (x is None or y is None) and isinstance(velocity, (tuple, list)) and len(velocity) >= 2:
                x = first_float(velocity[0])
                y = first_float(velocity[1])
            if x is not None and y is not None:
                speeds_mps.append(math.hypot(x, y))
    avg_speed_mps = mean(speeds_mps)
    return {
        "avg_speed_mph": None if avg_speed_mps is None else avg_speed_mps * 2.2369362921,
        "speed_samples": len(speeds_mps),
    }


def compile_scenario(args: Args, scenic_module: Any, set_debugging_options: Any, set_seed: Any, run_seed: int) -> Any:
    set_debugging_options(verbosity=int(args.verbosity))
    set_seed(int(run_seed))
    params = {
        "num_commuters": int(args.num_commuters),
        "length": int(args.length),
        "seed": int(run_seed),
        "address": args.address,
        "timestep": float(args.timestep),
        "town": args.town,
        "bubble_size": int(args.bubble_size),
        "export_folder": args.export_folder,
    }
    return scenic_module.scenarioFromFile(path=args.scenic_file, model=args.scenic_model, mode2D=True, params=params)


def generate_scene_for_run(scenario: Any, set_seed: Any, run_seed: int, run_name: Path) -> Any:
    """Generate a reseeded scene without recompiling Scenic's cached road model."""
    run_seed = int(run_seed)
    params = getattr(scenario, "params", None)
    if isinstance(params, dict):
        params["seed"] = run_seed
        if "run_name" in params:
            params["run_name"] = str(run_name)
    set_seed(run_seed)
    return scenario.generate()


def build_simulator(args: Args, cosim_simulator_cls: Any, run_name: Optional[Path] = None) -> Any:
    base_run_name = Path(run_name) if run_name is not None else run_data_base_path(args)
    metsr_output_path = None
    if args.metsr_sim_dir:
        metsr_output_path = resolve_metsr_sim_folder(args)
        print(f"Visualizing METS-R trajectory data from: {metsr_output_path}")
    return cosim_simulator_cls(
        metsr_host=args.metsr_host,
        metsr_port=int(args.metsr_port),
        address=args.address,
        carla_port=int(args.carla_port),
        timeout=float(args.carla_timeout_s),
        carla_map=args.town,
        xml_map=args.sumo_map,
        map_path=args.opendrive_map,
        timestep=float(args.timestep),
        bubble_size=int(args.bubble_size),
        run_name=str(base_run_name),
        metsr_sim_dir=metsr_output_path,
    )


def run(args: Args) -> int:
    try:
        import scenic
        from scenic import setDebuggingOptions
        from scenic.core.utils import setSeed
        from scenic.simulators.cosim import CosimSimulator
    except ImportError as exc:
        raise SystemExit(
            f"Scenic is not available in this Python environment. Install the METSRSim branch from {_SCENIC_SOURCE_URL}, then rerun this script."
        ) from exc

    patch_cosim_carla_motion_compat(args)

    run_seeds = [
        int(args.seed) + run_index if args.increment_seed else int(args.seed)
        for run_index in range(int(args.total_simulations))
    ]
    expected_ticks = max(0, int(round(float(args.length) / float(args.timestep))))
    session_id = f"{time.strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"
    first_run_name = run_data_base_path(args, run_seeds[0], 1, session_id) if run_seeds else run_data_base_path(args)

    dashboard = ScenicTRACRDashboard(
        args.dashboard_dir,
        int(args.dashboard_port),
        args.viz_url,
        bool(args.open_browser),
        metsr_viz_map=int(args.metsr_viz_map),
        metsr_viz_vehicle_type=int(args.metsr_viz_vehicle_type),
        speedy_mode=bool(args.speedy_mode),
    )
    dashboard.ego_vehicle_id = int(args.highlight_ego_vehicle_id)
    dashboard.metsr_vis_auto_highlight_ego = True
    dashboard.configure_runs(run_seeds, expected_ticks)
    worker: Optional[VizRenderWorker] = None
    sensor_worker: Optional[CarlaSensorWorker] = None
    dashboard_url = dashboard.start()
    try:
        dashboard.set_scenario(f"{Path(args.scenic_file).name} | seed {args.seed} | {args.total_simulations} run(s)")
        dashboard.set_status("Compiling Scenic scenario", run_state="compiling")
        print(f"TRACR Scenic dashboard: {dashboard_url}")

        print("Generating Scenic scenario")
        scenario = compile_scenario(args, scenic, setDebuggingOptions, setSeed, int(args.seed))
        print("Compilation complete")
        dashboard.set_status("Creating Scenic co-simulator", run_state="initializing")
        simulator = build_simulator(args, CosimSimulator, run_name=first_run_name)

        metsr_client = getattr(simulator, "metsr_client", None)
        if metsr_client is None:
            raise RuntimeError("CosimSimulator did not expose a METS-R client for visualization.")
        patch_metsr_client_viz_compat(metsr_client)
        metsr_client_lock = threading.RLock()
        worker = VizRenderWorker(args, dashboard, metsr_client, metsr_client_lock, owns_client=False)

        if args.carla_sensor_panels:
            sensor_worker = CarlaSensorWorker(args, dashboard, simulator)

        if args.viz_render_thread:
            install_metsr_client_lock(metsr_client, metsr_client_lock)
            worker.start()
            print("METS-R Vis render worker started using Scenic's single METS-R client with serialized access.")
        elif args.viz_render_on_step:
            install_scenic_step_viz_hook(simulator, worker, args, sensor_worker=sensor_worker)
            print("METS-R Vis render hook installed on Scenic steps using Scenic's single METS-R client.")
            try:
                stream_url = worker.start_stream()
                print(f"METS-R Vis stream started before Scenic simulation: {stream_url}")
                warmup_s = max(0.0, float(getattr(args, "viz_stream_warmup_s", 0.0) or 0.0))
                if warmup_s > 0:
                    dashboard.set_status(f"METS-R Vis stream ready; warming dashboard for {warmup_s:.1f}s", run_state="ready")
                    time.sleep(warmup_s)
            except Exception as exc:
                worker.last_error = str(exc).splitlines()[0]
                dashboard.set_status(f"METS-R Vis stream waiting: {worker.last_error}", run_state="waiting")
                if args.require_viz_client:
                    raise
        else:
            worker = None
            print("METS-R Vis live rendering disabled.")

        if sensor_worker is not None:
            sensor_worker.start()
            if args.speedy_mode:
                print("CARLA dashboard sensor worker started in speedy mode with bird-eye tracking camera only.")
            else:
                print("CARLA dashboard sensor worker started in the background.")
        if args.require_viz_client:
            if worker is None:
                raise RuntimeError("METS-R render client was required but live rendering is disabled.")
            if args.viz_render_thread and not worker.wait_until_ready(float(args.client_connect_wait_s) + 1.0):
                raise RuntimeError(f"METS-R render client did not connect: {worker.last_error or 'not ready'}")

        if args.wait_for_space:
            dashboard.set_status("Dashboard ready; waiting for terminal space bar", run_state="ready")
            wait_for_space()
        else:
            dashboard.set_status("Dashboard ready; starting Scenic simulation", run_state="ready")
            print("Starting simulation without space-bar wait. Use --wait-for-space to pause before Scenic starts.")

        summaries: List[Dict[str, Any]] = []
        summary_csv = Path(args.export_folder) / "tracr_demo2_run_summary.csv"
        exit_code = 0
        for run_index in range(int(args.total_simulations)):
            run_number = run_index + 1
            run_seed = run_seeds[run_index]
            run_base = run_data_base_path(args, run_seed, run_number, session_id)
            dashboard.set_scenario(f"{Path(args.scenic_file).name} | seed {run_seed} | run {run_number}/{args.total_simulations}")
            dashboard.begin_run(run_number, run_seed)
            simulator.run_name = str(run_base)
            print(f"Starting simulation number: {run_index} seed={run_seed}")

            try:
                dashboard.update_run_status(run_number, "running")
                dashboard.set_status(f"Starting Scenic simulation {run_number}/{args.total_simulations}", run_state="running")
                before_evlogs = snapshot_evlogs(args.output_root)
                run_start = time.time()
                scene, _ = generate_scene_for_run(scenario, setSeed, run_seed, run_base)
                simulation = simulator.simulate(scene)
                if not simulation:
                    raise RuntimeError(f"Scenic returned no simulation result for run {run_index}")

                dashboard.update_run_status(run_number, "finalizing")
                result = simulation.result
                records = getattr(result, "records", {}) or {}
                csv_path = run_base.with_name(run_base.name + "_trajectory.csv")
                print(f"Writing Scenic records to: {csv_path}")
                write_scenic_records(records, csv_path)

                evlogs = changed_evlogs(before_evlogs, args.output_root, run_start)
                evlog_summary = summarize_evlogs(evlogs, tick_seconds=float(args.metsr_tick_seconds))
                veh_path = run_base.with_name(run_base.name + "_veh_data.csv")
                veh_summary = summarize_veh_data(veh_path)
                veh_summary.update(summarize_simulation(simulation))
                live_summary = dashboard.current_run_telemetry()
                record_summary = summarize_scenic_records(records)
                if live_summary.get("avg_mph") is None:
                    live_summary["avg_mph"] = record_summary.get("avg_speed_mph")
                dashboard_row = summary_for_dashboard(run_number, run_seed, evlog_summary, veh_summary, live_summary)
                dashboard_row.update(
                    {
                        "status": "finished",
                        "progress": dashboard._progress_text(live_summary.get("last_tick")),
                        "artifact_base": str(run_base),
                        "error": "",
                    }
                )
                summaries.append(dashboard_row)
                write_summary_csv(summary_csv, summaries)
                dashboard.set_run_result(dashboard_row)
                dashboard.set_status(f"Finished Scenic simulation {run_number}/{args.total_simulations}", run_state="finished")
                print(f"Terminating simulation number: {run_index}")
            except (Exception, KeyboardInterrupt) as exc:
                exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
                status = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
                live_summary = dashboard.current_run_telemetry()
                failure_row = {
                    "run": run_number,
                    "seed": run_seed,
                    "status": status,
                    "progress": dashboard._progress_text(live_summary.get("last_tick")),
                    "completed_trips": 0,
                    "completed_routes": 0,
                    "ego_spawned_link": live_summary.get("ego_spawned_link") or "not observed",
                    "avg_speed_mph": live_summary.get("avg_mph"),
                    "max_active_vehicles": live_summary.get("max_vehicle_count", 0),
                    "artifact_base": str(run_base),
                    "error": str(exc).splitlines()[0],
                }
                summaries.append(failure_row)
                write_summary_csv(summary_csv, summaries)
                dashboard.set_run_result(failure_row)
                dashboard.set_status(f"Scenic simulation {run_number}/{args.total_simulations} {status}: {failure_row['error']}", run_state=status)
                print(f"Scenic simulation {run_number}/{args.total_simulations} {status}: {failure_row['error']}")
                break

        finished_runs = sum(1 for row in summaries if row.get("status") == "finished")
        if exit_code == 0:
            print(f"Successfully ran {finished_runs} simulation(s)")
            dashboard.set_status(f"Completed {finished_runs} Scenic simulation(s)", run_state="complete")
        else:
            print(f"Stopped after {finished_runs} completed simulation(s)")
        print(f"Run summary: {summary_csv}")
        if args.hold_dashboard and exit_code != 130:
            input("Runs complete. Press Enter to stop dashboard and exit.")
        return exit_code
    finally:
        if worker is not None:
            worker.stop()
        if sensor_worker is not None:
            sensor_worker.stop()
        dashboard.stop()


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
