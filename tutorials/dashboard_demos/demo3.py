"""Town06 TRACR dashboard with a PCLA-controlled ego vehicle.

METS-R owns traffic demand and road-level state while CARLA owns motion on the
co-simulation roads. One CARLA vehicle is driven by PCLA's tfv6_visiononly
agent. The dashboard has no V2X panel: it shows METS-R Viz, an ego tracking
camera, and the RGB camera tensors supplied to PCLA.

Point --pcla-dir (or PCLA_HOME) at https://github.com/MasoudJTehrani/PCLA and
run this script from the PCLA Conda environment after installing its weights.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
_DEFAULT_CONFIG = _REPO_ROOT / "configs" / "run_cosim_CARLAT6.json"
_DEFAULT_DASHBOARD_DIR = _REPO_ROOT / "output" / "tracr_demo3_dashboard"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
os.chdir(_REPO_ROOT)

from utils.cosim_support import (  # noqa: E402
    CarlaSensorPanel,
    TRACRDashboard,
    _start_viz_with_port_fallback,
    blank_png,
    image_array_to_png,
)
from utils.util import (  # noqa: E402
    METS_R_VIS_PRIVATE_VEHICLE_TYPE,
    stop_visualization_server,
)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Town06 TRACR demo with a PCLA tfv6_visiononly ego.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-r", "--run-config", default=str(_DEFAULT_CONFIG))
    parser.add_argument("--pcla-dir", default=os.environ.get("PCLA_HOME"))
    parser.add_argument("--pcla-agent", default="tfv6_visiononly")
    parser.add_argument("--pcla-route", default=None)
    parser.add_argument(
        "--route-output",
        default=str(_DEFAULT_DASHBOARD_DIR / "pcla_town06_route.xml"),
    )
    parser.add_argument("--route-end-spawn-index", type=int, default=None)
    parser.add_argument("--route-min-distance-m", type=float, default=250.0)
    parser.add_argument("--ego-vehicle-id", type=int, default=None)
    parser.add_argument("--private-vehicle-count", type=_positive_int, default=24)
    parser.add_argument("--start-vid", type=int, default=0)
    parser.add_argument("--ticks", type=_positive_int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timestep", type=float, default=0.05)
    parser.add_argument("--metsr-timeout-s", type=float, default=600.0)
    parser.add_argument("--metsr-poll-timeout-s", type=float, default=5.0)
    parser.add_argument("--viz-url", default="https://engineering.purdue.edu/HSEES/METSRVis/")
    parser.add_argument("--viz-stream-host", default="0.0.0.0")
    parser.add_argument("--viz-stream-port", type=int, default=8767)
    parser.add_argument("--dashboard-dir", default=str(_DEFAULT_DASHBOARD_DIR))
    parser.add_argument("--dashboard-port", type=int, default=8897)
    parser.add_argument(
        "--carla-host",
        default=None,
        help=(
            "CARLA server host override. When omitted under WSL with a Windows "
            "CARLA executable, the Windows host address is detected automatically."
        ),
    )
    parser.add_argument("--carla-camera-z", type=float, default=85.0)
    parser.add_argument("--render-every", type=_positive_int, default=2)
    parser.add_argument("--dashboard-every", type=_positive_int, default=2)
    parser.add_argument("--camera-every", type=_positive_int, default=2)
    parser.add_argument("--render-client-wait-timeout-s", type=float, default=0.0)
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--no-start-metsr", dest="start_metsr", action="store_false")
    parser.add_argument("--no-hold-dashboard", dest="hold_dashboard", action="store_false")
    parser.set_defaults(start_metsr=True, hold_dashboard=True)
    args = parser.parse_args(argv)
    if args.timestep <= 0:
        parser.error("--timestep must be greater than zero")
    if args.route_min_distance_m <= 0:
        parser.error("--route-min-distance-m must be greater than zero")
    return args


def _resolve_repo_path(path: str) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = _REPO_ROOT / candidate
    return candidate.resolve()


def _wsl_windows_host() -> Optional[str]:
    """Return the Windows-host gateway address when running inside WSL."""
    if os.name != "posix":
        return None

    try:
        kernel_release = Path("/proc/sys/kernel/osrelease").read_text(
            encoding="utf-8"
        )
    except OSError:
        kernel_release = ""
    if (
        "microsoft" not in kernel_release.lower()
        and "WSL_DISTRO_NAME" not in os.environ
    ):
        return None

    try:
        route_lines = Path("/proc/net/route").read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    for line in route_lines[1:]:
        fields = line.split()
        if len(fields) < 4 or fields[1] != "00000000":
            continue
        try:
            gateway = int(fields[2], 16)
            flags = int(fields[3], 16)
        except ValueError:
            continue
        if not flags & 0x2:
            continue
        address = ".".join(
            str((gateway >> (8 * index)) & 0xFF) for index in range(4)
        )
        if address != "0.0.0.0":
            return address
    return None


def _configure_carla_host(config: Any, override: Optional[str]) -> None:
    """Apply a CLI host override or route Windows CARLA through the WSL gateway."""
    if override:
        config.carla_host = str(override)
        return

    configured_host = str(getattr(config, "carla_host", "127.0.0.1"))
    carla_dir = str(getattr(config, "carla_dir", ""))
    uses_loopback = configured_host.lower() in {"127.0.0.1", "localhost", "::1"}
    uses_windows_carla = carla_dir.lower().endswith(".exe")
    if not (uses_loopback and uses_windows_carla):
        return

    windows_host = _wsl_windows_host()
    if windows_host:
        config.carla_host = windows_host
        print(
            f"WSL detected: using Windows host {windows_host} for CARLA "
            f"instead of {configured_host}."
        )


def _camera_label(spec: Mapping[str, Any], index: int) -> str:
    yaw = float(spec.get("yaw", 0.0) or 0.0)
    if abs(abs(yaw) - 180.0) <= 30.0:
        direction = "Rear"
    elif yaw < -8.0:
        direction = "Front left"
    elif yaw > 8.0:
        direction = "Front right"
    else:
        direction = "Front center"
    return f"PCLA {direction} · {spec.get('id', f'camera_{index + 1}')} · yaw {yaw:g}°"


class PCLADashboard(TRACRDashboard):
    """External dashboard specialized for PCLA's multiple RGB inputs."""

    def __init__(
        self,
        directory: Path,
        port: int,
        viz_url: str,
        stream_url: str,
        open_browser: bool,
    ) -> None:
        super().__init__(
            viz_url=viz_url,
            stream_url=stream_url,
            fullscreen=True,
            local_viz_patch=False,
            metsr_viz_map=9,
            metsr_viz_vehicle_type=METS_R_VIS_PRIVATE_VEHICLE_TYPE,
        )
        self.directory = Path(directory)
        self.port = int(port)
        self.open_browser_flag = bool(open_browser)
        self._pcla_lock = threading.RLock()
        self._camera_views: List[Dict[str, Any]] = [
            {
                "id": "waiting",
                "label": "Waiting for PCLA RGB sensors",
                "png": blank_png("Waiting for the PCLA ego vehicle"),
                "frame": None,
            }
        ]
        self._overhead_png = blank_png("Waiting for CARLA Town06 traffic")
        self._telemetry: Dict[str, Any] = {
            "state": "starting",
            "agent": "tfv6_visiononly",
            "tick": 0,
            "ego_vehicle": "waiting",
        }

    def start(self) -> str:
        return self.display_external(
            directory=str(self.directory),
            port=self.port,
            open_browser=self.open_browser_flag,
        )

    def configure_camera_views(self, specs: Sequence[Mapping[str, Any]]) -> None:
        views = []
        for index, spec in enumerate(specs):
            sensor_id = str(spec.get("id", f"camera_{index + 1}"))
            views.append(
                {
                    "id": sensor_id,
                    "label": _camera_label(spec, index),
                    "png": blank_png(f"Waiting for {sensor_id}"),
                    "frame": None,
                }
            )
        if not views:
            views = [
                {
                    "id": "unavailable",
                    "label": "PCLA RGB inputs unavailable",
                    "png": blank_png("PCLA did not expose an RGB camera sensor"),
                    "frame": None,
                }
            ]
        with self._pcla_lock:
            self._camera_views = views
        self._refresh_external_state(force=True)

    def publish(
        self,
        *,
        status: str,
        telemetry: Mapping[str, Any],
        ego_vehicle_id: Optional[Any] = None,
        overhead_png: Optional[bytes] = None,
        camera_views: Optional[Mapping[str, Tuple[str, bytes, Any]]] = None,
    ) -> None:
        sync_frame = False
        with self._pcla_lock:
            if ego_vehicle_id is not None and str(ego_vehicle_id) != str(self.ego_vehicle_id):
                self.ego_vehicle_id = ego_vehicle_id
                sync_frame = True
            self._status_text = str(status)
            self._telemetry = dict(telemetry)
            if overhead_png:
                self._overhead_png = overhead_png
            if camera_views:
                by_id = {str(row["id"]): row for row in self._camera_views}
                for sensor_id, (label, png, frame) in camera_views.items():
                    row = by_id.get(str(sensor_id))
                    if row is None:
                        row = {"id": str(sensor_id)}
                        self._camera_views.append(row)
                    row.update({"label": str(label), "png": png, "frame": frame})
        if sync_frame:
            self.viz_frame_url = self._metsr_vis_frame_url()
        self._refresh_external_state(force=sync_frame)

    def _external_css(self) -> str:
        return """
          :root {color-scheme: dark;}
          * {box-sizing: border-box;}
          html, body {margin: 0; width: 100%; height: 100%; overflow: hidden; background: #0b0d10;}
          body {font-family: Inter, system-ui, -apple-system, Segoe UI, sans-serif; color: #f8fafc;}
          .shell {height: 100vh; padding: 10px 12px 12px; display: flex; flex-direction: column; gap: 7px;}
          .title-row {display: flex; align-items: baseline; justify-content: space-between; gap: 14px;}
          h1 {font-size: 18px; line-height: 1.1; margin: 0;}
          #status {font-size: 11px; color: #cbd5e1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;}
          #metrics {display: flex; gap: 6px; min-height: 31px; overflow-x: auto;}
          .metric {min-width: 92px; padding: 4px 7px; border: 1px solid #303641; border-radius: 5px; background: #151920;}
          .metric-label {display: block; color: #94a3b8; font-size: 9px; text-transform: uppercase; letter-spacing: .06em;}
          .metric-value {display: block; color: #f8fafc; font: 600 11px ui-monospace, SFMono-Regular, Consolas, monospace; white-space: nowrap;}
          .dashboard-grid {flex: 1 1 auto; min-height: 0; display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); grid-template-rows: minmax(0, 3fr) minmax(0, 2fr); gap: 8px;}
          .panel {min-width: 0; min-height: 0; overflow: hidden; border: 1px solid #343a46; border-radius: 6px; background: #11151b; display: flex; flex-direction: column;}
          .panel h2 {flex: 0 0 auto; margin: 0; padding: 5px 7px; font-size: 11px; color: #dbeafe; border-bottom: 1px solid #2b3039; background: #171b22;}
          .panel img {display: block; width: 100%; height: 100%; min-height: 0; object-fit: cover; background: #090b0e;}
          .viz-panel, .overhead-panel {grid-column: span 6;}
          .viz-frame {flex: 1 1 auto; min-height: 0; position: relative;}
          .viz-frame iframe {display: block; width: 100%; height: 100%; border: 0; background: #090b0e;}
          .viz-link {position: absolute; right: 5px; bottom: 4px; padding: 2px 5px; border-radius: 3px; background: rgba(3,7,18,.82); color: #93c5fd; font-size: 9px; text-decoration: none;}
          #camera-grid {grid-column: 1 / -1; min-width: 0; min-height: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 8px;}
          .camera-panel {height: 100%;}
          @media (max-width: 900px) {
            html, body {overflow: auto;}
            .shell {height: auto; min-height: 100vh;}
            .dashboard-grid {display: flex; flex-direction: column;}
            .panel {min-height: 42vh;}
            #camera-grid {display: flex; flex-direction: column;}
          }
        """

    def _external_page_html(self) -> str:
        self.viz_frame_url = self._metsr_vis_frame_url()
        frame_url = escape(self.viz_frame_url or self.viz_url)
        return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TRACR Town06 PCLA Dashboard</title>
  <style>{self._external_css()}</style>
</head>
<body>
  <main class="shell">
    <div class="title-row">
      <h1>TRACR Town06 · PCLA vision-only driving</h1>
      <div id="status">Starting services…</div>
    </div>
    <div id="metrics"></div>
    <div class="dashboard-grid">
      <section class="panel viz-panel">
        <h2>METS-R Viz live traffic</h2>
        <div class="viz-frame">
          <iframe id="metsr-viz-frame" src="{frame_url}" allow="local-network-access; clipboard-read; clipboard-write" referrerpolicy="no-referrer-when-downgrade"></iframe>
          <a id="viz-popout" class="viz-link" href="{frame_url}" target="_blank" rel="noopener">open METS-R Viz</a>
        </div>
      </section>
      <section class="panel overhead-panel">
        <h2>CARLA ego tracking camera</h2>
        <img id="overhead-camera" alt="CARLA ego tracking camera">
      </section>
      <div id="camera-grid"></div>
    </div>
  </main>
  <script>
    const cameraNodes = new Map();

    function updateMetrics(values) {{
      const root = document.getElementById('metrics');
      root.replaceChildren();
      for (const [label, value] of Object.entries(values || {{}})) {{
        const card = document.createElement('div');
        card.className = 'metric';
        const labelNode = document.createElement('span');
        labelNode.className = 'metric-label';
        labelNode.textContent = label.replaceAll('_', ' ');
        const valueNode = document.createElement('span');
        valueNode.className = 'metric-value';
        valueNode.textContent = value === null || value === '' ? '—' : String(value);
        card.append(labelNode, valueNode);
        root.appendChild(card);
      }}
    }}

    function updateCameras(views) {{
      const root = document.getElementById('camera-grid');
      const live = new Set();
      for (const view of views || []) {{
        const key = String(view.id || view.label || 'camera');
        live.add(key);
        let nodes = cameraNodes.get(key);
        if (!nodes) {{
          const panel = document.createElement('section');
          panel.className = 'panel camera-panel';
          const heading = document.createElement('h2');
          const image = document.createElement('img');
          image.alt = view.label || key;
          panel.append(heading, image);
          root.appendChild(panel);
          nodes = {{panel, heading, image}};
          cameraNodes.set(key, nodes);
        }}
        nodes.heading.textContent = view.label || key;
        nodes.image.alt = view.label || key;
        if (view.png && nodes.image.src !== view.png) nodes.image.src = view.png;
      }}
      for (const [key, nodes] of cameraNodes.entries()) {{
        if (!live.has(key)) {{
          nodes.panel.remove();
          cameraNodes.delete(key);
        }}
      }}
    }}

    async function refresh() {{
      try {{
        const response = await fetch('state.json?ts=' + Date.now(), {{cache: 'no-store'}});
        if (!response.ok) return;
        const state = await response.json();
        document.getElementById('status').textContent = state.status || 'Running';
        document.getElementById('overhead-camera').src = state.overhead_png || '';
        updateMetrics(state.telemetry || {{}});
        updateCameras(state.camera_views || []);

        const frameUrl = state.metsr_vis_frame_url || state.viz_url || '';
        const frame = document.getElementById('metsr-viz-frame');
        if (frameUrl && frame.getAttribute('src') !== frameUrl) frame.setAttribute('src', frameUrl);
        const popout = document.getElementById('viz-popout');
        if (frameUrl && popout.getAttribute('href') !== frameUrl) popout.setAttribute('href', frameUrl);
        if (state.ego_vehicle_id && frame.contentWindow) {{
          frame.contentWindow.postMessage({{
            type: 'tracr-select-vehicle',
            vehicleId: state.ego_vehicle_id,
            vehicleType: state.metsr_vis_vehicle_type,
            map: state.metsr_vis_map,
            streamURL: state.stream_url,
            VehicleID: state.ego_vehicle_id,
            VehicleType: state.metsr_vis_vehicle_type,
            Map: state.metsr_vis_map,
            StreamURL: state.stream_url,
          }}, '*');
        }}
      }} catch (error) {{
        console.debug('TRACR PCLA dashboard refresh failed', error);
      }}
    }}
    refresh();
    setInterval(refresh, 250);
  </script>
</body>
</html>
"""

    def _external_state(self) -> Dict[str, Any]:
        with self._pcla_lock:
            camera_views = [
                {
                    "id": str(row.get("id", "camera")),
                    "label": str(row.get("label", "PCLA camera")),
                    "png": self._png_uri(row.get("png") or b""),
                    "frame": row.get("frame"),
                }
                for row in self._camera_views
            ]
            return {
                "status": str(self._status_text),
                "telemetry": dict(self._telemetry),
                "overhead_png": self._png_uri(self._overhead_png),
                "camera_views": camera_views,
                "stream_url": self.stream_url,
                "ego_vehicle_id": None if self.ego_vehicle_id is None else str(self.ego_vehicle_id),
                "metsr_vis_vehicle_type": str(self._metsr_vis_selected_vehicle_type()),
                "metsr_vis_map": str(self.metsr_viz_map),
                "metsr_vis_frame_url": self._metsr_vis_frame_url(),
                "viz_url": self.viz_url,
            }

    def stop_external(self) -> None:
        # Do not dynamically import a generic "utils" package here: PCLA agent
        # loading also uses that common top-level module name.
        if self.external_server_thread is None:
            return
        stop_visualization_server(
            self.external_stop_event,
            self.external_server_thread,
            port=self.external_port or self.port,
        )
        self.external_stop_event = None
        self.external_server_thread = None


class PCLACameraTap:
    """Copy PCLA camera callbacks without consuming its sensor queues."""

    def __init__(self, pcla: Any) -> None:
        agent = getattr(pcla, "agent_instance", None)
        interface = getattr(agent, "sensor_interface", None)
        if interface is None or not callable(getattr(interface, "update_sensor", None)):
            raise RuntimeError("PCLA agent did not expose sensor_interface.update_sensor")

        specs = list(agent.sensors())
        self.camera_specs = [
            dict(spec)
            for spec in specs
            if str(spec.get("type", "")).startswith("sensor.camera.rgb")
        ]
        self.camera_specs.sort(key=lambda spec: str(spec.get("id", "")))
        self._labels = {
            str(spec.get("id")): _camera_label(spec, index)
            for index, spec in enumerate(self.camera_specs)
        }
        self._camera_ids = set(self._labels)
        self._interface = interface
        self._original_update = interface.update_sensor
        self._lock = threading.Lock()
        self._latest: Dict[str, Tuple[Any, np.ndarray]] = {}
        self._png_cache: Dict[str, Tuple[Any, bytes]] = {}
        original_update = self._original_update

        def update_sensor(tag: Any, data: Any, timestamp: Any) -> Any:
            sensor_id = str(tag)
            if sensor_id in self._camera_ids:
                array = np.asarray(data)
                if array.ndim == 3 and array.shape[2] >= 3:
                    # PCLA's callback stores CARLA BGRA; the browser expects RGB.
                    rgb = array[:, :, :3][:, :, ::-1].copy()
                    with self._lock:
                        self._latest[sensor_id] = (timestamp, rgb)
            return original_update(tag, data, timestamp)

        self._wrapped_update = update_sensor
        interface.update_sensor = update_sensor

    def snapshots(self) -> Dict[str, Tuple[str, bytes, Any]]:
        result: Dict[str, Tuple[str, bytes, Any]] = {}
        with self._lock:
            latest = dict(self._latest)
        for sensor_id, (frame, rgb) in latest.items():
            cached = self._png_cache.get(sensor_id)
            if cached is not None and cached[0] == frame:
                png = cached[1]
            else:
                png = image_array_to_png(rgb)
                self._png_cache[sensor_id] = (frame, png)
            result[sensor_id] = (self._labels.get(sensor_id, sensor_id), png, frame)
        return result

    def close(self) -> None:
        if getattr(self._interface, "update_sensor", None) is self._wrapped_update:
            self._interface.update_sensor = self._original_update


def _import_pcla(pcla_dir: Optional[str]) -> Any:
    if pcla_dir:
        root = Path(pcla_dir).expanduser().resolve()
        if not (root / "PCLA.py").exists():
            raise RuntimeError(f"--pcla-dir does not contain PCLA.py: {root}")
        if str(root) not in sys.path:
            sys.path.append(str(root))
    try:
        import PCLA as pcla_module
    except ImportError as exc:
        raise RuntimeError(
            "Could not import PCLA. Activate the PCLA Conda environment and pass "
            "--pcla-dir or set PCLA_HOME to the PCLA checkout."
        ) from exc
    return pcla_module


def _distance(a: Any, b: Any) -> float:
    return math.sqrt(
        (float(a.x) - float(b.x)) ** 2
        + (float(a.y) - float(b.y)) ** 2
        + (float(a.z) - float(b.z)) ** 2
    )


def _generate_pcla_route(
    pcla_module: Any,
    carla_client: Any,
    ego_actor: Any,
    output_path: Path,
    end_spawn_index: Optional[int],
    min_distance_m: float,
) -> Path:
    world = carla_client.get_world()
    start_location = ego_actor.get_location()
    spawn_points = list(world.get_map().get_spawn_points())
    if len(spawn_points) < 2:
        raise RuntimeError("Town06 did not expose enough spawn points for a PCLA route")

    if end_spawn_index is not None:
        if not 0 <= int(end_spawn_index) < len(spawn_points):
            raise ValueError(
                f"--route-end-spawn-index must be in [0, {len(spawn_points) - 1}]"
            )
        candidates = [spawn_points[int(end_spawn_index)]]
    else:
        candidates = sorted(
            spawn_points,
            key=lambda transform: _distance(start_location, transform.location),
            reverse=True,
        )
        preferred = [
            transform
            for transform in candidates
            if _distance(start_location, transform.location) >= float(min_distance_m)
        ]
        candidates = (preferred or candidates)[:20]

    last_error: Optional[BaseException] = None
    for destination in candidates:
        try:
            waypoints = pcla_module.location_to_waypoint(
                carla_client,
                start_location,
                destination.location,
            )
            if len(waypoints) <= 1:
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pcla_module.route_maker(waypoints, str(output_path))
            if output_path.exists():
                return output_path
        except Exception as exc:
            last_error = exc
    raise RuntimeError("Could not generate a connected Town06 route for PCLA") from last_error


class PCLAController:
    def __init__(
        self,
        args: argparse.Namespace,
        carla_module: Any,
        carla_client: Any,
        ego_actor: Any,
    ) -> None:
        self.ego_actor = ego_actor
        self.last_control: Any = None
        self.tap: Optional[PCLACameraTap] = None
        self.pcla: Any = None

        try:
            ego_actor.set_autopilot(False)
        except Exception:
            pass
        try:
            ego_actor.set_simulate_physics(True)
        except Exception:
            pass
        ego_actor.apply_control(carla_module.VehicleControl(throttle=0.0, brake=1.0))

        pcla_module = _import_pcla(args.pcla_dir)
        if args.pcla_route:
            route_path = _resolve_repo_path(args.pcla_route)
            if not route_path.exists():
                raise FileNotFoundError(f"PCLA route does not exist: {route_path}")
        else:
            route_path = _generate_pcla_route(
                pcla_module,
                carla_client,
                ego_actor,
                _resolve_repo_path(args.route_output),
                args.route_end_spawn_index,
                args.route_min_distance_m,
            )
        self.route_path = route_path

        # PCLA clears a generic top-level "utils" module while loading agents.
        # Preserve this repository's package so dashboard cleanup remains stable.
        repo_utils = {
            name: module
            for name, module in sys.modules.items()
            if name == "utils" or name.startswith("utils.")
        }
        try:
            self.pcla = pcla_module.PCLA(
                args.pcla_agent,
                ego_actor,
                str(route_path),
                carla_client,
            )
        finally:
            for name in list(sys.modules):
                if name == "utils" or name.startswith("utils."):
                    sys.modules.pop(name, None)
            sys.modules.update(repo_utils)
        self.tap = PCLACameraTap(self.pcla)

    @property
    def camera_specs(self) -> Sequence[Mapping[str, Any]]:
        return [] if self.tap is None else self.tap.camera_specs

    def step(self) -> Any:
        control = self.pcla.get_action()
        self.ego_actor.apply_control(control)
        self.last_control = control
        return control

    def camera_snapshots(self) -> Dict[str, Tuple[str, bytes, Any]]:
        return {} if self.tap is None else self.tap.snapshots()

    def close(self) -> None:
        if self.tap is not None:
            self.tap.close()
            self.tap = None
        if self.pcla is not None:
            try:
                self.pcla.cleanup()
            except Exception as exc:
                print(f"PCLA cleanup warning: {str(exc).splitlines()[0]}")
            self.pcla = None


@dataclass
class RuntimeDependencies:
    carla: Any
    METSRClient: Any
    CarlaCosimState: Any
    destroy_carla_actor: Any
    open_carla: Any
    release_ready_cosim_vehicles_from_queue: Any
    step_carla_metsr_cosim: Any
    teleport_metsr_vehicle_from_carla: Any
    prepare_sim_dirs: Any
    read_run_config: Any
    run_simulation_in_docker: Any


def _runtime_dependencies() -> RuntimeDependencies:
    try:
        import carla
        from clients.METSRClient import METSRClient
        from utils.carla_util import (
            CarlaCosimState,
            destroy_carla_actor,
            open_carla,
            release_ready_cosim_vehicles_from_queue,
            step_carla_metsr_cosim,
            teleport_metsr_vehicle_from_carla,
        )
        from utils.util import prepare_sim_dirs, read_run_config, run_simulation_in_docker
    except ImportError as exc:
        raise RuntimeError(
            "The CARLA/METS-R Python dependencies are unavailable. Run this script "
            "from the PCLA environment with the matching CARLA Python API installed."
        ) from exc
    return RuntimeDependencies(
        carla=carla,
        METSRClient=METSRClient,
        CarlaCosimState=CarlaCosimState,
        destroy_carla_actor=destroy_carla_actor,
        open_carla=open_carla,
        release_ready_cosim_vehicles_from_queue=release_ready_cosim_vehicles_from_queue,
        step_carla_metsr_cosim=step_carla_metsr_cosim,
        teleport_metsr_vehicle_from_carla=teleport_metsr_vehicle_from_carla,
        prepare_sim_dirs=prepare_sim_dirs,
        read_run_config=read_run_config,
        run_simulation_in_docker=run_simulation_in_docker,
    )


def _network_roads(network_file: str) -> List[str]:
    path = _resolve_repo_path(network_file)
    root = ET.parse(path).getroot()
    return [
        str(edge.get("id"))
        for edge in root.findall("edge")
        if edge.get("id") and not str(edge.get("id")).startswith(":")
    ]


def _carla_roads(world: Any) -> List[int]:
    try:
        root = ET.fromstring(world.get_map().to_opendrive())
        return [int(road.get("id")) for road in root.findall("road") if road.get("id")]
    except Exception:
        # carla_util treats an empty list as all drivable CARLA roads.
        return []


def _actor_for_vehicle(state: Any, vehicle_id: Any) -> Optional[Tuple[Any, Any]]:
    for key, actor in getattr(state, "active_vehicles", {}).items():
        if str(key) == str(vehicle_id):
            try:
                if actor is not None and actor.is_alive:
                    return key, actor
            except RuntimeError:
                continue
    return None


def _select_ego_actor(
    state: Any,
    requested_vehicle_id: Optional[Any],
) -> Optional[Tuple[Any, Any]]:
    if requested_vehicle_id is not None:
        return _actor_for_vehicle(state, requested_vehicle_id)
    for vehicle_id, actor in getattr(state, "active_vehicles", {}).items():
        try:
            if actor is not None and actor.is_alive:
                return vehicle_id, actor
        except RuntimeError:
            continue
    return None


def _speed_kmh(actor: Optional[Any]) -> float:
    if actor is None:
        return 0.0
    velocity = actor.get_velocity()
    return 3.6 * math.sqrt(velocity.x ** 2 + velocity.y ** 2 + velocity.z ** 2)


def _control_value(control: Any, name: str) -> str:
    value = getattr(control, name, None) if control is not None else None
    return "—" if value is None else f"{float(value):.3f}"


def _shutdown_metsr(client: Any, terminate: bool) -> None:
    if client is None:
        return
    try:
        client.stop_viz()
    except Exception:
        pass
    if terminate:
        original_timeout = getattr(client, "timeout", None)
        try:
            if original_timeout is not None:
                client.timeout = min(float(original_timeout), 5.0)
            client.terminate()
            return
        except Exception as exc:
            print(f"METS-R termination warning: {str(exc).splitlines()[0]}")
        finally:
            if original_timeout is not None:
                client.timeout = original_timeout
    try:
        client.close()
    except Exception:
        pass


def run(args: argparse.Namespace) -> int:
    deps = _runtime_dependencies()
    config_path = _resolve_repo_path(args.run_config)
    config = deps.read_run_config(str(config_path))
    config.run_config = str(config_path)
    config.sim_step_size = float(args.timestep)
    config.random_seeds = [int(args.seed)]
    config.v2x = False
    config.display_all = False
    config.verbose = False
    _configure_carla_host(config, args.carla_host)

    sim_dirs: List[str] = []
    traffic_manager = None
    world = None
    metsr = None
    dashboard: Optional[PCLADashboard] = None
    overhead_panel: Optional[CarlaSensorPanel] = None
    controller: Optional[PCLAController] = None
    state = None
    ego_vehicle_id: Optional[Any] = None
    ego_actor: Optional[Any] = None
    exit_code = 0

    try:
        print(f"Loading Town06 configuration: {config_path}")
        sim_dirs = deps.prepare_sim_dirs(config)
        if args.start_metsr:
            print("Starting METS-R in Docker (V2X/Kafka disabled for this demo).")
            deps.run_simulation_in_docker(config)

        print("Connecting to CARLA Town06.")
        carla_client, traffic_manager = deps.open_carla(config)
        world = carla_client.get_world()

        port = int(config.ports[0] if hasattr(config, "ports") else config.metsr_port[0])
        metsr = deps.METSRClient(
            host=config.metsr_host,
            port=port,
            sim_folder=sim_dirs[0] if sim_dirs else None,
            timeout=float(args.metsr_timeout_s),
            config_json=str(config_path),
            config=config,
        )

        metsr_roads = _network_roads(config.network_file)
        if not metsr_roads:
            raise RuntimeError(f"No non-internal roads found in {config.network_file}")
        print(f"Marking all {len(metsr_roads)} Town06 roads as co-sim roads.")
        metsr.set_cosim_road(metsr_roads)

        generated_vehicle_ids = list(
            range(int(args.start_vid), int(args.start_vid) + int(args.private_vehicle_count))
        )
        metsr.generate_trip(generated_vehicle_ids, -1, -1)
        deps.release_ready_cosim_vehicles_from_queue(metsr)

        viz_info = _start_viz_with_port_fallback(
            metsr,
            {
                "server_port": int(args.viz_stream_port),
                "host": args.viz_stream_host,
                "tick_interval": 1,
                "transform_coords": False,
                "include_public": True,
                "include_private": True,
                "include_links": False,
            },
        )
        stream_url = str(viz_info.get("browser_url") or viz_info.get("url") or "")

        state = deps.CarlaCosimState()
        overhead_panel = CarlaSensorPanel(
            world,
            deps.carla,
            deps.destroy_carla_actor,
            vehicle_camera_enabled=False,
            lidar_enabled=False,
        )
        overhead_panel.spawn_overhead_camera(z=float(args.carla_camera_z))

        dashboard = PCLADashboard(
            _resolve_repo_path(args.dashboard_dir),
            int(args.dashboard_port),
            args.viz_url,
            stream_url,
            bool(args.open_browser),
        )
        dashboard_url = dashboard.start()
        print(f"TRACR PCLA dashboard: {dashboard_url}")
        print("Waiting for a METS-R vehicle to enter Town06 and become the PCLA ego.")

        carla_road_ids = _carla_roads(world)
        render_info: Dict[str, Any] = {}
        render_error = ""
        camera_views: Dict[str, Tuple[str, bytes, Any]] = {}
        overhead_png: Optional[bytes] = None

        for loop_index in range(int(args.ticks)):
            deps.step_carla_metsr_cosim(
                metsr,
                world,
                traffic_manager,
                state=state,
                carla_roads=carla_road_ids,
                metsr_roads=metsr_roads,
                display_all=False,
                release_ready_queue=True,
                metsr_wait_forever=True,
                metsr_poll_timeout=float(args.metsr_poll_timeout_s),
                verbose=False,
            )

            if controller is None:
                selected = _select_ego_actor(state, args.ego_vehicle_id)
                if selected is not None:
                    ego_vehicle_id, ego_actor = selected
                    print(
                        f"Initializing {args.pcla_agent} for METS-R vehicle "
                        f"{ego_vehicle_id} / CARLA actor {ego_actor.id}."
                    )
                    controller = PCLAController(
                        args,
                        deps.carla,
                        carla_client,
                        ego_actor,
                    )
                    dashboard.configure_camera_views(controller.camera_specs)
                    dashboard.ego_vehicle_id = ego_vehicle_id
                    dashboard.viz_frame_url = dashboard._metsr_vis_frame_url()
                    print(f"PCLA route: {controller.route_path}")

            control = None
            if controller is not None and ego_actor is not None:
                speed_mps = _speed_kmh(ego_actor) / 3.6
                deps.teleport_metsr_vehicle_from_carla(
                    metsr,
                    ego_vehicle_id,
                    True,
                    ego_actor,
                    transform_coords=True,
                    speed=speed_mps,
                )
                control = controller.step()

            final_tick = loop_index == int(args.ticks) - 1
            render_now = final_tick or loop_index % int(args.render_every) == 0
            camera_now = final_tick or loop_index % int(args.camera_every) == 0
            dashboard_now = final_tick or loop_index % int(args.dashboard_every) == 0

            if render_now:
                try:
                    result = metsr.render(
                        client_wait_timeout=float(args.render_client_wait_timeout_s)
                    )
                    render_info = result if isinstance(result, dict) else {}
                    render_error = ""
                except Exception as exc:
                    render_error = str(exc).splitlines()[0]

            if camera_now and overhead_panel is not None:
                preferred = [] if ego_vehicle_id is None else [ego_vehicle_id]
                overhead_panel.ensure_sensors(state, preferred_vehicle_ids=preferred)
                overhead_png = overhead_panel.camera_png()
                if controller is not None:
                    camera_views = controller.camera_snapshots()

            if dashboard_now and dashboard is not None:
                actor_count = len(state.active_vehicles) + len(state.display_vehicles)
                speed_kmh = _speed_kmh(ego_actor)
                current_tick = getattr(metsr, "current_tick", loop_index)
                pcla_state = "driving" if controller is not None else "waiting for ego"
                status = (
                    f"tick={current_tick} | PCLA {pcla_state} | CARLA actors={actor_count} "
                    f"| ego speed={speed_kmh:.1f} km/h"
                )
                if render_error:
                    status += f" | METS-R Viz waiting: {render_error}"
                telemetry = {
                    "state": pcla_state,
                    "agent": args.pcla_agent,
                    "tick": current_tick,
                    "ego_vehicle": ego_vehicle_id if ego_vehicle_id is not None else "waiting",
                    "carla_actor": getattr(ego_actor, "id", "waiting"),
                    "speed_kmh": f"{speed_kmh:.1f}",
                    "throttle": _control_value(
                        control or getattr(controller, "last_control", None),
                        "throttle",
                    ),
                    "steer": _control_value(
                        control or getattr(controller, "last_control", None),
                        "steer",
                    ),
                    "brake": _control_value(
                        control or getattr(controller, "last_control", None),
                        "brake",
                    ),
                    "camera_views": len(camera_views),
                    "viz_clients": render_info.get("client_count", 0),
                }
                dashboard.publish(
                    status=status,
                    telemetry=telemetry,
                    ego_vehicle_id=ego_vehicle_id,
                    overhead_png=overhead_png,
                    camera_views=camera_views,
                )

        if controller is None:
            requested = (
                f"requested vehicle {args.ego_vehicle_id}"
                if args.ego_vehicle_id is not None
                else "any generated vehicle"
            )
            raise RuntimeError(
                f"PCLA never started because {requested} did not enter a co-sim road "
                f"within {args.ticks} ticks"
            )

        dashboard.publish(
            status=f"Completed {args.ticks} synchronous Town06 ticks",
            telemetry={
                "state": "complete",
                "agent": args.pcla_agent,
                "tick": getattr(metsr, "current_tick", args.ticks),
                "ego_vehicle": ego_vehicle_id,
                "speed_kmh": f"{_speed_kmh(ego_actor):.1f}",
                "route": Path(controller.route_path).name,
            },
            ego_vehicle_id=ego_vehicle_id,
            overhead_png=overhead_png,
            camera_views=camera_views,
        )
        print(f"Completed {args.ticks} synchronous ticks.")
        if args.hold_dashboard:
            input("Simulation complete. Press Enter to close the dashboard and clean up.")
    except KeyboardInterrupt:
        print("TRACR PCLA demo interrupted by user.")
        exit_code = 130
    except Exception as exc:
        exit_code = 1
        message = str(exc).splitlines()[0]
        print(f"TRACR PCLA demo failed: {message}")
        if dashboard is not None:
            dashboard.publish(
                status=f"Demo failed: {message}",
                telemetry={"state": "failed", "error": message},
                ego_vehicle_id=ego_vehicle_id,
            )
    finally:
        if dashboard is not None:
            dashboard.stop_external()
        if overhead_panel is not None:
            try:
                overhead_panel.close()
            except Exception:
                pass
        if controller is not None:
            controller.close()
        if state is not None:
            for store in (state.active_vehicles, state.display_vehicles):
                for actor in list(store.values()):
                    try:
                        deps.destroy_carla_actor(actor)
                    except Exception:
                        pass
                store.clear()
        if traffic_manager is not None:
            try:
                traffic_manager.set_synchronous_mode(False)
            except Exception:
                pass
        if world is not None:
            try:
                settings = world.get_settings()
                settings.synchronous_mode = False
                settings.fixed_delta_seconds = None
                world.apply_settings(settings)
            except Exception:
                pass
        _shutdown_metsr(metsr, terminate=bool(args.start_metsr))
    return exit_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
