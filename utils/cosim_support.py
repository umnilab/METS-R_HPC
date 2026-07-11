import base64
import io
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from html import escape

import numpy as np

os.environ.setdefault("MPLBACKEND", "Agg")
try:
    import matplotlib

    matplotlib.use("Agg", force=True)
except Exception:
    pass

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)
os.chdir(_REPO_ROOT)

from clients.KafkaDataProcessor import (
    bsm_core_heading_degrees as _kafka_bsm_core_heading_degrees,
    bsm_core_latitude_degrees as _kafka_bsm_core_latitude_degrees,
    bsm_core_longitude_degrees as _kafka_bsm_core_longitude_degrees,
    bsm_core_speed_mps as _kafka_bsm_core_speed_mps,
    get_bsm_core_data as _kafka_get_bsm_core_data,
)
from utils.util import (
    METS_R_VIS_PRIVATE_VEHICLE_TYPE,
    METS_R_VIS_PURDUE_MAP_ID,
    METS_R_VIS_VEHICLE_TYPE_BY_GROUP,
    build_metsr_vis_url,
)


def _deps():
    cached = getattr(_deps, "_cached", None)
    if cached is not None:
        return cached

    import carla
    from clients.KafkaDataProcessor import KafkaDataProcessor
    from clients.METSRClient import METSRClient
    from clients.VeinsClient import VeinsClient, build_bsm_records, build_mobility_records
    from utils.carla_util import (
        CarlaCosimState,
        carla_velocity_vector,
        destroy_carla_actor,
        destroy_tracked_carla_vehicle,
        metsr_bearing_to_carla_yaw,
        metsr_to_carla_location,
        open_carla,
        set_overlook_camera,
        spawn_carla_vehicle,
        update_carla_vehicle_from_metsr,
    )
    from utils.util import prepare_sim_dirs, read_run_config, run_simulation_in_docker, clear_all

    clear_all()  # clear all mets-r related processes, if any

    cached = locals()
    setattr(_deps, "_cached", cached)
    return cached

def kafka_bootstrap_servers(config):
    return getattr(
        config,
        "kafka_bootstrap_servers",
        getattr(config, "kafka_bootstrap_server", "localhost:29092"),
    )


def docker_compose_command():
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    if shutil.which("docker"):
        return ["docker", "compose"]
    raise RuntimeError(
        "Docker Compose was not found. Install Docker Desktop or start Kafka manually on localhost:29092."
    )


def run_docker_compose(*args):
    subprocess.run(docker_compose_command() + list(args), cwd="docker", check=True)


def wait_for_kafka(bootstrap_servers="localhost:29092", timeout_s=90):
    from kafka import KafkaAdminClient

    deadline = time.time() + timeout_s
    last_error = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            admin.close()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(
        f"Kafka broker at {bootstrap_servers!r} did not become ready within {timeout_s} seconds."
    ) from last_error


def probe_viz_stream(stream_url, timeout_s=1.0):
    """Return a short WebSocket reachability probe for the METS-R Vis stream."""
    if not stream_url:
        return {"ok": False, "url": stream_url, "error": "stream URL is not set"}
    try:
        from websockets.sync.client import connect
    except ImportError as exc:
        return {
            "ok": False,
            "url": stream_url,
            "error": f"websockets package is not available: {exc}",
        }

    try:
        try:
            websocket = connect(stream_url, open_timeout=float(timeout_s or 1.0))
        except TypeError:
            websocket = connect(stream_url)
        with websocket:
            return {"ok": True, "url": stream_url, "error": ""}
    except Exception as exc:
        return {"ok": False, "url": stream_url, "error": str(exc).splitlines()[0]}



def _download_text(url, timeout_s=10):
    import urllib.request

    request = urllib.request.Request(url, headers={"User-Agent": "TRACR demo"})
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def _read_cached_text(path):
    with open(path, "r", encoding="utf-8") as input_file:
        return input_file.read()


def _write_text(path, text):
    with open(path, "w", encoding="utf-8", newline="\n") as output_file:
        output_file.write(text)


def _download_or_cached_text(url, cache_path, timeout_s=10):
    try:
        text = _download_text(url, timeout_s=timeout_s)
        _write_text(cache_path, text)
        return text
    except Exception:
        if os.path.exists(cache_path):
            return _read_cached_text(cache_path)
        raise


def _download_bytes(url, timeout_s=10):
    import urllib.request

    request = urllib.request.Request(url, headers={"User-Agent": "TRACR demo"})
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return response.read()


def _read_cached_bytes(path):
    with open(path, "rb") as input_file:
        return input_file.read()


def _write_bytes(path, data):
    with open(path, "wb") as output_file:
        output_file.write(data)


def _download_or_cached_bytes(url, cache_path, timeout_s=10):
    try:
        data = _download_bytes(url, timeout_s=timeout_s)
        _write_bytes(cache_path, data)
        return data
    except Exception:
        if os.path.exists(cache_path):
            return _read_cached_bytes(cache_path)
        raise


def _metsr_vis_chunk_filenames(script):
    import re

    chunk_ids = set(re.findall(r"\.e\((\d+)\)", script or ""))
    return [f"{chunk_id}.bundle.js" for chunk_id in sorted(chunk_ids, key=lambda value: int(value))]


def _metsr_vis_asset_filenames(*texts):
    import re

    filenames = set()
    asset_pattern = re.compile(
        r"(?<![A-Za-z0-9_./-])([0-9a-f]{32}\.(?:png|jpg|jpeg|gif|svg|wasm|json))(?![A-Za-z0-9_./-])",
        re.IGNORECASE,
    )
    for text in texts:
        if text:
            filenames.update(match.group(1) for match in asset_pattern.finditer(text))
    return sorted(filenames)


def _mirror_metsr_vis_file(local_dir, base_url, filename, timeout_s=10):
    import urllib.parse

    target_path = os.path.join(local_dir, filename)
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    url = urllib.parse.urljoin(base_url, filename)
    try:
        _download_or_cached_bytes(url, target_path, timeout_s=timeout_s)
        return True, ""
    except Exception as exc:
        if os.path.exists(target_path):
            return True, str(exc).splitlines()[0]
        return False, str(exc).splitlines()[0]


def _mirror_metsr_vis_resources(local_dir, base_url, script, style="", timeout_s=10):
    chunk_names = _metsr_vis_chunk_filenames(script)
    mirrored = 0
    missing = []
    chunk_texts = []

    for filename in chunk_names:
        ok, error = _mirror_metsr_vis_file(local_dir, base_url, filename, timeout_s=timeout_s)
        if ok:
            mirrored += 1
            try:
                chunk_texts.append(_read_cached_text(os.path.join(local_dir, filename)))
            except Exception:
                pass
        else:
            missing.append({"file": filename, "error": error})

    asset_names = _metsr_vis_asset_filenames(script, style, *chunk_texts)
    for filename in asset_names:
        ok, error = _mirror_metsr_vis_file(local_dir, base_url, filename, timeout_s=timeout_s)
        if ok:
            mirrored += 1
        else:
            missing.append({"file": filename, "error": error})

    return {
        "chunks": len(chunk_names),
        "assets": len(asset_names),
        "total": len(chunk_names) + len(asset_names),
        "mirrored": mirrored,
        "missing": missing,
    }

def _patch_metsr_vis_script_for_tracr(script, stream_url):
    patched = script.replace("ws://localhost:8765", stream_url)
    stream_replacement_count = script.count("ws://localhost:8765")
    selection_target = "window.METSRVis={connectStream:this.connectStream,disconnectStream:this.disconnectStream}"
    selection_patch = (
        "(function(e){window.METSRVis={connectStream:e.connectStream,disconnectStream:e.disconnectStream,"
        "selectVehicleById:function(t){if(null==t)return!1;for(var n=Object.keys(e.frameVehicles||{}),r=0;r<n.length;r++){"
        "var i=n[r],o=e.findVehicleIndexById(i,t);if(o>=0){var a=e.getVehicleAt(i,o);"
        "if(a){var s=e.state&&e.state.selected_vehicle;if(s&&String(s.id)===String(a.id)&&String(s.typeKey)===String(a.typeKey))return!0;"
        "return e.setState({selected_vehicle:a,selected_agent_history:[e.createAgentSample(a,e.currentTick)]}),!0}}}return!1}};"
        "window.addEventListener(\"message\",function(e){var t=e&&e.data||{};"
        "t&&\"tracr-select-vehicle\"===t.type&&null!=t.vehicleId&&window.METSRVis&&window.METSRVis.selectVehicleById(t.vehicleId)})})(this)"
    )
    selection_patch_count = 0
    if "tracr-select-vehicle" not in patched and selection_target in patched:
        patched = patched.replace(selection_target, selection_patch, 1)
        selection_patch_count = 1
    return patched, stream_replacement_count, selection_patch_count

def prepare_local_metsr_vis(directory, viz_url, stream_url, timeout_s=10):
    """Serve METS-R Vis locally with its Stream default pointed at stream_url."""
    import re
    import urllib.parse

    if not stream_url:
        return {"url": viz_url, "status": "METS-R Vis stream URL is not set."}

    local_dir = os.path.join(directory, "metsr_vis")
    os.makedirs(local_dir, exist_ok=True)
    base_url = viz_url if str(viz_url).endswith("/") else str(viz_url) + "/"
    html_url = urllib.parse.urljoin(base_url, "./")
    html_cache = os.path.join(local_dir, "index.remote.html")
    html = _download_or_cached_text(html_url, html_cache, timeout_s=timeout_s)

    script_match = re.search(r"<script[^>]+src=['\"]([^'\"]*index\.js[^'\"]*)['\"]", html)
    if not script_match:
        raise RuntimeError("Could not find METS-R Vis index.js in the page HTML.")
    script_src = script_match.group(1)
    script_url = urllib.parse.urljoin(base_url, script_src)
    script_cache = os.path.join(local_dir, "index.remote.js")
    script = _download_or_cached_text(script_url, script_cache, timeout_s=timeout_s)
    patched_script, replacement_count, selection_patch_count = _patch_metsr_vis_script_for_tracr(
        script,
        stream_url,
    )
    _write_text(os.path.join(local_dir, "index.js"), patched_script)

    style = ""
    style_match = re.search(r"<link[^>]+href=['\"]([^'\"]*style\.css[^'\"]*)['\"]", html)
    if style_match:
        style_src = style_match.group(1)
        style_url = urllib.parse.urljoin(base_url, style_src)
        style_cache = os.path.join(local_dir, "style.remote.css")
        style = _download_or_cached_text(style_url, style_cache, timeout_s=timeout_s)
        _write_text(os.path.join(local_dir, "style.css"), style)
        html = html.replace(style_src, "style.css")

    resource_info = _mirror_metsr_vis_resources(local_dir, base_url, script, style, timeout_s=timeout_s)

    license_url = urllib.parse.urljoin(base_url, "index.js.LICENSE.txt")
    try:
        license_text = _download_text(license_url, timeout_s=timeout_s)
        _write_text(os.path.join(local_dir, "index.js.LICENSE.txt"), license_text)
    except Exception:
        pass

    html = html.replace(script_src, "index.js")
    _write_text(os.path.join(local_dir, "index.html"), html)
    selection_status = "selection hook installed" if selection_patch_count else "selection hook unavailable"
    resource_total = resource_info.get("total", 0)
    resource_mirrored = resource_info.get("mirrored", 0)
    missing_resources = resource_info.get("missing", [])
    resource_status = f"{resource_mirrored}/{resource_total} resources mirrored"
    if missing_resources:
        resource_status += f"; {len(missing_resources)} missing"
    return {
        "url": "metsr_vis/index.html",
        "status": f"Embedded METS-R Vis Stream default patched to {stream_url} ({replacement_count} stream replacements; {selection_status}; {resource_status}).",
        "resource_info": resource_info,
    }


def fig_to_png(fig, tight=False, pad_inches=0.0):
    import matplotlib.pyplot as plt

    buffer = io.BytesIO()
    save_kwargs = {"format": "png", "dpi": 110}
    if tight:
        save_kwargs.update({"bbox_inches": "tight", "pad_inches": pad_inches})
    fig.savefig(buffer, **save_kwargs)
    plt.close(fig)
    return buffer.getvalue()


def blank_png(text, width=6.4, height=3.6):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(width, height))
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_facecolor("#111827")
    ax.text(0.5, 0.5, text, color="#e5e7eb", ha="center", va="center", fontsize=12)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    return fig_to_png(fig)


def _pil_png_from_array(array):
    try:
        from PIL import Image

        image = Image.fromarray(np.asarray(array, dtype=np.uint8))
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", compress_level=1)
        return buffer.getvalue()
    except Exception:
        return None


def image_array_to_png(rgb_array):
    png = _pil_png_from_array(rgb_array)
    if png is not None:
        return png

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.imshow(rgb_array)
    ax.set_axis_off()
    return fig_to_png(fig)

def first_present(record, *keys):
    if not isinstance(record, Mapping):
        return None
    for key in keys:
        value = record.get(key)
        if value is not None:
            return value
    return None


class CarlaSensorPanel:
    """Owns CARLA demo sensors and keeps only the latest callback frame."""

    def __init__(self, world, carla_module, destroy_actor_func, vehicle_camera_enabled=True, lidar_enabled=True):
        self.world = world
        self.carla = carla_module
        self.destroy_actor = destroy_actor_func
        self.vehicle_camera_enabled = bool(vehicle_camera_enabled)
        self.lidar_enabled = bool(lidar_enabled)
        self.camera_actor = None
        self.vehicle_camera_actor = None
        self.lidar_actor = None
        self.lidar_parent_id = None
        self.lidar_settings = None
        self.latest_lidar_yaw = None
        self.vehicle_camera_parent_id = None
        self.target_actor_id = None
        self.target_vehicle_id = None
        self.overhead_camera_z = 205.8
        self.overhead_camera_yaw = -90.0
        self.overhead_camera_pitch = -90.0
        self.latest_camera = None
        self.latest_vehicle_camera = None
        self.latest_lidar = None
        self.latest_camera_frame = None
        self.latest_vehicle_camera_frame = None
        self.latest_lidar_frame = None
        self.latest_lidar_yaw = None

    def spawn_overhead_camera(
        self,
        x=0.0,
        y=0.0,
        z=205.8,
        yaw=-90.0,
        pitch=-90.0,
        width=960,
        height=540,
        fov=80,
    ):
        self.overhead_camera_z = float(z)
        self.overhead_camera_yaw = float(yaw)
        self.overhead_camera_pitch = float(pitch)
        if self.camera_actor is not None:
            return self.camera_actor
        blueprint = self.world.get_blueprint_library().find("sensor.camera.rgb")
        blueprint.set_attribute("image_size_x", str(width))
        blueprint.set_attribute("image_size_y", str(height))
        blueprint.set_attribute("fov", str(fov))
        transform = self.carla.Transform(
            self.carla.Location(x=float(x), y=float(y), z=float(z)),
            self.carla.Rotation(pitch=float(pitch), yaw=float(yaw), roll=0.0),
        )
        self.camera_actor = self.world.spawn_actor(blueprint, transform)
        self.camera_actor.listen(self._on_camera)
        return self.camera_actor

    def _vehicle_camera_transform(self, parent_actor, x=1.7, z=2.0, pitch=-4.0):
        parent_transform = parent_actor.get_transform()
        location = parent_transform.location
        yaw = float(parent_transform.rotation.yaw)
        yaw_rad = np.deg2rad(yaw)
        camera_location = self.carla.Location(
            x=float(location.x) + float(x) * float(np.cos(yaw_rad)),
            y=float(location.y) + float(x) * float(np.sin(yaw_rad)),
            z=float(location.z) + float(z),
        )
        return self.carla.Transform(
            camera_location,
            self.carla.Rotation(pitch=float(pitch), yaw=yaw, roll=0.0),
        )

    def _sync_vehicle_camera_transform(self, parent_actor, x=1.7, z=2.0, pitch=-4.0):
        if self.vehicle_camera_actor is None:
            return
        try:
            self.vehicle_camera_actor.set_transform(
                self._vehicle_camera_transform(parent_actor, x=x, z=z, pitch=pitch)
            )
        except RuntimeError:
            pass

    def attach_vehicle_camera(
        self,
        parent_actor,
        x=1.7,
        z=2.0,
        pitch=-4.0,
        width=960,
        height=540,
        fov=95,
    ):
        parent_id = getattr(parent_actor, "id", None)
        if parent_id is None:
            return None
        if self.vehicle_camera_actor is not None and self.vehicle_camera_parent_id == parent_id:
            try:
                if self.vehicle_camera_actor.is_alive:
                    self._sync_vehicle_camera_transform(parent_actor, x=x, z=z, pitch=pitch)
                    return self.vehicle_camera_actor
            except RuntimeError:
                pass
        if self.vehicle_camera_actor is not None:
            self.destroy_actor(self.vehicle_camera_actor)
            self.vehicle_camera_actor = None
            self.vehicle_camera_parent_id = None
            self.latest_vehicle_camera = None

        blueprint = self.world.get_blueprint_library().find("sensor.camera.rgb")
        blueprint.set_attribute("image_size_x", str(width))
        blueprint.set_attribute("image_size_y", str(height))
        blueprint.set_attribute("fov", str(fov))
        transform = self._vehicle_camera_transform(parent_actor, x=x, z=z, pitch=pitch)
        self.vehicle_camera_actor = self.world.spawn_actor(blueprint, transform)
        self.vehicle_camera_parent_id = parent_id
        self.vehicle_camera_actor.listen(self._on_vehicle_camera)
        return self.vehicle_camera_actor

    def attach_lidar(
        self,
        parent_actor,
        z=2.0,
        lidar_range=100,
        channels=64,
        points_per_second=600000,
        rotation_frequency=20,
    ):
        parent_id = getattr(parent_actor, "id", None)
        if parent_id is None:
            return None
        settings = (
            float(z),
            float(lidar_range),
            int(channels),
            int(points_per_second),
            float(rotation_frequency),
        )
        if self.lidar_actor is not None and self.lidar_parent_id == parent_id:
            try:
                if self.lidar_actor.is_alive and self.lidar_settings == settings:
                    return self.lidar_actor
            except RuntimeError:
                pass
        if self.lidar_actor is not None:
            self.destroy_actor(self.lidar_actor)
            self.lidar_actor = None
            self.lidar_parent_id = None
            self.lidar_settings = None
            self.latest_lidar = None
            self.latest_lidar_yaw = None

        blueprint = self.world.get_blueprint_library().find("sensor.lidar.ray_cast")
        blueprint.set_attribute("range", str(lidar_range))
        blueprint.set_attribute("channels", str(channels))
        blueprint.set_attribute("points_per_second", str(points_per_second))
        blueprint.set_attribute("rotation_frequency", str(rotation_frequency))
        transform = self.carla.Transform(self.carla.Location(x=0.0, y=0.0, z=float(z)))
        self.lidar_actor = self.world.spawn_actor(blueprint, transform, attach_to=parent_actor)
        self.lidar_parent_id = parent_id
        self.lidar_settings = settings
        self.lidar_actor.listen(self._on_lidar)
        return self.lidar_actor

    def _select_target_actor(self, state, preferred_vehicle_ids=None):
        live_by_vehicle_id = {}
        for store in (getattr(state, "active_vehicles", {}), getattr(state, "display_vehicles", {})):
            for vehicle_id, actor in list(store.items()):
                try:
                    if actor is not None and actor.is_alive:
                        live_by_vehicle_id[str(vehicle_id)] = (vehicle_id, actor)
                except RuntimeError:
                    continue

        preferred_keys = [str(vehicle_id) for vehicle_id in (preferred_vehicle_ids or [])]
        target_pair = None
        if self.target_vehicle_id is not None and (
            not preferred_keys or str(self.target_vehicle_id) in preferred_keys
        ):
            target_pair = live_by_vehicle_id.get(str(self.target_vehicle_id))

        if target_pair is None:
            for vehicle_id in preferred_keys:
                target_pair = live_by_vehicle_id.get(vehicle_id)
                if target_pair is not None:
                    break

        if target_pair is None and self.target_vehicle_id is not None:
            target_pair = live_by_vehicle_id.get(str(self.target_vehicle_id))

        if target_pair is None and live_by_vehicle_id:
            target_pair = next(iter(live_by_vehicle_id.values()))

        if target_pair is None:
            self.target_vehicle_id = None
            self.target_actor_id = None
            return None

        vehicle_id, target = target_pair
        self.target_vehicle_id = vehicle_id
        self.target_actor_id = getattr(target, "id", None)
        return target

    def track_target_actor(self, parent_actor):
        try:
            target_transform = parent_actor.get_transform()
        except RuntimeError:
            return
        location = target_transform.location
        transform = self.carla.Transform(
            self.carla.Location(
                x=float(location.x),
                y=float(location.y),
                z=float(self.overhead_camera_z),
            ),
            self.carla.Rotation(
                pitch=float(self.overhead_camera_pitch),
                yaw=float(self.overhead_camera_yaw),
                roll=0.0,
            ),
        )
        actors = [self.camera_actor]
        try:
            actors.append(self.world.get_spectator())
        except RuntimeError:
            pass
        for actor in actors:
            if actor is None:
                continue
            try:
                actor.set_transform(transform)
            except RuntimeError:
                continue

    def ensure_sensors(self, state, preferred_vehicle_ids=None):
        self.spawn_overhead_camera()
        target_actor = self._select_target_actor(state, preferred_vehicle_ids=preferred_vehicle_ids)
        if target_actor is not None:
            self.track_target_actor(target_actor)
            if self.lidar_enabled:
                self.attach_lidar(target_actor)
            if self.vehicle_camera_enabled:
                self.attach_vehicle_camera(target_actor)

    def camera_png(self):
        if self.latest_camera is None:
            return blank_png("Waiting for CARLA bird-eye camera")
        return image_array_to_png(self.latest_camera)

    def vehicle_camera_png(self):
        if self.latest_vehicle_camera is None:
            return blank_png("Waiting for CARLA vehicle camera")
        return image_array_to_png(self.latest_vehicle_camera)

    def lidar_png(self):
        if self.latest_lidar is None or len(self.latest_lidar) == 0:
            return blank_png("Waiting for CARLA LiDAR")
        return lidar_points_to_png(
            self.latest_lidar,
            sensor_yaw_degrees=self._latest_lidar_yaw_degrees(),
        )

    def _latest_lidar_yaw_degrees(self):
        if self.latest_lidar_yaw is not None:
            return self.latest_lidar_yaw
        if self.lidar_actor is None:
            return None
        try:
            return float(self.lidar_actor.get_transform().rotation.yaw)
        except (RuntimeError, AttributeError, TypeError, ValueError):
            return None

    def close(self):
        for actor in (self.camera_actor, self.vehicle_camera_actor, self.lidar_actor):
            if actor is not None:
                self.destroy_actor(actor)
        self.camera_actor = None
        self.vehicle_camera_actor = None
        self.lidar_actor = None
        self.lidar_parent_id = None
        self.lidar_settings = None
        self.latest_lidar_yaw = None
        self.vehicle_camera_parent_id = None
        self.target_actor_id = None

        self.target_vehicle_id = None

    def _on_camera(self, image):
        array = np.frombuffer(image.raw_data, dtype=np.uint8)
        array = array.reshape((image.height, image.width, 4))
        rgb = array[:, :, :3][:, :, ::-1]
        self.latest_camera = rgb.copy()
        self.latest_camera_frame = image.frame

    def _on_vehicle_camera(self, image):
        array = np.frombuffer(image.raw_data, dtype=np.uint8)
        array = array.reshape((image.height, image.width, 4))
        rgb = array[:, :, :3][:, :, ::-1]
        self.latest_vehicle_camera = rgb.copy()
        self.latest_vehicle_camera_frame = image.frame

    def _on_lidar(self, measurement):
        points = np.frombuffer(measurement.raw_data, dtype=np.float32)
        if points.size == 0:
            self.latest_lidar = np.empty((0, 4), dtype=np.float32)
        else:
            self.latest_lidar = points.reshape((-1, 4)).copy()
        self.latest_lidar_frame = measurement.frame
        transform = getattr(measurement, "transform", None)
        rotation = getattr(transform, "rotation", None)
        yaw = getattr(rotation, "yaw", None)
        try:
            self.latest_lidar_yaw = None if yaw is None else float(yaw)
        except (TypeError, ValueError):
            self.latest_lidar_yaw = None


def _rotate_lidar_xy_to_carla_world(xy, sensor_yaw_degrees):
    if sensor_yaw_degrees is None:
        return xy
    try:
        yaw = float(sensor_yaw_degrees)
    except (TypeError, ValueError):
        return xy
    if not np.isfinite(yaw):
        return xy
    yaw_rad = np.deg2rad(yaw)
    cos_yaw = float(np.cos(yaw_rad))
    sin_yaw = float(np.sin(yaw_rad))
    rotated = np.empty_like(xy, dtype=np.float32)
    rotated[:, 0] = xy[:, 0] * cos_yaw - xy[:, 1] * sin_yaw
    rotated[:, 1] = xy[:, 0] * sin_yaw + xy[:, 1] * cos_yaw
    return rotated


def _paint_lidar_background(width, height):
    top = np.array([8, 13, 24], dtype=np.float32)
    bottom = np.array([15, 23, 42], dtype=np.float32)
    y = np.linspace(0.0, 1.0, int(height), dtype=np.float32)[:, None, None]
    canvas = (top * (1.0 - y) + bottom * y).repeat(int(width), axis=1)

    yy, xx = np.ogrid[:height, :width]
    cx = width * 0.5
    cy = height * 0.5
    distance = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)
    grid_color = np.array([42, 55, 78], dtype=np.float32)
    for radius in (0.18, 0.34, 0.50, 0.66, 0.82):
        ring = np.abs(distance - radius * min(width, height) * 0.47) <= 0.7
        canvas[ring] = canvas[ring] * 0.70 + grid_color * 0.30

    grid_step = max(32, min(width, height) // 5)
    center_x = int(cx)
    center_y = int(cy)
    canvas[:, center_x::grid_step] = canvas[:, center_x::grid_step] * 0.82 + grid_color * 0.18
    canvas[:, center_x::-grid_step] = canvas[:, center_x::-grid_step] * 0.82 + grid_color * 0.18
    canvas[center_y::grid_step, :] = canvas[center_y::grid_step, :] * 0.82 + grid_color * 0.18
    canvas[center_y::-grid_step, :] = canvas[center_y::-grid_step, :] * 0.82 + grid_color * 0.18
    canvas[center_y - 1 : center_y + 2, :] = canvas[center_y - 1 : center_y + 2, :] * 0.75 + grid_color * 0.25
    canvas[:, center_x - 1 : center_x + 2] = canvas[:, center_x - 1 : center_x + 2] * 0.75 + grid_color * 0.25
    return canvas


def _paint_lidar_points(canvas, px, py, colors, width, height):
    glow = np.zeros((height, width, 3), dtype=np.float32)
    alpha = np.zeros((height, width), dtype=np.float32)
    offsets = []
    for dy in range(-3, 4):
        for dx in range(-3, 4):
            distance = math.hypot(dx, dy)
            if distance <= 3.25:
                offsets.append((dx, dy, math.exp(-(distance * distance) / 5.0)))

    colors_f = colors.astype(np.float32, copy=False)
    for dx, dy, weight in offsets:
        xx = px + dx
        yy = py + dy
        valid = (xx >= 0) & (xx < width) & (yy >= 0) & (yy < height)
        if not np.any(valid):
            continue
        yy_valid = yy[valid]
        xx_valid = xx[valid]
        np.maximum.at(alpha, (yy_valid, xx_valid), min(0.88, 0.18 + 0.62 * weight))
        for channel in range(3):
            np.maximum.at(
                glow[:, :, channel],
                (yy_valid, xx_valid),
                colors_f[valid, channel] * (0.45 + 0.55 * weight),
            )

    alpha_3 = alpha[:, :, None]
    styled = canvas * (1.0 - alpha_3) + glow * alpha_3
    center_x = width // 2
    center_y = height // 2
    center_color = np.array([244, 63, 94], dtype=np.float32)
    styled[center_y - 2 : center_y + 3, center_x - 10 : center_x + 11] = center_color
    styled[center_y - 10 : center_y + 11, center_x - 2 : center_x + 3] = center_color
    return np.clip(styled, 0, 255).astype(np.uint8)


def _pil_lidar_png_from_array(array):
    try:
        from PIL import Image, ImageFilter

        image = Image.fromarray(np.asarray(array, dtype=np.uint8))
        image = image.filter(ImageFilter.UnsharpMask(radius=1.1, percent=110, threshold=3))
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", compress_level=1)
        return buffer.getvalue()
    except Exception:
        return None


def lidar_points_to_png(points, max_points=60000, width=704, height=396, sensor_yaw_degrees=None):
    points = np.asarray(points)
    if points.size == 0:
        return blank_png("Waiting for CARLA LiDAR")
    if points.ndim != 2 or points.shape[1] < 2:
        return blank_png("Waiting for CARLA LiDAR")
    if len(points) > max_points:
        step = max(1, len(points) // max_points)
        points = points[::step]

    xy = points[:, :2].astype(np.float32, copy=False)
    finite = np.isfinite(xy).all(axis=1)
    if not np.any(finite):
        return blank_png("Waiting for CARLA LiDAR")
    xy = xy[finite]
    kept_points = points[finite]
    xy = _rotate_lidar_xy_to_carla_world(xy, sensor_yaw_degrees)

    radius = max(
        abs(float(np.min(xy[:, 0]))),
        abs(float(np.max(xy[:, 0]))),
        abs(float(np.min(xy[:, 1]))),
        abs(float(np.max(xy[:, 1]))),
        1.0,
    )
    scale = 0.47 * min(width, height) / radius
    px = (width * 0.5 + xy[:, 0] * scale).astype(np.int32)
    py = (height * 0.5 + xy[:, 1] * scale).astype(np.int32)
    visible = (px >= 0) & (px < width) & (py >= 0) & (py < height)
    if not np.any(visible):
        return blank_png("Waiting for CARLA LiDAR")
    px = px[visible]
    py = py[visible]
    kept_points = kept_points[visible]

    if kept_points.shape[1] > 3:
        intensity = kept_points[:, 3].astype(np.float32, copy=False)
    else:
        intensity = np.hypot(kept_points[:, 0], kept_points[:, 1]).astype(np.float32, copy=False)
    finite_intensity = np.isfinite(intensity)
    if np.any(finite_intensity):
        low = float(np.percentile(intensity[finite_intensity], 5))
        high = float(np.percentile(intensity[finite_intensity], 95))
    else:
        low, high = 0.0, 1.0
    denom = max(high - low, 1e-6)
    t = np.clip((intensity - low) / denom, 0.0, 1.0)

    canvas = _paint_lidar_background(width, height)
    colors = np.stack(
        [
            (35 + 220 * t).astype(np.uint8),
            (205 + 45 * (1.0 - np.abs(t - 0.55))).clip(0, 255).astype(np.uint8),
            (120 + 110 * (1.0 - t)).astype(np.uint8),
        ],
        axis=1,
    )
    canvas = _paint_lidar_points(canvas, px, py, colors, width, height)

    png = _pil_lidar_png_from_array(canvas)
    if png is not None:
        return png

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.imshow(canvas, interpolation="bilinear", resample=True)
    ax.set_axis_off()
    return fig_to_png(fig)

def _format_bsm_value(value, precision=2):
    if value is None or value == "":
        return "NA"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{precision}f}"
    if isinstance(value, (int, np.integer)) and precision is not None:
        if int(precision) == 0:
            return str(int(value))
        return f"{float(value):.{precision}f}"
    return str(value)


def _safe_float(value):
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.strip().lower() in {"na", "nan", "none", "null"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    number = _safe_float(value)
    if number is None:
        return None
    try:
        return int(round(number))
    except (TypeError, ValueError):
        return None


def _as_mapping(value):
    return value if isinstance(value, Mapping) else {}


def _bsm_core_payload(value):
    payload = _as_mapping(value)
    for key in ("coreData", "BSMcoreData", "core_data"):
        core = payload.get(key)
        if isinstance(core, Mapping):
            return core
    return {}


def _bsm_core_data(record):
    core = _kafka_get_bsm_core_data(record)
    if core:
        return core
    record = _as_mapping(record)
    messaging = _as_mapping(record.get("messaging_layer"))
    payload = _as_mapping(record.get("payload"))
    frame = _as_mapping(record.get("messageFrame") or record.get("message_frame"))
    frame_value = _as_mapping(frame.get("value"))
    value = _as_mapping(record.get("value"))
    candidates = [
        record,
        messaging,
        payload,
        record.get("BasicSafetyMessage"),
        record.get("basicSafetyMessage"),
        payload.get("BasicSafetyMessage"),
        payload.get("basicSafetyMessage"),
        frame_value.get("BasicSafetyMessage"),
        frame_value.get("basicSafetyMessage"),
        value.get("BasicSafetyMessage"),
        value.get("basicSafetyMessage"),
    ]
    for candidate in candidates:
        core = _bsm_core_payload(candidate)
        if core:
            return core
    return {}


def _record_with_bsm_core(record):
    if not isinstance(record, Mapping):
        return {}
    core = _bsm_core_data(record)
    if not core:
        return record
    view = dict(record)
    view["coreData"] = core
    return view


def _bsm_core_value(record, *keys):
    return first_present(_bsm_core_data(record), *keys)


def _coordinate_to_degrees(value, limit, unavailable):
    number = _safe_float(value)
    if number is None:
        return None
    encoded = _safe_int(value)
    if encoded == unavailable:
        return None
    if -limit <= number <= limit:
        return number
    decoded = number / 10_000_000.0
    if -limit <= decoded <= limit:
        return decoded
    return None


def _bsm_lat_deg(record):
    value = _kafka_bsm_core_latitude_degrees(_record_with_bsm_core(record))
    if value is not None:
        return value
    value = first_present(record, "latitude", "lat_deg", "latitude_e7", "lat_e7", "lat")
    return _coordinate_to_degrees(value, 90.0, 900000001)


def _bsm_long_deg(record):
    value = _kafka_bsm_core_longitude_degrees(_record_with_bsm_core(record))
    if value is not None:
        return value
    value = first_present(record, "longitude", "lon", "long_deg", "longitude_e7", "lon_e7", "long")
    return _coordinate_to_degrees(value, 180.0, 1800000001)


def _bsm_elevation_m(record):
    value = first_present(record, "elevation_m", "elevation")
    if value is not None:
        return _safe_float(value)
    value = _bsm_core_value(record, "elev", "elevation_dm")
    if value is None:
        value = first_present(record, "elev", "elevation_dm")
    encoded = _safe_int(value)
    if encoded is not None and encoded != -4096:
        return encoded / 10.0
    return _safe_float(first_present(record, "z", "tx_z"))


def _bsm_speed_mps(record):
    value = _kafka_bsm_core_speed_mps(_record_with_bsm_core(record))
    if value is not None:
        return value
    encoded = _safe_int(first_present(record, "speed_units", "bsm_speed"))
    if encoded is not None and encoded != 8191:
        return encoded * 0.02
    return _safe_float(first_present(record, "speed_mps", "speed_ms", "velocity", "speed", "tx_speed_mps", "payload_speed_mps"))


def _bsm_heading_deg(record):
    value = _kafka_bsm_core_heading_degrees(_record_with_bsm_core(record))
    if value is not None:
        return value
    encoded = _safe_int(first_present(record, "heading_units", "bsm_heading"))
    if encoded is not None and encoded != 28800:
        return (encoded * 0.0125) % 360.0
    value = _safe_float(first_present(record, "heading_deg", "bearing", "heading", "tx_heading_deg", "payload_heading_deg"))
    return None if value is None else value % 360.0


def _bsm_message_count(record):
    value = _bsm_core_value(record, "msgCnt", "msg_count")
    if value is None:
        value = first_present(record, "message_count", "msg_count", "msgCnt")
    return value


def _bsm_sec_mark(record):
    value = _bsm_core_value(record, "secMark", "sec_mark")
    if value is None:
        value = first_present(record, "secMark", "sec_mark", "timestamp_ms", "tick")
    return value


def _format_bsm_id(value):
    if value is None or value == "":
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, (list, tuple)):
        try:
            return "".join(f"{int(item) & 0xFF:02x}" for item in value)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def bsm_sender_id(record):
    return first_present(record, "vid", "vehicle_id", "sender_id", "origin_vehicle_id")


def bsm_receiver_id(record):
    return first_present(record, "receiver_id", "target_vehicle_id", "rx_vehicle_id")


def _same_vehicle_id(left, right):
    if left is None or right is None:
        return False
    return str(left) == str(right)


def _format_vehicle_id(value, ego_vehicle_id=None):
    if value is None or value == "":
        return "broadcast"
    text = str(value)
    if _same_vehicle_id(value, ego_vehicle_id):
        return f"{text} (ego)"
    return text


def _bsm_display_id(record, ego_vehicle_id=None):
    display_id = _format_bsm_id(_bsm_core_value(record, "id", "temporary_id"))
    sender = bsm_sender_id(record)
    if display_id is None:
        display_id = _format_bsm_id(first_present(record, "temporary_id", "id"))
    if display_id is None:
        return _format_vehicle_id(sender, ego_vehicle_id)
    if _same_vehicle_id(sender, ego_vehicle_id):
        return f"{display_id} (ego)"
    return display_id


def _bsm_role(record, ego_vehicle_id=None):
    sender = bsm_sender_id(record)
    receiver = bsm_receiver_id(record)
    sender_text = _format_vehicle_id(sender, ego_vehicle_id)
    receiver_text = _format_vehicle_id(receiver, ego_vehicle_id)
    if isinstance(record, Mapping) and record.get("_tracr_ego_heard"):
        return f"heard from {sender_text}"
    if receiver is None:
        if ego_vehicle_id is not None and _same_vehicle_id(sender, ego_vehicle_id):
            return "ego broadcast"
        return f"broadcast from {sender_text}"
    if ego_vehicle_id is not None:
        if _same_vehicle_id(receiver, ego_vehicle_id):
            return f"rx from {sender_text}"
        if _same_vehicle_id(sender, ego_vehicle_id):
            return f"tx to {receiver_text}"
    return f"{sender_text} -> {receiver_text}"


def _bsm_brake_summary(record):
    brakes = _bsm_core_value(record, "brakes", "brakeSystemStatus")
    if brakes is None:
        brakes = first_present(record, "brakes", "brake_status", "brakeSystemStatus")
    if not isinstance(brakes, Mapping):
        return _format_bsm_value(brakes, 0)
    labels = {
        "wheelBrakes": "wheel",
        "traction": "tc",
        "abs": "abs",
        "scs": "scs",
        "brakeBoost": "boost",
        "auxBrakes": "aux",
    }
    pieces = [f"{label}:{brakes[key]}" for key, label in labels.items() if brakes.get(key) not in (None, "unavailable")]
    return ", ".join(pieces) if pieces else "NA"


def bsm_records_have_display_fields(records):
    """Return True when a batch has actual BSM table fields, not only routing metadata."""
    for record in records or []:
        if not isinstance(record, Mapping):
            continue
        values = (
            _bsm_message_count(record),
            _bsm_sec_mark(record),
            _bsm_lat_deg(record),
            _bsm_long_deg(record),
            _bsm_elevation_m(record),
            _bsm_speed_mps(record),
            _bsm_heading_deg(record),
        )
        if any(value is not None and value != "" for value in values):
            return True
        brake_summary = _bsm_brake_summary(record)
        if brake_summary not in (None, "", "NA"):
            return True
    return False


def _bsm_delivery_metadata(records):
    latencies = []
    ranges = []
    for record in records or []:
        latency = _safe_float(first_present(record, "latency_ms", "latency"))
        distance = _safe_float(first_present(record, "distance_m", "distance"))
        if latency is not None:
            latencies.append(latency)
        if distance is not None:
            ranges.append(distance)
    pieces = []
    if latencies:
        pieces.append(f"avg link latency {sum(latencies) / len(latencies):.2f} ms")
    if ranges:
        pieces.append(f"avg radio range {sum(ranges) / len(ranges):.1f} m")
    return " Link metadata hidden from BSM columns: " + "; ".join(pieces) + "." if pieces else ""


def _runtime_ego_vehicle_id(runtime, step_result=None, target_vehicle_id=None):
    projection_info = step_result.get("tracr_projection", {}) if isinstance(step_result, dict) else {}
    candidates = [
        projection_info.get("focus_vehicle") if isinstance(projection_info, dict) else None,
        target_vehicle_id,
        getattr(runtime, "focus_vehicle_id", None),
    ]
    sensor_panel = getattr(runtime, "sensor_panel", None)
    if sensor_panel is not None:
        candidates.append(getattr(sensor_panel, "target_vehicle_id", None))
    candidates.extend(getattr(runtime, "v2x_vehicle_ids", []) or [])
    for candidate in candidates:
        if candidate is not None:
            return candidate
    return None


def _bsm_road_id(record):
    return first_present(record, "roadID", "road_id", "road", "link_id", "edge_id")


def _runtime_vehicle_record_and_private_flag(runtime, vehicle_id):
    if runtime is None or vehicle_id is None:
        return None, None
    for candidate_id, private_flag, vehicle_state in getattr(runtime, "_tracr_last_vehicle_records", []) or []:
        if _same_vehicle_id(candidate_id, vehicle_id):
            return vehicle_state, private_flag
    return None, None


def _runtime_vehicle_record(runtime, vehicle_id):
    vehicle_state, _private_flag = _runtime_vehicle_record_and_private_flag(runtime, vehicle_id)
    return vehicle_state


def _metsr_vis_vehicle_group_key(record, private_flag=None):
    if not isinstance(record, Mapping):
        return None
    vehicle_class = _safe_int(first_present(record, "vehicleClass", "v_type", "vehicle_class"))
    state = _safe_int(first_present(record, "state", "vehicleState", "tripState"))
    if private_flag is None:
        private_flag = first_present(record, "_viz_private_veh")

    if vehicle_class == 0:
        return "vehicle"
    if vehicle_class == 2 or state == 3:
        return "bus"
    if vehicle_class == 1:
        if state == 4:
            return "ev_charging"
        if state == 1:
            return "ev_occupied"
        return "ev_relocation"
    if vehicle_class == 3:
        if state == 4:
            return "ev_charging"
        return "ev_private"
    if private_flag is True:
        return "ev_private"
    return None


def metsr_vis_vehicle_type_for_record(record, private_flag=None, default=METS_R_VIS_PRIVATE_VEHICLE_TYPE):
    """Return the numeric METS-R Viz VehicleType for a live vehicle record."""
    group_key = _metsr_vis_vehicle_group_key(record, private_flag=private_flag)
    if group_key is None:
        return default
    return METS_R_VIS_VEHICLE_TYPE_BY_GROUP.get(group_key, default)


def _runtime_metsr_vis_vehicle_type(runtime, vehicle_id, default=None):
    vehicle_state, private_flag = _runtime_vehicle_record_and_private_flag(runtime, vehicle_id)
    return metsr_vis_vehicle_type_for_record(vehicle_state, private_flag=private_flag, default=default)


def _broadcast_bsm_records_for_ego(records, ego_vehicle_id, ego_state=None, limit=80):
    candidates = [
        record
        for record in records or []
        if bsm_receiver_id(record) is None and not _same_vehicle_id(bsm_sender_id(record), ego_vehicle_id)
    ]
    if not candidates:
        return []

    ego_road = _bsm_road_id(ego_state or {})
    if ego_road is not None:
        same_road = [record for record in candidates if _same_vehicle_id(_bsm_road_id(record), ego_road)]
        if same_road:
            candidates = same_road

    selected = candidates[-int(limit or 80):]
    annotated = []
    for record in selected:
        copied = dict(record)
        copied["_tracr_ego_heard"] = True
        copied["_tracr_receiver_note"] = "broadcast heard by ego"
        annotated.append(copied)
    return annotated


def _filter_bsm_records_for_ego(
    records,
    ego_vehicle_id,
    ego_only=True,
    broadcast_as_ego=False,
    ego_state=None,
    broadcast_limit=80,
):
    records = list(records or [])
    if not ego_only or ego_vehicle_id is None:
        return records, "all"

    received = [record for record in records if _same_vehicle_id(bsm_receiver_id(record), ego_vehicle_id)]
    if received:
        return received, "received"

    if broadcast_as_ego:
        broadcast = _broadcast_bsm_records_for_ego(
            records,
            ego_vehicle_id,
            ego_state=ego_state,
            limit=broadcast_limit,
        )
        if broadcast:
            return broadcast, "broadcast"

    sent = [record for record in records if _same_vehicle_id(bsm_sender_id(record), ego_vehicle_id)]
    if sent:
        return sent, "sent"

    involved = [
        record
        for record in records
        if _same_vehicle_id(bsm_sender_id(record), ego_vehicle_id)
        or _same_vehicle_id(bsm_receiver_id(record), ego_vehicle_id)
    ]
    if involved:
        return involved, "involving"
    return [], "none"


def bsm_unique_sender_count(records):
    senders = set()
    fallback = 0
    for record in records or []:
        sender = bsm_sender_id(record)
        if sender is None:
            sender = f"record-{fallback}"
            fallback += 1
        senders.add(str(sender))
    return len(senders)


def _bsm_plot_position(record):
    x = _bsm_long_deg(record)
    y = _bsm_lat_deg(record)
    coord_kind = "latlon"
    if x is None or y is None:
        x = first_present(record, "local_x", "x_m", "origin_x", "tx_x", "payload_x", "x")
        y = first_present(record, "local_y", "y_m", "origin_y", "tx_y", "payload_y", "y")
        coord_kind = "local"
    if x is None or y is None:
        return None
    try:
        return float(x), float(y), coord_kind
    except (TypeError, ValueError):
        return None


def _bsm_point_matches_extent(point, expected_coord_kind):
    if point is None:
        return False
    x, y, coord_kind = point
    if expected_coord_kind is None or coord_kind != expected_coord_kind:
        return False
    if coord_kind == "latlon":
        return abs(x) <= 180.0 and abs(y) <= 90.0
    return True


def _bsm_ego_point_from_records(records, ego_vehicle_id, expected_coord_kind):
    if ego_vehicle_id is None:
        return None
    for record in reversed(list(records or [])):
        if not _same_vehicle_id(bsm_sender_id(record), ego_vehicle_id):
            continue
        point = _bsm_plot_position(record)
        if _bsm_point_matches_extent(point, expected_coord_kind):
            return point
    return None


def _short_bsm_node_label(value, limit=10):
    text = "broadcast" if value is None or value == "" else str(value)
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)] + "..."


def bsm_map_png(records, source_label="Kafka", ego_vehicle_id=None, ego_state=None, ego_marker_records=None):
    import matplotlib.pyplot as plt

    records = list(records or [])[-80:]
    if not records:
        return blank_png(f"Waiting for {source_label} BSM records", width=6.4, height=3.6)

    hub_key = "__ego__" if ego_vehicle_id is not None else "__bsm__"
    hub_label = f"ego {ego_vehicle_id}" if ego_vehicle_id is not None else "BSM"
    nodes = {}
    edges = []

    def touch_node(raw_id, role, index):
        if raw_id is None or raw_id == "":
            return None
        if ego_vehicle_id is not None and _same_vehicle_id(raw_id, ego_vehicle_id):
            return hub_key
        key = str(raw_id)
        node = nodes.get(key)
        if node is None:
            node = {
                "key": key,
                "raw_id": raw_id,
                "label": _short_bsm_node_label(raw_id),
                "role": role,
                "count": 0,
                "latest": -1,
            }
            nodes[key] = node
        node["count"] += 1
        node["latest"] = max(node["latest"], int(index))
        if node["role"] == "other" and role != "other":
            node["role"] = role
        return key

    def add_edge(source, target, role, index):
        if source is None or target is None or source == target:
            return
        edges.append({"source": source, "target": target, "role": role, "latest": int(index)})

    for index, record in enumerate(records):
        sender = bsm_sender_id(record)
        receiver = bsm_receiver_id(record)
        if sender is None:
            sender = f"record-{index}"
        sender_is_ego = ego_vehicle_id is not None and _same_vehicle_id(sender, ego_vehicle_id)
        receiver_is_ego = ego_vehicle_id is not None and _same_vehicle_id(receiver, ego_vehicle_id)
        heard_by_ego = bool(isinstance(record, Mapping) and record.get("_tracr_ego_heard"))

        if ego_vehicle_id is not None:
            if sender_is_ego:
                target = touch_node(receiver if receiver is not None else "broadcast", "outbound", index)
                add_edge(hub_key, target, "outbound", index)
            elif receiver_is_ego or heard_by_ego or receiver is None:
                source = touch_node(sender, "inbound", index)
                add_edge(source, hub_key, "inbound", index)
            else:
                source = touch_node(sender, "other", index)
                target = touch_node(receiver if receiver is not None else "broadcast", "other", index)
                add_edge(source, target, "other", index)
        else:
            source = touch_node(sender, "other", index)
            target = touch_node(receiver if receiver is not None else "broadcast", "other", index)
            add_edge(source, target, "other", index)


    node_items = sorted(nodes.values(), key=lambda item: (item["latest"], item["count"]), reverse=True)
    max_visible_nodes = 12
    visible_nodes = node_items[:max_visible_nodes]
    hidden_count = max(0, len(node_items) - len(visible_nodes))
    visible_keys = {node["key"] for node in visible_nodes}
    visible_edges = [
        edge
        for edge in edges[-120:]
        if (edge["source"] == hub_key or edge["source"] in visible_keys)
        and (edge["target"] == hub_key or edge["target"] in visible_keys)
    ]

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    fig.patch.set_facecolor("#050608")
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_facecolor("#050608")
    ax.set_xlim(-3.15, 3.15)
    ax.set_ylim(-1.78, 1.78)
    ax.set_aspect("equal", adjustable="box")

    ring = plt.Circle((0.0, 0.0), 1.18, color="#0f172a", fill=False, linewidth=1.0, alpha=0.46)
    ax.add_patch(ring)
    ring = plt.Circle((0.0, 0.0), 1.52, color="#1e293b", fill=False, linewidth=0.75, alpha=0.28)
    ax.add_patch(ring)

    positions = {hub_key: (0.0, 0.0)}
    count = max(1, len(visible_nodes))
    ring_radius = 1.42 if count > 6 else 1.22
    for index, node in enumerate(visible_nodes):
        angle = (2.0 * math.pi * index / count) - (math.pi / 2.0)
        positions[node["key"]] = (ring_radius * math.cos(angle), ring_radius * math.sin(angle))

    latest_edge_index = max((edge["latest"] for edge in visible_edges), default=-1)
    edge_styles = {
        "inbound": ("#22d3ee", 0.52),
        "outbound": ("#f59e0b", 0.58),
        "other": ("#64748b", 0.30),
    }
    for edge in visible_edges:
        source = positions.get(edge["source"])
        target = positions.get(edge["target"])
        if source is None or target is None:
            continue
        color, alpha = edge_styles.get(edge["role"], edge_styles["other"])
        width = 1.9 if edge["latest"] == latest_edge_index else 1.1
        ax.annotate(
            "",
            xy=target,
            xytext=source,
            arrowprops={
                "arrowstyle": "-|>",
                "color": color,
                "alpha": alpha,
                "lw": width,
                "shrinkA": 18,
                "shrinkB": 22,
                "mutation_scale": 10,
            },
            zorder=1,
        )

    for color, width, zorder in (("#ffffff", 8.0, 5), ("#f43f5e", 4.8, 6)):
        ax.plot([-0.24, 0.24], [0.0, 0.0], color=color, linewidth=width, solid_capstyle="round", zorder=zorder)
        ax.plot([0.0, 0.0], [-0.24, 0.24], color=color, linewidth=width, solid_capstyle="round", zorder=zorder)
    ax.text(
        0.0,
        -0.36,
        _short_bsm_node_label(hub_label, 14),
        ha="center",
        va="center",
        fontsize=9.2,
        color="#ffffff",
        weight="bold",
        bbox={"boxstyle": "round,pad=0.18", "fc": "#7f1d1d", "ec": "#fecaca", "alpha": 0.86},
        zorder=6,
    )

    node_colors = {"inbound": "#22d3ee", "outbound": "#f59e0b", "other": "#94a3b8", "ego": "#f43f5e"}
    edge_colors = {"inbound": "#cffafe", "outbound": "#ffedd5", "other": "#e2e8f0", "ego": "#ffffff"}
    latest_node_key = visible_nodes[0]["key"] if visible_nodes else None
    for node in visible_nodes:
        x, y = positions[node["key"]]
        role = node.get("role", "other")
        size = 390 if node["key"] != latest_node_key else 520
        ax.scatter(
            [x],
            [y],
            s=size,
            c=node_colors.get(role, node_colors["other"]),
            edgecolors=edge_colors.get(role, "#ffffff"),
            linewidths=1.3,
            alpha=0.96,
            zorder=4,
        )
        ax.text(
            x,
            y,
            node["label"],
            ha="center",
            va="center",
            fontsize=8.0,
            color="#020617",
            weight="bold",
            zorder=5,
        )
        if node["count"] > 1:
            ax.text(
                x + 0.24,
                y + 0.18,
                str(node["count"]),
                ha="center",
                va="center",
                fontsize=6.8,
                color="#f8fafc",
                weight="bold",
                bbox={"boxstyle": "circle,pad=0.12", "fc": "#0f172a", "ec": "#334155", "alpha": 0.90},
                zorder=6,
            )

    summary = f"{source_label} ego view"
    if ego_vehicle_id is None:
        summary = f"{source_label} BSM nodes"
    detail = f"{len(visible_nodes)} shown / {len(node_items)} nodes"
    if hidden_count:
        detail += f" (+{hidden_count})"
    ax.text(0.02, 0.94, summary, transform=ax.transAxes, ha="left", va="center", fontsize=8.6, color="#e2e8f0", weight="bold")
    ax.text(0.02, 0.875, detail, transform=ax.transAxes, ha="left", va="center", fontsize=7.4, color="#94a3b8")
    ax.text(0.98, 0.06, "cyan=in  amber=out", transform=ax.transAxes, ha="right", va="center", fontsize=7.2, color="#94a3b8")

    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    return fig_to_png(fig)


def bsm_table_html(records, limit=50, source_label="Kafka", ego_vehicle_id=None, total_records=None, view_mode="all"):
    records = list(records or [])
    total_records = len(records) if total_records is None else int(total_records or 0)
    latest_records = records[-limit:]
    unique_count = bsm_unique_sender_count(records)
    if not latest_records:
        if ego_vehicle_id is not None and view_mode == "none":
            return (
                f"<div class='tracr-empty'>No {escape(str(source_label))} BSMs involving "
                f"ego {escape(str(ego_vehicle_id))} in the latest batch.</div>"
            )
        return f"<div class='tracr-empty'>Waiting for {escape(str(source_label))} BSM records...</div>"

    metadata_note = _bsm_delivery_metadata(records)
    core_note = "Table shows SAE J2735 BSM coreData fields; radio latency/range are link metadata, not BSM fields."
    if view_mode == "broadcast":
        core_note += " Kafka bsm rows are broadcast, so ego view means heard by ego, not addressed to ego."
    if ego_vehicle_id is not None and view_mode != "all":
        direction = {
            "received": "received by ego",
            "sent": "sent by ego",
            "broadcast": "broadcast BSMs heard by ego",
            "involving": "involving ego",
            "none": "involving ego",
        }.get(view_mode, "involving ego")
        summary = (
            "<div class='bsm-summary'>"
            f"{escape(str(source_label))} ego view: ego={escape(str(ego_vehicle_id))}; "
            f"showing {len(records)}/{total_records} {direction} BSM rows from {unique_count} transmitters. "
            f"{escape(core_note + metadata_note)}"
            "</div>"
        )
    else:
        summary = (
            "<div class='bsm-summary'>"
            f"{escape(str(source_label))} BSM batch: {len(records)} messages from {unique_count} emitters. "
            f"Showing latest {min(limit, len(latest_records))}. {escape(core_note + metadata_note)}"
            "</div>"
        )

    headings = ["role", "id", "msgCnt", "secMark", "lat", "long", "elev(m)", "speed(m/s)", "heading", "brakes"]
    rows = []
    for record in reversed(latest_records):
        values = [
            _bsm_role(record, ego_vehicle_id),
            _bsm_display_id(record, ego_vehicle_id),
            _format_bsm_value(_bsm_message_count(record), 0),
            _format_bsm_value(_bsm_sec_mark(record), 0),
            _format_bsm_value(_bsm_lat_deg(record), 7),
            _format_bsm_value(_bsm_long_deg(record), 7),
            _format_bsm_value(_bsm_elevation_m(record), 1),
            _format_bsm_value(_bsm_speed_mps(record), 2),
            _format_bsm_value(_bsm_heading_deg(record), 1),
            _bsm_brake_summary(record),
        ]
        cells = []
        for idx, value in enumerate(values):
            klass = " class='num'" if idx in (2, 3, 4, 5, 6, 7, 8) else ""
            cells.append(f"<td{klass}>{escape(str(value))}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")

    header = "".join(f"<th>{escape(label)}</th>" for label in headings)
    body = "".join(rows)
    return f"{summary}<table class='bsm-table'><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"

def _tracr_bridge_vehicle_id(record):
    if not isinstance(record, dict):
        return None
    return first_present(record, "ID", "vehicle_id", "vid", "sender_id", "origin_vehicle_id")


def _tracr_bridge_vehicle_record(vehicle_id, private_flag, vehicle_state):
    record = dict(vehicle_state or {})
    record.setdefault("ID", vehicle_id)
    record.setdefault("vehicle_id", vehicle_id)
    record.setdefault("vid", vehicle_id)
    record["private_veh"] = bool(private_flag)
    record.setdefault("sensor_type", "cv2x")
    if record.get("road") is None:
        record["road"] = record.get("roadID", record.get("road_id"))
    if record.get("heading_deg") is None and record.get("bearing") is not None:
        record["heading_deg"] = record.get("bearing")
    if record.get("speed_mps") is None and record.get("speed") is not None:
        record["speed_mps"] = record.get("speed")
    return record


def _tracr_message_lookup(messages):
    by_id = {}
    by_link = {}
    for message in messages or []:
        message_id = message.get("message_id")
        if message_id is not None:
            by_id[str(message_id)] = message
        sender_id = first_present(message, "sender_id", "vehicle_id", "vid")
        receiver_id = first_present(message, "receiver_id", "target_vehicle_id")
        message_count = first_present(message, "message_count", "msg_count", "msgCnt")
        by_link[(str(sender_id), str(receiver_id), str(message_count))] = message
        by_link[(str(sender_id), str(receiver_id), "")] = message
    return by_id, by_link


def _tracr_simu5g_records_from_result(result, vehicles, messages):
    vehicles_by_id = {str(_tracr_bridge_vehicle_id(vehicle)): vehicle for vehicle in vehicles or []}
    messages_by_id, messages_by_link = _tracr_message_lookup(messages)
    rows = result.get("received_bsms") or []
    if not rows:
        rows = [row for row in result.get("link_metrics", []) or [] if row.get("delivered", True)]

    records = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sender_id = first_present(row, "sender_id", "vehicle_id", "origin_vehicle_id")
        receiver_id = first_present(row, "receiver_id", "target_vehicle_id", "rx_vehicle_id")
        message_count = first_present(row, "message_count", "msg_count", "msgCnt")
        message = messages_by_id.get(str(row.get("message_id"))) or messages_by_link.get(
            (str(sender_id), str(receiver_id), str(message_count)),
            messages_by_link.get((str(sender_id), str(receiver_id), ""), {}),
        )
        sender = vehicles_by_id.get(str(sender_id), {})
        receiver = vehicles_by_id.get(str(receiver_id), {})
        record = dict(row)
        record.setdefault("vid", sender_id)
        record.setdefault("vehicle_id", sender_id)
        record.setdefault("sender_id", sender_id)
        record.setdefault("origin_vehicle_id", sender_id)
        record.setdefault("receiver_id", receiver_id)
        record.setdefault("target_vehicle_id", receiver_id)
        record.setdefault("target_x", receiver.get("x"))
        record.setdefault("target_y", receiver.get("y"))
        record.setdefault("target_z", receiver.get("z"))
        record.setdefault("message_name", message.get("message_name", "BasicSafetyMessage"))
        record.setdefault("message_standard", message.get("message_standard", "SAE J2735-aligned over Simu5G"))
        record.setdefault("message_count", message_count if message_count is not None else message.get("message_count"))
        for target_key, keys in (
            ("x", ("x", "tx_x", "payload_x")),
            ("y", ("y", "tx_y", "payload_y")),
            ("z", ("z", "tx_z")),
            ("speed_mps", ("speed_mps", "speed", "tx_speed_mps", "payload_speed_mps")),
            ("heading_deg", ("heading_deg", "heading", "tx_heading_deg")),
        ):
            if record.get(target_key) is None:
                value = first_present(message, *keys)
                if value is None:
                    value = first_present(sender, *keys)
                if value is not None:
                    record[target_key] = value
        record.setdefault("sensor_type", "simu5g")
        record.setdefault(
            "sensor_type_name",
            first_present(row, "backend_implementation", "radio_access", "network_model") or "Simu5G",
        )
        records.append(record)
    return records


class TRACRKafkaBSMStream:
    def __init__(self, processor, topics=("v2x_rx_bsm", "bsm")):
        self.processor = processor
        self.topics = tuple(topics or ("bsm",))
        self.last_error = ""

    def close(self):
        # TRACRDemoRuntime owns the wrapped KafkaDataProcessor and closes it once.
        return None

    def process_bsm(self, timeout_ms=None, max_records=None, **kwargs):
        if self.processor is None:
            return []
        try:
            records = self.processor.process(
                timeout_ms=timeout_ms,
                max_records=max_records,
                topics=self.topics,
            ) or []
            self.last_error = ""
            return records
        except Exception as exc:
            self.last_error = str(exc).splitlines()[0]
            return []


class TRACRSimu5GBSMStream:
    def __init__(
        self,
        veins_client,
        build_mobility_records,
        build_bsm_records,
        duration_s=0.1,
        max_network_vehicles=80,
        max_messages=240,
        require_backend=None,
    ):
        self.veins_client = veins_client
        self.build_mobility_records = build_mobility_records
        self.build_bsm_records = build_bsm_records
        self.duration_s = duration_s
        self.max_network_vehicles = int(max_network_vehicles or 80)
        self.max_messages = int(max_messages or 240)
        self.require_backend = require_backend
        self.last_result = None
        self.last_error = ""

    def close(self):
        if self.veins_client is not None:
            self.veins_client.close()

    def process_bsm(self, runtime=None, timeout_ms=None, max_records=None, **kwargs):
        if runtime is None:
            return []
        entries = list(getattr(runtime, "_tracr_last_vehicle_records", []) or [])
        if not entries:
            return []

        by_id = {}
        for vehicle_id, private_flag, vehicle_state in entries:
            if not _vehicle_is_live(vehicle_state):
                continue
            by_id[str(vehicle_id)] = (vehicle_id, bool(private_flag), vehicle_state)

        preferred = []
        focus_vehicle = getattr(runtime, "focus_vehicle_id", None)
        if focus_vehicle is not None:
            preferred.append(focus_vehicle)
        preferred.extend(getattr(runtime, "v2x_vehicle_ids", []) or [])
        preferred.extend(by_id.keys())
        ordered_keys = [str(item) for item in _unique_ordered(preferred) if str(item) in by_id]
        ordered_keys = ordered_keys[: self.max_network_vehicles]
        if len(ordered_keys) < 2:
            return []

        vehicle_records = []
        private_flags = []
        records_by_key = {}
        sender_ids = {str(item) for item in (getattr(runtime, "v2x_vehicle_ids", []) or [])}
        sender_records = []
        sender_flags = []
        for key in ordered_keys:
            vehicle_id, private_flag, vehicle_state = by_id[key]
            record = _tracr_bridge_vehicle_record(vehicle_id, private_flag, vehicle_state)
            vehicle_records.append(record)
            private_flags.append(private_flag)
            records_by_key[key] = record
            if key in sender_ids:
                sender_records.append(record)
                sender_flags.append(private_flag)

        if not sender_records:
            return []

        ego_vehicle_id = _runtime_ego_vehicle_id(runtime)
        ego_key = None if ego_vehicle_id is None else str(ego_vehicle_id)
        receiver_records = [records_by_key[ego_key]] if ego_key in records_by_key else vehicle_records

        tick = int(getattr(runtime.metsr, "current_tick", 0) or 0)
        base_messages = self.build_bsm_records(
            sender_records,
            tick=tick,
            private_veh=sender_flags,
            sensor_type="cv2x",
        )
        messages = []
        sequence = 0
        for sender, base_message in zip(sender_records, base_messages):
            sender_id = _tracr_bridge_vehicle_id(sender)
            for receiver in receiver_records:
                receiver_id = _tracr_bridge_vehicle_id(receiver)
                if sender_id is None or receiver_id is None or str(sender_id) == str(receiver_id):
                    continue
                sequence += 1
                message = dict(base_message)
                message.update(
                    {
                        "message_id": f"tracr-simu5g:{tick}:{sender_id}>{receiver_id}:{sequence}",
                        "vehicle_id": sender_id,
                        "sender_id": sender_id,
                        "receiver_id": receiver_id,
                        "target_vehicle_id": receiver_id,
                        "message_count": (tick * 16 + sequence) % 128,
                        "payload_bytes": 300,
                        "tx_time_s": None if self.duration_s is None else tick * float(self.duration_s),
                        "radio_mode": "simu5g",
                        "sensor_type_name": "Simu5G",
                        "content": f"TRACR Simu5G BSM tick={tick} veh={sender_id} rx={receiver_id}",
                    }
                )
                messages.append(message)
                if len(messages) >= self.max_messages:
                    break
            if len(messages) >= self.max_messages:
                break

        if not messages:
            return []

        mobility = self.build_mobility_records(
            vehicle_records,
            private_veh=private_flags,
            sensor_type="cv2x",
        )
        try:
            result = self.veins_client.sync_tick(
                tick=tick,
                vehicles=mobility,
                bsm_messages=messages,
                attacks=[],
                duration_s=self.duration_s,
            )
            self.last_result = result
            self.last_error = ""
            implementation = result.get("backend_implementation") or result.get("backendImplementation")
            if self.require_backend and implementation != self.require_backend:
                raise RuntimeError(
                    f"Simu5G bridge backend mismatch: expected {self.require_backend}, got {implementation!r}."
                )
            records = _tracr_simu5g_records_from_result(result, vehicle_records, messages)
            if max_records is not None:
                records = records[-int(max_records):]
            return records
        except Exception as exc:
            self.last_error = str(exc).splitlines()[0]
            return []

class TRACRDashboard:
    def __init__(self, viz_url="https://engineering.purdue.edu/HSEES/METSRVis/", stream_url=None, fullscreen=False, local_viz_patch=False, bsm_stream_label="Kafka", bsm_ego_only=True, metsr_viz_map=METS_R_VIS_PURDUE_MAP_ID, metsr_viz_vehicle_type=METS_R_VIS_PRIVATE_VEHICLE_TYPE, lidar_min_update_interval_s=1.0, external_speedy_mode=False):
        try:
            import ipywidgets as widgets
        except ImportError:
            widgets = None

        self.widgets = widgets
        self.stream_url = stream_url
        self.viz_url = viz_url
        self.viz_frame_url = viz_url
        self.viz_frame_status = ""
        self.local_viz_patch = bool(local_viz_patch)
        self.metsr_viz_map = metsr_viz_map
        self.metsr_viz_vehicle_type = metsr_viz_vehicle_type
        self.bsm_stream_label = str(bsm_stream_label or "BSM")
        self.bsm_ego_only = bool(bsm_ego_only)
        self.fullscreen = bool(fullscreen)
        self._display_handle = None
        self._status_text = "Ready"
        self._camera_png = blank_png("Waiting for CARLA bird-eye camera")
        self._lidar_png = blank_png("Waiting for CARLA LiDAR")
        self._vehicle_camera_png = blank_png("Waiting for CARLA vehicle camera")
        self._bsm_map_png = blank_png(f"Waiting for {self.bsm_stream_label} BSM coordinates", width=3.5, height=2.5)
        self._bsm_table_html = f"<div class='tracr-empty'>Waiting for {escape(self.bsm_stream_label)} BSM records...</div>"
        self.external_directory = None
        self.external_url = None
        self.external_stop_event = None
        self.external_server_thread = None
        self.external_port = None
        self.external_speedy_mode = bool(external_speedy_mode)
        self.external_min_update_interval_s = 0.12
        self.media_min_update_interval_s = 0.12
        self.lidar_min_update_interval_s = float(lidar_min_update_interval_s or 0.0)
        self._external_last_write_time = 0.0
        self._media_last_update_time = 0.0
        self._lidar_last_update_time = 0.0
        self.stream_probe = None
        self.ego_vehicle_id = None
        self.metsr_vis_highlight_vehicle_id = None
        self.metsr_vis_highlight_vehicle_type = None
        self.metsr_vis_auto_highlight_ego = True
        self.viz_frame_url = self._metsr_vis_frame_url()

        if widgets is None:
            self.view = self
            return

        self.status = widgets.HTML()
        self.viz_panel = widgets.HTML(self._viz_html(self.viz_frame_url, stream_url))
        self.camera_image = widgets.Image(format="png", value=self._camera_png, layout=widgets.Layout(width="100%", height="100%", object_fit="cover"))
        self.lidar_image = widgets.Image(format="png", value=self._lidar_png, layout=widgets.Layout(width="100%", height="100%", object_fit="cover"))
        self.vehicle_camera_image = widgets.Image(format="png", value=self._vehicle_camera_png, layout=widgets.Layout(width="100%", height="100%", object_fit="cover"))
        self.bsm_map = widgets.Image(
            format="png",
            value=self._bsm_map_png,
            layout=widgets.Layout(width="60%", height="100%", object_fit="contain"),
        )
        self.bsm_table = widgets.HTML(
            self._bsm_table_html,
            layout=widgets.Layout(width="40%", height="100%", min_height="0", max_height="100%", overflow="auto"),
        )
        self.view = self._build_widget_view()
        self.update_status("Ready")

    def _metsr_vis_selected_vehicle_id(self):
        if self.metsr_vis_highlight_vehicle_id is not None:
            return self.metsr_vis_highlight_vehicle_id
        if self.metsr_vis_auto_highlight_ego:
            return self.ego_vehicle_id
        return None

    def _metsr_vis_selected_vehicle_type(self):
        if self.metsr_vis_highlight_vehicle_type is not None:
            return self.metsr_vis_highlight_vehicle_type
        return self.metsr_viz_vehicle_type

    def _metsr_vis_frame_url(self, vehicle_id=None, vehicle_type=None):
        selected_vehicle_id = self._metsr_vis_selected_vehicle_id() if vehicle_id is None else vehicle_id
        selected_vehicle_type = self._metsr_vis_selected_vehicle_type() if vehicle_type is None else vehicle_type
        return build_metsr_vis_url(
            self.viz_url,
            stream_url=self.stream_url,
            map_id=self.metsr_viz_map,
            vehicle_id=selected_vehicle_id,
            vehicle_type=selected_vehicle_type,
        )

    def _sync_metsr_vis_frame_url(self, force_external=False):
        frame_url = self._metsr_vis_frame_url()
        changed = frame_url != self.viz_frame_url
        if changed:
            self.viz_frame_url = frame_url
            if self.widgets is not None and hasattr(self, "viz_panel"):
                self.viz_panel.value = self._viz_html(self.viz_frame_url, self.stream_url)
            elif self.widgets is None:
                self._refresh_plain_display()
        if changed or force_external:
            self._refresh_external_state(force=True)
        return changed

    def highlight_metsr_vis_vehicle(self, vehicle_id, vehicle_type=None):
        """Request vehicle selection in the embedded METS-R Vis frame by vehicle ID."""
        self.metsr_vis_highlight_vehicle_id = vehicle_id
        self.metsr_vis_highlight_vehicle_type = None if vehicle_type is None else vehicle_type
        self.metsr_vis_auto_highlight_ego = False
        self._sync_metsr_vis_frame_url(force_external=True)
        return vehicle_id

    def select_metsr_vis_vehicle(self, vehicle_id, vehicle_type=None):
        """Alias for highlight_metsr_vis_vehicle."""
        return self.highlight_metsr_vis_vehicle(vehicle_id, vehicle_type=vehicle_type)

    def highlight_metsr_vis_element(self, element_id, element_type="vehicle"):
        """Request element selection in embedded METS-R Vis. Currently supports vehicles."""
        normalized_type = str(element_type or "vehicle").strip().lower()
        if normalized_type in {"vehicle", "veh", "car", "taxi"}:
            return self.highlight_metsr_vis_vehicle(element_id)
        raise NotImplementedError("METS-R Vis dashboard highlighting currently supports vehicle IDs only.")

    def clear_metsr_vis_highlight(self, auto_highlight_ego=True):
        """Clear any manual METS-R Vis vehicle selection and optionally return to auto ego selection."""
        self.metsr_vis_highlight_vehicle_id = None
        self.metsr_vis_highlight_vehicle_type = None
        self.metsr_vis_auto_highlight_ego = bool(auto_highlight_ego)
        self._sync_metsr_vis_frame_url(force_external=True)
        return self._metsr_vis_selected_vehicle_id()

    def set_fullscreen(self, enabled=True):
        self.fullscreen = bool(enabled)
        if self.widgets is not None:
            self.view = self._build_widget_view()
        else:
            self._refresh_plain_display()
        self._refresh_external_state(force=True)
        return self

    def _prepare_external_viz_frame(self):
        self.viz_frame_url = self._metsr_vis_frame_url()
        self.viz_frame_status = ""
        if not self.local_viz_patch:
            return
        if not self.external_directory or not self.stream_url:
            return
        try:
            info = prepare_local_metsr_vis(
                self.external_directory,
                self.viz_url,
                self.stream_url,
            )
            self.viz_frame_url = info.get("url") or self.viz_url
            self.viz_frame_status = info.get("status") or ""
        except Exception as exc:
            self.viz_frame_url = self._metsr_vis_frame_url()
            self.viz_frame_status = (
                "Embedded METS-R Vis is using the remote page; "
                f"local stream patch failed: {str(exc).splitlines()[0]}"
            )

    def display_external(self, directory="output/tracr_dashboard", port=8899, open_browser=False, speedy_mode=False):
        from IPython.display import HTML, display
        from utils.util import run_visualization_server

        self.external_directory = os.path.abspath(directory)
        self.external_port = int(port)
        self.external_speedy_mode = bool(speedy_mode)
        os.makedirs(self.external_directory, exist_ok=True)
        self._prepare_external_viz_frame()
        self._write_external_page()
        self.probe_stream()

        if self.external_server_thread is None:
            try:
                self.external_stop_event, self.external_server_thread = run_visualization_server(
                    self.external_directory,
                    server_port=self.external_port,
                )
            except OSError:
                # VS Code users often rerun the dashboard cell while the old
                # notebook object still has this server alive. The old server
                # serves the same directory, so continuing with the same URL is
                # usually the least surprising behavior.
                self.external_stop_event = None
                self.external_server_thread = None
        self.external_url = f"http://127.0.0.1:{self.external_port}/index.html"
        probe = self.stream_probe or {}
        if probe.get("ok"):
            stream_probe_text = f"Stream probe connected to {probe.get('url') or self.stream_url}."
        else:
            stream_probe_text = (
                f"Stream probe failed: {probe.get('error') or 'not reachable'}. "
                "Rerun launch_tracr_demo(), then use the exact WebSocket URL shown in the dashboard."
            )
        if open_browser:
            import webbrowser
            webbrowser.open(self.external_url)
        display(HTML(
            f"<p><b>TRACR dashboard:</b> "
            f"<a href='{escape(self.external_url)}' target='_blank'>{escape(self.external_url)}</a> "
            "(open in a browser, then press F11 for true full screen)"
            f"<br><span>{escape(stream_probe_text)}</span></p>"
        ))
        return self.external_url

    def probe_stream(self, stream_url=None, timeout_s=1.0):
        if stream_url is not None:
            self.stream_url = stream_url
        self.stream_probe = probe_viz_stream(self.stream_url, timeout_s=timeout_s)
        if self.widgets is not None:
            self.viz_frame_url = self._metsr_vis_frame_url()
            self.viz_panel.value = self._viz_html(self.viz_frame_url, self.stream_url)
        else:
            self._refresh_plain_display()
        self._refresh_external_state(force=True)
        return self.stream_probe

    def stop_external(self):
        if self.external_server_thread is None:
            return
        from utils.util import stop_visualization_server

        stop_visualization_server(
            self.external_stop_event,
            self.external_server_thread,
            port=self.external_port or 8899,
        )
        self.external_stop_event = None
        self.external_server_thread = None

    def _shell_class(self):
        classes = ["tracr-wrap"]
        if self.fullscreen:
            classes.append("tracr-fullscreen")
        return " ".join(classes)

    def _stream_probe_note_html(self):
        probe = self.stream_probe
        if not probe:
            return ""
        if probe.get("ok"):
            text = f"WebSocket probe connected: {probe.get('url') or self.stream_url}"
            css_class = "tracr-note--ok"
        else:
            text = f"WebSocket probe failed: {probe.get('error') or 'not reachable'}"
            css_class = "tracr-note--warn"
        return f"<div class='tracr-note {css_class}'>{escape(text)}</div>"

    def _bsm_panel_title(self):
        return str(getattr(self, "bsm_panel_title", "") or f"{self.bsm_stream_label} CV2X/BSM stream")

    def _bsm_panel_show_map(self):
        return bool(getattr(self, "bsm_show_map", True))

    def _external_speedy_mode_enabled(self):
        return bool(getattr(self, "external_speedy_mode", False))

    def _viz_html(self, viz_url, stream_url):
        stream = escape(stream_url or "not started yet")
        probe_note = self._stream_probe_note_html()
        return f"""
        <div class="tracr-frame">
          <iframe src="{escape(viz_url)}" allow="local-network-access; clipboard-read; clipboard-write" referrerpolicy="no-referrer-when-downgrade" style="width:100%;height:390px;border:0;"></iframe>
          <div class="tracr-note">METS-R Viz is preloaded with map, stream, and selected vehicle: <code>{stream}</code> | <a href="{escape(viz_url)}" target="_blank" rel="noopener">open top-level</a></div>
          {probe_note}
        </div>
        """

    def _styles(self):
        return """
        <style>
          .tracr-wrap {font-family: system-ui, -apple-system, Segoe UI, sans-serif;}
          .tracr-wrap h2 {margin: 0 0 6px 0; font-size: 20px;}
          .tracr-wrap h3 {margin: 0 0 6px 0; font-size: 14px;}
          .tracr-note {font-size: 12px; color: #475569; padding-top: 4px;}
          .tracr-note--ok {color: #15803d;}
          .tracr-note--warn {color: #b45309;}
          .tracr-note code {font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 11px;}
          .tracr-empty {padding: 16px; color: #64748b; font-size: 13px;}
          .tracr-table {font-size: 11px; border-collapse: collapse; max-width: 100%;}
          .tracr-table th, .tracr-table td {border: 1px solid #cbd5e1; padding: 3px 5px;}
          .tracr-table {font-size: 11px; border-collapse: collapse; max-width: 100%;}
          .tracr-table {font-size: 11px; border-collapse: collapse; max-width: 100%;}
          .tracr-table th, .tracr-table td {border: 1px solid #cbd5e1; padding: 3px 5px;}
          .tracr-table th {background: #f1f5f9;}
          .bsm-summary {font-size: 10px; line-height: 1.2; color: #475569; margin-bottom: 2px;}
          .bsm-table {width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 9.5px;}
          .bsm-table th, .bsm-table td {border-bottom: 1px solid #cbd5e1; padding: 2px 4px; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;}
          .bsm-table th {position: sticky; top: 0; z-index: 1; background: #f1f5f9; color: #334155; font-weight: 750;}
          .bsm-table td {color: #111827;}
          .bsm-table .num {font-variant-numeric: tabular-nums;}

          .tracr-grid {
            display: grid;
            grid-template-columns: repeat(8, minmax(0, 1fr));
            grid-template-rows: minmax(0, 6fr) minmax(0, 4fr);
            gap: 12px;
          }
          .tracr-panel--major {grid-column: span 4;}
          .tracr-panel--minor {grid-column: span 2;}
          .tracr-panel img {max-width: 100%; height: auto; display: block;}
          .bsm-layout {display: flex; gap: 6px; min-height: 0; height: 100%; overflow: hidden;}
          .bsm-map-inline {flex: 0 0 60%; width: 60%; height: 100%; min-width: 0; object-fit: contain;}
          .bsm-table-pane {flex: 1 1 40%; width: 40%; min-width: 0; height: 100%; max-height: 100%; overflow-x: auto; overflow-y: scroll; scrollbar-gutter: stable;}
          .tracr-fullscreen {
            position: fixed;
            inset: 0;
            z-index: 2147483000;
            box-sizing: border-box;
            width: 100vw;
            height: 100vh;
            overflow: auto;
            padding: 12px;
            background: #f8fafc;
          }
          .tracr-fullscreen .tracr-grid {
            height: calc(100vh - 58px);
            grid-template-rows: minmax(0, 6fr) minmax(0, 4fr);
          }
          .tracr-fullscreen .tracr-panel {overflow: auto;}
          .tracr-fullscreen .tracr-frame iframe {height: 100%; min-height: 320px;}
          .tracr-fullscreen .tracr-panel img {max-height: 100%; object-fit: cover;}
          @media (max-width: 900px) {
            .tracr-grid {grid-template-columns: 1fr; grid-template-rows: none;}
            .tracr-panel--major, .tracr-panel--minor {grid-column: auto;}
            .bsm-layout {flex-direction: column;}
            .bsm-map-inline {flex: 1 1 72%; width: 100%; height: 72%;}
            .bsm-table-pane {flex: 0 0 24%; width: 100%; height: 24%; max-height: 24%;}
            .tracr-fullscreen .tracr-grid {height: auto;}
          }
        </style>
        """

    def _build_widget_view(self):
        widgets = self.widgets
        style = widgets.HTML(self._styles())

        def panel(title, body, grid_column):
            box = widgets.VBox(
                [widgets.HTML(f"<h3>{escape(title)}</h3>"), body],
                layout=widgets.Layout(
                    min_width="0",
                    min_height="0",
                    overflow="hidden",
                    grid_column=grid_column,
                ),
            )
            try:
                box.add_class("tracr-panel")
                box.add_class("tracr-panel--major" if grid_column == "span 4" else "tracr-panel--minor")
            except Exception:
                pass
            return box

        bsm_body = widgets.HBox(
            [self.bsm_map, self.bsm_table],
            layout=widgets.Layout(gap="6px", min_height="0", height="100%", overflow="hidden"),
        )
        grid_height = "calc(100vh - 74px)" if self.fullscreen else "820px"
        grid = widgets.GridBox(
            children=[
                panel("METS-R Viz live stream", self.viz_panel, "span 4"),
                panel("CARLA bird-eye tracking camera", self.camera_image, "span 4"),
                panel(self._bsm_panel_title(), bsm_body, "span 4"),
                panel("CARLA LiDAR", self.lidar_image, "span 2"),
                panel("CARLA vehicle camera", self.vehicle_camera_image, "span 2"),
            ],
            layout=widgets.Layout(
                grid_template_columns="repeat(8, minmax(0, 1fr))",
                grid_template_rows="minmax(0, 6fr) minmax(0, 4fr)",
                grid_gap="12px",
                height=grid_height,
                min_height="0",
            ),
        )
        container = widgets.VBox(
            [style, widgets.HTML("<h2>TRACR Purdue Data Collection Demo</h2>"), self.status, grid],
            layout=widgets.Layout(width="100%"),
        )
        try:
            container.add_class("tracr-wrap")
            if self.fullscreen:
                container.add_class("tracr-fullscreen")
        except Exception:
            pass
        return container

    def _png_uri(self, data):
        return "data:image/png;base64," + base64.b64encode(data).decode("ascii")

    def _plain_html(self):
        return f"""
        {self._styles()}
        <div class="{self._shell_class()}">
          <h2>TRACR Purdue Data Collection Demo</h2>
          <div class="tracr-note">{escape(str(self._status_text))}</div>
          <div class="tracr-grid">
            <div class="tracr-panel tracr-panel--major"><h3>METS-R Viz live stream</h3>{self._viz_html(self._metsr_vis_frame_url(), self.stream_url)}</div>
            <div class="tracr-panel tracr-panel--major"><h3>CARLA bird-eye tracking camera</h3><img src="{self._png_uri(self._camera_png)}"></div>
            <div class="tracr-panel tracr-panel--major tracr-panel--bsm"><h3>{escape(self._bsm_panel_title())}</h3><div class="bsm-layout"><img class="bsm-map-inline" src="{self._png_uri(self._bsm_map_png)}"><div class="bsm-table-pane">{self._bsm_table_html}</div></div></div>
            <div class="tracr-panel tracr-panel--minor"><h3>CARLA LiDAR</h3><img src="{self._png_uri(self._lidar_png)}"></div>
            <div class="tracr-panel tracr-panel--minor"><h3>CARLA vehicle camera</h3><img src="{self._png_uri(self._vehicle_camera_png)}"></div>
          </div>
        </div>
        """

    def _external_css(self):
        return """
          :root {color-scheme: dark;}
          html, body {
            margin: 0;
            width: 100%;
            height: 100%;
            overflow: hidden;
            background: #101114;
          }
          body {font-family: system-ui, -apple-system, Segoe UI, sans-serif;}
          .tracr-wrap {
            width: 100vw;
            height: 100vh;
            box-sizing: border-box;
            padding: 10px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            overflow: hidden;
            background: #101114;
            color: #f3f4f6;
          }
          .tracr-wrap h2 {
            margin: 0;
            font-size: 20px;
            line-height: 1.1;
            font-weight: 700;
            letter-spacing: 0;
          }
          .tracr-wrap h3 {
            margin: 0 0 6px 0;
            font-size: 13px;
            line-height: 1.15;
            font-weight: 650;
            letter-spacing: 0;
            color: #f3f4f6;
          }
          .tracr-note {
            min-height: 16px;
            font-size: 12px;
            line-height: 1.25;
            color: #a7b0c0;
          }
          .tracr-note--ok {color: #86efac;}
          .tracr-note--warn {color: #fbbf24;}
          .tracr-note code {font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 11px; color: #f8fafc;}

          .tracr-grid {
            flex: 1 1 auto;
            min-height: 0;
            display: grid;
            grid-template-columns: repeat(8, minmax(0, 1fr));
            grid-template-rows: minmax(0, 6fr) minmax(0, 4fr);
            gap: 10px;
          }
          .tracr-panel {
            min-width: 0;
            min-height: 0;
            overflow: hidden;
            box-sizing: border-box;
            padding: 8px;
            display: flex;
            flex-direction: column;
            background: #181a1f;
            border: 1px solid #303744;
            border-radius: 8px;
            box-shadow: 0 10px 24px rgba(0, 0, 0, 0.22);
          }
          .tracr-panel--major {grid-column: span 4;}
          .tracr-panel--minor {grid-column: span 2;}
          .tracr-panel--bsm {gap: 6px;}
          .bsm-layout {
            flex: 1 1 auto;
            min-height: 0;
            display: flex;
            gap: 6px;
            overflow: hidden;
          }
          .bsm-map-inline {
            flex: 0 0 60%;
            width: 60%;
            height: 100%;
            min-width: 0;
            object-fit: contain;
          }
          .bsm-table-pane {
            flex: 1 1 40%;
            width: 40%;
            min-width: 0;
            height: 100%;
            overflow: auto;
          }
          .tracr-frame {
            flex: 1 1 auto;
            min-height: 0;
            display: flex;
            flex-direction: column;
            gap: 4px;
          }
          .tracr-frame iframe {
            flex: 1 1 auto;
            min-height: 0;
            width: 100%;
            height: 100%;
            border: 0;
            background: #ffffff;
          }
          .tracr-frame .tracr-note {
            flex: 0 0 auto;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
          }
          .tracr-panel > img {
            flex: 1 1 auto;
            min-height: 0;
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
            background: #050608;
            border-radius: 4px;
          }
          #bsm-map {
            flex: 0 0 60%;
            width: 60%;
            height: 100%;
            min-width: 0;
            min-height: 0;
            margin: 0;
            object-fit: contain;
          }
          #bsm-table {
            flex: 1 1 40%;
            width: 40%;
            height: 100%;
            max-height: 100%;
            min-width: 0;
            min-height: 0;
            overflow-x: auto;
            overflow-y: scroll;
            scrollbar-gutter: stable;
            background: transparent;
            border-radius: 4px;
          }
          .bsm-summary {
            flex: 0 0 auto;
            font-size: 9.5px;
            line-height: 1.15;
            color: #cbd5e1;
            padding: 0 0 2px;
          }
          .bsm-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
            font-size: 9px;
            background: #f8fafc;
            color: #0f172a;
          }
          .bsm-table th, .bsm-table td {
            border-bottom: 1px solid #cbd5e1;
            padding: 2px 4px;
            text-align: left;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          }
          .bsm-table th {
            position: sticky;
            top: 0;
            z-index: 1;
            background: #e2e8f0;
            color: #334155;
            font-weight: 800;
          }
          .bsm-table td {
            color: #111827;
          }
          .bsm-table .num {
            font-variant-numeric: tabular-nums;
          }
          .bsm-layout--table-only #bsm-map, .bsm-layout--table-only .bsm-map-inline {
            display: none;
          }
          .bsm-layout--table-only #bsm-table, .bsm-layout--table-only .bsm-table-pane {
            flex: 1 1 100%;
            width: 100%;
            height: 100%;
            max-height: 100%;
          }
          .tracr-table th, .tracr-table td {
            border: 1px solid #cbd5e1;
            padding: 3px 5px;
            white-space: nowrap;
          }
          .tracr-table th {
            position: sticky;
            top: 0;
            background: #e2e8f0;
            z-index: 1;
          }
          @media (max-width: 900px) {
            body {overflow: auto;}
            .tracr-wrap {height: auto; min-height: 100vh; overflow: visible;}
            .tracr-grid {grid-template-columns: 1fr; grid-template-rows: none;}
            .tracr-panel, .tracr-panel--major, .tracr-panel--minor {grid-column: auto; min-height: 42vh;}
            .bsm-layout {flex-direction: column;}
            #bsm-map, .bsm-map-inline {flex: 1 1 72%; width: 100%; height: 72%;}
            #bsm-table, .bsm-table-pane {flex: 0 0 24%; width: 100%; height: 24%; max-height: 24%;}
          }
        """

    def _external_page_html(self):
        stream = escape(self.stream_url or "not started yet")
        self.viz_frame_url = self._metsr_vis_frame_url()
        frame_url = escape(self.viz_frame_url or self.viz_url)
        frame_status = escape(self.viz_frame_status or "")
        bsm_panel_title = escape(self._bsm_panel_title())
        bsm_show_map = self._bsm_panel_show_map()
        bsm_layout_class = "bsm-layout" if bsm_show_map else "bsm-layout bsm-layout--table-only"
        bsm_map_hidden = "" if bsm_show_map else " hidden"
        speedy_mode = self._external_speedy_mode_enabled()
        speedy_hidden = " hidden" if speedy_mode else ""
        return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TRACR Purdue Dashboard</title>
  <style>{self._external_css()}</style>
</head>
<body>
  <div class="tracr-wrap">
    <h2>TRACR Purdue Data Collection Demo</h2>
    <div id="status" class="tracr-note">Ready</div>
    <div class="tracr-grid">
      <div class="tracr-panel tracr-panel--major"><h3>METS-R Viz live stream</h3><div class="tracr-frame"><iframe id="metsr-viz-frame" src="{frame_url}" allow="local-network-access; clipboard-read; clipboard-write" referrerpolicy="no-referrer-when-downgrade"></iframe><div class="tracr-note">METS-R Viz URL carries map, stream, and vehicle selection: <code id="stream-url">{stream}</code> | <a id="viz-popout" href="{frame_url}" target="_blank" rel="noopener">open top-level</a></div><div class="tracr-note">{frame_status}</div><div id="stream-probe" class="tracr-note"></div></div></div>
      <div id="camera-panel" class="tracr-panel tracr-panel--major"><h3>CARLA bird-eye tracking camera</h3><img id="camera" alt="CARLA bird-eye tracking camera"></div>
      <div id="bsm-panel" class="tracr-panel tracr-panel--major tracr-panel--bsm"{speedy_hidden}><h3 id="bsm-panel-title">{bsm_panel_title}</h3><div id="bsm-layout" class="{bsm_layout_class}"><img id="bsm-map" alt="{bsm_panel_title}"{bsm_map_hidden}><div id="bsm-table"></div></div></div>
      <div id="lidar-panel" class="tracr-panel tracr-panel--minor"{speedy_hidden}><h3>CARLA LiDAR</h3><img id="lidar" alt="CARLA LiDAR"></div>
      <div id="vehicle-camera-panel" class="tracr-panel tracr-panel--minor"{speedy_hidden}><h3>CARLA vehicle camera</h3><img id="vehicle-camera" alt="CARLA vehicle camera"></div>
    </div>
  </div>
  <script>
    async function refresh() {{
      try {{
        const response = await fetch('state.json?ts=' + Date.now(), {{cache: 'no-store'}});
        if (!response.ok) return;
        const state = await response.json();
        document.getElementById('status').textContent = state.status || 'Ready';
        const streamUrl = state.stream_url || 'not started yet';
        const streamUrlNode = document.getElementById('stream-url');
        if (streamUrlNode) streamUrlNode.textContent = streamUrl;
        const frameUrl = state.metsr_vis_frame_url || state.viz_url || "";
        const selectedVehicleId = state.metsr_vis_selected_vehicle_id || state.ego_vehicle_id || "";
        const selectedVehicleType = state.metsr_vis_selected_vehicle_type || "";
        const selectedMap = state.metsr_vis_map || "";
        const vizFrame = document.getElementById("metsr-viz-frame");
        if (frameUrl && vizFrame && vizFrame.getAttribute("src") !== frameUrl) {{
          vizFrame.setAttribute("src", frameUrl);
        }}
        const vizPopout = document.getElementById("viz-popout");
        if (frameUrl && vizPopout && vizPopout.getAttribute("href") !== frameUrl) {{
          vizPopout.setAttribute("href", frameUrl);
        }}
        if (selectedVehicleId && vizFrame && vizFrame.contentWindow) {{
          vizFrame.contentWindow.postMessage({{type: "tracr-select-vehicle", vehicleId: selectedVehicleId, vehicleType: selectedVehicleType, map: selectedMap, streamURL: streamUrl, VehicleID: selectedVehicleId, VehicleType: selectedVehicleType, Map: selectedMap, StreamURL: streamUrl}}, "*");
        }}
        const streamProbe = state.stream_probe || null;
        const streamProbeNode = document.getElementById('stream-probe');
        if (streamProbeNode) {{
          if (streamProbe) {{
            const ok = !!streamProbe.ok;
            const detail = ok
              ? 'WebSocket probe connected: ' + (streamProbe.url || streamUrl)
              : 'WebSocket probe failed: ' + (streamProbe.error || 'not reachable');
            streamProbeNode.textContent = detail;
            streamProbeNode.className = 'tracr-note ' + (ok ? 'tracr-note--ok' : 'tracr-note--warn');
          }} else {{
            streamProbeNode.textContent = '';
            streamProbeNode.className = 'tracr-note';
          }}
        }}
        const speedyMode = !!state.speedy_mode;
        for (const id of ['vehicle-camera-panel', 'lidar-panel', 'bsm-panel']) {{
          const node = document.getElementById(id);
          if (node) node.hidden = speedyMode;
        }}
        document.getElementById('camera').src = state.camera_png || '';
        if (!speedyMode) {{
          document.getElementById('vehicle-camera').src = state.vehicle_camera_png || '';
          document.getElementById('lidar').src = state.lidar_png || '';
        }}
        const bsmPanelTitle = document.getElementById('bsm-panel-title');
        if (bsmPanelTitle) bsmPanelTitle.textContent = state.bsm_panel_title || 'BSM stream';
        const showBsmMap = state.bsm_show_map !== false;
        const bsmLayout = document.getElementById('bsm-layout');
        if (bsmLayout) bsmLayout.classList.toggle('bsm-layout--table-only', !showBsmMap);
        const bsmMap = document.getElementById('bsm-map');
        if (bsmMap) {{
          bsmMap.hidden = speedyMode || !showBsmMap;
          bsmMap.src = (!speedyMode && showBsmMap) ? (state.bsm_map_png || '') : '';
        }}
        if (!speedyMode) document.getElementById('bsm-table').innerHTML = state.bsm_table_html || '';
      }} catch (error) {{
        console.debug('TRACR dashboard refresh failed', error);
      }}
    }}
    refresh();
    setInterval(refresh, 500);
  </script>
</body>
</html>
"""

    def _external_state(self):
        speedy_mode = self._external_speedy_mode_enabled()
        return {
            "status": str(self._status_text),
            "speedy_mode": speedy_mode,
            "camera_png": self._png_uri(self._camera_png),
            "lidar_png": "" if speedy_mode else self._png_uri(self._lidar_png),
            "vehicle_camera_png": "" if speedy_mode else self._png_uri(self._vehicle_camera_png),
            "bsm_map_png": "" if speedy_mode else self._png_uri(self._bsm_map_png),
            "bsm_table_html": "" if speedy_mode else self._bsm_table_html,
            "stream_url": self.stream_url,
            "ego_vehicle_id": None if self.ego_vehicle_id is None else str(self.ego_vehicle_id),
            "metsr_vis_selected_vehicle_id": None if self._metsr_vis_selected_vehicle_id() is None else str(self._metsr_vis_selected_vehicle_id()),
            "metsr_vis_selected_vehicle_type": None if self._metsr_vis_selected_vehicle_type() is None else str(self._metsr_vis_selected_vehicle_type()),
            "metsr_vis_map": None if self.metsr_viz_map is None else str(self.metsr_viz_map),
            "metsr_vis_frame_url": self._metsr_vis_frame_url(),
            "metsr_vis_auto_highlight_ego": self.metsr_vis_auto_highlight_ego,
            "stream_probe": self.stream_probe,
            "viz_url": self.viz_url,
            "bsm_stream_label": self.bsm_stream_label,
            "bsm_panel_title": self._bsm_panel_title(),
            "bsm_show_map": self._bsm_panel_show_map(),
            "bsm_ego_only": self.bsm_ego_only,
        }

    def _write_external_page(self):
        if not self.external_directory:
            return
        with open(os.path.join(self.external_directory, "index.html"), "w", encoding="utf-8") as output:
            output.write(self._external_page_html())

    def _refresh_external_state(self, force=False):
        if not self.external_directory:
            return
        now = time.time()
        if (
            not force
            and self.external_min_update_interval_s > 0
            and now - self._external_last_write_time < self.external_min_update_interval_s
        ):
            return
        os.makedirs(self.external_directory, exist_ok=True)
        tmp_path = os.path.join(
            self.external_directory,
            f"state.{os.getpid()}.{id(self)}.tmp",
        )
        state_path = os.path.join(self.external_directory, "state.json")
        payload = json.dumps(self._external_state())
        try:
            with open(tmp_path, "w", encoding="utf-8") as output:
                output.write(payload)
            for attempt in range(8):
                try:
                    os.replace(tmp_path, state_path)
                    self._external_last_write_time = time.time()
                    return
                except PermissionError:
                    time.sleep(0.015 * (attempt + 1))
            self._external_last_write_time = time.time()
            return
        except PermissionError:
            self._external_last_write_time = time.time()
            return
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass

    def _ipython_display_(self):
        self.display()

    def display(self, fullscreen=None):
        from IPython.display import HTML, display

        if fullscreen is not None:
            self.fullscreen = bool(fullscreen)
            if self.widgets is not None:
                self.view = self._build_widget_view()
        if self.widgets is not None:
            display(self.view)
            return None
        self._display_handle = display(HTML(self._plain_html()), display_id=True)
        return self._display_handle

    def _refresh_plain_display(self):
        if self._display_handle is None:
            return
        from IPython.display import HTML

        self._display_handle.update(HTML(self._plain_html()))

    def update_status(self, text, force_external=True):
        self._status_text = text
        if self.widgets is not None:
            self.status.value = f"<div class='tracr-note'>{escape(str(text))}</div>"
        else:
            self._refresh_plain_display()
        self._refresh_external_state(force=force_external)
    def update(self, runtime, step_result, bsm_records, render_info=None, render_error=None):
        speedy_mode = self._external_speedy_mode_enabled()
        target_vehicle_id = None
        target_actor_id = None
        if runtime.sensor_panel is not None:
            target_actor_id = getattr(runtime.sensor_panel, "target_actor_id", None)
            target_vehicle_id = getattr(runtime.sensor_panel, "target_vehicle_id", None)

        ego_vehicle_id = _runtime_ego_vehicle_id(runtime, step_result, target_vehicle_id=target_vehicle_id)
        self.ego_vehicle_id = ego_vehicle_id
        selected_vehicle_id = self._metsr_vis_selected_vehicle_id()
        selected_vehicle_type = _runtime_metsr_vis_vehicle_type(runtime, selected_vehicle_id)
        if selected_vehicle_type is not None:
            if self.metsr_vis_highlight_vehicle_id is None:
                self.metsr_viz_vehicle_type = selected_vehicle_type
            elif self.metsr_vis_highlight_vehicle_type is None:
                self.metsr_vis_highlight_vehicle_type = selected_vehicle_type
        self._sync_metsr_vis_frame_url(force_external=selected_vehicle_type is not None)
        if speedy_mode:
            ego_state = None
            bsm_display_records = []
            bsm_view_mode = "speedy"
        else:
            ego_state = _runtime_vehicle_record(runtime, ego_vehicle_id)
            bsm_display_records, bsm_view_mode = _filter_bsm_records_for_ego(
                bsm_records,
                ego_vehicle_id,
                ego_only=self.bsm_ego_only,
                broadcast_as_ego=str(getattr(runtime, "bsm_stream_source", "")).lower() == "kafka",
                ego_state=ego_state,
            )

        now = time.time()
        refresh_media = (
            self.media_min_update_interval_s <= 0
            or now - self._media_last_update_time >= self.media_min_update_interval_s
        )
        refresh_lidar = (
            self.lidar_min_update_interval_s <= 0
            or now - self._lidar_last_update_time >= self.lidar_min_update_interval_s
        )
        if refresh_media or (not speedy_mode and refresh_lidar):
            camera_png = self._camera_png
            lidar_png = self._lidar_png
            vehicle_camera_png = self._vehicle_camera_png
            if runtime.sensor_panel is not None:
                if refresh_media:
                    camera_png = runtime.sensor_panel.camera_png()
                    if not speedy_mode and getattr(runtime.sensor_panel, "vehicle_camera_enabled", False):
                        vehicle_camera_fn = getattr(runtime.sensor_panel, "vehicle_camera_png", None)
                        if callable(vehicle_camera_fn):
                            vehicle_camera_png = vehicle_camera_fn()
                if not speedy_mode and refresh_lidar:
                    lidar_png = runtime.sensor_panel.lidar_png()

            bsm_has_fields = False
            if not speedy_mode:
                source_label = getattr(runtime, "bsm_stream_label", self.bsm_stream_label)
                self.bsm_stream_label = str(source_label or self.bsm_stream_label)
                bsm_has_fields = bsm_records_have_display_fields(bsm_display_records)
                if refresh_media and bsm_has_fields:
                    bsm_png = bsm_map_png(
                        bsm_display_records,
                        source_label=self.bsm_stream_label,
                        ego_vehicle_id=ego_vehicle_id,
                        ego_state=ego_state,
                        ego_marker_records=bsm_records,
                    )
                    bsm_html = bsm_table_html(
                        bsm_display_records,
                        source_label=self.bsm_stream_label,
                        ego_vehicle_id=ego_vehicle_id,
                        total_records=len(bsm_records or []),
                        view_mode=bsm_view_mode,
                    )

            if refresh_media:
                self._camera_png = camera_png
                if not speedy_mode:
                    self._vehicle_camera_png = vehicle_camera_png
                    if bsm_has_fields:
                        self._bsm_map_png = bsm_png
                        self._bsm_table_html = bsm_html
                self._media_last_update_time = now
            if not speedy_mode and refresh_lidar:
                self._lidar_png = lidar_png
                self._lidar_last_update_time = now
            if self.widgets is not None:
                if refresh_media:
                    self.camera_image.value = camera_png
                    if not speedy_mode:
                        self.vehicle_camera_image.value = vehicle_camera_png
                        if bsm_has_fields:
                            self.bsm_map.value = bsm_png
                            self.bsm_table.value = bsm_html
                if not speedy_mode and refresh_lidar:
                    self.lidar_image.value = lidar_png

        state = step_result.get("state") if isinstance(step_result, dict) else None
        carla_actors = 0
        if state is not None:
            carla_actors = len(state.active_vehicles) + len(state.display_vehicles)
        tick = getattr(runtime.metsr, "current_tick", None)
        configured_v2x = len(getattr(runtime, "v2x_vehicle_ids", []) or [])
        if speedy_mode:
            message = f"tick={tick} | CARLA actors={carla_actors} | speedy external visualization"
        else:
            unique_bsm = bsm_unique_sender_count(bsm_display_records)
            source_label = getattr(runtime, "bsm_stream_label", self.bsm_stream_label)
            message = f"tick={tick} | CARLA actors={carla_actors} | {source_label} ego BSM rows={len(bsm_display_records)}/{len(bsm_records or [])} | BSM emitters={unique_bsm}/{configured_v2x}"
            if ego_vehicle_id is not None:
                message += f" | BSM ego={ego_vehicle_id} view={bsm_view_mode}"
        bsm_stream_error = step_result.get("bsm_stream_error", "") if isinstance(step_result, dict) else ""
        if bsm_stream_error:
            message += f" | BSM stream waiting: {bsm_stream_error}"
        projection_info = step_result.get("tracr_projection", {}) if isinstance(step_result, dict) else {}
        if projection_info:
            message += f" | projected local={projection_info.get('live', 0)}/{projection_info.get('queried', 0)} roads={projection_info.get('road_count', 0)}"
            if projection_info.get("focus_vehicle") is not None:
                message += f" | ego={projection_info.get('focus_vehicle')}@{projection_info.get('focus_road', '')}"
            if projection_info.get("error"):
                message += f" | projection waiting: {projection_info.get('error')}"
        if target_vehicle_id is not None:
            message += f" | sensor target veh={target_vehicle_id}"
            if target_actor_id is not None:
                message += f" actor={target_actor_id}"
        elif target_actor_id is not None:
            message += f" | sensor target actor={target_actor_id}"
        if render_info:
            if render_info.get("skipped"):
                message += " | METS-R Viz skipped"
            else:
                message += f" | METS-R Viz clients={render_info.get('client_count', 'NA')}"
                selected_vehicle = self._metsr_vis_selected_vehicle_id()
                if selected_vehicle is not None:
                    message += f" | selected vehicle={selected_vehicle} type={self._metsr_vis_selected_vehicle_type()}"
        if render_error:
            message += f" | METS-R Viz waiting: {render_error}"
        profile_ms = step_result.get("profile_ms", {}) if isinstance(step_result, dict) else {}
        if profile_ms:
            message += f" | loop={profile_ms.get('total', 0.0):.1f}ms"
        self.update_status(message, force_external=False)

def highlight_metsr_vis_vehicle(dashboard, vehicle_id, vehicle_type=None):
    """Request vehicle selection in a TRACRDashboard embedded METS-R Vis frame."""
    if not hasattr(dashboard, "highlight_metsr_vis_vehicle"):
        raise TypeError("dashboard must be a TRACRDashboard-like object with highlight_metsr_vis_vehicle().")
    return dashboard.highlight_metsr_vis_vehicle(vehicle_id, vehicle_type=vehicle_type)


def highlight_metsr_vis_element(dashboard, element_id, element_type="vehicle"):
    """Request element selection in a TRACRDashboard embedded METS-R Vis frame."""
    if not hasattr(dashboard, "highlight_metsr_vis_element"):
        raise TypeError("dashboard must be a TRACRDashboard-like object with highlight_metsr_vis_element().")
    return dashboard.highlight_metsr_vis_element(element_id, element_type=element_type)


def clear_metsr_vis_highlight(dashboard, auto_highlight_ego=True):
    """Clear a manual METS-R Vis vehicle highlight on a TRACRDashboard-like object."""
    if not hasattr(dashboard, "clear_metsr_vis_highlight"):
        raise TypeError("dashboard must be a TRACRDashboard-like object with clear_metsr_vis_highlight().")
    return dashboard.clear_metsr_vis_highlight(auto_highlight_ego=auto_highlight_ego)

@dataclass
class TRACRDemoRuntime:
    config: object
    sim_dirs: list
    metsr: object
    carla_client: object
    carla_tm: object
    world: object
    carla_state: object
    kafka_processor: object
    sensor_panel: object
    viz_info: dict
    generated_vehicle_ids: list
    v2x_vehicle_ids: list
    bsm_stream: object = None
    bsm_stream_source: str = "kafka"
    bsm_stream_label: str = "Kafka"
    veins_client: object = None
    started_kafka: bool = False
    bsm_poll_timeout_ms: int = 1
    bsm_max_records: int = 120
    projection_heading_smoothing: float = 0.35
    projection_z_offset: float = 0.05
    random_seed: int = None

    def close(self, stop_kafka=False):
        if self.sensor_panel is not None:
            self.sensor_panel.close()
        state = self.carla_state
        if state is not None:
            for store in (state.active_vehicles, state.display_vehicles):
                for actor in list(store.values()):
                    try:
                        actor.destroy()
                    except Exception:
                        pass
                store.clear()
        if self.kafka_processor is not None:
            try:
                self.kafka_processor.close()
            except Exception:
                pass
        if self.bsm_stream is not None and self.bsm_stream is not self.kafka_processor:
            try:
                self.bsm_stream.close()
            except Exception:
                pass
        if self.metsr is not None:
            try:
                self.metsr.stop_viz()
            except Exception:
                pass
            try:
                self.metsr.terminate()
            except Exception:
                try:
                    self.metsr.close()
                except Exception:
                    pass
        if stop_kafka and self.started_kafka:
            run_docker_compose("down")


def _is_address_in_use_error(exc):
    if exc is None:
        return False
    if isinstance(exc, OSError):
        if getattr(exc, "winerror", None) == 10048:
            return True
        if getattr(exc, "errno", None) in (98, 10048):
            return True
    return _is_address_in_use_error(getattr(exc, "__cause__", None))


def _resolve_tracr_random_seed(random_seed):
    if random_seed is None or random_seed is False:
        return None
    if random_seed is True:
        return int.from_bytes(os.urandom(4), "big") & 0x7FFFFFFF
    if isinstance(random_seed, str):
        normalized = random_seed.strip().lower()
        if normalized in {"", "none", "default", "config", "false"}:
            return None
        if normalized in {"auto", "random", "fresh", "new"}:
            return int.from_bytes(os.urandom(4), "big") & 0x7FFFFFFF
    return int(random_seed)


def _apply_tracr_random_seed(config, random_seed):
    seed = _resolve_tracr_random_seed(random_seed)
    if seed is None:
        return None
    seed = int(seed)
    random.seed(seed)
    np.random.seed(seed % (2 ** 32))
    num_simulations = max(1, int(getattr(config, "num_simulations", 1) or 1))
    config.random_seeds = [seed + offset for offset in range(num_simulations)]
    return seed

def _start_viz_with_port_fallback(metsr, viz_kwargs, max_extra_ports=20):
    from utils.util import stop_all_metsr_client_servers

    base_kwargs = dict(viz_kwargs or {})
    base_port = int(base_kwargs.get("server_port", 8765))
    cleanup_attempted = False
    last_error = None

    for offset in range(int(max_extra_ports) + 1):
        port = base_port + offset
        attempt_kwargs = dict(base_kwargs)
        if offset > 0 or "server_port" in attempt_kwargs:
            attempt_kwargs["server_port"] = port
        try:
            info = metsr.start_viz(**attempt_kwargs)
            if offset > 0:
                print(f"METS-R Vis stream port {base_port} was busy; using {port} instead.")
            return info
        except OSError as exc:
            if not _is_address_in_use_error(exc):
                raise
            last_error = exc
            if offset == 0 and not cleanup_attempted:
                cleanup_attempted = True
                stopped = stop_all_metsr_client_servers(verbose=True)
                if stopped:
                    try:
                        return metsr.start_viz(**attempt_kwargs)
                    except OSError as retry_exc:
                        if not _is_address_in_use_error(retry_exc):
                            raise
                        last_error = retry_exc

    raise RuntimeError(
        f"Could not start METS-R Vis stream; ports {base_port}-{base_port + int(max_extra_ports)} are busy. "
        "Close old notebooks/kernels or call clear_all(), then rerun launch_tracr_demo()."
    ) from last_error


def launch_tracr_demo(
    run_config="configs/run_cosim_CARLAPurdue.json",
    private_vehicle_count=60,
    v2x_vehicle_count=20,
    private_vehicle_start_id=1000,
    start_kafka=None,
    start_metsr=True,
    start_carla=True,
    viz_stream_port=None,
    viz_stream_host=None,
    carla_camera_z=205.8,
    bsm_stream_source="kafka",
    simu5g_host=None,
    simu5g_port=None,
    simu5g_connect_timeout=10,
    simu5g_request_timeout=30,
    simu5g_max_network_vehicles=80,
    simu5g_max_messages=240,
    require_simu5g_backend=False,
    bsm_poll_timeout_ms=1,
    bsm_max_records=120,
    projection_heading_smoothing=0.35,
    projection_z_offset=0.05,
    vehicle_camera_enabled=True,
    lidar_enabled=True,
    random_seed=None,
):
    deps = _deps()
    config = deps["read_run_config"](run_config)
    resolved_random_seed = _apply_tracr_random_seed(config, random_seed)
    config.display_all = False
    config.verbose = False
    config.v2x = True
    config.kafka_bootstrap_servers = kafka_bootstrap_servers(config)
    config.kafka_topics = ["bsm", "v2x_rx_bsm"]
    config.kafka_poll_timeout_ms = int(bsm_poll_timeout_ms)
    bsm_stream_source = str(bsm_stream_source or "kafka").strip().lower()
    if bsm_stream_source in {"sim5g", "simu5g", "veins"}:
        bsm_stream_source = "simu5g"
    elif bsm_stream_source != "kafka":
        raise ValueError("bsm_stream_source must be 'kafka' or 'simu5g'.")
    if start_kafka is None:
        start_kafka = bsm_stream_source == "kafka"

    if start_kafka:
        run_docker_compose("up", "-d")
        wait_for_kafka(config.kafka_bootstrap_servers)

    sim_dirs = deps["prepare_sim_dirs"](config)
    if start_metsr:
        deps["run_simulation_in_docker"](config)

    port = int(config.ports[0] if hasattr(config, "ports") else config.metsr_port[0])
    metsr = deps["METSRClient"](
        host=config.metsr_host,
        sim_folder=sim_dirs[0],
        port=port,
        timeout=600,
        config_json=run_config,
        config=config,
    )

    carla_client = carla_tm = world = None
    if start_carla:
        carla_client, carla_tm = deps["open_carla"](config)
        world = carla_client.get_world()
        deps["set_overlook_camera"](
            world,
            x=0.0,
            y=0.0,
            z=float(carla_camera_z),
            yaw=-90.0,
            pitch=-90.0,
        )

    kafka_processor = None
    bsm_stream = None
    bsm_stream_label = "Kafka" if bsm_stream_source == "kafka" else "Simu5G"
    veins_client = None
    if bsm_stream_source == "kafka":
        kafka_processor = deps["KafkaDataProcessor"](config, topics=("bsm", "v2x_rx_bsm"))
        kafka_processor.clear(max_empty_polls=10)
        bsm_stream = TRACRKafkaBSMStream(kafka_processor)
    else:
        veins_client = deps["VeinsClient"](
            config=config,
            host=simu5g_host or getattr(config, "veins_host", "127.0.0.1"),
            port=simu5g_port or getattr(config, "veins_port", 9099),
            connect_timeout=simu5g_connect_timeout,
            request_timeout=simu5g_request_timeout,
        )
        veins_client.connect()
        bsm_stream = TRACRSimu5GBSMStream(
            veins_client,
            deps["build_mobility_records"],
            deps["build_bsm_records"],
            duration_s=float(getattr(config, "sim_step_size", 0.1)),
            max_network_vehicles=simu5g_max_network_vehicles,
            max_messages=simu5g_max_messages,
            require_backend="simu5g_cellular_uu" if require_simu5g_backend else None,
        )

    vehicle_ids = list(range(private_vehicle_start_id, private_vehicle_start_id + private_vehicle_count))
    if vehicle_ids:
        metsr.generate_trip(vehicle_ids, -1, -1)
    v2x_ids = vehicle_ids[: max(0, min(v2x_vehicle_count, len(vehicle_ids)))]
    if v2x_ids:
        metsr.update_vehicle_sensor_type(v2x_ids, "cv2x", True)

    viz_kwargs = {}
    if viz_stream_port is not None:
        viz_kwargs["server_port"] = int(viz_stream_port)
    if viz_stream_host is not None:
        viz_kwargs["host"] = viz_stream_host
    viz_info = _start_viz_with_port_fallback(metsr, viz_kwargs)

    carla_state = deps["CarlaCosimState"]()
    sensor_panel = None
    if world is not None:
        sensor_panel = CarlaSensorPanel(
            world,
            deps["carla"],
            deps["destroy_carla_actor"],
            vehicle_camera_enabled=vehicle_camera_enabled,
            lidar_enabled=lidar_enabled,
        )
        sensor_panel.spawn_overhead_camera(z=carla_camera_z)

    return TRACRDemoRuntime(
        config=config,
        sim_dirs=sim_dirs,
        metsr=metsr,
        carla_client=carla_client,
        carla_tm=carla_tm,
        world=world,
        carla_state=carla_state,
        kafka_processor=kafka_processor,
        sensor_panel=sensor_panel,
        viz_info=viz_info,
        generated_vehicle_ids=vehicle_ids,
        v2x_vehicle_ids=v2x_ids,
        bsm_stream=bsm_stream,
        bsm_stream_source=bsm_stream_source,
        bsm_stream_label=bsm_stream_label,
        veins_client=veins_client,
        started_kafka=start_kafka,
        bsm_poll_timeout_ms=int(bsm_poll_timeout_ms),
        bsm_max_records=int(bsm_max_records),
        projection_heading_smoothing=float(projection_heading_smoothing),
        projection_z_offset=float(projection_z_offset),
        random_seed=resolved_random_seed,
    )


def _unique_ordered(values):
    seen = set()
    result = []
    for value in values or []:
        if value is None:
            continue
        key = str(value)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _as_road_ids(value):
    if value is None:
        return []
    if isinstance(value, (str, int, float)):
        return [str(value)]
    try:
        return [str(item) for item in value if item is not None]
    except TypeError:
        return [str(value)]


def _road_id_from_vehicle_record(record):
    if not isinstance(record, dict):
        return None
    for key in ("roadID", "road_id", "road"):
        value = record.get(key)
        if value is not None:
            return str(value)
    return None


def _road_id_from_road_record(record):
    if not isinstance(record, dict):
        return None
    for key in ("ID", "roadID", "road_id", "road", "origID", "orig_id", "originID"):
        value = record.get(key)
        if value is not None:
            return str(value)
    return None


def _vehicle_is_live(vehicle_state):
    if not isinstance(vehicle_state, dict):
        return False
    try:
        return float(vehicle_state.get("state", 0) or 0) > 0
    except (TypeError, ValueError):
        return False


def _query_tracr_road_graph(runtime, batch_size=500):
    cached = getattr(runtime, "_tracr_road_graph", None)
    if cached is not None:
        return cached

    graph = {"downstream": {}, "upstream": {}, "error": ""}
    try:
        index = runtime.metsr.query_road()
        road_ids = index.get("orig_id") or index.get("id_list") or []
        road_ids = [str(road_id) for road_id in road_ids if road_id is not None]
        batch_size = max(1, int(batch_size or 1))
        for start in range(0, len(road_ids), batch_size):
            batch = road_ids[start:start + batch_size]
            response = runtime.metsr.query_road(id=batch)
            for record in response.get("DATA", []) or []:
                road_id = _road_id_from_road_record(record)
                if road_id is None:
                    continue
                downstream = _unique_ordered(_as_road_ids(
                    record.get("down_stream_road")
                    or record.get("downstream_road")
                    or record.get("downstreamRoad")
                    or record.get("downstreamRoads")
                ))
                graph["downstream"][road_id] = downstream
                for downstream_road in downstream:
                    graph["upstream"].setdefault(str(downstream_road), []).append(road_id)
        graph["upstream"] = {
            road_id: _unique_ordered(upstream_roads)
            for road_id, upstream_roads in graph["upstream"].items()
        }
    except Exception as exc:
        graph["error"] = str(exc).splitlines()[0]

    setattr(runtime, "_tracr_road_graph", graph)
    return graph


def _expand_tracr_road_context(runtime, focus_road, upstream_depth=3, downstream_depth=3):
    focus_road = None if focus_road is None else str(focus_road)
    if not focus_road:
        return [], ""

    graph = _query_tracr_road_graph(runtime)
    roads = []
    seen = set()

    def add(road_id):
        if road_id is None:
            return
        road_key = str(road_id)
        if road_key in seen:
            return
        seen.add(road_key)
        roads.append(road_key)

    add(focus_road)
    frontier = [focus_road]
    for _ in range(max(0, int(downstream_depth))):
        next_frontier = []
        for road_id in frontier:
            for downstream_road in graph.get("downstream", {}).get(str(road_id), []) or []:
                add(downstream_road)
                next_frontier.append(str(downstream_road))
        frontier = next_frontier

    frontier = [focus_road]
    for _ in range(max(0, int(upstream_depth))):
        next_frontier = []
        for road_id in frontier:
            for upstream_road in graph.get("upstream", {}).get(str(road_id), []) or []:
                add(upstream_road)
                next_frontier.append(str(upstream_road))
        frontier = next_frontier

    return roads, graph.get("error", "")


def _query_tracr_focus_vehicle(runtime):
    candidates = []
    focus_vehicle_id = getattr(runtime, "focus_vehicle_id", None)
    if focus_vehicle_id is not None:
        candidates.append(focus_vehicle_id)
    sensor_panel = getattr(runtime, "sensor_panel", None)
    sensor_target = getattr(sensor_panel, "target_vehicle_id", None) if sensor_panel is not None else None
    if sensor_target is not None:
        candidates.append(sensor_target)
    candidates.extend(getattr(runtime, "v2x_vehicle_ids", []) or [])
    candidates.extend(getattr(runtime, "generated_vehicle_ids", []) or [])
    candidates = _unique_ordered(candidates)
    if not candidates:
        return None, None, ""

    try:
        response = runtime.metsr.query_vehicle(
            id=candidates,
            private_veh=[True] * len(candidates),
            transform_coords=True,
        )
    except Exception as exc:
        return None, None, str(exc).splitlines()[0]

    first_live = None
    for vehicle_id, record in zip(candidates, response.get("DATA", []) or []):
        if not _vehicle_is_live(record):
            continue
        road_id = _road_id_from_vehicle_record(record)
        if first_live is None:
            first_live = (vehicle_id, record)
        if road_id is not None:
            setattr(runtime, "focus_vehicle_id", vehicle_id)
            setattr(runtime, "focus_road_id", road_id)
            return vehicle_id, record, ""

    if first_live is not None:
        vehicle_id, record = first_live
        setattr(runtime, "focus_vehicle_id", vehicle_id)
        return vehicle_id, record, ""
    return None, None, ""


def _query_tracr_road_vehicle_ids(runtime, road_ids):
    private_ids = []
    public_ids = []
    try:
        fleet = runtime.metsr.query_on_road_vehicles(roadID=road_ids)
    except Exception as exc:
        return private_ids, public_ids, str(exc).splitlines()[0]

    if not isinstance(fleet, dict) or fleet.get("CODE") == "KO":
        return private_ids, public_ids, ""
    if fleet.get("DATA"):
        for road_record in fleet.get("DATA", []) or []:
            if isinstance(road_record, dict) and road_record.get("STATUS") != "KO":
                private_ids.extend(road_record.get("private_vids") or [])
                public_ids.extend(road_record.get("public_vids") or [])
    else:
        private_ids.extend(fleet.get("private_vids") or [])
        public_ids.extend(fleet.get("public_vids") or [])
    return _unique_ordered(private_ids), _unique_ordered(public_ids), ""


def _query_tracr_vehicle_records(runtime, vehicle_ids, private_flag, batch_size=1000):
    records = []
    vehicle_ids = list(vehicle_ids or [])
    batch_size = max(1, int(batch_size or 1))
    for start in range(0, len(vehicle_ids), batch_size):
        batch = vehicle_ids[start:start + batch_size]
        if not batch:
            continue
        try:
            response = runtime.metsr.query_vehicle(
                id=batch,
                private_veh=[bool(private_flag)] * len(batch),
                transform_coords=True,
            )
        except Exception as exc:
            return records, str(exc).splitlines()[0]
        for vehicle_id, record in zip(batch, response.get("DATA", []) or []):
            if isinstance(record, dict):
                records.append((vehicle_id, bool(private_flag), record))
    return records, ""


def _angle_delta_degrees(target, current):
    return (float(target) - float(current) + 180.0) % 360.0 - 180.0


def _smooth_yaw_degrees(previous, target, alpha=0.35):
    target = float(target) % 360.0
    if previous is None:
        return target
    alpha = max(0.0, min(1.0, float(alpha)))
    return (float(previous) + _angle_delta_degrees(target, previous) * alpha) % 360.0


def _zero_projection_actor_motion(actor, carla_module):
    if actor is None:
        return
    try:
        actor.set_autopilot(False)
    except Exception:
        pass
    try:
        actor.set_simulate_physics(False)
    except Exception:
        pass
    zero = None
    if carla_module is not None:
        try:
            zero = carla_module.Vector3D(x=0.0, y=0.0, z=0.0)
        except Exception:
            zero = None
    if zero is not None:
        for method_name in ("set_target_velocity", "set_target_angular_velocity"):
            try:
                getattr(actor, method_name)(zero)
            except Exception:
                pass
    try:
        actor.apply_control(carla_module.VehicleControl(throttle=0.0, brake=1.0))
    except Exception:
        pass


def _stabilize_projection_vehicle(runtime, deps, veh_id, actor, vehicle_state):
    if actor is None or vehicle_state is None:
        return False
    world = getattr(runtime, "world", None)
    if world is None:
        return False

    carla_module = deps["carla"]
    try:
        location = deps["metsr_to_carla_location"](
            world,
            vehicle_state["x"],
            vehicle_state["y"],
            z_offset=getattr(runtime, "projection_z_offset", 0.05),
        )
    except Exception:
        return False

    cache = getattr(runtime, "_tracr_projection_pose_cache", None)
    if cache is None:
        cache = {}
        setattr(runtime, "_tracr_projection_pose_cache", cache)
    key = str(veh_id)
    previous = cache.get(key, {})

    bearing = first_present(vehicle_state, "bearing", "heading", "heading_deg")
    target_yaw = deps["metsr_bearing_to_carla_yaw"](bearing if bearing is not None else 0.0)
    previous_location = previous.get("location")
    if previous_location is not None:
        dx = float(location.x) - float(previous_location.x)
        dy = float(location.y) - float(previous_location.y)
        if math.hypot(dx, dy) > 0.15:
            target_yaw = math.degrees(math.atan2(dy, dx)) % 360.0

    yaw = _smooth_yaw_degrees(
        previous.get("yaw"),
        target_yaw,
        alpha=getattr(runtime, "projection_heading_smoothing", 0.35),
    )
    transform = carla_module.Transform(
        location,
        carla_module.Rotation(pitch=0.0, yaw=float(yaw), roll=0.0),
    )
    try:
        _zero_projection_actor_motion(actor, carla_module)
        actor.set_transform(transform)
        _zero_projection_actor_motion(actor, carla_module)
    except RuntimeError:
        return False

    cache[key] = {"location": location, "yaw": yaw}
    return True


def _sync_tracr_road_context_vehicles(runtime, deps):
    world = getattr(runtime, "world", None)
    state = getattr(runtime, "carla_state", None)
    info = {
        "focus_vehicle": None,
        "focus_road": "",
        "road_count": 0,
        "context_roads": [],
        "private_ids": 0,
        "public_ids": 0,
        "queried": 0,
        "live": 0,
        "spawned": 0,
        "updated": 0,
        "destroyed": 0,
        "failed": 0,
        "error": "",
    }
    if world is None or state is None:
        return info

    focus_vehicle_id, focus_state, error = _query_tracr_focus_vehicle(runtime)
    if error:
        info["error"] = error
        return info
    focus_road = _road_id_from_vehicle_record(focus_state)
    info["focus_vehicle"] = focus_vehicle_id
    info["focus_road"] = focus_road or ""
    if focus_road is None:
        return info

    context_roads, graph_error = _expand_tracr_road_context(runtime, focus_road)
    info["context_roads"] = context_roads
    info["road_count"] = len(context_roads)
    if graph_error:
        info["error"] = graph_error
    private_ids, public_ids, road_error = _query_tracr_road_vehicle_ids(runtime, context_roads)
    if road_error:
        info["error"] = road_error
    if focus_vehicle_id is not None:
        private_ids = _unique_ordered([focus_vehicle_id] + list(private_ids))
    info["private_ids"] = len(private_ids)
    info["public_ids"] = len(public_ids)

    private_records, private_error = _query_tracr_vehicle_records(runtime, private_ids, True)
    public_records, public_error = _query_tracr_vehicle_records(runtime, public_ids, False)
    for query_error in (private_error, public_error):
        if query_error and not info["error"]:
            info["error"] = query_error
    vehicle_records = private_records + public_records
    setattr(runtime, "_tracr_last_vehicle_records", list(vehicle_records))
    info["queried"] = len(vehicle_records)

    desired_ids = set()
    for veh_id, private_flag, vehicle_state in vehicle_records:
        if not _vehicle_is_live(vehicle_state):
            continue
        desired_ids.add(veh_id)
        info["live"] += 1
        actor = state.active_vehicles.get(veh_id) or state.display_vehicles.get(veh_id)
        actor_alive = False
        if actor is not None:
            try:
                actor_alive = bool(actor.is_alive)
            except RuntimeError:
                actor_alive = False

        if actor is not None and actor_alive:
            try:
                if _stabilize_projection_vehicle(runtime, deps, veh_id, actor, vehicle_state):
                    info["updated"] += 1
                    continue
            except RuntimeError:
                pass
            deps["destroy_tracked_carla_vehicle"](state, veh_id)

        actor = deps["spawn_carla_vehicle"](
            world,
            getattr(runtime, "carla_tm", None),
            veh_id,
            bool(private_flag),
            vehicle_state,
            actor_store=state.display_vehicles,
            autopilot=False,
            verbose=False,
        )
        if actor is None:
            info["failed"] += 1
        else:
            _stabilize_projection_vehicle(runtime, deps, veh_id, actor, vehicle_state)
            info["spawned"] += 1

    for veh_id, actor in list(state.display_vehicles.items()):
        if veh_id in desired_ids:
            continue
        try:
            deps["destroy_carla_actor"](actor)
        finally:
            state.display_vehicles.pop(veh_id, None)
            info["destroyed"] += 1
    return info


def _elapsed_ms(start):
    return (time.perf_counter() - start) * 1000.0


def _summarize_profile_samples(samples):
    samples = [sample for sample in samples or [] if isinstance(sample, dict)]
    if not samples:
        return {}
    keys = sorted({key for sample in samples for key in sample})
    summary = {}
    for key in keys:
        values = np.asarray([sample[key] for sample in samples if key in sample], dtype=float)
        if values.size == 0:
            continue
        summary[key] = {
            "mean": float(values.mean()),
            "p50": float(np.percentile(values, 50)),
            "p95": float(np.percentile(values, 95)),
            "max": float(values.max()),
            "total": float(values.sum()),
        }
    return summary


def _should_run_every(index, every, total_ticks=None):
    every = max(1, int(every or 1))
    if every <= 1:
        return True
    if total_ticks is not None and index == int(total_ticks) - 1:
        return True
    return index % every == 0


def step_tracr_demo(
    runtime,
    dashboard=None,
    render_wait_timeout=0,
    profile=False,
    render=True,
    update_dashboard=True,
    update_sensors=True,
    poll_bsm=True,
):
    timings = {}
    total_start = time.perf_counter()

    start = time.perf_counter()
    deps = _deps()
    timings["deps"] = _elapsed_ms(start)

    start = time.perf_counter()
    if runtime.world is not None:
        runtime.world.tick()
    runtime.metsr.tick(wait_forever=True, poll_timeout=5)
    step_result = {
        "state": runtime.carla_state,
        "vehicles": [],
        "fleet_controlled": True,
    }
    timings["metsr_step"] = _elapsed_ms(start)

    start = time.perf_counter()
    projection_info = _sync_tracr_road_context_vehicles(runtime, deps)
    timings["road_projection"] = _elapsed_ms(start)
    step_result["tracr_projection"] = projection_info

    start = time.perf_counter()
    _keep_carla_projection_passive(runtime.carla_state, deps["carla"])
    timings["passive_carla"] = _elapsed_ms(start)

    if update_sensors and runtime.sensor_panel is not None:
        start = time.perf_counter()
        preferred_vehicle_ids = []
        if projection_info.get("focus_vehicle") is not None:
            preferred_vehicle_ids.append(projection_info.get("focus_vehicle"))
        preferred_vehicle_ids.extend(getattr(runtime, "v2x_vehicle_ids", None) or [])
        runtime.sensor_panel.ensure_sensors(
            runtime.carla_state,
            preferred_vehicle_ids=preferred_vehicle_ids,
        )
        timings["sensor_sync"] = _elapsed_ms(start)
    else:
        timings["sensor_sync"] = 0.0

    start = time.perf_counter()
    bsm_stream = getattr(runtime, "bsm_stream", None) or getattr(runtime, "kafka_processor", None)
    bsm_records = list(getattr(runtime, "_tracr_last_bsm_records", []) or [])
    if bsm_stream is not None and poll_bsm:
        timeout_ms = int(getattr(runtime, "bsm_poll_timeout_ms", 1) or 1)
        max_records = int(getattr(runtime, "bsm_max_records", 120) or 120)
        try:
            bsm_records = bsm_stream.process_bsm(runtime=runtime, timeout_ms=timeout_ms, max_records=max_records) or []
        except TypeError:
            bsm_records = bsm_stream.process_bsm(timeout_ms=timeout_ms, max_records=max_records) or []
        setattr(runtime, "_tracr_last_bsm_records", list(bsm_records))
    if getattr(bsm_stream, "last_error", ""):
        step_result["bsm_stream_error"] = getattr(bsm_stream, "last_error", "")
    timings["bsm_stream"] = _elapsed_ms(start) if poll_bsm else 0.0

    render_info = None
    render_error = None
    if render:
        start = time.perf_counter()
        try:
            render_info = runtime.metsr.render(client_wait_timeout=render_wait_timeout)
            ego_vehicle_id = _runtime_ego_vehicle_id(runtime, step_result)
            if isinstance(render_info, dict):
                render_info["tracr_viz_selected_vehicle_id"] = ego_vehicle_id
                ego_vehicle_type = _runtime_metsr_vis_vehicle_type(runtime, ego_vehicle_id)
                if ego_vehicle_type is not None:
                    render_info["tracr_viz_selected_vehicle_type"] = ego_vehicle_type
        except Exception as exc:
            render_error = str(exc).splitlines()[0]
        timings["metsr_viz_render"] = _elapsed_ms(start)
    else:
        render_info = {"skipped": True}
        timings["metsr_viz_render"] = 0.0

    if dashboard is not None and update_dashboard:
        start = time.perf_counter()
        dashboard.update(runtime, step_result, bsm_records, render_info=render_info, render_error=render_error)
        timings["dashboard_update"] = _elapsed_ms(start)
    else:
        timings["dashboard_update"] = 0.0

    timings["total"] = _elapsed_ms(total_start)
    result = {
        "step_result": step_result,
        "bsm_records": bsm_records,
        "render_info": render_info,
        "render_error": render_error,
    }
    if profile:
        result["profile_ms"] = timings
        step_result["profile_ms"] = timings
    return result


def run_tracr_demo(
    runtime,
    dashboard,
    ticks=600,
    sleep_s=0.0,
    render_wait_timeout=0,
    profile=False,
    render_every=2,
    dashboard_every=3,
    sensor_every=1,
    bsm_every=3,
):
    result = None
    samples = []
    ticks = int(ticks)
    for tick_index in range(ticks):
        result = step_tracr_demo(
            runtime,
            dashboard=dashboard,
            render_wait_timeout=render_wait_timeout,
            profile=profile,
            render=_should_run_every(tick_index, render_every, ticks),
            update_dashboard=_should_run_every(tick_index, dashboard_every, ticks),
            update_sensors=_should_run_every(tick_index, sensor_every, ticks),
            poll_bsm=_should_run_every(tick_index, bsm_every, ticks),
        )
        if profile and result and result.get("profile_ms"):
            samples.append(result["profile_ms"])
        if sleep_s:
            time.sleep(float(sleep_s))
    if profile and result is not None:
        result = dict(result)
        result["profile_samples"] = len(samples)
        result["profile_summary_ms"] = _summarize_profile_samples(samples)
    return result


def benchmark_tracr_demo(runtime, dashboard=None, ticks=60, **kwargs):
    kwargs.setdefault("render_every", 2)
    kwargs.setdefault("dashboard_every", 3)
    kwargs.setdefault("bsm_every", 3)
    kwargs.setdefault("sensor_every", 1)
    return run_tracr_demo(runtime, dashboard, ticks=ticks, profile=True, **kwargs)

def _keep_carla_projection_passive(state, carla_module=None):
    if state is None:
        return
    for store in (getattr(state, "active_vehicles", {}), getattr(state, "display_vehicles", {})):
        for actor in list(store.values()):
            _zero_projection_actor_motion(actor, carla_module)




