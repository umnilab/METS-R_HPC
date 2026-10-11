"""Recolor a projected stop-sign region before a policy receives each image.

The tap wraps sensor ingress without consuming queues or advancing simulation.
Projection uses the ego transform at callback time, as in the original demo;
exact camera-frame/pose alignment requires synchronized callbacks upstream.
PNG encoding is optional and imports Pillow only when snapshots are requested.
"""

from __future__ import annotations

import io
import threading
from typing import Any, Callable, Dict, Optional, Tuple

import numpy as np

from .geometry import _distance, project_stop_sign
from .targets import StopSignCandidate, StopSignPatchSettings


def _encode_png(rgb: np.ndarray) -> bytes:
    # Rendering is optional: no imaging or simulator dependencies are loaded
    # until a caller actually requests dashboard snapshots.
    from PIL import Image

    buffer = io.BytesIO()
    Image.fromarray(np.asarray(rgb, dtype=np.uint8)).save(
        buffer, format="PNG", compress_level=1
    )
    return buffer.getvalue()


class StopSignColorPatchTap:
    """Patch one projected stop sign before PCLA consumes CARLA BGRA frames."""

    def __init__(
        self,
        pcla: Any,
        ego_actor: Any,
        candidate: StopSignCandidate,
        enabled: bool,
        args: Any = StopSignPatchSettings(),
        *,
        image_encoder: Optional[Callable[[np.ndarray], bytes]] = None,
        active: Optional[Callable[[], bool]] = None,
    ) -> None:
        agent = getattr(pcla, "agent_instance", None)
        interface = getattr(agent, "sensor_interface", None)
        if interface is None or not callable(getattr(interface, "update_sensor", None)):
            raise RuntimeError("PCLA agent did not expose sensor_interface.update_sensor")
        specs = [
            dict(spec) for spec in agent.sensors()
            if str(spec.get("type", "")).startswith("sensor.camera.rgb")
        ]
        if enabled and not specs:
            raise RuntimeError("Stop-sign patch requires at least one RGB camera sensor")
        specs.sort(key=lambda spec: str(spec.get("id", "")))
        self.camera_specs = specs
        self._specs = {str(spec.get("id")): spec for spec in specs}
        phase = "ATTACKED red->blue" if enabled else "BASELINE unmodified"
        self._labels = {}
        reference_labels = {}
        for index, (sensor_id, spec) in enumerate(self._specs.items()):
            yaw = float(spec.get("yaw", 0.0) or 0.0)
            if abs(abs(yaw) - 180.0) <= 30.0:
                direction = "Rear"
            elif yaw < -8.0:
                direction = "Front left"
            elif yaw > 8.0:
                direction = "Front right"
            else:
                direction = "Front center"
            label = f"PCLA {direction} | {spec.get('id', f'camera_{index + 1}')} | yaw {yaw:g} deg"
            self._labels[sensor_id] = f"{phase} | {label}"
            if enabled:
                reference_labels[f"{sensor_id}__reference"] = f"REFERENCE red original | {label}"
        self._labels.update(reference_labels)
        self._interface = interface
        self._original_update = interface.update_sensor
        self._ego_actor = ego_actor
        self._candidate = candidate
        self._enabled = enabled
        self._args = args
        self._image_encoder = image_encoder or _encode_png
        self._lock = threading.Lock()
        self._latest: Dict[str, Tuple[Any, np.ndarray]] = {}
        self._png_cache: Dict[str, Tuple[Any, bytes]] = {}
        self.frames_patched = 0
        self.pixels_patched = 0
        self.projected_frames = 0
        self.meaningful_patched_frames = 0
        self.max_pixels_patched_per_frame = 0
        self.closest_patched_distance_m: Optional[float] = None
        self.last_target_distance_m: Optional[float] = None
        self.minimum_target_distance_m: Optional[float] = None
        original_update = self._original_update

        # Preserve the callback contract: prepare a copied image, snapshot the
        # actual delivered pixels, then forward the original tag and timestamp.
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
                    if (
                        enabled
                        and (active is None or active())
                        and distance <= args.attack_max_distance_m
                    ):
                        projection = project_stop_sign(
                            ego_actor, self._specs[sensor_id], candidate.visual_location,
                            array.shape, candidate.visual_extent_m, args.attack_roi_scale,
                        )
                        if projection is not None:
                            self.projected_frames += 1
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
                                self.max_pixels_patched_per_frame = max(
                                    self.max_pixels_patched_per_frame, count
                                )
                                self.closest_patched_distance_m = min(
                                    distance,
                                    self.closest_patched_distance_m
                                    if self.closest_patched_distance_m is not None
                                    else distance,
                                )
                                if count >= args.attack_min_patch_pixels:
                                    self.meaningful_patched_frames += 1
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
    def recolor_red_bgra(
        region: np.ndarray, minimum_red: int = 90, dominance: float = 1.25,
    ) -> int:
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

    @property
    def pixels_per_patched_frame(self) -> float:
        return (
            0.0
            if self.frames_patched <= 0
            else self.pixels_patched / self.frames_patched
        )

    def snapshots(self) -> Dict[str, Tuple[str, bytes, Any]]:
        result: Dict[str, Tuple[str, bytes, Any]] = {}
        with self._lock:
            latest = dict(self._latest)
        for sensor_id, (frame, rgb) in latest.items():
            cached = self._png_cache.get(sensor_id)
            if cached is None or cached[0] != frame:
                cached = (frame, self._image_encoder(rgb))
                self._png_cache[sensor_id] = cached
            result[sensor_id] = (self._labels.get(sensor_id, sensor_id), cached[1], frame)
        return result

    def close(self) -> None:
        if getattr(self._interface, "update_sensor", None) is self._wrapped_update:
            self._interface.update_sensor = self._original_update
