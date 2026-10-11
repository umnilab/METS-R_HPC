"""CARLA world-to-camera geometry and dashboard target annotation.

Inputs use CARLA axes (x forward, y right, z up). These functions require only
NumPy and simulator-like objects; they never connect to or advance a simulator.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Optional, Sequence, Tuple

import numpy as np


def _distance(a: Any, b: Any) -> float:
    return math.sqrt(
        (float(a.x) - float(b.x)) ** 2
        + (float(a.y) - float(b.y)) ** 2
        + (float(a.z) - float(b.z)) ** 2
    )


def _horizontal_distance(a: Any, b: Any) -> float:
    return math.hypot(float(a.x) - float(b.x), float(a.y) - float(b.y))


def _transform_matrix(
    x: float, y: float, z: float, roll: float, pitch: float, yaw: float,
) -> np.ndarray:
    matrix = np.eye(4, dtype=float)
    roll, pitch, yaw = np.radians([roll, pitch, yaw])
    cr, sr = math.cos(roll), math.sin(roll)
    cp, sp = math.cos(pitch), math.sin(pitch)
    cy, sy = math.cos(yaw), math.sin(yaw)
    matrix[:3, :3] = np.array(
        [
            [cp * cy, cy * sp * sr - sy * cr, -cy * sp * cr - sy * sr],
            [cp * sy, sy * sp * sr + cy * cr, -sy * sp * cr + cy * sr],
            [sp, -cp * sr, cp * cr],
        ],
        dtype=float,
    )
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
        *(
            float(spec.get(key, 0.0) or 0.0)
            for key in ("x", "y", "z", "roll", "pitch", "yaw")
        )
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
