"""Copy-on-write sensor perturbations, independent of simulator/controller setup."""
from __future__ import annotations

import math
from typing import Any, Mapping


def perturb_sensor_batch(
    batch: Mapping[str, tuple[Any, Any]],
    sensor_id: str,
    security: str,
    config: Mapping[str, Any],
) -> dict[str, tuple[Any, Any]]:
    """Copy the selected payload while preserving its timestamp and other feeds.

    Speedometer input is a mapping; GNSS/IMU inputs are fixed-size vectors. Camera
    input is BGR/BGRA, so RGB patch colors are reversed and alpha is preserved.
    NumPy is loaded only when a sensor effect is actually applied.
    """
    import numpy as np

    if sensor_id not in batch:
        raise RuntimeError(f"PCLA sensor batch is missing configured sensor {sensor_id!r}")
    frame, payload = batch[sensor_id]
    if security == "sensor_spoofing":
        sensor_config = config["sensor"]
        kind = sensor_config["kind"]
        if kind == "speedometer":
            payload = dict(payload)
            payload["speed"] = max(0.0, float(payload["speed"]) + sensor_config["speed_bias_mps"])
        else:
            payload = np.array(payload, copy=True)
            offset_key = "gnss_offset" if kind == "gnss" else "imu_offset"
            offset = sensor_config[offset_key]
            if payload.shape != (len(offset),):
                raise RuntimeError(f"Unexpected {kind} sensor shape: {payload.shape}")
            payload = payload.astype(float) + np.asarray(offset)
    else:
        payload = np.array(payload, copy=True)
        if payload.ndim != 3 or payload.shape[2] not in (3, 4):
            raise RuntimeError(f"Expected an RGB camera's BGR/BGRA frame, got {payload.shape}")
        perception = config["perception"]
        if perception["mode"] == "blackout":
            payload[..., :3] = 0
        else:
            image_height, image_width = payload.shape[:2]
            left, top, width, height = perception["roi"]
            x0 = int(left * image_width)
            y0 = int(top * image_height)
            x1 = min(image_width, max(x0 + 1, math.ceil((left + width) * image_width)))
            y1 = min(image_height, max(y0 + 1, math.ceil((top + height) * image_height)))
            payload[y0:y1, x0:x1, :3] = perception["color_rgb"][::-1]
    result = dict(batch)
    result[sensor_id] = (frame, payload)
    return result
