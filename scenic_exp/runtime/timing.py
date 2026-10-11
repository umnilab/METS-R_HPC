"""Simulation-time event windows and center-distance/TTC encounter triggers."""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping


def encounter(ego: Any, other: Any) -> tuple[float, float | None]:
    """Return center distance and radial closing TTC, or None when not closing."""
    delta_x = other.position.x - ego.position.x
    delta_y = other.position.y - ego.position.y
    distance = math.hypot(delta_x, delta_y)
    relative_vx = other.velocity.x - ego.velocity.x
    relative_vy = other.velocity.y - ego.velocity.y
    closing_speed = -(delta_x * relative_vx + delta_y * relative_vy) / max(distance, 1e-9)
    ttc = distance / closing_speed if closing_speed > 0.01 else None
    return distance, ttc


def trigger_reached(
    now: float,
    ego: Any,
    hazards: Iterable[Any],
    config: Mapping[str, Any],
) -> bool:
    """Check time, distance, or TTC after the configured earliest start time."""
    trigger = config["trigger"]
    if now + 1e-9 < trigger["start_s"]:
        return False
    if trigger["mode"] == "time":
        return True
    encounters = [encounter(ego, hazard) for hazard in hazards]
    if trigger["mode"] == "distance":
        return any(distance <= trigger["distance_m"] for distance, _ in encounters)
    return any(ttc is not None and ttc <= trigger["ttc_s"] for _, ttc in encounters)


def event_active(now: float, onset: float | None, config: Mapping[str, Any]) -> bool:
    """Check a half-open event window, optionally repeated from the latched onset."""
    if onset is None or now + 1e-9 < onset:
        return False
    elapsed = max(0.0, now - onset)
    period = config["event"]["repeat_s"]
    phase = (elapsed + 1e-9) % period if period else elapsed + 1e-9
    return phase < config["event"]["duration_s"]
