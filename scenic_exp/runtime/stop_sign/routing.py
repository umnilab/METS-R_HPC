"""Build and validate continuous PCLA routes through a selected stop lane.

Callers supply the PCLA module and CARLA client. Each proposed route is planned
from ego to one destination and checked around the stop; independently planned
legs are never stitched together across an ambiguous lane trigger.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

from .geometry import _distance, _horizontal_distance
from .targets import StopSignCandidate


def _waypoint_location(waypoint: Any) -> Optional[Any]:
    return getattr(getattr(waypoint, "transform", None), "location", None)


def _route_through_stop_quality(
    waypoints: Sequence[Any],
    candidate: StopSignCandidate,
    minimum_downstream_m: float,
) -> Optional[Tuple[int, float]]:
    """Validate one continuous route through the selected stop-sign lane."""
    target_waypoint = candidate.route_waypoint
    target_location = (
        _waypoint_location(target_waypoint)
        if target_waypoint is not None
        else candidate.route_location
    )
    if target_location is None or len(waypoints) <= 1:
        return None

    target_road = getattr(target_waypoint, "road_id", candidate.road_id)
    target_lane = getattr(target_waypoint, "lane_id", None)
    matches: List[Tuple[float, int]] = []
    for index, waypoint in enumerate(waypoints):
        location = _waypoint_location(waypoint)
        if location is None:
            continue
        same_lane = (
            target_lane is None
            or (
                getattr(waypoint, "road_id", None) == target_road
                and getattr(waypoint, "lane_id", None) == target_lane
            )
        )
        if same_lane:
            matches.append((_horizontal_distance(location, target_location), index))
    if not matches:
        return None
    target_distance, target_index = min(matches)
    if target_distance > 5.0:
        return None

    # Detect the exact failure reproduced in the original route: immediately
    # after the stop, the stitched route moved 3.5 m sideways and then backward.
    window_start = max(0, target_index - 5)
    window_end = min(len(waypoints), target_index + 9)
    for first, second in zip(
        waypoints[window_start:window_end],
        waypoints[window_start + 1:window_end],
    ):
        first_location = _waypoint_location(first)
        second_location = _waypoint_location(second)
        rotation = getattr(getattr(first, "transform", None), "rotation", None)
        if first_location is None or second_location is None or rotation is None:
            continue
        yaw = math.radians(float(getattr(rotation, "yaw", 0.0)))
        dx = float(second_location.x) - float(first_location.x)
        dy = float(second_location.y) - float(first_location.y)
        longitudinal = dx * math.cos(yaw) + dy * math.sin(yaw)
        lateral = -dx * math.sin(yaw) + dy * math.cos(yaw)
        if longitudinal < -0.25:
            return None
        if longitudinal < 0.25 and abs(lateral) > 0.5:
            return None

    locations = [
        location
        for waypoint in waypoints[target_index:]
        if (location := _waypoint_location(waypoint)) is not None
    ]
    downstream_m = sum(
        _distance(first, second)
        for first, second in zip(locations, locations[1:])
    )
    if downstream_m < minimum_downstream_m:
        return None
    return target_index, downstream_m


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
    target_waypoint = candidate.route_waypoint or world_map.get_waypoint(
        candidate.route_location, project_to_road=True
    )
    if target_waypoint is None:
        raise RuntimeError(f"Could not project {candidate.label} onto a driving lane")
    target = target_waypoint.transform.location

    # Prefer destinations reached by following the selected stop lane forward.
    # Each candidate is planned once from start to destination; independently
    # planned legs are never concatenated at the ambiguous stop trigger.
    desired = max(10.0, float(min_distance_m))
    requested_distances: List[float] = []
    for value in (desired, desired * 0.8, desired * 0.6, desired * 0.4, 50.0, 25.0):
        value = max(10.0, value)
        if not any(
            math.isclose(value, seen, abs_tol=0.1)
            for seen in requested_distances
        ):
            requested_distances.append(value)
    options: List[Tuple[Any, float, str]] = []
    for requested in requested_distances:
        try:
            downstream = list(target_waypoint.next(requested) or [])
        except Exception:
            downstream = []
        for waypoint in downstream:
            location = _waypoint_location(waypoint)
            if location is not None:
                options.append((location, max(10.0, requested * 0.8), "forward lane"))

    spawn_points = sorted(
        world_map.get_spawn_points(),
        key=lambda transform: _horizontal_distance(target, transform.location),
        reverse=True,
    )
    options.extend(
        (transform.location, 10.0, "map spawn")
        for transform in spawn_points
        if _horizontal_distance(target, transform.location) >= 10.0
    )

    deduplicated: List[Tuple[Any, float, str]] = []
    seen_destinations: set[Tuple[float, float]] = set()
    for location, required_downstream, source in options:
        key = (round(float(location.x), 1), round(float(location.y), 1))
        if key not in seen_destinations:
            deduplicated.append((location, required_downstream, source))
            seen_destinations.add(key)

    last_error: Optional[BaseException] = None
    for destination, required_downstream, source in deduplicated:
        try:
            waypoints = list(
                pcla_module.location_to_waypoint(client, start, destination)
            )
            quality = _route_through_stop_quality(
                waypoints, candidate, required_downstream
            )
            if quality is None:
                continue
            target_index, downstream_m = quality
            output_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                pcla_module.route_maker(waypoints, str(output_path))
            except TypeError:
                pcla_module.route_maker(waypoints, savePath=str(output_path))
            if not output_path.exists():
                raise RuntimeError(f"PCLA route_maker did not create {output_path}")
            print(
                f"Continuous PCLA route via {candidate.label}: "
                f"{len(waypoints)} waypoints, stop waypoint {target_index}, "
                f"{downstream_m:.1f} m downstream ({source})."
            )
            return output_path
        except Exception as exc:
            last_error = exc
    raise RuntimeError(
        f"Could not generate a continuous PCLA route through {candidate.label}"
    ) from last_error
