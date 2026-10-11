"""Mapped-lane safety encounter placement and simple hazard vehicle controls."""
from __future__ import annotations

import math
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence


def _line(points: Iterable[Sequence[float]]) -> Any:
    """Build a planar lane path without importing Shapely until geometry is used."""
    from shapely.geometry import LineString

    return LineString([(float(point[0]), float(point[1])) for point in points])


def _pose(line: Any, distance: float) -> tuple[float, float, float]:
    """Return a clamped path position and heading in Scenic's coordinate convention."""
    distance = max(0.0, min(line.length, distance))
    point = line.interpolate(distance)
    before = line.interpolate(max(0.0, distance - 0.1))
    after = line.interpolate(min(line.length, distance + 0.1))
    heading = math.atan2(-(after.x - before.x), after.y - before.y)
    return point.x, point.y, heading


def safety_sites(
    network: Any,
    mapped_keys: Any,
    case: str,
    config: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Find mapped lane geometry suitable for a safety encounter.

    Interception crosses a lane; cut-in joins ahead and merge joins from behind.
    All initial centers lie on mapped lanes, without town-specific coordinates.
    """
    if case == "none":
        return ()
    from shapely.geometry import Point

    placement = config["placement"]
    gap = placement["gap_m"]
    changes_lane = case in ("cut_in", "merge", "vehicle_interception")
    plans = []
    for lane in network.laneSections:
        if f"{lane.road.id}_{lane.lane.id}" not in mapped_keys:
            continue
        if placement["road_id"] is not None and str(lane.road.id) != str(placement["road_id"]):
            continue
        if placement["lane_id"] is not None and str(lane.openDriveID) != str(placement["lane_id"]):
            continue
        ego_line = _line(lane.centerline.points)
        if ego_line.length < 2 * gap + 35:
            continue
        ego_distance = gap + 10
        ego_pose = _pose(ego_line, ego_distance)
        ego_path = tuple(ego_line.coords)
        adjacent = [
            other for other in lane.adjacentLanes
            if other.isForward == lane.isForward
            and f"{other.road.id}_{other.lane.id}" in mapped_keys
        ]
        if (changes_lane or case == "traffic_blockage") and not adjacent:
            continue
        hazard_lane = adjacent[0] if changes_lane else lane
        hazard_line = _line(hazard_lane.centerline.points)
        anchor = hazard_line.project(Point(ego_pose[:2]))
        offset = -gap * 0.5 if case == "merge" else gap
        hazard_distance = anchor + offset
        if (
            hazard_distance < 8
            or hazard_distance + config["hazard"]["merge_distance_m"] > hazard_line.length - 8
        ):
            continue
        hazard_pose = _pose(hazard_line, hazard_distance)
        hazards = [{
            "pose": hazard_pose,
            "path": tuple(hazard_line.coords),
            "target_path": ego_path,
        }]
        if case == "traffic_blockage":
            adjacent_line = _line(adjacent[0].centerline.points)
            adjacent_distance = adjacent_line.project(Point(hazard_pose[:2]))
            hazards.append({
                "pose": _pose(adjacent_line, adjacent_distance),
                "path": tuple(adjacent_line.coords),
                "target_path": ego_path,
            })
        plans.append({
            "id": f"{lane.road.id}:{lane.openDriveID}:{len(plans)}",
            "ego_pose": ego_pose,
            "ego_path": ego_path,
            "hazards": hazards,
        })
    if not plans:
        raise ValueError(
            f"No mapped lane geometry supports {case}; "
            "choose another town/road or reduce placement.gap_m"
        )
    index = placement["site_index"]
    if index is not None:
        if index >= len(plans):
            raise ValueError(f"placement.site_index={index} exceeds the {len(plans)} available sites")
        plans = [plans[index]]
    return tuple(plans)


def merge_path(
    position: Any,
    heading: float,
    target_points: Iterable[Sequence[float]],
    distance: float,
    cross_lane: bool = False,
) -> list[tuple[float, float]]:
    """Build a cubic lane-change path, optionally ending beyond the target center."""
    from shapely.geometry import Point

    target_line = _line(target_points)
    projected_distance = target_line.project(Point(float(position.x), float(position.y)))
    end_distance = min(target_line.length, projected_distance + distance)
    end_x, end_y, end_heading = _pose(target_line, end_distance)
    start = (float(position.x), float(position.y))
    end = (end_x, end_y)
    if cross_lane:
        # End one meter beyond the lane center, opposite the starting side.
        normal = (math.cos(end_heading), math.sin(end_heading))
        side = (start[0] - end[0]) * normal[0] + (start[1] - end[1]) * normal[1]
        direction = -1 if side > 0 else 1
        end = (end[0] + direction * normal[0], end[1] + direction * normal[1])
        end_heading = math.atan2(-(end[0] - start[0]), end[1] - start[1])
    start_control = (
        start[0] - math.sin(heading) * distance / 3,
        start[1] + math.cos(heading) * distance / 3,
    )
    end_control = (
        end[0] + math.sin(end_heading) * distance / 3,
        end[1] - math.cos(end_heading) * distance / 3,
    )
    path = []
    for sample in range(21):
        fraction = sample / 20
        path.append(tuple(
            (1 - fraction) ** 3 * start[axis]
            + 3 * (1 - fraction) ** 2 * fraction * start_control[axis]
            + 3 * (1 - fraction) * fraction * fraction * end_control[axis]
            + fraction ** 3 * end[axis]
            for axis in (0, 1)
        ))
    if not cross_lane:
        for progress in range(math.ceil(end_distance) + 1, math.floor(target_line.length) + 1, 2):
            point = target_line.interpolate(progress)
            path.append((point.x, point.y))
    return path


def path_control(
    obj: Any,
    points: Iterable[Sequence[float]],
    speed_mps: float,
    lookahead_m: float,
) -> SimpleNamespace:
    """Compute hazard steering and speed control independently of the ego policy."""
    from shapely.geometry import Point

    line = _line(points)
    progress = line.project(Point(float(obj.position.x), float(obj.position.y)))
    target = line.interpolate(min(line.length, progress + lookahead_m))
    desired_heading = math.atan2(-(target.x - obj.position.x), target.y - obj.position.y)
    heading_error = math.remainder(desired_heading - float(obj.heading), 2 * math.pi)
    steer = max(-1.0, min(1.0, -1.5 * heading_error))
    if line.length - progress < 1:
        speed_mps = 0
    speed_error = speed_mps - float(obj.speed or 0)
    return SimpleNamespace(
        brake=min(1.0, max(0.0, -speed_error * 0.25)),
        throttle=min(0.75, max(0.0, speed_error * 0.3)),
        steer=steer,
    )
