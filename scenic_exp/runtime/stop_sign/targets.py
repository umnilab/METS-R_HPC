"""Stop-sign data, physical-mesh discovery, and upstream approach selection.

Discovery pairs lane-trigger actors with upright sign meshes, then returns a
stable coordinate-sorted list. The runner chooses from those candidates; no
controller, route file, sensor hook, or simulator connection is created here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .geometry import _distance, _horizontal_distance


@dataclass(frozen=True)
class StopSignPatchSettings:
    """Patch parameters; argparse namespaces with these fields also work."""

    attack_max_distance_m: float = 90.0
    attack_min_red: int = 90
    attack_red_dominance: float = 1.25
    attack_roi_scale: float = 1.7
    attack_min_patch_pixels: int = 25


@dataclass(frozen=True)
class StopSignCandidate:
    index: int
    actor_id: str
    route_location: Any
    visual_location: Any
    visual_extent_m: float
    source: str
    approach_transform: Any
    approach_distance_m: float
    same_road_distance_m: float
    road_id: Optional[int]
    route_is_junction: bool
    route_waypoint: Any = None
    visual_id: str = ""
    visual_name: str = ""

    @property
    def location_text(self) -> str:
        loc = self.route_location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def initial_location_text(self) -> str:
        if self.approach_transform is None:
            return "unavailable"
        loc = self.approach_transform.location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def visual_location_text(self) -> str:
        loc = self.visual_location
        return f"({float(loc.x):.1f}, {float(loc.y):.1f}, {float(loc.z):.1f})"

    @property
    def label(self) -> str:
        road = "?" if self.road_id is None else str(self.road_id)
        return (
            f"stop[{self.index}] road={road} id={self.actor_id} "
            f"{self.location_text} start={self.approach_distance_m:.1f}m upstream"
        )


def _environment_location(obj: Any) -> Optional[Any]:
    location = getattr(getattr(obj, "bounding_box", None), "location", None)
    if location is not None:
        return location
    return getattr(getattr(obj, "transform", None), "location", None)


def _match_stop_actors_to_signs(
    actor_routes: Mapping[str, Any],
    upright_stops: Sequence[Any],
    maximum_distance_m: float = 15.0,
) -> Dict[str, Tuple[float, str, Any, Any]]:
    """Return a deterministic maximum-cardinality, minimum-cost assignment."""
    visuals = sorted(
        [
            (
                str(getattr(obj, "id", id(obj))),
                obj,
                _environment_location(obj),
            )
            for obj in upright_stops
            if _environment_location(obj) is not None
        ],
        key=lambda row: row[0],
    )
    # mask -> (total XY distance, assignment records)
    states: Dict[int, Tuple[float, Tuple[Tuple[str, str, Any, Any, float], ...]]] = {
        0: (0.0, ())
    }
    for actor_id in sorted(actor_routes):
        route = actor_routes[actor_id]
        updated = dict(states)
        for mask, (cost, assignments) in states.items():
            for index, (visual_id, obj, visual) in enumerate(visuals):
                bit = 1 << index
                if mask & bit:
                    continue
                distance = _horizontal_distance(route, visual)
                if distance > maximum_distance_m:
                    continue
                new_mask = mask | bit
                new_assignments = assignments + (
                    (actor_id, visual_id, obj, visual, distance),
                )
                candidate_value = (cost + distance, new_assignments)
                current = updated.get(new_mask)
                candidate_key = (
                    candidate_value[0],
                    tuple((row[0], row[1]) for row in candidate_value[1]),
                )
                current_key = (
                    math.inf,
                    (),
                ) if current is None else (
                    current[0],
                    tuple((row[0], row[1]) for row in current[1]),
                )
                if candidate_key < current_key:
                    updated[new_mask] = candidate_value
        states = updated

    _, assignments = min(
        states.values(),
        key=lambda value: (
            -len(value[1]),
            value[0],
            tuple((row[0], row[1]) for row in value[1]),
        ),
    )
    return {
        actor_id: (distance, visual_id, obj, visual)
        for actor_id, visual_id, obj, visual, distance in assignments
    }


def _stop_trigger_location(actor: Any, carla_module: Any) -> Any:
    """Return the world-space center of a stop actor's lane trigger."""
    actor_location = actor.get_location()
    volume_location = getattr(getattr(actor, "trigger_volume", None), "location", None)
    transform = getattr(actor, "get_transform", lambda: None)()
    if volume_location is None or transform is None:
        return actor_location
    route = carla_module.Location(
        x=float(volume_location.x), y=float(volume_location.y), z=float(volume_location.z),
    )
    try:
        transformed = transform.transform(route)
        return route if transformed is None else transformed
    except Exception:
        return actor_location


def _find_stop_approach(
    world_map: Any,
    actor: Any,
    route: Any,
    requested_distance_m: float,
) -> Tuple[Any, float, float, Optional[int], bool, Any]:
    """Find an upstream start outside junctions, preferring the stop sign's road."""
    waypoints: List[Any] = []
    affected = getattr(actor, "get_affected_lane_waypoints", None)
    if callable(affected):
        try:
            waypoints.extend(list(affected() or []))
        except Exception:
            pass
    try:
        projected = world_map.get_waypoint(route, project_to_road=True)
    except Exception:
        projected = None
    if projected is not None:
        waypoints.append(projected)
    unique: Dict[Tuple[Any, ...], Any] = {}
    for waypoint in waypoints:
        transform = getattr(waypoint, "transform", None)
        location = getattr(transform, "location", None)
        if location is None:
            continue
        key = (
            getattr(waypoint, "road_id", None),
            getattr(waypoint, "section_id", None),
            getattr(waypoint, "lane_id", None),
            round(float(location.x), 2),
            round(float(location.y), 2),
        )
        unique[key] = waypoint
    waypoints = list(unique.values())
    if not waypoints:
        return None, 0.0, 0.0, None, False, None
    route_waypoint = min(
        waypoints,
        key=lambda waypoint: _distance(waypoint.transform.location, route),
    )
    road_id = getattr(route_waypoint, "road_id", None)
    is_junction = bool(getattr(route_waypoint, "is_junction", False))
    distances: List[float] = []
    distance = float(requested_distance_m)
    while distance >= 5.0:
        distances.append(distance)
        distance -= 5.0
    if not distances or not math.isclose(distances[-1], 5.0):
        distances.append(5.0)

    # Rank by: outside junction, same road, requested lead-in, same-road
    # distance, and actual lead-in. Keep the transforms only as result payload.
    options: List[Tuple[bool, bool, float, float, float, Any, Any]] = []
    same_road_distance = 0.0
    for waypoint in waypoints:
        for requested in distances:
            try:
                previous = list(waypoint.previous(requested) or [])
            except Exception:
                previous = []
            for upstream in previous:
                transform = getattr(upstream, "transform", None)
                location = getattr(transform, "location", None)
                if location is None:
                    continue
                actual = _distance(route, location)
                same_road = (
                    actual if getattr(upstream, "road_id", None) == road_id else 0.0
                )
                same_road_distance = max(same_road_distance, same_road)
                outside_junction = not bool(getattr(upstream, "is_junction", False))
                options.append((
                    outside_junction,
                    outside_junction and same_road > 0.0,
                    requested,
                    same_road,
                    actual,
                    transform,
                    waypoint,
                ))
    if not options:
        return None, 0.0, same_road_distance, road_id, is_junction, route_waypoint
    # Starting midway through a junction can make PCLA's route planner lose
    # the ongoing turn command. Prefer a nonjunction start on this road even
    # when reaching the requested lead-in would place the ego inside a turn.
    # Keep other upstream candidates as a fallback for short approach roads.
    _, _, _, _, actual, transform, route_waypoint = max(
        options,
        key=lambda option: option[:5],
    )
    return transform, actual, same_road_distance, road_id, is_junction, route_waypoint


def discover_stop_signs(
    world: Any,
    carla_module: Any,
    approach_distance_m: float = 35.0,
) -> List[StopSignCandidate]:
    try:
        actors = list(world.get_actors().filter("traffic.stop*"))
    except Exception:
        actors = []
    unique = {
        str(getattr(actor, "id", id(actor))): actor
        for actor in actors
        if actor is not None
    }

    sign_objects: List[Any] = []
    label = getattr(getattr(carla_module, "CityObjectLabel", None), "TrafficSigns", None)
    if label is not None and callable(getattr(world, "get_environment_objects", None)):
        try:
            sign_objects = list(world.get_environment_objects(label))
        except Exception:
            pass
    upright_stops = []
    for obj in sign_objects:
        name = str(getattr(obj, "name", "")).lower()
        if "stop" not in name or any(
            marker in name for marker in ("stencil", "stopline", "stop_line", "roadmark", "road_mark")
        ):
            continue
        if _environment_location(obj) is None:
            continue
        # Reject flat STOP road paint; upright sign meshes have vertical extent.
        extent = getattr(getattr(obj, "bounding_box", None), "extent", None)
        if abs(float(getattr(extent, "z", 0.0) or 0.0)) >= 0.25:
            upright_stops.append(obj)

    world_map = world.get_map()
    actor_routes = {
        actor_id: _stop_trigger_location(actor, carla_module)
        for actor_id, actor in unique.items()
    }
    matches = _match_stop_actors_to_signs(actor_routes, upright_stops)
    # Trigger actors require an actual sign mesh. Maps without trigger actors
    # can use upright environment signs directly; both share approach selection.
    targets = unique.items() if unique else ((None, obj) for obj in upright_stops)
    candidates = []
    for actor_id, actor in targets:
        if unique:
            match = matches.get(actor_id)
            if match is None:
                continue
            distance, visual_id, obj, visual = match
            route = _stop_trigger_location(actor, carla_module)
            visual_name = str(getattr(obj, "name", ""))
            source = f"traffic.stop + upright {visual_name} ({distance:.1f} m XY)"
        else:
            obj = actor
            route = visual = _environment_location(obj)
            if route is None:
                continue
            actor_id = visual_id = str(getattr(obj, "id", len(candidates)))
            visual_name = str(getattr(obj, "name", ""))
            source = f"upright CARLA environment stop sign {visual_name}"
        (
            approach,
            approach_distance,
            same_road_distance,
            road_id,
            is_junction,
            route_waypoint,
        ) = _find_stop_approach(world_map, actor, route, approach_distance_m)
        extent = getattr(getattr(obj, "bounding_box", None), "extent", None)
        extents = [abs(float(getattr(extent, axis, 0.0) or 0.0)) for axis in ("x", "y", "z")]
        candidates.append(StopSignCandidate(
            index=0,
            actor_id=actor_id,
            route_location=route,
            visual_location=visual,
            visual_extent_m=max((value for value in extents if 0.05 <= value <= 5.0), default=0.65),
            source=source,
            approach_transform=approach,
            approach_distance_m=approach_distance,
            same_road_distance_m=same_road_distance,
            road_id=road_id,
            route_is_junction=is_junction,
            route_waypoint=route_waypoint,
            visual_id=visual_id,
            visual_name=visual_name,
        ))
    candidates.sort(key=lambda candidate: (
        float(candidate.route_location.x), float(candidate.route_location.y), candidate.actor_id,
    ))
    return [replace(candidate, index=index) for index, candidate in enumerate(candidates)]


def select_candidates(
    candidates: Sequence[StopSignCandidate],
    indices_text: str,
    offset: int,
    limit: int,
) -> List[StopSignCandidate]:
    if not candidates:
        raise RuntimeError("The CARLA map exposed no modeled stop signs")
    if indices_text.strip():
        try:
            indices = [
                int(value.strip())
                for value in indices_text.split(",")
                if value.strip()
            ]
        except ValueError as exc:
            raise ValueError("--stop-sign-indices must be comma-separated integers") from exc
        invalid = [index for index in indices if index < 0 or index >= len(candidates)]
        if not indices or invalid:
            raise ValueError(
                f"invalid stop-sign indices {invalid or indices}; valid range is "
                f"[0, {len(candidates) - 1}]"
            )
        selected = [candidates[index] for index in indices]
    else:
        ranked = sorted(
            candidates,
            key=lambda candidate: (
                candidate.approach_transform is None,
                candidate.route_is_junction,
                -candidate.same_road_distance_m,
                -candidate.approach_distance_m,
                candidate.index,
            ),
        )
        selected = list(ranked[offset : offset + limit])
    if not selected:
        raise ValueError("the candidate offset/limit selected no stop signs")
    missing = [
        candidate.index
        for candidate in selected
        if candidate.approach_transform is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not find an upstream driving waypoint for stop signs {missing}"
        )
    return selected
