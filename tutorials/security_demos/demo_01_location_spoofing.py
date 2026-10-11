"""Paired location-spoofing experiment for an autonomous ride-hailing fleet.

This simulator-free demo is inspired by Jinghan Yang, Andrew Estornell, and
Yevgeniy Vorobeychik, "Location Spoofing Attacks on Autonomous Fleets,"
VehicleSec 2023, https://doi.org/10.14722/vehiclesec.2023.23031.

The paper studies fixed and time-varying spoofers with delay and service-
failure objectives.  This teaching replication preserves those threat-model
ideas, a simultaneous-device budget, separate true/perceived locations, and
paired baseline/attack demand.  It does *not* reproduce the paper's NYC road
graph, ILP, greedy proofs, or deep reinforcement learner.  Instead it uses a
small deterministic Manhattan grid, nearest-perceived-taxi dispatch, and
transparent greedy attackers so it can run offline with the Python standard
library.  It models effects on a controlled synthetic system; it does not emit
or interfere with GPS/radio signals.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable, Mapping, Sequence


_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tutorials.security_demos.dashboard import serve_dashboard, write_dashboard


GRID_SIDE = 9
ZONE_SIDE = 3
STEP_MILES = 0.5
ATTACK_RECOVERY_FRACTION = 0.72
PAPER_URL = "https://doi.org/10.14722/vehiclesec.2023.23031"
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "output" / "security_demos" / "location_spoofing"
Position = tuple[int, int]

ZONE_NAMES = (
    "Southwest",
    "South",
    "Southeast",
    "West",
    "Central",
    "East",
    "Northwest",
    "North",
    "Northeast",
)
PICKUP_ZONE_WEIGHTS = (1.0, 1.4, 0.9, 1.4, 3.2, 1.5, 0.8, 1.3, 0.9)
DESTINATION_HUBS: tuple[Position, ...] = (
    (1, 1),
    (4, 1),
    (7, 1),
    (1, 4),
    (4, 4),
    (7, 4),
    (1, 7),
    (4, 7),
    (7, 7),
)
DESTINATION_WEIGHTS = (1.0, 1.5, 0.8, 1.2, 3.5, 1.3, 0.8, 1.4, 0.9)


@dataclass(frozen=True)
class RequestSpec:
    request_id: int
    release_minute: int
    pickup: Position
    dropoff: Position
    pickup_zone: int


@dataclass
class RequestState:
    spec: RequestSpec
    status: str = "scheduled"
    taxi_id: int | None = None
    assigned_minute: int | None = None
    pickup_minute: int | None = None
    dropoff_minute: int | None = None
    wrong_dropoff: Position | None = None


@dataclass
class TaxiState:
    taxi_id: int
    true_position: Position
    status: str = "idle"
    request_id: int | None = None
    busy_minutes: int = 0
    occupied_minutes: int = 0
    empty_miles: float = 0.0
    loaded_miles: float = 0.0


@dataclass(frozen=True)
class StaticDevice:
    location: Position
    spoof_target: Position
    evidence_score: float
    supporting_routes: int


@dataclass(frozen=True)
class RunArtifacts:
    output_dir: Path
    results_json: Path
    summary_csv: Path
    zone_csv: Path
    dashboard_html: Path


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run paired baseline, static-static, and dynamic location-spoofing "
            "ride-hailing experiments on a deterministic synthetic grid."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--seed", type=int, default=2023)
    parser.add_argument(
        "--duration",
        type=_positive_int,
        default=180,
        help="Experiment horizon in one-minute simulation ticks.",
    )
    parser.add_argument("--fleet", type=_positive_int, default=30, help="Taxi count.")
    parser.add_argument(
        "--requests",
        type=_positive_int,
        default=150,
        help="Total requests shared by all paired scenarios.",
    )
    parser.add_argument(
        "--spoofer-budget",
        type=_nonnegative_int,
        default=3,
        help="Maximum simultaneously active spoofing devices.",
    )
    parser.add_argument(
        "--objective",
        choices=("delay", "failure"),
        default="failure",
        help="Greedy attacker objective from the paper's two objective classes.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Artifact directory; relative paths resolve from the repository root.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve the standalone dashboard until Ctrl-C after the run.",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the dashboard in a browser (implies --serve).",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard server host.")
    parser.add_argument("--port", type=_nonnegative_int, default=0, help="Dashboard port; zero selects a free port.")
    return parser.parse_args(argv)


def _position_dict(position: Position | None) -> dict[str, int] | None:
    if position is None:
        return None
    return {"x": int(position[0]), "y": int(position[1])}


def _position_label(position: Position) -> str:
    return f"({position[0]},{position[1]})"


def _manhattan(left: Position, right: Position) -> int:
    return abs(left[0] - right[0]) + abs(left[1] - right[1])


def _route(start: Position, destination: Position) -> list[Position]:
    """Return the deterministic x-first Manhattan path, including both ends."""

    x, y = start
    path = [(x, y)]
    while x != destination[0]:
        x += 1 if destination[0] > x else -1
        path.append((x, y))
    while y != destination[1]:
        y += 1 if destination[1] > y else -1
        path.append((x, y))
    return path


def _route_action(perceived: Position, destination: Position) -> Position:
    if perceived[0] != destination[0]:
        return (1 if destination[0] > perceived[0] else -1, 0)
    if perceived[1] != destination[1]:
        return (0, 1 if destination[1] > perceived[1] else -1)
    return (0, 0)


def _apply_action(true_position: Position, action: Position) -> Position:
    candidate = (true_position[0] + action[0], true_position[1] + action[1])
    if 0 <= candidate[0] < GRID_SIDE and 0 <= candidate[1] < GRID_SIDE:
        return candidate
    # A physical controller cannot execute a turn beyond the modeled network.
    return true_position


def _move_from_perception(
    true_position: Position, perceived_position: Position, destination: Position
) -> Position:
    return _apply_action(true_position, _route_action(perceived_position, destination))


def _zone_index(position: Position) -> int:
    return min(position[1] // ZONE_SIDE, 2) * 3 + min(position[0] // ZONE_SIDE, 2)


def _zone_cell(rng: random.Random, zone_index: int) -> Position:
    x_band = zone_index % 3
    y_band = zone_index // 3
    return (
        rng.randrange(x_band * ZONE_SIDE, (x_band + 1) * ZONE_SIDE),
        rng.randrange(y_band * ZONE_SIDE, (y_band + 1) * ZONE_SIDE),
    )


def _generate_requests(seed: int, duration: int, count: int) -> list[RequestSpec]:
    rng = random.Random(seed)
    latest_release = max(1, int(duration * 0.68))
    requests: list[RequestSpec] = []
    for index in range(count):
        anchor = int(index * latest_release / max(count, 1))
        release = min(latest_release - 1, max(0, anchor + rng.randint(-2, 2)))
        pickup_zone = rng.choices(range(9), weights=PICKUP_ZONE_WEIGHTS, k=1)[0]
        pickup = _zone_cell(rng, pickup_zone)
        dropoff = rng.choices(DESTINATION_HUBS, weights=DESTINATION_WEIGHTS, k=1)[0]
        if dropoff == pickup:
            alternatives = [hub for hub in DESTINATION_HUBS if hub != pickup]
            dropoff = alternatives[(index + seed) % len(alternatives)]
        requests.append(
            RequestSpec(
                request_id=index + 1,
                release_minute=release,
                pickup=pickup,
                dropoff=dropoff,
                pickup_zone=pickup_zone,
            )
        )
    return sorted(requests, key=lambda request: (request.release_minute, request.request_id))


def _generate_fleet_positions(seed: int, fleet_size: int) -> list[Position]:
    rng = random.Random(seed ^ 0x5A17)
    cells = [(x, y) for y in range(GRID_SIDE) for x in range(GRID_SIDE)]
    positions: list[Position] = []
    while len(positions) < fleet_size:
        cycle = list(cells)
        rng.shuffle(cycle)
        positions.extend(cycle)
    return positions[:fleet_size]


def _route_contexts(
    requests: Iterable[RequestSpec],
) -> dict[Position, Counter[Position]]:
    contexts: dict[Position, Counter[Position]] = defaultdict(Counter)
    for request in requests:
        passenger_path = _route(request.pickup, request.dropoff)
        for location in passenger_path[1:-1]:
            contexts[location][request.dropoff] += 1
    return contexts


def _delay_gain(
    true_position: Position, spoof_target: Position, destination: Position
) -> float:
    if spoof_target == destination:
        return -1000.0
    moved = _move_from_perception(true_position, spoof_target, destination)
    if moved == true_position:
        return 0.35
    return float(
        _manhattan(moved, destination) - _manhattan(true_position, destination)
    )


def _best_delay_target(
    location: Position,
    destination_weights: Mapping[Position, int],
    forbidden_targets: set[Position] | None = None,
) -> tuple[Position, float]:
    forbidden = forbidden_targets or set()
    best_target = location
    best_score = -math.inf
    for target_y in range(GRID_SIDE):
        for target_x in range(GRID_SIDE):
            target = (target_x, target_y)
            if target == location or target in forbidden:
                continue
            score = sum(
                weight * _delay_gain(location, target, destination)
                for destination, weight in destination_weights.items()
            )
            if score > best_score or (score == best_score and target < best_target):
                best_target = target
                best_score = score
    if not math.isfinite(best_score):
        return location, 0.0
    return best_target, best_score


def _build_static_devices(
    requests: Sequence[RequestSpec], objective: str, budget: int
) -> list[StaticDevice]:
    if budget <= 0:
        return []
    contexts = _route_contexts(requests)
    candidates: list[StaticDevice] = []
    if objective == "failure":
        for location, destinations in contexts.items():
            destination, count = min(
                destinations.items(), key=lambda item: (-item[1], item[0])
            )
            candidates.append(
                StaticDevice(
                    location=location,
                    spoof_target=destination,
                    evidence_score=float(count),
                    supporting_routes=int(count),
                )
            )
    else:
        all_dropoffs = {request.dropoff for request in requests}
        for location, destinations in contexts.items():
            target, score = _best_delay_target(
                location, destinations, forbidden_targets=all_dropoffs
            )
            candidates.append(
                StaticDevice(
                    location=location,
                    spoof_target=target,
                    evidence_score=float(score),
                    supporting_routes=int(sum(destinations.values())),
                )
            )
    candidates.sort(
        key=lambda device: (
            -device.evidence_score,
            -device.supporting_routes,
            device.location,
            device.spoof_target,
        )
    )
    return candidates[: min(budget, len(candidates))]


def _request_target(taxi: TaxiState, requests: Mapping[int, RequestState]) -> Position | None:
    if taxi.request_id is None:
        return None
    request = requests[taxi.request_id]
    if taxi.status == "to_pickup":
        return request.spec.pickup
    if taxi.status == "occupied":
        return request.spec.dropoff
    return None


def _dynamic_devices(
    taxis: Sequence[TaxiState],
    requests: Mapping[int, RequestState],
    pending: Sequence[RequestState],
    objective: str,
    budget: int,
) -> tuple[dict[Position, Position], list[dict[str, Any]]]:
    """Choose up to ``budget`` adaptive device location/target pairs.

    This transparent greedy policy assumes an omniscient attacker can relocate
    once per minute.  The simplification is intentionally called out in the
    dashboard fidelity boundary and is not the paper's learned policy.
    """

    if budget <= 0:
        return {}, []

    candidates: list[tuple[float, Position, Position, str]] = []
    idle_taxis = [taxi for taxi in taxis if taxi.status == "idle"]

    for taxi in taxis:
        destination = _request_target(taxi, requests)
        if destination is None:
            continue
        if objective == "failure" and taxi.status == "occupied":
            if taxi.true_position != destination:
                score = 2000.0 + _manhattan(taxi.true_position, destination)
                candidates.append(
                    (
                        score,
                        taxi.true_position,
                        destination,
                        f"taxi {taxi.taxi_id} wrong-dropoff opportunity",
                    )
                )
            continue

        target, gain = _best_delay_target(
            taxi.true_position, {destination: 1}, forbidden_targets={destination}
        )
        role_weight = 800.0 if taxi.status == "occupied" else 600.0
        score = role_weight + 20.0 * gain + _manhattan(taxi.true_position, destination)
        candidates.append(
            (
                score,
                taxi.true_position,
                target,
                f"taxi {taxi.taxi_id} {taxi.status} route detour",
            )
        )

    # Perceived positions are also consumed by matching.  A far idle taxi that
    # appears at a pickup can displace the truly nearest taxi and add empty miles.
    for request in pending[: max(1, budget * 2)]:
        if not idle_taxis:
            break
        taxi = max(
            idle_taxis,
            key=lambda item: (
                _manhattan(item.true_position, request.spec.pickup),
                -item.taxi_id,
            ),
        )
        distance = _manhattan(taxi.true_position, request.spec.pickup)
        if distance:
            base = 1000.0 if objective == "delay" else 250.0
            candidates.append(
                (
                    base + distance,
                    taxi.true_position,
                    request.spec.pickup,
                    f"taxi {taxi.taxi_id} false-nearest match for request {request.spec.request_id}",
                )
            )

    candidates.sort(key=lambda item: (-item[0], item[1], item[2], item[3]))
    selected: dict[Position, Position] = {}
    details: list[dict[str, Any]] = []
    for score, location, target, reason in candidates:
        if location in selected:
            continue
        selected[location] = target
        details.append(
            {
                "location": _position_dict(location),
                "spoof_target": _position_dict(target),
                "score": round(score, 3),
                "reason": reason,
            }
        )
        if len(selected) >= budget:
            break
    return selected, details


def _release_requests(requests: Mapping[int, RequestState], minute: int) -> None:
    for request in requests.values():
        if request.status == "scheduled" and request.spec.release_minute <= minute:
            request.status = "pending"


def _pending_requests(requests: Mapping[int, RequestState]) -> list[RequestState]:
    return sorted(
        (request for request in requests.values() if request.status == "pending"),
        key=lambda request: (request.spec.release_minute, request.spec.request_id),
    )


def _assign_requests(
    taxis: Sequence[TaxiState],
    requests: Mapping[int, RequestState],
    perceived_positions: Mapping[int, Position],
    minute: int,
) -> tuple[int, list[str]]:
    available = [taxi for taxi in taxis if taxi.status == "idle"]
    spoofed_matches = 0
    events: list[str] = []
    for request in _pending_requests(requests):
        if not available:
            break
        selected = min(
            available,
            key=lambda taxi: (
                _manhattan(perceived_positions[taxi.taxi_id], request.spec.pickup),
                taxi.taxi_id,
            ),
        )
        true_best = min(
            available,
            key=lambda taxi: (
                _manhattan(taxi.true_position, request.spec.pickup),
                taxi.taxi_id,
            ),
        )
        request.status = "assigned"
        request.taxi_id = selected.taxi_id
        request.assigned_minute = minute
        selected.status = "to_pickup"
        selected.request_id = request.spec.request_id
        available.remove(selected)
        if selected.taxi_id != true_best.taxi_id:
            spoofed_matches += 1
            events.append(
                f"t={minute}: request {request.spec.request_id} matched taxi "
                f"{selected.taxi_id} by perceived location; true-nearest was taxi {true_best.taxi_id}"
            )
    return spoofed_matches, events


def _clear_taxi(taxi: TaxiState) -> None:
    taxi.status = "idle"
    taxi.request_id = None


def _advance_taxi(
    taxi: TaxiState,
    requests: Mapping[int, RequestState],
    perceived_position: Position,
    minute: int,
) -> tuple[str | None, str | None]:
    """Advance one taxi and return ``(outcome, event)`` when a trip ends."""

    if taxi.status == "idle" or taxi.request_id is None:
        return None, None

    taxi.busy_minutes += 1
    request = requests[taxi.request_id]
    if taxi.status == "to_pickup" and taxi.true_position == request.spec.pickup:
        request.status = "occupied"
        request.pickup_minute = minute
        taxi.status = "occupied"

    if taxi.status == "occupied":
        taxi.occupied_minutes += 1
        if perceived_position == request.spec.dropoff:
            request.dropoff_minute = minute
            if taxi.true_position == request.spec.dropoff:
                request.status = "completed"
                event = (
                    f"t={minute}: request {request.spec.request_id} completed correctly "
                    f"at {_position_label(taxi.true_position)}"
                )
                _clear_taxi(taxi)
                return "completed", event
            request.status = "wrong_dropoff"
            request.wrong_dropoff = taxi.true_position
            event = (
                f"t={minute}: request {request.spec.request_id} wrong dropoff at "
                f"{_position_label(taxi.true_position)}; perceived destination "
                f"was {_position_label(request.spec.dropoff)}"
            )
            _clear_taxi(taxi)
            return "wrong_dropoff", event

    destination = _request_target(taxi, requests)
    if destination is None:
        return None, None
    before = taxi.true_position
    taxi.true_position = _move_from_perception(before, perceived_position, destination)
    if taxi.true_position != before:
        if taxi.status == "to_pickup":
            taxi.empty_miles += STEP_MILES
        else:
            taxi.loaded_miles += STEP_MILES
    return None, None


def _outcome_record(request: RequestState, duration: int) -> dict[str, Any]:
    pickup_minute = request.pickup_minute
    wait = (
        pickup_minute - request.spec.release_minute
        if pickup_minute is not None
        else max(0, duration - request.spec.release_minute)
    )
    shortest_ride = _manhattan(request.spec.pickup, request.spec.dropoff)
    service_minutes = (
        request.dropoff_minute - pickup_minute
        if request.dropoff_minute is not None and pickup_minute is not None
        else None
    )
    correct_delay = (
        max(0, service_minutes - shortest_ride)
        if request.status == "completed" and service_minutes is not None
        else None
    )
    return {
        "request_id": request.spec.request_id,
        "release_minute": request.spec.release_minute,
        "pickup": _position_dict(request.spec.pickup),
        "dropoff": _position_dict(request.spec.dropoff),
        "pickup_zone": ZONE_NAMES[request.spec.pickup_zone],
        "status": request.status,
        "taxi_id": request.taxi_id,
        "assigned_minute": request.assigned_minute,
        "pickup_minute": pickup_minute,
        "dropoff_minute": request.dropoff_minute,
        "wrong_dropoff": _position_dict(request.wrong_dropoff),
        "passenger_wait_min": wait,
        "shortest_ride_min": shortest_ride,
        "service_minutes": service_minutes,
        "correct_service_delay_min": correct_delay,
    }


def _zone_rows(outcomes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for zone_index, zone_name in enumerate(ZONE_NAMES):
        zone_outcomes = [row for row in outcomes if row["pickup_zone"] == zone_name]
        total = len(zone_outcomes)
        completed = sum(row["status"] == "completed" for row in zone_outcomes)
        wrong = sum(row["status"] == "wrong_dropoff" for row in zone_outcomes)
        unfinished = total - completed - wrong
        rows.append(
            {
                "zone_index": zone_index,
                "zone": zone_name,
                "pickup_requests": total,
                "completed_requests": completed,
                "wrong_dropoffs": wrong,
                "unfinished_requests": unfinished,
                "service_rate_pct": 100.0 * completed / total if total else 0.0,
            }
        )
    return rows


def _simulate_scenario(
    scenario_id: str,
    label: str,
    color: str,
    request_specs: Sequence[RequestSpec],
    fleet_positions: Sequence[Position],
    duration: int,
    objective: str,
    budget: int,
    static_devices: Sequence[StaticDevice],
) -> dict[str, Any]:
    requests = {
        spec.request_id: RequestState(spec=spec) for spec in request_specs
    }
    taxis = [
        TaxiState(taxi_id=index + 1, true_position=position)
        for index, position in enumerate(fleet_positions)
    ]
    static_map = {
        device.location: device.spoof_target for device in static_devices
    }
    series = {
        "waiting_passengers": [],
        "correct_completions": [],
        "wrong_dropoffs": [],
        "utilization": [],
        "spoof_exposures": [],
    }
    events: list[str] = []
    placement_history: list[dict[str, Any]] = []
    spoof_exposures = 0
    spoofed_matches = 0
    reconfigurations = 0
    correct_completions = 0
    wrong_dropoffs = 0
    prior_pairs: set[tuple[Position, Position]] = set()
    sample_stride = max(1, duration // 180)
    attack_end = max(1, int(duration * ATTACK_RECOVERY_FRACTION))

    if scenario_id == "static_static":
        for index, device in enumerate(static_devices, start=1):
            events.append(
                f"device {index}: fixed {_position_label(device.location)} -> "
                f"{_position_label(device.spoof_target)}; supporting routes={device.supporting_routes}"
            )

    for minute in range(duration):
        _release_requests(requests, minute)
        pending = _pending_requests(requests)

        device_map: dict[Position, Position] = {}
        placement_details: list[dict[str, Any]] = []
        attack_active = objective == "failure" or minute < attack_end
        if scenario_id == "static_static" and attack_active:
            device_map = dict(static_map)
        elif scenario_id == "dynamic" and attack_active:
            device_map, placement_details = _dynamic_devices(
                taxis, requests, pending, objective, budget
            )

        pairs = set(device_map.items())
        if scenario_id == "dynamic":
            reconfigurations += len(pairs - prior_pairs)
            if placement_details:
                placement_history.append(
                    {"minute": minute, "devices": placement_details}
                )
        prior_pairs = pairs

        perceived_positions: dict[int, Position] = {}
        for taxi in taxis:
            perceived = device_map.get(taxi.true_position, taxi.true_position)
            perceived_positions[taxi.taxi_id] = perceived
            if perceived != taxi.true_position:
                spoof_exposures += 1
                if len(events) < 18:
                    events.append(
                        f"t={minute}: taxi {taxi.taxi_id} true "
                        f"{_position_label(taxi.true_position)} perceived "
                        f"{_position_label(perceived)}"
                    )

        new_spoofed_matches, match_events = _assign_requests(
            taxis, requests, perceived_positions, minute
        )
        spoofed_matches += new_spoofed_matches
        for event in match_events:
            if len(events) < 18:
                events.append(event)

        for taxi in taxis:
            outcome, event = _advance_taxi(
                taxi, requests, perceived_positions[taxi.taxi_id], minute
            )
            if outcome == "completed":
                correct_completions += 1
            elif outcome == "wrong_dropoff":
                wrong_dropoffs += 1
            if event and (outcome == "wrong_dropoff" or len(events) < 18):
                if len(events) < 24:
                    events.append(event)

        if minute % sample_stride == 0 or minute == duration - 1:
            waiting = sum(
                request.status in {"pending", "assigned"}
                for request in requests.values()
            )
            busy_minutes = sum(taxi.busy_minutes for taxi in taxis)
            elapsed_capacity = len(taxis) * (minute + 1)
            series["waiting_passengers"].append([minute, waiting])
            series["correct_completions"].append([minute, correct_completions])
            series["wrong_dropoffs"].append([minute, wrong_dropoffs])
            series["utilization"].append(
                [minute, 100.0 * busy_minutes / elapsed_capacity]
            )
            series["spoof_exposures"].append([minute, spoof_exposures])

    outcomes = [_outcome_record(requests[key], duration) for key in sorted(requests)]
    completed = [row for row in outcomes if row["status"] == "completed"]
    wrong = [row for row in outcomes if row["status"] == "wrong_dropoff"]
    unfinished = [
        row for row in outcomes if row["status"] not in {"completed", "wrong_dropoff"}
    ]
    picked = [row for row in outcomes if row["pickup_minute"] is not None]
    all_waits = [float(row["passenger_wait_min"]) for row in outcomes]
    picked_waits = [float(row["passenger_wait_min"]) for row in picked]
    correct_delays = [
        float(row["correct_service_delay_min"])
        for row in completed
        if row["correct_service_delay_min"] is not None
    ]
    fleet_capacity = max(1, len(taxis) * duration)
    metrics = {
        "completed_requests": len(completed),
        "failed_requests": len(wrong) + len(unfinished),
        "wrong_dropoffs": len(wrong),
        "unfinished_requests": len(unfinished),
        "service_rate_pct": 100.0 * len(completed) / len(outcomes),
        "avg_passenger_wait_min": fmean(all_waits) if all_waits else 0.0,
        "avg_picked_wait_min": fmean(picked_waits) if picked_waits else 0.0,
        "avg_correct_service_delay_min": fmean(correct_delays) if correct_delays else 0.0,
        "empty_miles": sum(taxi.empty_miles for taxi in taxis),
        "loaded_miles": sum(taxi.loaded_miles for taxi in taxis),
        "utilization_pct": 100.0 * sum(taxi.busy_minutes for taxi in taxis) / fleet_capacity,
        "spoof_exposures": spoof_exposures,
        "spoofed_matches": spoofed_matches,
        "spoofer_reconfigurations": reconfigurations,
        "max_zone_service_loss_pp": 0.0,
    }
    attack_plan: dict[str, Any] = {
        "mode": scenario_id,
        "objective": objective,
        "budget": budget if scenario_id != "baseline" else 0,
        "attack_end_minute": (
            duration if objective == "failure" else attack_end
        ),
    }
    if scenario_id == "static_static":
        attack_plan["devices"] = [
            {
                "location": _position_dict(device.location),
                "spoof_target": _position_dict(device.spoof_target),
                "evidence_score": round(device.evidence_score, 3),
                "supporting_routes": device.supporting_routes,
            }
            for device in static_devices
        ]
    elif scenario_id == "dynamic":
        attack_plan["placement_history"] = placement_history

    return {
        "id": scenario_id,
        "label": label,
        "color": color,
        "metrics": metrics,
        "series": series,
        "events": events,
        "zone_service": _zone_rows(outcomes),
        "request_outcomes": outcomes,
        "attack_plan": attack_plan,
    }


def _add_zone_service_loss(scenarios: Sequence[dict[str, Any]]) -> None:
    baseline_rates = {
        row["zone_index"]: float(row["service_rate_pct"])
        for row in scenarios[0]["zone_service"]
    }
    for scenario in scenarios:
        losses = []
        for row in scenario["zone_service"]:
            baseline_rate = baseline_rates[row["zone_index"]]
            loss = baseline_rate - float(row["service_rate_pct"])
            row["baseline_service_rate_pct"] = baseline_rate
            row["service_loss_vs_baseline_pp"] = loss
            losses.append(loss)
        scenario["metrics"]["max_zone_service_loss_pp"] = max(
            [0.0, *losses]
        )
        scenario["series"]["zone_service_loss"] = [
            [row["zone_index"], row["service_loss_vs_baseline_pp"]]
            for row in scenario["zone_service"]
        ]
        worst = max(
            scenario["zone_service"],
            key=lambda row: (row["service_loss_vs_baseline_pp"], -row["zone_index"]),
        )
        scenario["events"].append(
            f"largest pickup-zone service loss: {worst['zone']} "
            f"({worst['service_loss_vs_baseline_pp']:.1f} percentage points vs baseline)"
        )


def _round_metrics(scenarios: Sequence[dict[str, Any]]) -> None:
    for scenario in scenarios:
        for key, value in list(scenario["metrics"].items()):
            if isinstance(value, float):
                scenario["metrics"][key] = round(value, 4)
        for row in scenario["zone_service"]:
            for key, value in list(row.items()):
                if isinstance(value, float):
                    row[key] = round(value, 4)


def _dashboard_payload(
    args: argparse.Namespace,
    requests: Sequence[RequestSpec],
    fleet_positions: Sequence[Position],
    static_devices: Sequence[StaticDevice],
    scenarios: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "title": "Location spoofing against a ride-hailing fleet",
        "eyebrow": "METS-R HPC · Security Demo 01",
        "subtitle": (
            "Paired true-versus-perceived location experiment: no attack, "
            "fixed device/fixed target spoofing, and adaptive dynamic spoofing."
        ),
        "paper": {
            "title": "Yang, Estornell & Vorobeychik · Location Spoofing Attacks on Autonomous Fleets",
            "venue": "VehicleSec 2023",
            "url": PAPER_URL,
            "doi": "10.14722/vehiclesec.2023.23031",
        },
        "citation": (
            "J. Yang, A. Estornell, and Y. Vorobeychik, Location Spoofing "
            "Attacks on Autonomous Fleets, VehicleSec 2023."
        ),
        "fidelity_boundary": (
            "Threat-model replication only: deterministic 9x9 Manhattan grid "
            "and transparent greedy attacks, not the paper's NYC data, ILP, "
            "SF-GREEDY optimality implementation, or DQN/MARL policy."
        ),
        "parameters": {
            "seed": args.seed,
            "duration (minutes)": args.duration,
            "fleet size": args.fleet,
            "request count": args.requests,
            "spoofer budget": args.spoofer_budget,
            "objective": args.objective,
            "grid": f"{GRID_SIDE}x{GRID_SIDE} intersections",
            "distance per edge": f"{STEP_MILES} mile",
            "delay attack recovery": f"last {(1.0 - ATTACK_RECOVERY_FRACTION) * 100:.0f}% of horizon",
        },
        "metric_cards": [
            {"key": "completed_requests", "label": "Correctly completed", "precision": 0},
            {"key": "failed_requests", "label": "Failed / unfinished", "precision": 0},
            {"key": "wrong_dropoffs", "label": "Wrong dropoffs", "precision": 0},
            {"key": "avg_passenger_wait_min", "label": "Passenger wait", "unit": "min"},
            {"key": "avg_correct_service_delay_min", "label": "Correct-service delay", "unit": "min"},
            {"key": "empty_miles", "label": "Fleet empty miles", "unit": "mi"},
            {"key": "utilization_pct", "label": "Fleet utilization", "unit": "%"},
            {"key": "max_zone_service_loss_pp", "label": "Worst zone loss", "unit": "pp"},
        ],
        "charts": [
            {
                "key": "waiting_passengers",
                "title": "Passengers waiting for pickup",
                "x_label": "simulation minute",
                "caption": "Pending plus assigned-but-not-picked-up requests.",
            },
            {
                "key": "correct_completions",
                "title": "Cumulative correct completions",
                "x_label": "simulation minute",
            },
            {
                "key": "wrong_dropoffs",
                "title": "Cumulative wrong dropoffs",
                "x_label": "simulation minute",
            },
            {
                "key": "spoof_exposures",
                "title": "Cumulative taxi spoof exposures",
                "x_label": "simulation minute",
            },
            {
                "key": "utilization",
                "title": "Cumulative fleet utilization",
                "x_label": "simulation minute",
                "caption": "Busy taxi-minutes divided by available fleet-minutes.",
            },
            {
                "key": "zone_service_loss",
                "title": "Pickup-zone service loss versus baseline",
                "x_label": "zone index 0–8 (southwest to northeast)",
                "caption": "Percentage-point loss in correct completion rate; exact zone labels are in results.json and zone_service_loss.csv.",
                "wide": True,
            },
        ],
        "notes": [
            "Every scenario reuses the exact request stream and initial taxi positions; no random choices occur after setup.",
            "Nearest-taxi matching and route actions consume perceived coordinates, while physical pickup and dropoff outcomes use true coordinates.",
            "A wrong dropoff occurs only when an occupied taxi physically differs from the destination but its spoofed perception equals that destination.",
            "Static-static devices and targets remain fixed. The dynamic teaching heuristic may relocate and retarget up to the budget once per minute with full state knowledge; this is stronger and simpler than modeling device motion or reproducing the paper's learned policy.",
            "For the delay objective, spoofing stops before the final recovery window and spoof targets equal to an occupied trip's destination are excluded, emphasizing delay among correctly completed trips.",
            "Passenger wait averages include horizon-censored wait for requests never picked up; avg_picked_wait_min in results.json reports picked-up requests only.",
            "Use only with synthetic or controlled simulation systems. This program does not transmit, generate, or interfere with real navigation signals.",
        ],
        "artifacts": {
            "results": "results.json",
            "scenario_metrics": "scenario_metrics.csv",
            "zone_service_loss": "zone_service_loss.csv",
            "dashboard": "dashboard.html",
        },
        "input_digest": {
            "request_release_checksum": sum(
                request.request_id * (request.release_minute + 1) for request in requests
            ),
            "initial_fleet_positions": [_position_dict(position) for position in fleet_positions],
            "static_device_count": len(static_devices),
        },
        "scenarios": list(scenarios),
    }


SUMMARY_FIELDS = (
    "scenario",
    "objective",
    "completed_requests",
    "failed_requests",
    "wrong_dropoffs",
    "unfinished_requests",
    "service_rate_pct",
    "avg_passenger_wait_min",
    "avg_picked_wait_min",
    "avg_correct_service_delay_min",
    "empty_miles",
    "loaded_miles",
    "utilization_pct",
    "spoof_exposures",
    "spoofed_matches",
    "spoofer_reconfigurations",
    "max_zone_service_loss_pp",
)


def _write_summary_csv(
    path: Path, scenarios: Sequence[Mapping[str, Any]], objective: str
) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for scenario in scenarios:
            row = {
                "scenario": scenario["id"],
                "objective": objective,
                **scenario["metrics"],
            }
            writer.writerow({key: row.get(key, "") for key in SUMMARY_FIELDS})


ZONE_FIELDS = (
    "scenario",
    "zone_index",
    "zone",
    "pickup_requests",
    "completed_requests",
    "wrong_dropoffs",
    "unfinished_requests",
    "service_rate_pct",
    "baseline_service_rate_pct",
    "service_loss_vs_baseline_pp",
)


def _write_zone_csv(path: Path, scenarios: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=ZONE_FIELDS)
        writer.writeheader()
        for scenario in scenarios:
            for zone in scenario["zone_service"]:
                row = {"scenario": scenario["id"], **zone}
                writer.writerow({key: row.get(key, "") for key in ZONE_FIELDS})


def _resolve_output_dir(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = _REPO_ROOT / path
    return path.resolve()


def run(args: argparse.Namespace) -> RunArtifacts:
    """Run all paired scenarios and persist deterministic research artifacts."""

    output_dir = _resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    request_specs = _generate_requests(args.seed, args.duration, args.requests)
    fleet_positions = _generate_fleet_positions(args.seed, args.fleet)
    effective_budget = min(args.spoofer_budget, GRID_SIDE * GRID_SIDE)
    static_devices = _build_static_devices(
        request_specs, args.objective, effective_budget
    )

    scenarios = [
        _simulate_scenario(
            "baseline",
            "Baseline · trusted locations",
            "#42d3a5",
            request_specs,
            fleet_positions,
            args.duration,
            args.objective,
            0,
            (),
        ),
        _simulate_scenario(
            "static_static",
            "Static-static spoofing",
            "#ffbd59",
            request_specs,
            fleet_positions,
            args.duration,
            args.objective,
            effective_budget,
            static_devices,
        ),
        _simulate_scenario(
            "dynamic",
            "Dynamic adaptive spoofing",
            "#ff6b6b",
            request_specs,
            fleet_positions,
            args.duration,
            args.objective,
            effective_budget,
            static_devices,
        ),
    ]
    _add_zone_service_loss(scenarios)
    _round_metrics(scenarios)
    payload = _dashboard_payload(
        args, request_specs, fleet_positions, static_devices, scenarios
    )

    results_json = output_dir / "results.json"
    summary_csv = output_dir / "scenario_metrics.csv"
    zone_csv = output_dir / "zone_service_loss.csv"
    results_json.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_summary_csv(summary_csv, scenarios, args.objective)
    _write_zone_csv(zone_csv, scenarios)
    dashboard_html = write_dashboard(payload, output_dir, filename="dashboard.html")
    return RunArtifacts(
        output_dir=output_dir,
        results_json=results_json,
        summary_csv=summary_csv,
        zone_csv=zone_csv,
        dashboard_html=dashboard_html,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts = run(args)
    print(f"Results JSON: {artifacts.results_json}")
    print(f"Scenario CSV: {artifacts.summary_csv}")
    print(f"Zone CSV: {artifacts.zone_csv}")
    print(f"Dashboard: {artifacts.dashboard_html}")
    if args.serve or args.open_browser:
        serve_dashboard(
            artifacts.dashboard_html,
            host=args.host,
            port=args.port,
            open_browser=args.open_browser,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
