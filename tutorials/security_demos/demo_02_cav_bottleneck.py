"""Deterministic CAV bottleneck cyberattack teaching replication.

This dependency-free study is inspired by the VehicleSec work on cyberattacks
against connected and automated vehicles (CAVs) at traffic bottlenecks.  It
models two 400 m approaches that share one merge point and compares truthful
telemetry with one slow Sybil vehicle, a constant longitudinal-position bias,
and an exact timestamp replay delay (3.0 seconds by default).

The controller is deliberately labelled a *CBF-inspired/FIFO approximation*.
It uses a FIFO merge reservation and the familiar rear-gap barrier quantity

    h = gap - delta - phi * follower_speed

to cap acceleration.  It is not the paper's optimal-control program, network
stack, or proof implementation.  The program sends no traffic and should be
used only as a deterministic, inspectable research demonstration.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import Any, Mapping, Sequence


_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tutorials.security_demos.dashboard import serve_dashboard, write_dashboard


# Constants reported in the paper's bottleneck case study.
DT = 0.1
CONTROL_ZONE_M = 400.0
ACCEL_MAX_MPS2 = 5.98
ACCEL_MIN_MPS2 = -5.98
SPEED_MAX_MPS = 30.0
DELTA_M = 3.74
PHI_S = 1.8

# Explicit teaching-model choices (not claimed as paper constants).
ENTRY_SPEED_MIN_MPS = 12.0
ENTRY_SPEED_MAX_MPS = 18.0
GHOST_SPEED_MPS = 5.0
HARD_BRAKE_MPS2 = -3.0
CBF_ALPHA = 0.9
SPEED_GAIN = 1.25
MIN_NUMERICAL_GAP_M = 0.05
MERGE_TOLERANCE_S = 0.005  # numerical crossing interpolation tolerance
PAPER_URL = "https://www.ndss-symposium.org/wp-content/uploads/2023/02/vehiclesec2023-23082-paper.pdf"
DEFAULT_OUTPUT = _REPO_ROOT / "output" / "security_demos" / "cav_bottleneck"


@dataclass(frozen=True)
class DemandVehicle:
    vehicle_id: int
    lane: int
    arrival_step: int
    arrival_s: float
    entry_speed_mps: float


@dataclass
class VehicleState:
    spec: DemandVehicle
    x_m: float
    speed_mps: float
    previous_command_mps2: float = 0.0


@dataclass(frozen=True)
class Telemetry:
    vehicle_key: str
    vehicle_id: int | None
    lane: int
    x_m: float
    speed_mps: float
    timestamp_step: int
    first_seen_s: float
    forged: bool = False


@dataclass(frozen=True)
class AttackPlan:
    target_vehicle_id: int | None
    start_step: int
    end_step: int
    delay_steps: int


@dataclass(frozen=True)
class RunArtifacts:
    output_dir: Path
    results_json: Path
    summary_csv: Path
    timeseries_csv: Path
    events_csv: Path
    dashboard_html: Path


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return parsed


def _port(value: str) -> int:
    parsed = int(value)
    if not 0 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("must be between 0 and 65535")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a deterministic two-lane CAV merge under truthful telemetry, "
            "one slow Sybil, position-bias FDI, and an exact replay delay."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--seed", type=int, default=2024, help="Paired demand seed.")
    parser.add_argument(
        "--duration",
        type=_positive_float,
        default=120.0,
        help="Experiment horizon in seconds (quantized to the 0.1 s controller tick).",
    )
    parser.add_argument(
        "--arrival",
        type=_positive_float,
        default=1800.0,
        help="Combined arrival rate over both lanes, vehicles/hour.",
    )
    parser.add_argument(
        "--bias",
        type=float,
        default=12.0,
        help=(
            "Signed constant added to the compromised vehicle's reported "
            "longitudinal position, metres."
        ),
    )
    parser.add_argument(
        "--delay",
        type=_positive_float,
        default=3.0,
        help="Timing-replay delay in seconds; must be a multiple of 0.1 s.",
    )
    parser.add_argument(
        "--output",
        "--output-dir",
        dest="output",
        default=str(DEFAULT_OUTPUT),
        help="Artifact directory; relative paths resolve from the repository root.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve the generated dashboard until Ctrl-C after the study.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard bind host.")
    parser.add_argument("--port", type=_port, default=0, help="Dashboard port; 0 selects a free port.")
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Serve the dashboard and ask the default browser to open it.",
    )
    return parser.parse_args(argv)


def _steps_for_seconds(seconds: float, name: str) -> int:
    steps = int(round(seconds / DT))
    if steps <= 0 or not math.isclose(steps * DT, seconds, abs_tol=1e-9):
        raise ValueError(f"{name} must be a positive multiple of {DT} seconds")
    return steps


def _resolve_output(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = _REPO_ROOT / path
    return path.resolve()


def _minimum_travel_time(distance_m: float, speed_mps: float) -> float:
    """Kinematic lower bound with the paper's speed/acceleration limits."""

    distance = max(0.0, distance_m)
    speed = min(SPEED_MAX_MPS, max(0.0, speed_mps))
    if distance == 0:
        return 0.0
    if speed >= SPEED_MAX_MPS - 1e-12:
        return distance / SPEED_MAX_MPS
    distance_to_limit = (SPEED_MAX_MPS**2 - speed**2) / (2.0 * ACCEL_MAX_MPS2)
    if distance <= distance_to_limit:
        return (-speed + math.sqrt(speed**2 + 2.0 * ACCEL_MAX_MPS2 * distance)) / ACCEL_MAX_MPS2
    acceleration_time = (SPEED_MAX_MPS - speed) / ACCEL_MAX_MPS2
    return acceleration_time + (distance - distance_to_limit) / SPEED_MAX_MPS


def _generate_demand(seed: int, duration_steps: int, arrival_vph: float) -> list[DemandVehicle]:
    """Generate paired, lane-independent Poisson arrivals and quantize to ticks."""

    lane_rate_per_second = arrival_vph / 7200.0
    staged: list[tuple[int, int, float]] = []
    for lane in (0, 1):
        rng = random.Random(seed * 1009 + lane * 7919 + 17)
        mean_headway = 1.0 / lane_rate_per_second
        raw_time = rng.uniform(0.15 * mean_headway, 0.85 * mean_headway)
        previous_step = -1
        while raw_time < duration_steps * DT:
            # Retain the Poisson draw while avoiding an already-unsafe entry gap.
            arrival_step = max(previous_step + 35, int(math.ceil(raw_time / DT - 1e-12)))
            if arrival_step >= duration_steps:
                break
            entry_speed = rng.uniform(ENTRY_SPEED_MIN_MPS, ENTRY_SPEED_MAX_MPS)
            staged.append((arrival_step, lane, entry_speed))
            previous_step = arrival_step
            raw_time += rng.expovariate(lane_rate_per_second)

    staged.sort(key=lambda row: (row[0], row[1]))
    return [
        DemandVehicle(
            vehicle_id=index,
            lane=lane,
            arrival_step=step,
            arrival_s=round(step * DT, 10),
            entry_speed_mps=speed,
        )
        for index, (step, lane, speed) in enumerate(staged, start=1)
    ]


def _build_attack_plan(
    demand: Sequence[DemandVehicle], duration_steps: int, delay_steps: int
) -> AttackPlan:
    desired_start = int(round(0.25 * duration_steps))
    latest_start = max(delay_steps + 1, int(round(0.65 * duration_steps)))
    viable = [
        vehicle
        for vehicle in demand
        if vehicle.arrival_step + delay_steps <= latest_start
        and vehicle.arrival_step <= int(round(0.55 * duration_steps))
    ]
    if viable:
        target = min(
            viable,
            key=lambda vehicle: (
                abs(vehicle.arrival_step + delay_steps - desired_start),
                vehicle.vehicle_id,
            ),
        )
        start_step = target.arrival_step + delay_steps
        target_id: int | None = target.vehicle_id
    else:
        target_id = None
        start_step = min(max(delay_steps, desired_start), max(0, duration_steps - 1))
    end_step = min(duration_steps, max(start_step + 1, int(round(0.80 * duration_steps))))
    return AttackPlan(target_id, start_step, end_step, delay_steps)


def _event(
    time_s: float,
    event_type: str,
    label: str,
    vehicle_id: int | str | None = None,
    details: str = "",
) -> dict[str, Any]:
    return {
        "time_s": round(time_s, 3),
        "event_type": event_type,
        "vehicle_id": vehicle_id,
        "label": label,
        "details": details,
    }


def _attack_events(
    scenario_id: str,
    plan: AttackPlan,
    bias_m: float,
    delay_s: float,
    target_crossing_s: float | None,
) -> list[dict[str, Any]]:
    start_s = plan.start_step * DT
    end_s = plan.end_step * DT
    target = plan.target_vehicle_id
    if scenario_id == "baseline":
        return [
            _event(
                0.0,
                "no_attack",
                "Truthful, current telemetry; no forged or dropped messages",
                details="Reference run for the paired demand stream.",
            )
        ]
    if scenario_id == "sybil":
        return [
            _event(
                start_s,
                "sybil_injected",
                f"One slow plausible Sybil joins lane 0 at {GHOST_SPEED_MPS:.1f} m/s",
                "sybil-1",
                "The forged identifier advances continuously from the controlled-zone entrance.",
            ),
            _event(
                end_s,
                "sybil_removed",
                "The forged Sybil identifier stops transmitting",
                "sybil-1",
            ),
        ]
    if target is None:
        return [
            _event(
                start_s,
                "attack_not_applied",
                "No suitable real CAV was available for the single-vehicle attack",
            )
        ]
    stop_s = min(end_s, target_crossing_s) if target_crossing_s is not None else end_s
    if scenario_id == "fdi":
        return [
            _event(
                start_s,
                "fdi_started",
                f"Vehicle {target} reports true x {bias_m:+.1f} m",
                target,
                "The same signed longitudinal-position offset is applied to every attacked report.",
            ),
            _event(
                stop_s,
                "fdi_stopped",
                f"Constant-bias reporting ends for vehicle {target}",
                target,
            ),
        ]
    return [
        _event(
            start_s,
            "timing_replay_started",
            f"Vehicle {target} reports state from exactly {delay_s:.1f} s earlier",
            target,
            f"Replay offset is {plan.delay_steps} controller ticks at dt={DT:.1f} s.",
        ),
        _event(
            stop_s,
            "timing_replay_stopped",
            f"Timestamp replay ends for vehicle {target}",
            target,
        ),
    ]


def _simulate(
    scenario_id: str,
    label: str,
    color: str,
    demand: Sequence[DemandVehicle],
    duration_steps: int,
    plan: AttackPlan,
    bias_m: float,
) -> dict[str, Any]:
    arrivals: dict[int, list[DemandVehicle]] = {}
    demand_by_id = {vehicle.vehicle_id: vehicle for vehicle in demand}
    for vehicle in demand:
        arrivals.setdefault(vehicle.arrival_step, []).append(vehicle)

    active: dict[int, VehicleState] = {}
    target_history: dict[int, Telemetry] = {}
    crossing_times: dict[int, float] = {}
    schedules: dict[str, float] = {}
    last_merge_crossing: float | None = None
    latest_merge_margin: float | None = None
    min_merge_margin: float | None = None
    merge_violation_count = 0
    merge_headway_shortfall_s = 0.0
    min_rear_margin: float | None = None
    rear_violation_exposure_s = 0.0
    rear_violation_episodes = 0
    previously_unsafe: set[tuple[int, int]] = set()
    hard_braking_events = 0
    hard_braking_exposure_s = 0.0
    braking_last_step: set[int] = set()
    minimum_acceleration: float | None = None
    maximum_abs_jerk = 0.0
    control_effort = 0.0
    max_queue = 0
    messages_received = 0
    forged_messages = 0
    messages_lost = 0
    latency_sum = 0.0
    max_latency = 0.0
    attacked_messages = 0
    attacked_latency_values: list[float] = []
    observations: list[dict[str, Any]] = []

    sample_stride = max(1, int(round(0.5 / DT)))
    series: dict[str, list[list[float]]] = {
        "queue_vehicles": [],
        "cumulative_throughput": [],
        "mean_traversal_delay_s": [],
        "rear_safety_margin_m": [],
        "merge_safety_margin_s": [],
        "minimum_acceleration_mps2": [],
        "control_effort": [],
        "message_latency_s": [],
        "message_loss_pct": [],
        "attack_active": [],
    }

    for step in range(duration_steps):
        time_s = step * DT
        for spec in arrivals.get(step, ()):  # paired, identical in every scenario
            active[spec.vehicle_id] = VehicleState(
                spec=spec,
                x_m=0.0,
                speed_mps=spec.entry_speed_mps,
            )

        raw_reports: dict[int, Telemetry] = {}
        for vehicle_id, state in active.items():
            report = Telemetry(
                vehicle_key=str(vehicle_id),
                vehicle_id=vehicle_id,
                lane=state.spec.lane,
                x_m=state.x_m,
                speed_mps=state.speed_mps,
                timestamp_step=step,
                first_seen_s=state.spec.arrival_s,
            )
            raw_reports[vehicle_id] = report
            if vehicle_id == plan.target_vehicle_id:
                target_history[step] = report

        reports: list[Telemetry] = []
        step_latency_sum = 0.0
        step_receipts = 0
        target_under_attack = False
        for vehicle_id in sorted(raw_reports):
            report = raw_reports[vehicle_id]
            attacked = (
                vehicle_id == plan.target_vehicle_id
                and plan.start_step <= step < plan.end_step
            )
            if scenario_id == "fdi" and attacked:
                report = Telemetry(
                    vehicle_key=report.vehicle_key,
                    vehicle_id=report.vehicle_id,
                    lane=report.lane,
                    x_m=report.x_m + bias_m,
                    speed_mps=report.speed_mps,
                    timestamp_step=report.timestamp_step,
                    first_seen_s=report.first_seen_s,
                )
                attacked_messages += 1
                attacked_latency_values.append(0.0)
                target_under_attack = True
            elif scenario_id == "timing" and attacked:
                replay = target_history.get(step - plan.delay_steps)
                if replay is not None:
                    report = replay
                    latency = (step - report.timestamp_step) * DT
                    expected = plan.delay_steps * DT
                    if not math.isclose(latency, expected, abs_tol=1e-12):
                        raise RuntimeError("timing replay did not preserve the exact configured delay")
                    step_latency_sum += latency
                    latency_sum += latency
                    max_latency = max(max_latency, latency)
                    attacked_messages += 1
                    attacked_latency_values.append(latency)
                    target_under_attack = True
            reports.append(report)
            messages_received += 1
            step_receipts += 1

        sybil_active = scenario_id == "sybil" and plan.start_step <= step < plan.end_step
        if sybil_active:
            ghost_x = GHOST_SPEED_MPS * (step - plan.start_step) * DT
            reports.append(
                Telemetry(
                    vehicle_key="sybil-1",
                    vehicle_id=None,
                    lane=0,
                    x_m=ghost_x,
                    speed_mps=GHOST_SPEED_MPS,
                    timestamp_step=step,
                    first_seen_s=plan.start_step * DT,
                    forged=True,
                )
            )
            messages_received += 1
            forged_messages += 1
            step_receipts += 1

        # FIFO merge times are recomputed from received state.  For real CAVs,
        # the earliest time uses the acceleration/speed bounds.  The Sybil is a
        # plausible non-cooperative road user, so its constant-speed ETA is
        # honored and it can hold the FIFO sequence.
        cursor = time_s
        if last_merge_crossing is not None:
            cursor = max(cursor, last_merge_crossing + PHI_S)
        schedules = {}
        for report in sorted(reports, key=lambda item: (item.first_seen_s, item.vehicle_key)):
            remaining = max(0.0, CONTROL_ZONE_M - report.x_m)
            if report.forged:
                earliest = time_s + remaining / max(0.1, report.speed_mps)
            else:
                earliest = time_s + _minimum_travel_time(remaining, report.speed_mps)
            slot = max(earliest, cursor)
            schedules[report.vehicle_key] = slot
            cursor = slot + PHI_S

        report_by_id = {
            report.vehicle_id: report for report in reports if report.vehicle_id is not None
        }
        commands: dict[int, float] = {}
        for vehicle_id, state in active.items():
            report = report_by_id[vehicle_id]
            remaining = max(0.0, CONTROL_ZONE_M - report.x_m)
            time_to_slot = max(DT, schedules[report.vehicle_key] - time_s)
            desired_speed = min(SPEED_MAX_MPS, remaining / time_to_slot)
            command = SPEED_GAIN * (desired_speed - report.speed_mps)
            commands[vehicle_id] = min(ACCEL_MAX_MPS2, max(ACCEL_MIN_MPS2, command))

        # CBF-inspired rear-gap clamp: dh + alpha*h >= 0 yields the upper bound
        # below.  It consumes received (possibly attacked) telemetry.
        for lane in (0, 1):
            lane_reports = sorted(
                (report for report in reports if report.lane == lane),
                key=lambda item: (-item.x_m, item.vehicle_key),
            )
            for leader, follower in zip(lane_reports, lane_reports[1:]):
                if follower.vehicle_id is None or follower.vehicle_id not in commands:
                    continue
                margin = leader.x_m - follower.x_m - DELTA_M - PHI_S * follower.speed_mps
                cbf_upper = (
                    leader.speed_mps
                    - follower.speed_mps
                    + CBF_ALPHA * margin
                ) / PHI_S
                commands[follower.vehicle_id] = max(
                    ACCEL_MIN_MPS2,
                    min(commands[follower.vehicle_id], cbf_upper, ACCEL_MAX_MPS2),
                )

        queue_count = 0
        for vehicle_id, state in active.items():
            ideal = time_s + _minimum_travel_time(
                CONTROL_ZONE_M - state.x_m, state.speed_mps
            )
            slot = schedules.get(str(vehicle_id), ideal)
            if slot - ideal > 0.5 or state.speed_mps < 2.0:
                queue_count += 1
        max_queue = max(max_queue, queue_count)

        step_min_acceleration: float | None = None
        braking_now: set[int] = set()
        proposed: dict[int, tuple[float, float]] = {}
        for vehicle_id, state in active.items():
            command = commands[vehicle_id]
            step_min_acceleration = (
                command
                if step_min_acceleration is None
                else min(step_min_acceleration, command)
            )
            minimum_acceleration = (
                command
                if minimum_acceleration is None
                else min(minimum_acceleration, command)
            )
            jerk = (command - state.previous_command_mps2) / DT
            maximum_abs_jerk = max(maximum_abs_jerk, abs(jerk))
            control_effort += 0.5 * command * command * DT
            if command <= HARD_BRAKE_MPS2:
                braking_now.add(vehicle_id)
                hard_braking_exposure_s += DT
                if vehicle_id not in braking_last_step:
                    hard_braking_events += 1
            next_speed = min(
                SPEED_MAX_MPS,
                max(0.0, state.speed_mps + command * DT),
            )
            next_x = state.x_m + state.speed_mps * DT + 0.5 * command * DT * DT
            proposed[vehicle_id] = (max(state.x_m, next_x), next_speed)
            state.previous_command_mps2 = command
        braking_last_step = braking_now

        # A tiny numerical non-overlap floor prevents order inversion; it is
        # intentionally far below delta and does not enforce the safety rule.
        for lane in (0, 1):
            lane_ids = sorted(
                (vehicle_id for vehicle_id, state in active.items() if state.spec.lane == lane),
                key=lambda vehicle_id: (-active[vehicle_id].x_m, vehicle_id),
            )
            leader_x: float | None = None
            leader_speed = SPEED_MAX_MPS
            for vehicle_id in lane_ids:
                x_m, speed_mps = proposed[vehicle_id]
                if leader_x is not None and x_m > leader_x - MIN_NUMERICAL_GAP_M:
                    x_m = leader_x - MIN_NUMERICAL_GAP_M
                    speed_mps = min(speed_mps, leader_speed)
                proposed[vehicle_id] = (x_m, speed_mps)
                leader_x, leader_speed = x_m, speed_mps

        crossing_batch: list[tuple[float, int]] = []
        for vehicle_id, state in active.items():
            old_x = state.x_m
            new_x, new_speed = proposed[vehicle_id]
            if old_x < CONTROL_ZONE_M <= new_x:
                fraction = (CONTROL_ZONE_M - old_x) / max(1e-12, new_x - old_x)
                crossing_batch.append((time_s + fraction * DT, vehicle_id))
            state.x_m = new_x
            state.speed_mps = new_speed

        for crossing_s, vehicle_id in sorted(crossing_batch):
            crossing_times[vehicle_id] = crossing_s
            if last_merge_crossing is not None:
                raw_margin = crossing_s - last_merge_crossing - PHI_S
                margin = 0.0 if abs(raw_margin) <= MERGE_TOLERANCE_S else raw_margin
                latest_merge_margin = margin
                min_merge_margin = margin if min_merge_margin is None else min(min_merge_margin, margin)
                if margin < 0.0:
                    merge_violation_count += 1
                    merge_headway_shortfall_s += -margin
                    if len(observations) < 8:
                        observations.append(
                            _event(
                                crossing_s,
                                "merge_headway_violation",
                                f"Vehicle {vehicle_id} crossed with {margin:.2f} s merge margin",
                                vehicle_id,
                            )
                        )
            last_merge_crossing = crossing_s

        for _, vehicle_id in crossing_batch:
            active.pop(vehicle_id, None)
            braking_last_step.discard(vehicle_id)

        unsafe_now: set[tuple[int, int]] = set()
        instantaneous_rear_margin: float | None = None
        for lane in (0, 1):
            lane_states = sorted(
                (state for state in active.values() if state.spec.lane == lane),
                key=lambda state: (-state.x_m, state.spec.vehicle_id),
            )
            for leader, follower in zip(lane_states, lane_states[1:]):
                margin = (
                    leader.x_m
                    - follower.x_m
                    - DELTA_M
                    - PHI_S * follower.speed_mps
                )
                instantaneous_rear_margin = (
                    margin
                    if instantaneous_rear_margin is None
                    else min(instantaneous_rear_margin, margin)
                )
                min_rear_margin = margin if min_rear_margin is None else min(min_rear_margin, margin)
                if margin < 0.0:
                    pair = (leader.spec.vehicle_id, follower.spec.vehicle_id)
                    unsafe_now.add(pair)
                    rear_violation_exposure_s += DT
        newly_unsafe = unsafe_now - previously_unsafe
        rear_violation_episodes += len(newly_unsafe)
        for leader_id, follower_id in sorted(newly_unsafe):
            if len(observations) < 8:
                observations.append(
                    _event(
                        time_s + DT,
                        "rear_margin_violation",
                        f"Rear safety margin became negative for {leader_id} <- {follower_id}",
                        follower_id,
                    )
                )
        previously_unsafe = unsafe_now

        step_avg_latency = step_latency_sum / step_receipts if step_receipts else 0.0
        cumulative_loss_pct = (
            100.0 * messages_lost / (messages_received + messages_lost)
            if messages_received + messages_lost
            else 0.0
        )
        if step % sample_stride == 0 or step == duration_steps - 1:
            completed_delays = []
            for vehicle_id, crossing_s in crossing_times.items():
                spec = demand_by_id[vehicle_id]
                free_flow_s = spec.arrival_s + _minimum_travel_time(
                    CONTROL_ZONE_M, spec.entry_speed_mps
                )
                completed_delays.append(max(0.0, crossing_s - free_flow_s))
            sample_time = round(min(duration_steps * DT, time_s + DT), 3)
            series["queue_vehicles"].append([sample_time, float(queue_count)])
            series["cumulative_throughput"].append([sample_time, float(len(crossing_times))])
            series["mean_traversal_delay_s"].append(
                [sample_time, fmean(completed_delays) if completed_delays else 0.0]
            )
            series["rear_safety_margin_m"].append(
                [sample_time, instantaneous_rear_margin if instantaneous_rear_margin is not None else 0.0]
            )
            series["merge_safety_margin_s"].append(
                [sample_time, latest_merge_margin if latest_merge_margin is not None else 0.0]
            )
            series["minimum_acceleration_mps2"].append(
                [sample_time, step_min_acceleration if step_min_acceleration is not None else 0.0]
            )
            series["control_effort"].append([sample_time, control_effort])
            series["message_latency_s"].append([sample_time, step_avg_latency])
            series["message_loss_pct"].append([sample_time, cumulative_loss_pct])
            series["attack_active"].append(
                [sample_time, float(sybil_active or target_under_attack)]
            )

    completed_delays: list[float] = []
    traversal_times: list[float] = []
    crossing_rows = []
    for vehicle_id, crossing_s in sorted(crossing_times.items(), key=lambda item: item[1]):
        spec = demand_by_id[vehicle_id]
        free_flow_crossing_s = spec.arrival_s + _minimum_travel_time(
            CONTROL_ZONE_M, spec.entry_speed_mps
        )
        delay_s = max(0.0, crossing_s - free_flow_crossing_s)
        completed_delays.append(delay_s)
        traversal_times.append(crossing_s - spec.arrival_s)
        crossing_rows.append(
            {
                "vehicle_id": vehicle_id,
                "lane": spec.lane,
                "arrival_s": spec.arrival_s,
                "crossing_s": crossing_s,
                "traversal_s": crossing_s - spec.arrival_s,
                "free_flow_crossing_s": free_flow_crossing_s,
                "delay_s": delay_s,
            }
        )

    target_crossing = (
        crossing_times.get(plan.target_vehicle_id)
        if plan.target_vehicle_id is not None
        else None
    )
    events = _attack_events(
        scenario_id,
        plan,
        bias_m,
        plan.delay_steps * DT,
        target_crossing,
    )
    dashboard_events = [*events, *observations]
    metrics: dict[str, int | float] = {
        "vehicles_generated": len(demand),
        "vehicles_crossed": len(crossing_times),
        "unfinished_vehicles": len(demand) - len(crossing_times),
        "throughput_vph": 3600.0 * len(crossing_times) / (duration_steps * DT),
        "max_queue_vehicles": max_queue,
        "avg_traversal_time_s": fmean(traversal_times) if traversal_times else 0.0,
        "avg_traversal_delay_s": fmean(completed_delays) if completed_delays else 0.0,
        "min_rear_safety_margin_m": min_rear_margin if min_rear_margin is not None else 0.0,
        "rear_violation_time_s": rear_violation_exposure_s,
        "rear_violation_count": rear_violation_episodes,
        "min_merge_safety_margin_s": min_merge_margin if min_merge_margin is not None else 0.0,
        "merge_violation_time_s": merge_headway_shortfall_s,
        "merge_violation_count": merge_violation_count,
        "hard_braking_events": hard_braking_events,
        "hard_braking_time_s": hard_braking_exposure_s,
        "min_acceleration_mps2": minimum_acceleration if minimum_acceleration is not None else 0.0,
        "max_abs_jerk_mps3": maximum_abs_jerk,
        "control_effort_half_u2": control_effort,
        "messages_received": messages_received,
        "forged_messages": forged_messages,
        "messages_lost": messages_lost,
        "message_loss_pct": 0.0,
        "avg_message_latency_s": latency_sum / messages_received if messages_received else 0.0,
        "max_message_latency_s": max_latency,
        "attacked_messages": attacked_messages,
        "attacked_message_latency_s": (
            fmean(attacked_latency_values) if attacked_latency_values else 0.0
        ),
    }
    return {
        "id": scenario_id,
        "label": label,
        "color": color,
        "metrics": metrics,
        "series": series,
        "events": dashboard_events,
        "attack_events": events,
        "crossings": crossing_rows,
        "attack_plan": {
            "target_vehicle_id": plan.target_vehicle_id if scenario_id in {"fdi", "timing"} else None,
            "start_s": plan.start_step * DT,
            "end_s": plan.end_step * DT,
            "position_bias_m": bias_m if scenario_id == "fdi" else 0.0,
            "replay_delay_s": plan.delay_steps * DT if scenario_id == "timing" else 0.0,
            "replay_delay_ticks": plan.delay_steps if scenario_id == "timing" else 0,
            "sybil_count": 1 if scenario_id == "sybil" else 0,
            "sybil_speed_mps": GHOST_SPEED_MPS if scenario_id == "sybil" else None,
        },
    }


def _round_scenario(scenario: dict[str, Any]) -> None:
    for key, value in list(scenario["metrics"].items()):
        if isinstance(value, float):
            scenario["metrics"][key] = round(value, 6)
    for samples in scenario["series"].values():
        for sample in samples:
            sample[0] = round(float(sample[0]), 3)
            sample[1] = round(float(sample[1]), 6)
    for row in scenario["crossings"]:
        for key, value in list(row.items()):
            if isinstance(value, float):
                row[key] = round(value, 6)


def _dashboard_payload(
    args: argparse.Namespace,
    demand: Sequence[DemandVehicle],
    plan: AttackPlan,
    duration_s: float,
    scenarios: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "title": "CAV bottleneck telemetry attacks",
        "eyebrow": "METS-R HPC - Security Demo 02",
        "subtitle": (
            "Paired two-lane, 400 m merge study with truthful messages, one slow "
            "Sybil, constant-position-bias FDI, and exact timestamp replay."
        ),
        "paper": {
            "title": "Evaluations of Cyberattacks on Cooperative Control of CAVs at Bottleneck Points",
            "venue": "VehicleSec 2023",
            "url": PAPER_URL,
        },
        "citation": (
            "H. M. S. Ahmad et al., Evaluations of Cyberattacks on Cooperative "
            "Control of Connected and Autonomous Vehicles at Bottleneck Points, "
            "VehicleSec 2023."
        ),
        "fidelity_boundary": (
            "CBF-inspired/FIFO teaching approximation only; it does not reproduce "
            "the paper's exact optimal-control, traffic, or communications implementation."
        ),
        "parameters": {
            "seed": args.seed,
            "duration": f"{duration_s:.1f} s",
            "combined arrival rate": f"{args.arrival:.1f} veh/h",
            "generated real CAVs": len(demand),
            "controller tick": f"{DT:.1f} s",
            "controlled-zone length": f"{CONTROL_ZONE_M:.0f} m per lane",
            "acceleration bounds": f"[{ACCEL_MIN_MPS2:.2f}, {ACCEL_MAX_MPS2:.2f}] m/s^2",
            "maximum speed": f"{SPEED_MAX_MPS:.0f} m/s",
            "delta": f"{DELTA_M:.2f} m",
            "phi": f"{PHI_S:.1f} s",
            "FDI position bias": f"{args.bias:+.1f} m",
            "timing replay": f"{plan.delay_steps * DT:.1f} s ({plan.delay_steps} exact ticks)",
            "single target CAV": plan.target_vehicle_id if plan.target_vehicle_id is not None else "none",
            "packet-loss attack": "not modeled (0 loss in every scenario)",
        },
        "metric_cards": [
            {"key": "throughput_vph", "label": "Throughput", "unit": "veh/h"},
            {"key": "max_queue_vehicles", "label": "Maximum queue", "unit": "CAVs", "precision": 0},
            {"key": "avg_traversal_delay_s", "label": "Traversal delay", "unit": "s"},
            {"key": "min_rear_safety_margin_m", "label": "Minimum rear margin", "unit": "m"},
            {"key": "min_merge_safety_margin_s", "label": "Minimum merge margin", "unit": "s"},
            {"key": "rear_violation_time_s", "label": "Rear violation exposure", "unit": "pair-s"},
            {"key": "merge_violation_count", "label": "Merge violations", "precision": 0},
            {"key": "hard_braking_events", "label": "Hard-brake episodes", "precision": 0},
            {"key": "control_effort_half_u2", "label": "1/2 integral u^2", "unit": "m2/s3"},
            {"key": "avg_message_latency_s", "label": "Mean message latency", "unit": "s"},
            {"key": "message_loss_pct", "label": "Message loss", "unit": "%"},
        ],
        "charts": [
            {
                "key": "queue_vehicles",
                "title": "FIFO reservation queue",
                "x_label": "simulation time (s)",
                "caption": "Real CAVs delayed >0.5 s beyond their kinematic earliest merge time.",
            },
            {
                "key": "cumulative_throughput",
                "title": "Cumulative merge throughput",
                "x_label": "simulation time (s)",
            },
            {
                "key": "mean_traversal_delay_s",
                "title": "Mean completed traversal delay",
                "x_label": "simulation time (s)",
                "caption": "Relative to acceleration- and speed-limited free flow; unfinished CAVs are excluded.",
            },
            {
                "key": "rear_safety_margin_m",
                "title": "Instantaneous minimum rear margin",
                "x_label": "simulation time (s)",
                "caption": "h = gap - delta - phi*v; negative values are violations.",
            },
            {
                "key": "merge_safety_margin_s",
                "title": "Latest merge headway margin",
                "x_label": "simulation time (s)",
                "caption": "Consecutive crossing headway minus phi; negative values are violations.",
            },
            {
                "key": "minimum_acceleration_mps2",
                "title": "Minimum commanded acceleration",
                "x_label": "simulation time (s)",
            },
            {
                "key": "control_effort",
                "title": "Cumulative control effort",
                "x_label": "simulation time (s)",
                "caption": "One half of the time integral of squared acceleration commands.",
            },
            {
                "key": "message_latency_s",
                "title": "Per-tick mean message latency",
                "x_label": "simulation time (s)",
                "caption": "The attacked timing report is replayed at the exact configured tick offset.",
            },
            {
                "key": "message_loss_pct",
                "title": "Cumulative message loss",
                "x_label": "simulation time (s)",
                "caption": "Zero by construction: jamming is outside this demo.",
                "wide": True,
            },
        ],
        "notes": [
            "All four scenarios reuse the exact generated arrivals, lanes, and entry speeds. Randomness occurs only during paired demand generation.",
            "The FIFO coordinator reserves one shared merge sequence. The rear controller clamps acceleration with dh + alpha*h >= 0 using received telemetry; both are transparent approximations.",
            "The Sybil scenario injects exactly one continuously moving 5 m/s forged identifier. It is intentionally slow but physically plausible and never becomes a real simulated vehicle.",
            "FDI adds one signed constant to a single CAV's reported longitudinal position. Timing replays the same CAV's actual state from exactly delay/dt ticks earlier.",
            "Rear violation time is pair-seconds below h=0. Merge violation time is the sum of headway shortfalls below phi; merge violation count counts unsafe crossing gaps.",
            "The maximum queue counts real CAVs whose assigned merge time is more than 0.5 s later than their current kinematic earliest arrival, or whose speed is below 2 m/s.",
            "Mean traversal delay uses completed CAVs only; throughput and unfinished_vehicles expose horizon censoring.",
            "No packets are dropped in this study. Jamming is deliberately omitted rather than conflated with the paper's evaluated Sybil, FDI, and timing cases.",
            "Use only in synthetic or controlled simulation environments; this script does not communicate with vehicles or roadside equipment.",
        ],
        "artifacts": {
            "results": "results.json",
            "scenario_metrics": "scenario_metrics.csv",
            "time_series": "time_series.csv",
            "attack_events": "attack_events.csv",
            "dashboard": "dashboard.html",
        },
        "input_digest": {
            "vehicle_count": len(demand),
            "arrival_checksum": sum(
                vehicle.vehicle_id * (vehicle.arrival_step + 1) * (vehicle.lane + 1)
                for vehicle in demand
            ),
            "target_vehicle_id": plan.target_vehicle_id,
            "attack_start_s": plan.start_step * DT,
            "attack_end_s": plan.end_step * DT,
        },
        "scenarios": list(scenarios),
    }


SUMMARY_FIELDS = (
    "scenario",
    "vehicles_generated",
    "vehicles_crossed",
    "unfinished_vehicles",
    "throughput_vph",
    "max_queue_vehicles",
    "avg_traversal_time_s",
    "avg_traversal_delay_s",
    "min_rear_safety_margin_m",
    "rear_violation_time_s",
    "rear_violation_count",
    "min_merge_safety_margin_s",
    "merge_violation_time_s",
    "merge_violation_count",
    "hard_braking_events",
    "hard_braking_time_s",
    "min_acceleration_mps2",
    "max_abs_jerk_mps3",
    "control_effort_half_u2",
    "messages_received",
    "forged_messages",
    "messages_lost",
    "message_loss_pct",
    "avg_message_latency_s",
    "max_message_latency_s",
    "attacked_messages",
    "attacked_message_latency_s",
)


SERIES_FIELDS = (
    "scenario",
    "time_s",
    "queue_vehicles",
    "cumulative_throughput",
    "mean_traversal_delay_s",
    "rear_safety_margin_m",
    "merge_safety_margin_s",
    "minimum_acceleration_mps2",
    "control_effort",
    "message_latency_s",
    "message_loss_pct",
    "attack_active",
)


EVENT_FIELDS = (
    "scenario",
    "time_s",
    "event_type",
    "vehicle_id",
    "label",
    "details",
)


def _write_summary(path: Path, scenarios: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for scenario in scenarios:
            row = {"scenario": scenario["id"], **scenario["metrics"]}
            writer.writerow({field: row.get(field, "") for field in SUMMARY_FIELDS})


def _write_timeseries(path: Path, scenarios: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=SERIES_FIELDS)
        writer.writeheader()
        for scenario in scenarios:
            series = scenario["series"]
            sample_count = len(series["queue_vehicles"])
            for index in range(sample_count):
                row: dict[str, Any] = {
                    "scenario": scenario["id"],
                    "time_s": series["queue_vehicles"][index][0],
                }
                for field in SERIES_FIELDS[2:]:
                    row[field] = series[field][index][1]
                writer.writerow(row)


def _write_events(path: Path, scenarios: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=EVENT_FIELDS)
        writer.writeheader()
        for scenario in scenarios:
            for event in scenario["attack_events"]:
                row = {"scenario": scenario["id"], **event}
                writer.writerow({field: row.get(field, "") for field in EVENT_FIELDS})


def run(args: argparse.Namespace) -> RunArtifacts:
    """Run all paired scenarios and persist JSON, CSV, and HTML evidence."""

    duration_steps = _steps_for_seconds(float(args.duration), "duration")
    delay_steps = _steps_for_seconds(float(args.delay), "delay")
    if not math.isfinite(float(args.bias)):
        raise ValueError("bias must be finite")
    if not math.isfinite(float(args.arrival)) or float(args.arrival) <= 0:
        raise ValueError("arrival must be a finite number greater than zero")

    demand = _generate_demand(int(args.seed), duration_steps, float(args.arrival))
    plan = _build_attack_plan(demand, duration_steps, delay_steps)
    scenarios = [
        _simulate(
            "baseline",
            "Baseline - truthful telemetry",
            "#42d3a5",
            demand,
            duration_steps,
            plan,
            float(args.bias),
        ),
        _simulate(
            "sybil",
            "One slow plausible Sybil",
            "#ffbd59",
            demand,
            duration_steps,
            plan,
            float(args.bias),
        ),
        _simulate(
            "fdi",
            f"Constant-bias FDI ({float(args.bias):+.1f} m)",
            "#ff6b6b",
            demand,
            duration_steps,
            plan,
            float(args.bias),
        ),
        _simulate(
            "timing",
            f"Exact {delay_steps * DT:.1f} s timing replay",
            "#75a7ff",
            demand,
            duration_steps,
            plan,
            float(args.bias),
        ),
    ]
    for scenario in scenarios:
        _round_scenario(scenario)

    duration_s = duration_steps * DT
    payload = _dashboard_payload(args, demand, plan, duration_s, scenarios)
    output_dir = _resolve_output(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    results_json = output_dir / "results.json"
    summary_csv = output_dir / "scenario_metrics.csv"
    timeseries_csv = output_dir / "time_series.csv"
    events_csv = output_dir / "attack_events.csv"
    results_json.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _write_summary(summary_csv, scenarios)
    _write_timeseries(timeseries_csv, scenarios)
    _write_events(events_csv, scenarios)
    dashboard_html = write_dashboard(payload, output_dir, filename="dashboard.html")
    return RunArtifacts(
        output_dir=output_dir,
        results_json=results_json,
        summary_csv=summary_csv,
        timeseries_csv=timeseries_csv,
        events_csv=events_csv,
        dashboard_html=dashboard_html,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts = run(args)
    print(f"Results JSON: {artifacts.results_json}")
    print(f"Scenario CSV: {artifacts.summary_csv}")
    print(f"Time-series CSV: {artifacts.timeseries_csv}")
    print(f"Attack events CSV: {artifacts.events_csv}")
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
