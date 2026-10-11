"""ChargeX/J1772 state-manipulation consequence demo.

The script models the control-pilot states and voltage equations reported in
Zhou et al., *Breaking the Charge: Exploiting State Manipulation in EV
Charging* (VehicleSec 2025 short), then maps a controlled attack schedule to EV
taxi charging and passenger-service outcomes.  It compares an unattacked run,
an attacked run, and a timeout/replug/reroute fallback under identical demand.

This is a safe software state-machine and operational-impact abstraction.  It
does not drive charging hardware or provide a construction workflow.  The
paper demonstrated state switching, denial of service, error latching, and
power supplied while a Tesla reported stopped charging; it did *not*
demonstrate battery damage or overcharging, and this demo makes no such claim.

Paper: https://www.usenix.org/system/files/vehiclesec25-zhou.pdf
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence


_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tutorials.security_demos.dashboard import serve_dashboard, write_dashboard  # noqa: E402


PAPER_URL = "https://www.usenix.org/system/files/vehiclesec25-zhou.pdf"
DEMO_SITE_URL = "https://chargerattacks.github.io/"


@dataclass(frozen=True)
class PilotObservation:
    design: str
    requested_state: str
    attacker_resistance_ohm: float | None
    evse_voltage: float
    ev_voltage: float
    evse_state: str
    ev_state: str
    divergent: bool


@dataclass(frozen=True)
class Passenger:
    passenger_id: int
    arrival_minute: int
    trip_minutes: int
    energy_kwh_per_minute: float


@dataclass
class Taxi:
    taxi_id: int
    battery_kwh: float
    state: str = "idle"
    remaining_minutes: int = 0
    station_id: int | None = None
    queue_entered: int | None = None
    charge_started: int | None = None
    latched_since: int | None = None
    passenger_id: int | None = None
    trip_energy_kwh_per_minute: float = 0.0


@dataclass
class Port:
    taxi_id: int | None = None
    occupied_since: int | None = None
    forced_since: int | None = None


@dataclass
class Station:
    station_id: int
    ports: list[Port]
    queue: deque[int] = field(default_factory=deque)


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    label: str
    color: str
    attacked: bool
    fallback: bool


def pilot_state(voltage: float, error_boundary_v: float = 4.4) -> str:
    """Classify the B/C/F-focused pilot states used by the paper's attacks."""

    if voltage > 10.6:
        return "A"  # disconnected
    if voltage > 7.8:
        return "B"  # EV detected / standby
    if voltage > error_boundary_v:
        return "C"  # charging
    return "F"  # error / negative-low pilot abstraction


def pilot_observation(
    design: str,
    requested_state: str,
    attacker_resistance_ohm: float | None,
    *,
    error_boundary_v: float = 4.4,
) -> PilotObservation:
    """Evaluate the paper's serial or parallel attachment equations.

    Resistance values are used only as inputs to this software model.  State B
    uses the paper's 2740-ohm vehicle load; state C uses 882 ohms.
    """

    if requested_state not in {"B", "C"}:
        raise ValueError("requested_state must be B or C")
    vehicle_resistance = 2740.0 if requested_state == "B" else 882.0
    if design == "none":
        load = vehicle_resistance
        voltage = 12.0 - 1000.0 * 12.0 / (1000.0 + load)
        evse_voltage = ev_voltage = voltage
        resistance = None
    else:
        if attacker_resistance_ohm is None or attacker_resistance_ohm <= 0:
            raise ValueError("attacker_resistance_ohm must be positive for an attack")
        resistance = float(attacker_resistance_ohm)
        if design == "serial":
            denominator = 1000.0 + resistance + vehicle_resistance
            evse_voltage = 12.0 - 1000.0 * 12.0 / denominator
            ev_voltage = 12.0 - (1000.0 + resistance) * 12.0 / denominator
        elif design == "parallel":
            parallel_load = 1.0 / (1.0 / vehicle_resistance + 1.0 / resistance)
            evse_voltage = ev_voltage = 12.0 - 1000.0 * 12.0 / (1000.0 + parallel_load)
        else:
            raise ValueError("design must be none, serial, or parallel")
    evse_state = pilot_state(evse_voltage, error_boundary_v)
    ev_state = pilot_state(ev_voltage, error_boundary_v)
    return PilotObservation(
        design=design,
        requested_state=requested_state,
        attacker_resistance_ohm=resistance,
        evse_voltage=evse_voltage,
        ev_voltage=ev_voltage,
        evse_state=evse_state,
        ev_state=ev_state,
        divergent=evse_state != ev_state,
    )


def _poisson(rng: random.Random, mean: float) -> int:
    if mean <= 0:
        return 0
    limit = math.exp(-mean)
    product = 1.0
    count = 0
    while product > limit:
        count += 1
        product *= rng.random()
    return count - 1


def build_demand(seed: int, duration_minutes: int, arrival_rate: float) -> list[list[Passenger]]:
    rng = random.Random(seed + 701)
    schedule: list[list[Passenger]] = []
    passenger_id = 0
    for minute in range(duration_minutes):
        arrivals: list[Passenger] = []
        for _ in range(_poisson(rng, arrival_rate)):
            passenger_id += 1
            arrivals.append(
                Passenger(
                    passenger_id=passenger_id,
                    arrival_minute=minute,
                    trip_minutes=rng.randint(7, 18),
                    energy_kwh_per_minute=rng.uniform(0.105, 0.155),
                )
            )
        schedule.append(arrivals)
    return schedule


def build_initial_soc(seed: int, fleet_size: int, capacity_kwh: float) -> list[float]:
    rng = random.Random(seed + 113)
    return [capacity_kwh * rng.uniform(0.22, 0.86) for _ in range(fleet_size)]


def _station_load(station: Station) -> int:
    return len(station.queue) + sum(port.taxi_id is not None for port in station.ports)


def _choose_station(
    taxi: Taxi,
    stations: Sequence[Station],
    attacked_station: int,
    attack_active: bool,
    fallback: bool,
) -> Station:
    preferred = taxi.taxi_id % len(stations)
    candidates = list(stations)
    if fallback and attack_active and len(stations) > 1:
        candidates = [station for station in stations if station.station_id != attacked_station]
    return min(
        candidates,
        key=lambda station: (
            _station_load(station) + (0.35 if station.station_id != preferred else 0.0),
            station.station_id,
        ),
    )


def _release_port(stations: Sequence[Station], taxi_id: int) -> None:
    for station in stations:
        for port in station.ports:
            if port.taxi_id == taxi_id:
                port.taxi_id = None
                port.occupied_since = None
                port.forced_since = None
                return


def simulate(
    scenario: Scenario,
    args: argparse.Namespace,
    initial_soc: Sequence[float],
    demand: Sequence[Sequence[Passenger]],
    attack_observation: PilotObservation,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    taxis = [Taxi(index, initial_soc[index]) for index in range(args.fleet_size)]
    stations = [
        Station(index, [Port() for _ in range(args.ports_per_station)])
        for index in range(args.stations)
    ]
    waiting: deque[Passenger] = deque()
    series: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    passenger_waits: list[int] = []
    charge_delays: list[int] = []
    completed = 0
    cancelled = 0
    battery_shortfalls = 0
    failed_charges = 0
    downtime_taxi_minutes = 0
    exposure_kwh = 0.0
    delivered_kwh = 0.0
    peak_queue = 0
    recovery_minute: int | None = None
    attack_end = args.attack_start_minute + args.attack_duration_minutes

    for minute in range(args.duration_minutes):
        attack_active = (
            scenario.attacked
            and args.attack_start_minute <= minute < attack_end
        )
        waiting.extend(demand[minute])

        # Progress passenger trips using the true battery state.
        for taxi in taxis:
            if taxi.state != "trip":
                continue
            taxi.battery_kwh -= taxi.trip_energy_kwh_per_minute
            taxi.remaining_minutes -= 1
            if taxi.battery_kwh <= args.capacity_kwh * 0.04:
                taxi.battery_kwh = max(0.0, taxi.battery_kwh)
                taxi.state = "idle"
                taxi.remaining_minutes = 0
                taxi.passenger_id = None
                battery_shortfalls += 1
                cancelled += 1
                events.append({"minute": minute, "label": f"Taxi {taxi.taxi_id} battery shortfall"})
            elif taxi.remaining_minutes <= 0:
                completed += 1
                taxi.state = "idle"
                taxi.passenger_id = None

        # Error/stuck recovery.  The paper notes that a Tesla error remained
        # latched until a replug; we model that explicit action and delay.
        for taxi in taxis:
            if taxi.state not in {"latched", "replugging", "forced"}:
                continue
            age = minute - (taxi.latched_since if taxi.latched_since is not None else minute)
            if taxi.state == "forced":
                exposure_kwh += args.forced_output_kw / 60.0
                if scenario.fallback and age >= args.fallback_timeout_minutes:
                    _release_port(stations, taxi.taxi_id)
                    taxi.state = "replugging"
                    taxi.remaining_minutes = args.replug_minutes
                    events.append({"minute": minute, "label": f"Fallback disconnected forced state on taxi {taxi.taxi_id}"})
                elif not attack_active:
                    _release_port(stations, taxi.taxi_id)
                    taxi.state = "idle"
                    taxi.station_id = None
                    taxi.latched_since = None
                continue
            if taxi.state == "latched":
                timeout = args.fallback_timeout_minutes if scenario.fallback else args.manual_replug_minutes
                eligible = age >= timeout and (scenario.fallback or not attack_active)
                if eligible:
                    _release_port(stations, taxi.taxi_id)
                    taxi.state = "replugging"
                    taxi.remaining_minutes = args.replug_minutes
                    events.append({"minute": minute, "label": f"Taxi {taxi.taxi_id} began replug/reset"})
                continue
            taxi.remaining_minutes -= 1
            if taxi.remaining_minutes <= 0:
                taxi.state = "idle"
                taxi.station_id = None
                taxi.latched_since = None
                if taxi.battery_kwh < args.capacity_kwh * args.charge_trigger:
                    station = _choose_station(
                        taxi, stations, args.attacked_station, attack_active, scenario.fallback
                    )
                    station.queue.append(taxi.taxi_id)
                    taxi.state = "queued"
                    taxi.station_id = station.station_id
                    taxi.queue_entered = minute

        # Send low-SOC idle taxis to charging queues.
        for taxi in taxis:
            if taxi.state == "idle" and taxi.battery_kwh <= args.capacity_kwh * args.charge_trigger:
                station = _choose_station(
                    taxi, stations, args.attacked_station, attack_active, scenario.fallback
                )
                station.queue.append(taxi.taxi_id)
                taxi.state = "queued"
                taxi.station_id = station.station_id
                taxi.queue_entered = minute

        # Fill free ports and progress charging.
        for station in stations:
            for port in station.ports:
                if port.taxi_id is None and station.queue:
                    taxi_id = station.queue.popleft()
                    taxi = taxis[taxi_id]
                    port.taxi_id = taxi_id
                    port.occupied_since = minute
                    taxi.state = "charging"
                    taxi.charge_started = minute
            for port in station.ports:
                if port.taxi_id is None:
                    continue
                taxi = taxis[port.taxi_id]
                targeted = attack_active and station.station_id == args.attacked_station
                if targeted and taxi.state == "charging" and args.attack_mode in {"serial_dos", "parallel_error"}:
                    taxi.state = "latched"
                    taxi.latched_since = minute
                    failed_charges += 1
                    events.append(
                        {
                            "minute": minute,
                            "label": (
                                f"Pilot {attack_observation.evse_state}/{attack_observation.ev_state} "
                                f"latched taxi {taxi.taxi_id}"
                            ),
                        }
                    )
                    continue
                if targeted and taxi.state == "charging" and args.attack_mode == "parallel_force_charge" and taxi.battery_kwh >= args.capacity_kwh * args.charge_target:
                    taxi.state = "forced"
                    taxi.latched_since = minute
                    port.forced_since = minute
                    events.append({"minute": minute, "label": f"Forced C-state retained taxi {taxi.taxi_id}"})
                    continue
                if taxi.state != "charging":
                    continue
                energy = args.charge_power_kw / 60.0
                taxi.battery_kwh = min(args.capacity_kwh, taxi.battery_kwh + energy)
                delivered_kwh += energy
                if taxi.battery_kwh >= args.capacity_kwh * args.charge_target:
                    if targeted and args.attack_mode == "parallel_force_charge":
                        taxi.state = "forced"
                        taxi.latched_since = minute
                        port.forced_since = minute
                    else:
                        _release_port(stations, taxi.taxi_id)
                        taxi.state = "idle"
                        taxi.station_id = None
                        if taxi.queue_entered is not None:
                            charge_delays.append(minute - taxi.queue_entered + 1)
                        taxi.queue_entered = None
                        taxi.charge_started = None

        # Cancel passengers who have exceeded the service-level wait, then
        # dispatch remaining jobs to service-capable taxis.
        while waiting and minute - waiting[0].arrival_minute > args.max_passenger_wait_minutes:
            waiting.popleft()
            cancelled += 1
        available = [
            taxi
            for taxi in taxis
            if taxi.state == "idle" and taxi.battery_kwh > args.capacity_kwh * args.charge_trigger
        ]
        available.sort(key=lambda taxi: (-taxi.battery_kwh, taxi.taxi_id))
        while waiting and available:
            passenger = waiting.popleft()
            taxi = available.pop()
            passenger_waits.append(minute - passenger.arrival_minute)
            taxi.state = "trip"
            taxi.remaining_minutes = passenger.trip_minutes
            taxi.passenger_id = passenger.passenger_id
            taxi.trip_energy_kwh_per_minute = passenger.energy_kwh_per_minute

        nonservice = sum(taxi.state in {"queued", "charging", "latched", "replugging", "forced"} for taxi in taxis)
        downtime_taxi_minutes += nonservice
        service_capable = sum(taxi.state in {"idle", "trip"} for taxi in taxis)
        charging = sum(taxi.state == "charging" for taxi in taxis)
        latched = sum(taxi.state in {"latched", "forced", "replugging"} for taxi in taxis)
        station_queue = sum(len(station.queue) for station in stations)
        occupied_ports = sum(port.taxi_id is not None for station in stations for port in station.ports)
        peak_queue = max(peak_queue, station_queue)
        if scenario.attacked and minute >= attack_end and recovery_minute is None:
            attacked_station = stations[args.attacked_station]
            affected = any(
                taxi.station_id == args.attacked_station and taxi.state in {"latched", "forced", "replugging"}
                for taxi in taxis
            )
            if not affected and len(attacked_station.queue) <= args.ports_per_station:
                recovery_minute = minute
        series.append(
            {
                "scenario": scenario.scenario_id,
                "minute": minute,
                "passenger_queue": len(waiting),
                "completed_requests": completed,
                "cancelled_requests": cancelled,
                "service_capable_taxis": service_capable,
                "charging_taxis": charging,
                "latched_taxis": latched,
                "station_queue": station_queue,
                "occupied_ports": occupied_ports,
                "mean_soc_percent": 100.0 * statistics.fmean(taxi.battery_kwh for taxi in taxis) / args.capacity_kwh,
            }
        )

    cancelled += len(waiting)
    unfinished = sum(taxi.state == "trip" for taxi in taxis)
    generated = sum(len(minute) for minute in demand)
    total_ports = args.stations * args.ports_per_station
    metrics = {
        "completed_requests": float(completed),
        "cancelled_requests": float(cancelled),
        "unfinished_requests": float(unfinished),
        "service_completion_rate": 100.0 * completed / max(1, generated),
        "mean_passenger_wait_min": statistics.fmean(passenger_waits) if passenger_waits else 0.0,
        "failed_charge_attempts": float(failed_charges),
        "mean_charge_delay_min": statistics.fmean(charge_delays) if charge_delays else 0.0,
        "taxi_downtime_hours": downtime_taxi_minutes / 60.0,
        "battery_shortfalls": float(battery_shortfalls),
        "peak_station_queue": float(peak_queue),
        "mean_port_occupancy_percent": 100.0 * statistics.fmean(row["occupied_ports"] for row in series) / max(1, total_ports),
        "mean_fleet_availability_percent": 100.0 * statistics.fmean(row["service_capable_taxis"] for row in series) / args.fleet_size,
        "delivered_energy_kwh": delivered_kwh,
        "forced_output_exposure_kwh": exposure_kwh,
        "recovery_after_attack_min": (
            float(max(0, recovery_minute - attack_end))
            if scenario.attacked and recovery_minute is not None
            else None
        ),
        "recovery_observed": scenario.attacked and recovery_minute is not None,
    }
    return metrics, series, events


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    fieldnames: Sequence[str] | None = None,
) -> None:
    if fieldnames is None:
        if not rows:
            raise ValueError("fieldnames are required when writing an empty CSV")
        fields = list(rows[0])
    else:
        fields = list(fieldnames)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    design = "serial" if args.attack_mode == "serial_dos" else "parallel"
    requested_state = "B" if args.attack_mode == "parallel_force_charge" else "C"
    default_resistance = {
        "serial_dos": 720.0,
        "parallel_force_charge": 2000.0,
        "parallel_error": 500.0,
    }[args.attack_mode]
    resistance = args.attacker_resistance_ohm or default_resistance
    observation = pilot_observation(
        design,
        requested_state,
        resistance,
        error_boundary_v=args.error_boundary_v,
    )
    initial_soc = build_initial_soc(args.seed, args.fleet_size, args.capacity_kwh)
    demand = build_demand(args.seed, args.duration_minutes, args.passenger_arrival_rate)
    scenarios = (
        Scenario("baseline", "Baseline", "#42d3a5", False, False),
        Scenario("attack", "ChargeX consequence", "#ff6b6b", True, False),
        Scenario("fallback", "Attack + fallback", "#75a7ff", True, True),
    )
    results: dict[str, dict[str, Any]] = {}
    timeline: list[dict[str, Any]] = []
    event_map: dict[str, list[dict[str, Any]]] = {}
    for scenario in scenarios:
        metrics, rows, events = simulate(
            scenario, args, initial_soc, demand, observation
        )
        results[scenario.scenario_id] = metrics
        timeline.extend(rows)
        event_map[scenario.scenario_id] = events

    pilot_data = {
        **observation.__dict__,
        "paper_state_boundaries_v": {"A_to_B": 10.6, "B_to_C": 7.8, "C_to_F_demo_profile": args.error_boundary_v},
        "warning": "Voltage/resistance equations are evaluated only in software; no hardware actuation is performed.",
    }
    summary = {
        "paper": PAPER_URL,
        "authors_demo_site": DEMO_SITE_URL,
        "seed": args.seed,
        "attack_mode": args.attack_mode,
        "pilot_observation": pilot_data,
        "metrics": results,
        "parameters": vars(args),
    }
    (output_dir / "metrics.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _write_csv(
        output_dir / "timeline.csv",
        timeline,
        fieldnames=(
            "scenario", "minute", "passenger_queue", "completed_requests",
            "cancelled_requests", "service_capable_taxis", "charging_taxis",
            "latched_taxis", "station_queue", "occupied_ports", "mean_soc_percent",
        ),
    )
    _write_csv(
        output_dir / "events.csv",
        [
            {"scenario": scenario_id, **event}
            for scenario_id, events in event_map.items()
            for event in events
        ],
        fieldnames=("scenario", "minute", "label"),
    )

    dashboard_scenarios: list[dict[str, Any]] = []
    for scenario in scenarios:
        rows = [row for row in timeline if row["scenario"] == scenario.scenario_id]
        dashboard_scenarios.append(
            {
                "id": scenario.scenario_id,
                "label": scenario.label,
                "color": scenario.color,
                "metrics": results[scenario.scenario_id],
                "series": {
                    "passenger_queue": [[row["minute"], row["passenger_queue"]] for row in rows],
                    "fleet_availability": [[row["minute"], row["service_capable_taxis"]] for row in rows],
                    "station_queue": [[row["minute"], row["station_queue"]] for row in rows],
                    "mean_soc": [[row["minute"], row["mean_soc_percent"]] for row in rows],
                    "completed": [[row["minute"], row["completed_requests"]] for row in rows],
                    "latched": [[row["minute"], row["latched_taxis"]] for row in rows],
                },
                "events": [
                    f"t={event['minute']} min · {event['label']}"
                    for event in event_map[scenario.scenario_id][:30]
                ],
            }
        )
    dashboard = write_dashboard(
        {
            "title": "ChargeX state-manipulation consequences",
            "subtitle": "J1772 pilot-state observations drive a paired EV-taxi charging and service experiment.",
            "paper": {"title": "Breaking the Charge", "venue": "VehicleSec 2025 short", "url": PAPER_URL},
            "scenarios": dashboard_scenarios,
            "metric_cards": [
                {"key": "completed_requests", "label": "Completed requests", "precision": 0},
                {"key": "cancelled_requests", "label": "Cancelled requests", "precision": 0},
                {"key": "unfinished_requests", "label": "Trips active at horizon", "precision": 0},
                {"key": "mean_passenger_wait_min", "label": "Passenger wait", "unit": "min", "precision": 2},
                {"key": "failed_charge_attempts", "label": "Failed charge attempts", "precision": 0},
                {"key": "taxi_downtime_hours", "label": "Aggregate taxi downtime", "unit": "h", "precision": 1},
                {"key": "battery_shortfalls", "label": "Battery shortfalls", "precision": 0},
                {"key": "mean_fleet_availability_percent", "label": "Fleet availability", "unit": "%", "precision": 1},
                {
                    "key": "recovery_after_attack_min",
                    "label": "Observed post-attack recovery",
                    "unit": "min",
                    "precision": 1,
                    "hint": "— means not applicable, not recovered, or horizon-censored",
                },
                {"key": "forced_output_exposure_kwh", "label": "Forced-output exposure", "unit": "kWh", "precision": 2},
            ],
            "charts": [
                {"key": "completed", "title": "Passenger service", "x_label": "simulation minute", "y_label": "cumulative completed requests"},
                {"key": "passenger_queue", "title": "Passenger queue", "x_label": "simulation minute", "y_label": "waiting requests"},
                {"key": "fleet_availability", "title": "Service-capable taxis", "x_label": "simulation minute", "y_label": "taxis"},
                {"key": "station_queue", "title": "Charging-station queue", "x_label": "simulation minute", "y_label": "taxis"},
                {"key": "latched", "title": "Latched / resetting taxis", "x_label": "simulation minute", "y_label": "taxis"},
                {"key": "mean_soc", "title": "Mean fleet state of charge", "x_label": "simulation minute", "y_label": "SOC (%)"},
            ],
            "parameters": {
                "attack abstraction": args.attack_mode.replace("_", " "),
                "attack window": f"{args.attack_start_minute}–{args.attack_start_minute + args.attack_duration_minutes} min",
                "target station": args.attacked_station,
                "attachment model": design,
                "Ratt (software input)": f"{resistance:.0f} Ω",
                "EVSE / EV state": f"{observation.evse_state} / {observation.ev_state}",
                "EVSE / EV pilot": f"{observation.evse_voltage:.2f} / {observation.ev_voltage:.2f} V",
                "fallback": f"detect {args.fallback_timeout_minutes} min, replug {args.replug_minutes} min, reroute",
                "seed": args.seed,
            },
            "notes": [
                "Exact state thresholds and pilot equations are paper anchors; the fleet, demand, charger power, attack window, and fallback timings are explicit demo assumptions.",
                "A serial mismatch or forced error is modeled as a latched failure that requires a simulated unplug/replug, matching the paper's observed Tesla recovery behavior.",
                "Forced-output exposure uses the paper's approximately 6 A × 211 V observation as station-side energy exposure. It is not credited to the battery and is not evidence of overcharging or damage.",
                "Passenger service, battery shortfall, station occupancy, and resilience are METS-R consequence metrics; they were not evaluated by the ChargeX paper.",
                "Requests still on a valid passenger trip at the simulation horizon are reported separately as unfinished rather than misclassified as completed or cancelled.",
                "Post-attack recovery is reported only when recovery is observed in a simulated sample at or after the attack ends. A null/— value is not applicable for baseline or, in an attacked scenario, means not recovered or horizon-censored; recovery_observed is then false.",
            ],
        },
        output_dir,
    )
    print(
        f"Pilot observation: EVSE {observation.evse_voltage:.2f} V/state {observation.evse_state}; "
        f"EV {observation.ev_voltage:.2f} V/state {observation.ev_state}"
    )
    print(f"Wrote {output_dir / 'metrics.json'}")
    print(f"Wrote {output_dir / 'timeline.csv'}")
    print(f"Wrote {output_dir / 'events.csv'}")
    print(f"Wrote {dashboard}")
    if args.serve:
        serve_dashboard(dashboard, host=args.host, port=args.port, open_browser=args.open_browser)
    elif args.open_browser:
        import webbrowser

        webbrowser.open(dashboard.as_uri())
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the ChargeX/J1772 operational-consequence demo.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--attack-mode",
        choices=("serial_dos", "parallel_force_charge", "parallel_error"),
        default="serial_dos",
    )
    parser.add_argument("--seed", type=int, default=2505)
    parser.add_argument("--duration-minutes", type=int, default=300)
    parser.add_argument("--attack-start-minute", type=int, default=80)
    parser.add_argument("--attack-duration-minutes", type=int, default=70)
    parser.add_argument("--fleet-size", type=int, default=42)
    parser.add_argument("--stations", type=int, default=4)
    parser.add_argument("--ports-per-station", type=int, default=3)
    parser.add_argument("--attacked-station", type=int, default=0)
    parser.add_argument("--passenger-arrival-rate", type=float, default=3.0, help="Expected requests per minute.")
    parser.add_argument("--max-passenger-wait-minutes", type=int, default=12)
    parser.add_argument("--capacity-kwh", type=float, default=65.0)
    parser.add_argument("--charge-power-kw", type=float, default=50.0)
    parser.add_argument("--charge-trigger", type=float, default=0.28, help="SOC fraction that triggers charging.")
    parser.add_argument("--charge-target", type=float, default=0.78, help="SOC fraction that ends normal charging.")
    parser.add_argument("--fallback-timeout-minutes", type=int, default=4)
    parser.add_argument("--replug-minutes", type=int, default=2)
    parser.add_argument("--manual-replug-minutes", type=int, default=18)
    parser.add_argument("--forced-output-kw", type=float, default=1.266, help="Station-side forced-output exposure; not battery energy.")
    parser.add_argument("--attacker-resistance-ohm", type=float, help="Software equation input; mode-specific paper-inspired default if omitted.")
    parser.add_argument("--error-boundary-v", type=float, default=4.4, help="C/F boundary for the modeled charger profile.")
    parser.add_argument(
        "--output-dir",
        default=str(_REPO_ROOT / "output" / "security_demos" / "chargex_state_manipulation"),
    )
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--open-browser", action="store_true")
    args = parser.parse_args(argv)
    float_inputs = (
        "passenger_arrival_rate", "capacity_kwh", "charge_power_kw", "charge_trigger",
        "charge_target", "forced_output_kw", "error_boundary_v",
    )
    for name in float_inputs:
        if not math.isfinite(getattr(args, name)):
            parser.error(f"--{name.replace('_', '-')} must be finite")
    if args.attacker_resistance_ohm is not None and not math.isfinite(args.attacker_resistance_ohm):
        parser.error("--attacker-resistance-ohm must be finite")
    positive = (
        "duration_minutes", "attack_duration_minutes", "fleet_size", "stations",
        "ports_per_station", "passenger_arrival_rate", "capacity_kwh", "charge_power_kw",
        "fallback_timeout_minutes", "replug_minutes", "manual_replug_minutes", "forced_output_kw",
    )
    for name in positive:
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.attack_start_minute < 0 or args.attack_start_minute >= args.duration_minutes:
        parser.error("--attack-start-minute must fall within the simulation")
    if args.attack_start_minute + args.attack_duration_minutes > args.duration_minutes:
        parser.error("attack window extends past --duration-minutes")
    if not 0 <= args.attacked_station < args.stations:
        parser.error("--attacked-station must be a valid station index")
    if not 0.05 <= args.charge_trigger < args.charge_target <= 0.98:
        parser.error("require 0.05 <= charge-trigger < charge-target <= 0.98")
    if args.attacker_resistance_ohm is not None and args.attacker_resistance_ohm <= 0:
        parser.error("--attacker-resistance-ohm must be positive")
    if not 0.0 < args.error_boundary_v < 7.8:
        parser.error("--error-boundary-v must be between 0 and 7.8")
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
