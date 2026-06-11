"""Run a small CARLA Town 05 BSM exchange through the OMNeT++ bridge.

This example assumes METS-R and the OMNeT++ bridge are already listening on the
configured host/ports. It does not start METS-R, SUMO, a Python sidecar, or a
local range fallback. By default, the script uses METSRClient to query at least
four active METS-R vehicles, treats their coordinates as CARLA Town 05 vehicle
states, sends Basic Safety Messages between each pair, and prints a table of
communication records returned by the bridge.
"""

import argparse
import csv
import math
import os
import random
import sys


_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from clients.VeinsClient import VeinsClient, build_mobility_records
from utils.util import read_run_config


TOWN05_VEHICLES = [
    {
        "ID": 501,
        "role": "northbound",
        "x": -52.0,
        "y": -8.0,
        "z": 0.3,
        "speed": 9.5,
        "bearing": 90.0,
        "sensor_type": "cv2x",
    },
    {
        "ID": 502,
        "role": "southbound",
        "x": -49.0,
        "y": 64.0,
        "z": 0.3,
        "speed": 8.0,
        "bearing": 270.0,
        "sensor_type": "cv2x",
    },
    {
        "ID": 503,
        "role": "eastbound",
        "x": -96.0,
        "y": 27.0,
        "z": 0.3,
        "speed": 10.0,
        "bearing": 0.0,
        "sensor_type": "cv2x",
    },
    {
        "ID": 504,
        "role": "westbound",
        "x": -8.0,
        "y": 30.0,
        "z": 0.3,
        "speed": 7.0,
        "bearing": 180.0,
        "sensor_type": "cv2x",
    },
]


def get_arguments(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Send synthetic BSMs through an OMNeT++/Veins bridge and report "
            "communication records with locations, distance, content, and latency."
        )
    )
    parser.add_argument(
        "-r",
        "--run_config",
        default="configs/run_v2x_veins_Template.json",
        help="Run config containing veins_host/veins_port.",
    )
    parser.add_argument(
        "--scenario",
        choices=("town05_bsm", "noise"),
        default="town05_bsm",
        help="Run the four-vehicle CARLA Town 05 BSM scenario or the older noise load scenario.",
    )
    parser.add_argument(
        "--vehicle_source",
        choices=("metsr", "town05_seed"),
        default="metsr",
        help="For town05_bsm, use METSRClient vehicle states or static Town 05 seed vehicles.",
    )
    parser.add_argument("--metsr_host", default=None, help="Override METS-R websocket host.")
    parser.add_argument("--metsr_port", type=int, default=None, help="Override METS-R websocket port.")
    parser.add_argument(
        "--metsr_vehicle_ids",
        default=None,
        help="Comma-separated METS-R vehicle IDs to use; otherwise first active vehicles are selected.",
    )
    parser.add_argument(
        "--metsr_private_flags",
        default=None,
        help="Comma-separated true/false flags matching --metsr_vehicle_ids.",
    )
    parser.add_argument(
        "--metsr_vehicle_count",
        type=int,
        default=4,
        help="Number of METS-R vehicles to use when --metsr_vehicle_ids is omitted.",
    )
    parser.add_argument(
        "--metsr_timeout",
        type=float,
        default=None,
        help="METS-R request timeout in seconds.",
    )
    parser.add_argument(
        "--metsr_connect_wait",
        type=float,
        default=5.0,
        help="Maximum seconds to wait when connecting to METS-R.",
    )
    parser.add_argument(
        "--no_metsr_step",
        action="store_true",
        help="Do not advance METS-R with tick() before each BSM exchange.",
    )
    parser.add_argument(
        "--metsr_transform_coords",
        action="store_true",
        help="Ask METS-R for transformed lon/lat coordinates instead of network coordinates.",
    )
    parser.add_argument("--host", default=None, help="Override Veins bridge host.")
    parser.add_argument("--port", type=int, default=None, help="Override Veins bridge port.")
    parser.add_argument("--ticks", type=int, default=100)
    parser.add_argument("--duration_s", type=float, default=0.1)
    parser.add_argument("--target_id", type=int, default=1)
    parser.add_argument("--noise_senders", type=int, default=40)
    parser.add_argument("--messages_per_sender", type=int, default=5)
    parser.add_argument("--sender_radius_m", type=float, default=80.0)
    parser.add_argument(
        "--radius_end_m",
        type=float,
        default=None,
        help="If set, linearly sweep sender radius from sender_radius_m to this value.",
    )
    parser.add_argument("--payload_bytes", type=int, default=300)
    parser.add_argument(
        "--radio_mode",
        default=None,
        help="Optional per-message radio_mode label passed to the bridge.",
    )
    parser.add_argument(
        "--bsm_attack",
        choices=(
            "none",
            "position_offset",
            "speed_offset",
            "heading_offset",
            "acceleration_offset",
            "fake_emergency_brake",
            "ghost_vehicle",
            "dos",
        ),
        default="none",
        help="Optional BSM semantic attack or DoS load injection.",
    )
    parser.add_argument(
        "--attack_sender_ids",
        default=None,
        help="Comma-separated sender IDs to attack; default attacks the first available sender.",
    )
    parser.add_argument("--attack_probability", type=float, default=1.0)
    parser.add_argument("--attack_start_tick", type=int, default=0)
    parser.add_argument("--attack_end_tick", type=int, default=None)
    parser.add_argument("--attack_seed", type=int, default=7)
    parser.add_argument("--attack_position_offset_x_m", type=float, default=25.0)
    parser.add_argument("--attack_position_offset_y_m", type=float, default=0.0)
    parser.add_argument("--attack_speed_offset_mps", type=float, default=10.0)
    parser.add_argument("--attack_heading_offset_deg", type=float, default=180.0)
    parser.add_argument("--attack_acceleration_offset_mps2", type=float, default=4.0)
    parser.add_argument("--attack_dos_messages", type=int, default=20)
    parser.add_argument("--ghost_vehicle_id", type=int, default=900001)
    parser.add_argument("--csv", default=None, help="Optional CSV output path.")
    parser.add_argument(
        "--message_csv",
        default=None,
        help="Optional per-message link metric CSV output path.",
    )
    parser.add_argument(
        "--trace_messages",
        type=int,
        default=0,
        help="Print this many per-message link metrics at each printed tick.",
    )
    parser.add_argument(
        "--table_rows",
        type=int,
        default=24,
        help="Print this many communication-record rows after the run; use 0 to disable.",
    )
    parser.add_argument("--print_every", type=int, default=10)
    parser.add_argument("--connect_timeout", type=float, default=None)
    parser.add_argument("--request_timeout", type=float, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def sender_radius_for_tick(args, tick):
    if args.radius_end_m is None or args.ticks <= 1:
        return args.sender_radius_m
    fraction = tick / float(args.ticks - 1)
    return args.sender_radius_m + (args.radius_end_m - args.sender_radius_m) * fraction


def config_get(config, name, default=None):
    if config is None:
        return default
    return getattr(config, name, default)


def parse_int_list(raw):
    if raw is None or raw == "":
        return None
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def parse_bool_list(raw):
    if raw is None or raw == "":
        return None
    values = []
    for item in raw.split(","):
        item = item.strip().lower()
        if not item:
            continue
        values.append(item in ("1", "true", "t", "yes", "y", "private"))
    return values


def metsr_port_from_config(config):
    ports = config_get(config, "ports")
    if ports:
        return int(ports[0])
    metsr_port = config_get(config, "metsr_port")
    return None if metsr_port is None else int(metsr_port)


def connect_metsr_client(args, config):
    try:
        from clients.METSRClient import METSRClient
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "METS-R vehicle source requires clients.METSRClient and its websocket dependency. "
            "Install project requirements, or use --vehicle_source town05_seed for an offline static demo."
        ) from exc

    host = args.metsr_host or config_get(config, "metsr_host", "localhost")
    port = args.metsr_port if args.metsr_port is not None else metsr_port_from_config(config)
    if port is None:
        raise ValueError(
            "METS-R vehicle source requires --metsr_port or a config with ports/metsr_port. "
            "Use --vehicle_source town05_seed only for an offline static demo."
        )
    sim_folder = config_get(config, "sim_folder")
    sim_dirs = config_get(config, "sim_dirs")
    if sim_folder is None and sim_dirs:
        sim_folder = sim_dirs[0]
    client = METSRClient(
        host=host,
        port=port,
        sim_folder=sim_folder,
        timeout=args.metsr_timeout or config_get(config, "timeout", 30),
        verbose=args.verbose,
        max_connection_attempts=1,
        max_connection_wait=args.metsr_connect_wait,
    )
    return client


def active_vehicle(record):
    return int(record.get("state", 1)) > 0 and record.get("x") is not None and record.get("y") is not None


def normalize_metsr_vehicle(record, private_flag=False, role=None):
    vehicle_id = record.get("vehicle_id", record.get("ID", record.get("vid")))
    return {
        "ID": vehicle_id,
        "role": role or ("private" if private_flag else "public"),
        "x": record.get("x"),
        "y": record.get("y"),
        "z": record.get("z", 0.0),
        "speed": record.get("speed", record.get("speed_mps", 0.0)),
        "bearing": record.get("bearing", record.get("heading_deg", 0.0)),
        "acc": record.get("acc", record.get("acceleration_mps2")),
        "road": record.get("road", record.get("road_id")),
        "lane": record.get("lane", record.get("lane_id")),
        "state": record.get("state"),
        "v_type": record.get("v_type", record.get("vehicle_type")),
        "private_veh": bool(private_flag),
        "sensor_type": "cv2x",
        "map_name": "Town05",
    }


def query_selected_metsr_vehicles(metsr, args):
    vehicle_ids = parse_int_list(args.metsr_vehicle_ids)
    private_flags = parse_bool_list(args.metsr_private_flags)
    if vehicle_ids:
        if private_flags is None:
            private_flags = [False] * len(vehicle_ids)
        if len(private_flags) != len(vehicle_ids):
            raise ValueError("--metsr_private_flags must match --metsr_vehicle_ids length")
        response = metsr.query_vehicle(
            id=vehicle_ids,
            private_veh=private_flags,
            transform_coords=args.metsr_transform_coords,
        )
        return [
            normalize_metsr_vehicle(record, private_flag=private_flag, role=f"metsr_{index + 1}")
            for index, (record, private_flag) in enumerate(zip(response.get("DATA", []), private_flags))
            if active_vehicle(record)
        ]

    fleet = metsr.query_vehicle()
    candidates = []
    for private_flag, key in ((False, "public_vids"), (True, "private_vids")):
        ids = list(fleet.get(key, []))
        for start in range(0, len(ids), 25):
            batch = ids[start : start + 25]
            if not batch:
                continue
            response = metsr.query_vehicle(
                id=batch,
                private_veh=[private_flag] * len(batch),
                transform_coords=args.metsr_transform_coords,
            )
            for record in response.get("DATA", []):
                if active_vehicle(record):
                    candidates.append(
                        normalize_metsr_vehicle(
                            record,
                            private_flag=private_flag,
                            role=f"metsr_{len(candidates) + 1}",
                        )
                    )
                if len(candidates) >= args.metsr_vehicle_count:
                    return candidates
    return candidates


def step_metsr(metsr):
    if metsr.current_tick is None:
        metsr.query_tick()
    metsr.tick()


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    fraction = rank - low
    return ordered[low] * (1.0 - fraction) + ordered[high] * fraction


def make_vehicle_records(args, tick):
    sender_radius = sender_radius_for_tick(args, tick)
    vehicles = [
        {
            "ID": args.target_id,
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "speed": 0.0,
            "bearing": 0.0,
            "sensor_type": "cv2x",
        }
    ]
    for index in range(args.noise_senders):
        angle = 2.0 * math.pi * index / max(args.noise_senders, 1)
        angle += 0.01 * tick
        vehicles.append(
            {
                "ID": 1000 + index,
                "x": sender_radius * math.cos(angle),
                "y": sender_radius * math.sin(angle),
                "z": 0.0,
                "speed": 0.0,
                "bearing": math.degrees(angle) % 360.0,
                "sensor_type": "cv2x",
            }
        )
    return vehicles


def make_town05_vehicle_records(args, tick):
    vehicles = []
    elapsed_s = tick * args.duration_s
    for template in TOWN05_VEHICLES:
        heading_rad = math.radians(template["bearing"])
        x = template["x"] + math.cos(heading_rad) * template["speed"] * elapsed_s
        y = template["y"] + math.sin(heading_rad) * template["speed"] * elapsed_s
        vehicle = dict(template)
        vehicle.update(
            {
                "x": x,
                "y": y,
                "map_name": "Town05",
            }
        )
        vehicles.append(vehicle)
    return vehicles


def bsm_content(vehicle, tick):
    return (
        "Town05 BSM "
        f"tick={tick} veh={vehicle['ID']} role={vehicle.get('role', '')} "
        f"pos=({vehicle['x']:.1f},{vehicle['y']:.1f},{vehicle.get('z', 0.0):.1f}) "
        f"speed={vehicle.get('speed', 0.0):.1f}mps heading={vehicle.get('bearing', 0.0):.1f}deg"
    )


def make_town05_bsm_messages(args, vehicles, tick):
    messages = []
    sequence = 0
    for sender in vehicles:
        sender_id = sender["ID"]
        for receiver in vehicles:
            receiver_id = receiver["ID"]
            if sender_id == receiver_id:
                continue
            sequence += 1
            messages.append(
                {
                    "message_id": f"town05:{tick}:{sender_id}>{receiver_id}:{sequence}",
                    "tick": tick,
                    "vehicle_id": sender_id,
                    "sender_id": sender_id,
                    "receiver_id": receiver_id,
                    "target_vehicle_id": receiver_id,
                    "message_name": "BasicSafetyMessage",
                    "message_standard": "SAE J2735-aligned",
                    "message_count": (tick * 16 + sequence) % 128,
                    "payload_bytes": args.payload_bytes,
                    "tx_time_s": tick * args.duration_s,
                    "radio_mode": args.radio_mode,
                    "content": bsm_content(sender, tick),
                    "map_name": "Town05",
                    "sender_role": sender.get("role"),
                    "receiver_role": receiver.get("role"),
                    "x": sender["x"],
                    "y": sender["y"],
                    "z": sender.get("z", 0.0),
                    "speed_mps": sender.get("speed", 0.0),
                    "heading_deg": sender.get("bearing", 0.0),
                }
            )
    return messages


def make_noise_messages(args, vehicles, tick):
    messages = []
    sequence = 0
    for vehicle in vehicles:
        sender_id = vehicle["ID"]
        if sender_id == args.target_id:
            continue
        for message_index in range(args.messages_per_sender):
            sequence += 1
            messages.append(
                {
                    "message_id": f"noise:{tick}:{sender_id}>{args.target_id}:{sequence}",
                    "tick": tick,
                    "vehicle_id": sender_id,
                    "sender_id": sender_id,
                    "receiver_id": args.target_id,
                    "target_vehicle_id": args.target_id,
                    "message_name": "NoiseMessage",
                    "message_standard": "synthetic-load",
                    "message_count": (tick + sequence + message_index) % 128,
                    "payload_bytes": args.payload_bytes,
                    "tx_time_s": tick * args.duration_s,
                    "radio_mode": args.radio_mode,
                    "x": vehicle["x"],
                    "y": vehicle["y"],
                    "z": vehicle.get("z", 0.0),
                    "speed_mps": vehicle.get("speed", 0.0),
                    "heading_deg": vehicle.get("bearing", 0.0),
                }
            )
    return messages


def attack_is_active(args, tick):
    if args.bsm_attack == "none":
        return False
    if tick < args.attack_start_tick:
        return False
    if args.attack_end_tick is not None and tick > args.attack_end_tick:
        return False
    return True


def selected_attack_senders(args, vehicles):
    configured = parse_int_list(args.attack_sender_ids)
    if configured:
        return set(configured)
    for vehicle in vehicles:
        vehicle_id = vehicle.get("ID", vehicle.get("vehicle_id"))
        if vehicle_id is not None:
            return {vehicle_id}
    return set()


def append_attack_content(message, attack_type):
    content = message.get("content")
    if not content:
        content = (
            f"BSM veh={message.get('sender_id', message.get('vehicle_id'))} "
            f"pos=({message.get('x', 'NA')},{message.get('y', 'NA')},{message.get('z', 0.0)}) "
            f"speed={message.get('speed_mps', 'NA')}mps "
            f"heading={message.get('heading_deg', 'NA')}deg"
        )
    message["content"] = f"{content} attack={attack_type}"


def mark_attacked_message(message, attack_id, attack_type):
    message["attacked"] = True
    message["attack_id"] = attack_id
    message["attack_type"] = attack_type
    for key in (
        "x",
        "y",
        "z",
        "speed_mps",
        "heading_deg",
        "acceleration_mps2",
    ):
        if key in message:
            message[f"truth_{key}"] = message.get(key)


def mutate_bsm_message(args, message, attack_id, attack_type):
    mark_attacked_message(message, attack_id, attack_type)
    if attack_type == "position_offset":
        message["x"] = float(message.get("x", 0.0)) + args.attack_position_offset_x_m
        message["y"] = float(message.get("y", 0.0)) + args.attack_position_offset_y_m
    elif attack_type == "speed_offset":
        message["speed_mps"] = float(message.get("speed_mps", 0.0)) + args.attack_speed_offset_mps
        message["speed"] = message["speed_mps"]
    elif attack_type == "heading_offset":
        message["heading_deg"] = (
            float(message.get("heading_deg", 0.0)) + args.attack_heading_offset_deg
        ) % 360.0
        message["heading"] = message["heading_deg"]
    elif attack_type == "acceleration_offset":
        message["acceleration_mps2"] = (
            float(message.get("acceleration_mps2", 0.0))
            + args.attack_acceleration_offset_mps2
        )
    elif attack_type == "fake_emergency_brake":
        message["event_flag"] = "fake_emergency_brake"
        message["acceleration_mps2"] = -8.0
    append_attack_content(message, attack_type)


def make_attack_event(args, tick, attack_id, attack_type, sender_ids, affected_messages):
    return {
        "attack_id": attack_id,
        "attack_type": attack_type,
        "tick": tick,
        "policy": "sporadic" if args.attack_probability < 1.0 else "persistent",
        "sender_ids": sorted(sender_ids),
        "affected_messages": affected_messages,
    }


def apply_bsm_attack(args, vehicles, messages, tick):
    if not attack_is_active(args, tick):
        return vehicles, messages, []

    attack_type = args.bsm_attack
    attack_id = f"{attack_type}:{tick}"
    sender_ids = selected_attack_senders(args, vehicles)
    rng = random.Random(args.attack_seed + tick)
    updated_vehicles = list(vehicles)
    updated_messages = [dict(message) for message in messages]
    affected = 0

    if attack_type == "ghost_vehicle":
        base_vehicle = vehicles[0] if vehicles else {"x": 0.0, "y": 0.0, "z": 0.0}
        ghost = dict(base_vehicle)
        ghost.update(
            {
                "ID": args.ghost_vehicle_id,
                "role": "ghost_attacker",
                "x": float(base_vehicle.get("x", 0.0)) + args.attack_position_offset_x_m,
                "y": float(base_vehicle.get("y", 0.0)) + args.attack_position_offset_y_m,
                "speed": float(base_vehicle.get("speed", 0.0)) + args.attack_speed_offset_mps,
                "bearing": (
                    float(base_vehicle.get("bearing", 0.0)) + args.attack_heading_offset_deg
                )
                % 360.0,
                "map_name": base_vehicle.get("map_name", "Town05"),
            }
        )
        updated_vehicles.append(ghost)
        sequence = 0
        for receiver in vehicles:
            receiver_id = receiver.get("ID")
            if receiver_id is None:
                continue
            sequence += 1
            message = {
                "message_id": f"ghost:{tick}:{ghost['ID']}>{receiver_id}:{sequence}",
                "tick": tick,
                "vehicle_id": ghost["ID"],
                "sender_id": ghost["ID"],
                "receiver_id": receiver_id,
                "target_vehicle_id": receiver_id,
                "message_name": "BasicSafetyMessage",
                "message_standard": "SAE J2735-aligned",
                "message_count": (tick * 32 + sequence) % 128,
                "payload_bytes": args.payload_bytes,
                "tx_time_s": tick * args.duration_s,
                "radio_mode": args.radio_mode,
                "content": bsm_content(ghost, tick),
                "map_name": ghost.get("map_name", "Town05"),
                "sender_role": "ghost_attacker",
                "receiver_role": receiver.get("role"),
                "x": ghost["x"],
                "y": ghost["y"],
                "z": ghost.get("z", 0.0),
                "speed_mps": ghost.get("speed", 0.0),
                "heading_deg": ghost.get("bearing", 0.0),
            }
            mutate_bsm_message(args, message, attack_id, attack_type)
            updated_messages.append(message)
            affected += 1
        sender_ids = {ghost["ID"]}
    elif attack_type == "dos":
        base_messages = [
            message
            for message in messages
            if message.get("sender_id", message.get("vehicle_id")) in sender_ids
        ]
        if not base_messages:
            base_messages = messages[:1]
        for index in range(max(0, args.attack_dos_messages)):
            template = base_messages[index % len(base_messages)]
            message = dict(template)
            message.update(
                {
                    "message_id": (
                        f"dos:{tick}:{message.get('sender_id')}>"
                        f"{message.get('receiver_id', message.get('target_vehicle_id'))}:{index}"
                    ),
                    "message_name": "DoSNoiseMessage",
                    "message_standard": "synthetic-load",
                    "message_count": (tick * 64 + index) % 128,
                    "content": f"DoS load tick={tick} index={index}",
                }
            )
            mark_attacked_message(message, attack_id, attack_type)
            updated_messages.append(message)
            affected += 1
    else:
        for message in updated_messages:
            sender_id = message.get("sender_id", message.get("vehicle_id"))
            if sender_id not in sender_ids:
                continue
            if rng.random() > args.attack_probability:
                continue
            mutate_bsm_message(args, message, attack_id, attack_type)
            affected += 1

    event = make_attack_event(args, tick, attack_id, attack_type, sender_ids, affected)
    return updated_vehicles, updated_messages, [event]


def row_matches_target(row, target_id):
    if target_id is None:
        return True
    return row.get("receiver_id", row.get("target_vehicle_id")) == target_id


def latency_values_for_target(result, target_id):
    received = [
        row
        for row in result.get("received_bsms", [])
        if row_matches_target(row, target_id)
    ]
    latencies = [
        float(row["latency_ms"])
        for row in received
        if row.get("latency_ms") is not None
    ]
    if latencies:
        return latencies, received

    metrics = [
        row
        for row in result.get("link_metrics", [])
        if row_matches_target(row, target_id)
        and row.get("latency_ms") is not None
        and row.get("delivered", True)
    ]
    return [float(row["latency_ms"]) for row in metrics], metrics


def summarize_tick(tick, transmitted, result, target_id):
    latencies, latency_rows = latency_values_for_target(result, target_id)
    metrics = result.get("link_metrics", [])
    distances = [
        float(row["distance_m"])
        for row in metrics
        if row_matches_target(row, target_id)
        and row.get("distance_m") is not None
    ]
    delivered_metrics = [
        row
        for row in metrics
        if row_matches_target(row, target_id)
        and row.get("delivered", False)
    ]
    target_metrics = [row for row in metrics if row_matches_target(row, target_id)]
    summary = {
        "tick": tick,
        "tx_noise_messages": transmitted,
        "rx_to_target": len(latency_rows),
        "delivery_rate": None,
        "dropped": None,
        "link_metrics": len(metrics),
        "distance_min_m": None,
        "distance_mean_m": None,
        "distance_max_m": None,
        "latency_mean_ms": None,
        "latency_p50_ms": None,
        "latency_p95_ms": None,
        "latency_max_ms": None,
    }
    if target_metrics:
        summary["delivery_rate"] = len(delivered_metrics) / len(target_metrics)
        summary["dropped"] = len(target_metrics) - len(delivered_metrics)
    if distances:
        summary.update(
            {
                "distance_min_m": min(distances),
                "distance_mean_m": sum(distances) / len(distances),
                "distance_max_m": max(distances),
            }
        )
    if latencies:
        summary.update(
            {
                "latency_mean_ms": sum(latencies) / len(latencies),
                "latency_p50_ms": percentile(latencies, 50),
                "latency_p95_ms": percentile(latencies, 95),
                "latency_max_ms": max(latencies),
            }
        )
    return summary


def metric_rows_for_csv(result):
    rows = []
    for metric in result.get("link_metrics", []):
        row = dict(metric)
        if row.get("latency_ms") is None:
            row["latency_ms"] = ""
        rows.append(row)
    return rows


def vehicle_by_id(vehicles):
    return {vehicle["ID"]: vehicle for vehicle in vehicles if "ID" in vehicle}


def message_by_link(messages):
    lookup = {}
    for message in messages:
        key = (
            message.get("sender_id", message.get("vehicle_id")),
            message.get("receiver_id", message.get("target_vehicle_id")),
            message.get("message_count"),
        )
        lookup[key] = message
        fallback_key = (key[0], key[1], None)
        if fallback_key not in lookup:
            lookup[fallback_key] = message
    return lookup


def communication_records_from_result(result, vehicles, messages):
    vehicles_by_id = vehicle_by_id(vehicles)
    messages_by_link = message_by_link(messages)
    records = []
    for metric in result.get("link_metrics", []):
        sender_id = metric.get("sender_id")
        receiver_id = metric.get("receiver_id", metric.get("target_vehicle_id"))
        message_count = metric.get("message_count")
        message = messages_by_link.get(
            (sender_id, receiver_id, message_count),
            messages_by_link.get((sender_id, receiver_id, None), {}),
        )
        sender = vehicles_by_id.get(sender_id, {})
        receiver = vehicles_by_id.get(receiver_id, {})
        records.append(
            {
                "tick": metric.get("tick"),
                "origin_vehicle_id": sender_id,
                "target_vehicle_id": receiver_id,
                "origin_role": sender.get("role", message.get("sender_role", "")),
                "target_role": receiver.get("role", message.get("receiver_role", "")),
                "map_name": message.get("map_name", sender.get("map_name", "")),
                "origin_x": sender.get("x"),
                "origin_y": sender.get("y"),
                "origin_z": sender.get("z"),
                "target_x": receiver.get("x"),
                "target_y": receiver.get("y"),
                "target_z": receiver.get("z"),
                "distance_m": metric.get("distance_m"),
                "message_name": metric.get("message_name", message.get("message_name")),
                "message_id": metric.get("message_id", message.get("message_id")),
                "message_count": message_count,
                "message_content": message.get("content", metric.get("message_content", "")),
                "attacked": metric.get("attacked", message.get("attacked", False)),
                "attack_id": metric.get("attack_id", message.get("attack_id", "")),
                "attack_type": metric.get("attack_type", message.get("attack_type", "")),
                "delivered": metric.get("delivered"),
                "drop_reason": metric.get("drop_reason", ""),
                "latency_ms": "" if metric.get("latency_ms") is None else metric.get("latency_ms"),
                "packet_error_rate": metric.get("packet_error_rate"),
                "delivery_probability": metric.get("delivery_probability"),
                "radio_mode": metric.get("radio_mode", message.get("radio_mode")),
                "bridge_backend": metric.get("bridge_backend"),
                "backend_implementation": metric.get("backend_implementation"),
                "radio_access": metric.get("radio_access"),
                "bridge_model": metric.get("bridge_model"),
                "network_model": metric.get("network_model"),
                "tx_x": message.get("x"),
                "tx_y": message.get("y"),
                "tx_speed_mps": message.get("speed_mps"),
                "tx_heading_deg": message.get("heading_deg"),
                "truth_x": message.get("truth_x"),
                "truth_y": message.get("truth_y"),
                "truth_speed_mps": message.get("truth_speed_mps"),
                "truth_heading_deg": message.get("truth_heading_deg"),
            }
        )
    return records


def fmt_number(value, digits=1):
    if value is None or value == "":
        return "NA"
    return f"{float(value):.{digits}f}"


def truncate(value, width):
    text = "" if value is None else str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def print_communication_table(records, limit):
    if limit <= 0 or not records:
        return
    columns = [
        ("tick", "tick", 4),
        ("origin_vehicle_id", "origin", 6),
        ("target_vehicle_id", "target", 6),
        ("origin_loc", "origin xyz", 21),
        ("target_loc", "target xyz", 21),
        ("distance_m", "dist m", 8),
        ("attack_type", "attack", 16),
        ("message_content", "message content", 52),
        ("latency_ms", "lat ms", 9),
        ("delivered", "rx", 5),
    ]

    def cell(record, key):
        if key == "origin_loc":
            return "({x},{y},{z})".format(
                x=fmt_number(record.get("origin_x")),
                y=fmt_number(record.get("origin_y")),
                z=fmt_number(record.get("origin_z")),
            )
        if key == "target_loc":
            return "({x},{y},{z})".format(
                x=fmt_number(record.get("target_x")),
                y=fmt_number(record.get("target_y")),
                z=fmt_number(record.get("target_z")),
            )
        if key in ("distance_m", "latency_ms"):
            return fmt_number(record.get(key), digits=3 if key == "latency_ms" else 1)
        if key == "delivered":
            return "yes" if record.get(key) else "no"
        return record.get(key, "")

    visible = records[:limit]
    header = " | ".join(truncate(title, width).ljust(width) for _, title, width in columns)
    separator = "-+-".join("-" * width for _, _, width in columns)
    print("communication_records")
    print(header)
    print(separator)
    for record in visible:
        print(
            " | ".join(
                truncate(cell(record, key), width).ljust(width)
                for key, _, width in columns
            )
        )
    if len(records) > limit:
        print(f"... {len(records) - limit} more rows; use --table_rows to change this.")


def print_metric_trace(metrics, limit):
    if limit <= 0:
        return
    for row in metrics[:limit]:
        latency = format_latency(row.get("latency_ms"))
        drop_reason = row.get("drop_reason", "")
        print(
            "  msg tick={tick} {sender}->{receiver} count={count} "
            "dist_m={distance:.1f} delivered={delivered} latency_ms={latency} "
            "per={per:.3f} {drop}".format(
                tick=row.get("tick"),
                sender=row.get("sender_id"),
                receiver=row.get("receiver_id"),
                count=row.get("message_count", ""),
                distance=float(row.get("distance_m", 0.0)),
                delivered=row.get("delivered"),
                latency=latency,
                per=float(row.get("packet_error_rate", 0.0)),
                drop=f"drop_reason={drop_reason}" if drop_reason else "",
            ).rstrip()
        )


def bridge_metadata(result):
    keys = (
        "bridge_backend",
        "backend_implementation",
        "bridge_model",
        "network_model",
        "radio_access",
        "backend_note",
    )
    metadata = {key: result.get(key) for key in keys if result.get(key)}
    raw = result.get("raw", {})
    if isinstance(raw, dict):
        data = raw.get("data", {})
        sources = [data, raw] if isinstance(data, dict) else [raw]
        for source in sources:
            for key in keys:
                if key not in metadata and source.get(key):
                    metadata[key] = source.get(key)
    return metadata


def bridge_model_name(result):
    metadata = bridge_metadata(result)
    return metadata.get("bridge_backend") or metadata.get("bridge_model")


def format_latency(value):
    if value is None or value == "":
        return "NA"
    return f"{float(value):.3f}"


def write_csv(path, rows):
    if not path or not rows:
        return
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with open(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    args = get_arguments(sys.argv[1:] if argv is None else argv)
    config = read_run_config(args.run_config) if args.run_config else None
    summary_target_id = args.target_id if args.scenario == "noise" else None
    metsr = None
    if args.scenario == "town05_bsm" and args.vehicle_source == "metsr":
        metsr = connect_metsr_client(args, config)
        print(f"connected_to_metsr host={metsr.host} port={metsr.port}")

    client = VeinsClient(
        config=config,
        host=args.host,
        port=args.port,
        connect_timeout=args.connect_timeout,
        request_timeout=args.request_timeout,
        verbose=args.verbose,
    )
    rows = []
    message_rows = []
    reported_model = False
    try:
        client.connect()
        print(f"connected_to_veins_bridge host={client.host} port={client.port}")
        for tick in range(args.ticks):
            if args.scenario == "town05_bsm":
                if metsr is not None:
                    if not args.no_metsr_step:
                        step_metsr(metsr)
                    vehicle_records = query_selected_metsr_vehicles(metsr, args)
                    if len(vehicle_records) < 4:
                        raise RuntimeError(
                            "METS-R returned fewer than four active vehicles for the Town 05 BSM example. "
                            "Pass --metsr_vehicle_ids with active vehicles, warm up METS-R, or use "
                            "--vehicle_source town05_seed for a static offline demo."
                        )
                    vehicle_records = vehicle_records[: max(4, args.metsr_vehicle_count)]
                else:
                    vehicle_records = make_town05_vehicle_records(args, tick)
                messages = make_town05_bsm_messages(args, vehicle_records, tick)
            else:
                vehicle_records = make_vehicle_records(args, tick)
                messages = make_noise_messages(args, vehicle_records, tick)
            vehicle_records, messages, attack_events = apply_bsm_attack(
                args, vehicle_records, messages, tick
            )
            mobility = build_mobility_records(vehicle_records)
            result = client.sync_tick(
                tick=tick,
                vehicles=mobility,
                bsm_messages=messages,
                attacks=attack_events,
                duration_s=args.duration_s,
            )
            if not reported_model:
                metadata = bridge_metadata(result)
                if metadata:
                    print(
                        "bridge_backend={backend} implementation={implementation} "
                        "radio_access={radio} network_model={model}".format(
                            backend=metadata.get("bridge_backend", "unknown"),
                            implementation=metadata.get(
                                "backend_implementation", "unknown"
                            ),
                            radio=metadata.get("radio_access", "unknown"),
                            model=metadata.get(
                                "network_model",
                                metadata.get("bridge_model", "unknown"),
                            ),
                        )
                    )
                    if metadata.get("backend_note"):
                        print(f"bridge_note={metadata['backend_note']}")
                    reported_model = True
            summary = summarize_tick(
                tick=tick,
                transmitted=len(messages),
                result=result,
                target_id=summary_target_id,
            )
            summary["sender_radius_m"] = (
                sender_radius_for_tick(args, tick) if args.scenario == "noise" else None
            )
            summary["scenario"] = args.scenario
            summary["bsm_attack"] = args.bsm_attack
            summary["attack_events"] = len(attack_events)
            summary["attacked_messages"] = sum(
                1 for message in messages if message.get("attacked")
            )
            for key, value in bridge_metadata(result).items():
                summary[key] = value
            rows.append(summary)
            tick_records = communication_records_from_result(result, vehicle_records, messages)
            message_rows.extend(tick_records)
            if args.print_every > 0 and (tick % args.print_every == 0 or tick == args.ticks - 1):
                print(
                    "tick={tick} scenario={scenario} attack={attack} radius_m={radius} "
                    "tx={tx} attacked={attacked} rx={rx} delivery={delivery} "
                    "dist_mean_m={dist} mean_ms={mean} "
                    "p95_ms={p95} max_ms={max_latency}".format(
                        tick=summary["tick"],
                        scenario=args.scenario,
                        attack=args.bsm_attack,
                        radius=(
                            "NA"
                            if summary["sender_radius_m"] is None
                            else f"{summary['sender_radius_m']:.1f}"
                        ),
                        tx=summary["tx_noise_messages"],
                        attacked=summary["attacked_messages"],
                        rx=summary["rx_to_target"],
                        delivery=(
                            "NA"
                            if summary["delivery_rate"] is None
                            else f"{summary['delivery_rate']:.3f}"
                        ),
                        dist=format_latency(summary["distance_mean_m"]),
                        mean=format_latency(summary["latency_mean_ms"]),
                        p95=format_latency(summary["latency_p95_ms"]),
                        max_latency=format_latency(summary["latency_max_ms"]),
                    )
                )
                print_metric_trace(metric_rows_for_csv(result), args.trace_messages)
    finally:
        client.close()
        if metsr is not None:
            metsr.close()

    write_csv(args.csv, rows)
    write_csv(args.message_csv, message_rows)
    print_communication_table(message_rows, args.table_rows)
    latency_means = [
        row["latency_mean_ms"] for row in rows if row["latency_mean_ms"] is not None
    ]
    if latency_means:
        print(
            "summary ticks={ticks} target={target} senders={senders} "
            "messages_per_sender={mps} avg_mean_ms={avg} worst_tick_mean_ms={worst}".format(
                ticks=len(rows),
                target=args.target_id,
                senders=args.noise_senders,
                mps=args.messages_per_sender,
                avg=format_latency(sum(latency_means) / len(latency_means)),
                worst=format_latency(max(latency_means)),
            )
        )
    else:
        print(
            "summary ticks={ticks} target={target} no latency values were returned "
            "for the target vehicle".format(ticks=len(rows), target=args.target_id)
        )


if __name__ == "__main__":
    main()
