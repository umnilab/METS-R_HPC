"""Measure Veins latency when noise messages converge on one receiver.

This example assumes the OMNeT++ bridge is already listening on the configured
host/port. It does not start METS-R, SUMO, a Python sidecar, or a local range
fallback. The script sends synthetic vehicle positions and a burst of noise
messages targeted at one receiver, then summarizes the latency values returned
by the bridge.
"""

import argparse
import csv
import math
import os
import sys


_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from clients.VeinsClient import VeinsClient, build_mobility_records
from utils.util import read_run_config


def get_arguments(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Send many synthetic V2X noise messages to one target vehicle and "
            "measure latency reported by an OMNeT++/Veins bridge."
        )
    )
    parser.add_argument(
        "-r",
        "--run_config",
        default="configs/run_v2x_veins_Template.json",
        help="Run config containing veins_host/veins_port.",
    )
    parser.add_argument("--host", default=None, help="Override Veins bridge host.")
    parser.add_argument("--port", type=int, default=None, help="Override Veins bridge port.")
    parser.add_argument("--ticks", type=int, default=100)
    parser.add_argument("--duration_s", type=float, default=0.1)
    parser.add_argument("--target_id", type=int, default=1)
    parser.add_argument("--noise_senders", type=int, default=40)
    parser.add_argument("--messages_per_sender", type=int, default=5)
    parser.add_argument("--sender_radius_m", type=float, default=80.0)
    parser.add_argument("--payload_bytes", type=int, default=300)
    parser.add_argument("--csv", default=None, help="Optional CSV output path.")
    parser.add_argument("--print_every", type=int, default=10)
    parser.add_argument("--connect_timeout", type=float, default=None)
    parser.add_argument("--request_timeout", type=float, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


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
                "x": args.sender_radius_m * math.cos(angle),
                "y": args.sender_radius_m * math.sin(angle),
                "z": 0.0,
                "speed": 0.0,
                "bearing": math.degrees(angle) % 360.0,
                "sensor_type": "cv2x",
            }
        )
    return vehicles


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
                    "tick": tick,
                    "vehicle_id": sender_id,
                    "sender_id": sender_id,
                    "receiver_id": args.target_id,
                    "target_vehicle_id": args.target_id,
                    "message_name": "NoiseMessage",
                    "message_standard": "synthetic-load",
                    "message_count": (tick + sequence + message_index) % 128,
                    "payload_bytes": args.payload_bytes,
                    "x": vehicle["x"],
                    "y": vehicle["y"],
                    "z": vehicle.get("z", 0.0),
                    "speed_mps": vehicle.get("speed", 0.0),
                    "heading_deg": vehicle.get("bearing", 0.0),
                }
            )
    return messages


def latency_values_for_target(result, target_id):
    received = [
        row
        for row in result.get("received_bsms", [])
        if row.get("receiver_id", row.get("target_vehicle_id")) == target_id
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
        if row.get("receiver_id", row.get("target_vehicle_id")) == target_id
        and row.get("latency_ms") is not None
        and row.get("delivered", True)
    ]
    return [float(row["latency_ms"]) for row in metrics], metrics


def summarize_tick(tick, transmitted, result, target_id):
    latencies, latency_rows = latency_values_for_target(result, target_id)
    summary = {
        "tick": tick,
        "tx_noise_messages": transmitted,
        "rx_to_target": len(latency_rows),
        "link_metrics": len(result.get("link_metrics", [])),
        "latency_mean_ms": None,
        "latency_p50_ms": None,
        "latency_p95_ms": None,
        "latency_max_ms": None,
    }
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


def bridge_model_name(result):
    raw = result.get("raw", {})
    if not isinstance(raw, dict):
        return None
    data = raw.get("data", {})
    if isinstance(data, dict):
        return data.get("bridge_model")
    return raw.get("bridge_model")


def format_latency(value):
    return "NA" if value is None else f"{value:.3f}"


def write_csv(path, rows):
    if not path or not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    args = get_arguments(sys.argv[1:] if argv is None else argv)
    config = read_run_config(args.run_config) if args.run_config else None

    client = VeinsClient(
        config=config,
        host=args.host,
        port=args.port,
        connect_timeout=args.connect_timeout,
        request_timeout=args.request_timeout,
        verbose=args.verbose,
    )
    rows = []
    reported_model = False
    try:
        client.connect()
        print(f"connected_to_veins_bridge host={client.host} port={client.port}")
        for tick in range(args.ticks):
            vehicle_records = make_vehicle_records(args, tick)
            mobility = build_mobility_records(vehicle_records)
            messages = make_noise_messages(args, vehicle_records, tick)
            result = client.sync_tick(
                tick=tick,
                vehicles=mobility,
                bsm_messages=messages,
                duration_s=args.duration_s,
            )
            if not reported_model:
                model = bridge_model_name(result)
                if model:
                    print(f"bridge_model={model}")
                    reported_model = True
            summary = summarize_tick(
                tick=tick,
                transmitted=len(messages),
                result=result,
                target_id=args.target_id,
            )
            rows.append(summary)
            if args.print_every > 0 and (tick % args.print_every == 0 or tick == args.ticks - 1):
                print(
                    "tick={tick} tx={tx} rx_target={rx} mean_ms={mean} "
                    "p95_ms={p95} max_ms={max_latency}".format(
                        tick=summary["tick"],
                        tx=summary["tx_noise_messages"],
                        rx=summary["rx_to_target"],
                        mean=format_latency(summary["latency_mean_ms"]),
                        p95=format_latency(summary["latency_p95_ms"]),
                        max_latency=format_latency(summary["latency_max_ms"]),
                    )
                )
    finally:
        client.close()

    write_csv(args.csv, rows)
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
