"""Client-side bridge for coupling METS-R with an OMNeT++/VEINS sidecar.

The bridge intentionally uses a small JSON-lines protocol.  METS-R remains the
mobility/control authority; OMNeT++/VEINS receives vehicle states and intended
BSMs, then returns the packets that were actually delivered by the network
simulation.
"""

import json
import os
import socket
import subprocess
import time
from collections.abc import Mapping, Sequence


PROTOCOL_NAME = "metsr-veins-jsonl"
PROTOCOL_VERSION = 1


def _config_get(config, name, default=None):
    if config is None:
        return default
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _is_sequence(value):
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _clean_record(record):
    return {key: value for key, value in record.items() if value is not None}


class VeinsConnectionError(RuntimeError):
    """Raised when the VEINS sidecar process cannot be reached."""


class VeinsProtocolError(RuntimeError):
    """Raised when the VEINS sidecar returns a malformed response."""


def build_mobility_records(vehicle_records, private_veh=False, sensor_type=None):
    """Convert METS-R vehicle query records to VEINS bridge mobility records."""
    vehicles = _as_list(vehicle_records)
    if _is_sequence(private_veh):
        private_flags = list(private_veh)
    else:
        private_flags = [private_veh] * len(vehicles)

    records = []
    for index, vehicle in enumerate(vehicles):
        if not isinstance(vehicle, Mapping):
            continue
        private_flag = private_flags[index] if index < len(private_flags) else False
        vehicle_id = vehicle.get("vehicle_id", vehicle.get("vid", vehicle.get("ID")))
        if vehicle_id is None:
            continue
        records.append(
            _clean_record(
                {
                    "vehicle_id": vehicle_id,
                    "private_veh": bool(vehicle.get("private_veh", private_flag)),
                    "vehicle_type": vehicle.get("v_type", vehicle.get("vehicle_type")),
                    "x": vehicle.get("x"),
                    "y": vehicle.get("y"),
                    "z": vehicle.get("z", 0.0),
                    "speed_mps": vehicle.get("speed", vehicle.get("speed_mps")),
                    "heading_deg": vehicle.get("bearing", vehicle.get("heading_deg")),
                    "acceleration_mps2": vehicle.get(
                        "acc", vehicle.get("acceleration_mps2")
                    ),
                    "road_id": vehicle.get("road", vehicle.get("road_id")),
                    "lane_id": vehicle.get("lane", vehicle.get("lane_id")),
                    "sensor_type": vehicle.get("sensor_type", sensor_type),
                    "state": vehicle.get("state"),
                }
            )
        )
    return records


def build_bsm_records(vehicle_records, tick=None, private_veh=False, sensor_type=None):
    """Build SAE J2735-aligned BSM semantic records from METS-R vehicle states."""
    mobility_records = build_mobility_records(
        vehicle_records,
        private_veh=private_veh,
        sensor_type=sensor_type,
    )
    sec_mark = None if tick is None else int(tick) % 60000
    msg_count = None if tick is None else int(tick) % 128

    records = []
    for vehicle in mobility_records:
        records.append(
            _clean_record(
                {
                    "tick": tick,
                    "vehicle_id": vehicle.get("vehicle_id"),
                    "vid": vehicle.get("vehicle_id"),
                    "private_veh": vehicle.get("private_veh"),
                    "message_name": "BasicSafetyMessage",
                    "message_standard": "SAE J2735-aligned",
                    "message_count": msg_count,
                    "sec_mark": sec_mark,
                    "x": vehicle.get("x"),
                    "y": vehicle.get("y"),
                    "z": vehicle.get("z"),
                    "speed_mps": vehicle.get("speed_mps"),
                    "speed": vehicle.get("speed_mps"),
                    "heading_deg": vehicle.get("heading_deg"),
                    "heading": vehicle.get("heading_deg"),
                    "acceleration_mps2": vehicle.get("acceleration_mps2"),
                    "road_id": vehicle.get("road_id"),
                    "lane_id": vehicle.get("lane_id"),
                    "sensor_type": vehicle.get("sensor_type"),
                }
            )
        )
    return records


class VeinsClient:
    """TCP JSON-lines client for an OMNeT++/VEINS network sidecar."""

    def __init__(
        self,
        config=None,
        host=None,
        port=None,
        command=None,
        cwd=None,
        auto_launch=None,
        connect_timeout=None,
        request_timeout=None,
        retry_interval=None,
        verbose=None,
    ):
        self.config = config
        self.host = host or _config_get(config, "veins_host", "127.0.0.1")
        self.port = int(port or _config_get(config, "veins_port", 9099))
        self.command = command or _config_get(config, "veins_command")
        self.cwd = cwd or _config_get(config, "veins_cwd")
        self.auto_launch = (
            bool(_config_get(config, "veins_auto_launch", False))
            if auto_launch is None
            else bool(auto_launch)
        )
        self.connect_timeout = float(
            connect_timeout
            if connect_timeout is not None
            else _config_get(config, "veins_connect_timeout", 20)
        )
        self.request_timeout = float(
            request_timeout
            if request_timeout is not None
            else _config_get(config, "veins_request_timeout", 10)
        )
        self.retry_interval = float(
            retry_interval
            if retry_interval is not None
            else _config_get(config, "veins_retry_interval", 0.25)
        )
        self.verbose = bool(
            _config_get(config, "verbose", False) if verbose is None else verbose
        )
        self.process = None
        self.socket = None
        self.reader = None
        self.request_id = 0
        self.bridge_info = {}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    @property
    def connected(self):
        return self.socket is not None

    def launch(self):
        """Launch the configured OMNeT++ sidecar command."""
        if not self.command:
            raise VeinsConnectionError(
                "No veins_command was configured for launching the VEINS sidecar."
            )
        if self.process is not None and self.process.poll() is None:
            return self.process

        cwd = os.path.abspath(self.cwd) if self.cwd else None
        shell = isinstance(self.command, str)
        self.process = subprocess.Popen(self.command, cwd=cwd, shell=shell)
        return self.process

    def connect(self):
        """Connect to the VEINS bridge, optionally launching it first."""
        if self.connected:
            return
        if self.auto_launch:
            self.launch()

        deadline = time.time() + self.connect_timeout
        last_error = None
        while time.time() <= deadline:
            try:
                sock = socket.create_connection(
                    (self.host, self.port),
                    timeout=min(max(self.retry_interval, 1.0), self.connect_timeout),
                )
                sock.settimeout(self.request_timeout)
                self.socket = sock
                self.reader = sock.makefile("r", encoding="utf-8", newline="\n")
                self.bridge_info = self.hello()
                return
            except OSError as exc:
                last_error = exc
                self.close(close_process=False)
                time.sleep(self.retry_interval)

        raise VeinsConnectionError(
            f"Could not connect to VEINS sidecar at {self.host}:{self.port}"
        ) from last_error

    def hello(self):
        return self.request(
            "hello",
            protocol=PROTOCOL_NAME,
            version=PROTOCOL_VERSION,
        )

    def ping(self):
        return self.request("ping")

    def reset(self, **fields):
        return self.request("reset", **fields)

    def update_mobility(self, tick, vehicles):
        return self.request(
            "update_mobility",
            tick=int(tick),
            vehicles=list(vehicles or []),
        )

    def inject_bsm(self, tick, messages):
        return self.request(
            "inject_bsm",
            tick=int(tick),
            messages=list(messages or []),
        )

    def inject_attacks(self, tick, attacks):
        return self.request(
            "inject_attacks",
            tick=int(tick),
            attacks=list(attacks or []),
        )

    def step_network(self, tick, duration_s=None):
        message = {"tick": int(tick)}
        if duration_s is not None:
            message["duration_s"] = float(duration_s)
        return self.request("step_network", **message)

    def sync_tick(
        self,
        tick,
        vehicles,
        bsm_messages=None,
        attacks=None,
        duration_s=None,
    ):
        """Advance the network sidecar for one METS-R tick."""
        message = {
            "tick": int(tick),
            "vehicles": list(vehicles or []),
            "bsm_messages": list(bsm_messages or []),
        }
        if attacks:
            message["attacks"] = list(attacks)
        if duration_s is not None:
            message["duration_s"] = float(duration_s)

        response = self.request("sync_tick", **message)
        data = response.get("data", response)
        return {
            "received_bsms": data.get("received_bsms", data.get("rx_bsms", [])),
            "link_metrics": data.get("link_metrics", data.get("metrics", [])),
            "attack_events": data.get("attack_events", []),
            "bridge_backend": data.get(
                "bridge_backend", response.get("bridge_backend")
            ),
            "backend_implementation": data.get(
                "backend_implementation", response.get("backend_implementation")
            ),
            "bridge_model": data.get("bridge_model", response.get("bridge_model")),
            "network_model": data.get("network_model", response.get("network_model")),
            "radio_access": data.get("radio_access", response.get("radio_access")),
            "backend_note": data.get("backend_note", response.get("backend_note")),
            "raw": response,
        }

    def request(self, message_type, **fields):
        self.connect()
        self.request_id += 1
        message = {
            "type": message_type,
            "request_id": self.request_id,
            **fields,
        }
        if self.verbose:
            print("VEINS SENT:", message)
        self._write_json(message)
        response = self._read_json()
        if self.verbose:
            print("VEINS RECEIVED:", response)
        self._validate_response(message, response)
        return response

    def _write_json(self, message):
        raw = json.dumps(message, separators=(",", ":")) + "\n"
        try:
            self.socket.sendall(raw.encode("utf-8"))
        except OSError as exc:
            self.close(close_process=False)
            raise VeinsConnectionError("Failed to write to VEINS sidecar.") from exc

    def _read_json(self):
        try:
            line = self.reader.readline()
        except OSError as exc:
            self.close(close_process=False)
            raise VeinsConnectionError("Failed to read from VEINS sidecar.") from exc
        if not line:
            self.close(close_process=False)
            raise VeinsConnectionError("VEINS sidecar closed the connection.")
        try:
            return json.loads(line)
        except json.JSONDecodeError as exc:
            raise VeinsProtocolError(
                f"VEINS sidecar returned invalid JSON: {line!r}"
            ) from exc

    def _validate_response(self, request, response):
        if not isinstance(response, Mapping):
            raise VeinsProtocolError("VEINS sidecar response must be a JSON object.")
        request_id = response.get("request_id")
        if request_id is not None and request_id != request.get("request_id"):
            raise VeinsProtocolError(
                f"VEINS sidecar returned request_id={request_id}, "
                f"expected {request.get('request_id')}."
            )
        status = response.get("status", "ok")
        if status not in ("ok", "OK", "success"):
            message = response.get("message", response.get("error", response))
            raise VeinsProtocolError(f"VEINS sidecar rejected request: {message}")

    def close(self, close_process=True):
        if self.reader is not None:
            try:
                self.reader.close()
            except OSError:
                pass
            self.reader = None
        if self.socket is not None:
            try:
                self.socket.close()
            except OSError:
                pass
            self.socket = None
        if close_process and self.process is not None:
            try:
                self.process.terminate()
            except OSError:
                pass
            self.process = None


def create_v2x_client(config=None, **overrides):
    """Create the real OMNeT++/VEINS bridge client."""
    if bool(_config_get(config, "veins_use_local_fallback", False)):
        raise ValueError(
            "The local V2X fallback has been removed. Start an OMNeT++/VEINS "
            "bridge and connect with VeinsClient instead."
        )
    if overrides.pop("use_local_fallback", False):
        raise ValueError(
            "The local V2X fallback has been removed. Start an OMNeT++/VEINS "
            "bridge and connect with VeinsClient instead."
        )
    return VeinsClient(config=config, **overrides)
