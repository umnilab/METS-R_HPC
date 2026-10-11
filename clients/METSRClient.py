"""METS-R client connection, requests, configuration, profiling, and visualization.

Connection, configuration, requests, and profiling live explicitly in this class.
Visualization methods live in ``utils.visualization.client``; query and control
command APIs remain grouped by topic under ``models``.

Acknowledgement: Eric Vin for helping with the revision of the code.
"""

import inspect
import json
import os
import socket
import threading
import time
import uuid
from datetime import datetime

from websockets.sync.client import connect

from models.control_APIs import ControlAPIs
from models.query_APIs import QueryAPIs
from utils.configuration import (
    _normalize_config_json_path,
    _config_signature_from_path,
    _port_from_config,
    _sim_folder_from_config,
    read_run_config,
)
from utils.primitives import (
    VEHICLE_SENSOR_CV2X,
    VEHICLE_SENSOR_DSRC,
    VEHICLE_SENSOR_MOBILE_DEVICE,
    VEHICLE_SENSOR_TYPES,
)
from utils.simulation_setup import prepare_sim_dirs
from utils.simulation_launch import run_simulation_in_docker
from utils.simulation_diagnostics import read_fatal_simulator_error, read_simulator_startup_timings
from utils.visualization.registry import register_metsr_client, unregister_metsr_client
from utils.visualization.client import VisualizationMixin


class METSRClient(QueryAPIs, ControlAPIs, VisualizationMixin):
    """WebSocket client for the native METS-R SIM API.

    Construction connects and waits for ``ready`` (or a late ``step`` heartbeat).
    Query/control methods call ``send_receive_msg``, which serializes requests
    under a lock and handles matching responses and retries. Restart preserves
    visualization settings; ``close`` releases the connection and client servers.

    Requests use ``messageType``/``data``; responses add ``status`` and canonical
    camelCase fields. Retired TYPE/DATA/CODE and schemaVersion envelopes are
    unsupported.
    """

    SENSOR_DSRC = VEHICLE_SENSOR_DSRC
    SENSOR_CV2X = VEHICLE_SENSOR_CV2X
    SENSOR_MOBILE_DEVICE = VEHICLE_SENSOR_MOBILE_DEVICE
    VEHICLE_SENSOR_TYPES = VEHICLE_SENSOR_TYPES

    def __init__(
        self,
        host,
        port,
        sim_folder=None,
        manager=None,
        max_connection_attempts=5,
        timeout=30,
        verbose=False,
        connection_retry_interval=0.5,
        connection_open_timeout=1,
        max_connection_wait=None,
        config_json=None,
        run_config=None,
        config=None,
        sim_index=0,
    ):
        super().__init__()
        self.sim_folder = sim_folder
        self.manager = manager

        self.config_json = self.config_signature = self.config = None
        self._client_config_values = {}
        self.sim_index = int(sim_index)
        self._remember_config(config_json or run_config, config=config)
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self._connection_settings = {
            "max_connection_attempts": max_connection_attempts,
            "connection_retry_interval": connection_retry_interval,
            "connection_open_timeout": connection_open_timeout,
            "max_connection_wait": max_connection_wait,
        }

        # Socket and protocol state are ready before the startup handshake.
        self.ws = None
        self.state = "connecting"
        self.timeout = timeout  # time out for resending the same message if no response
        self.verbose = verbose
        self._messagesLog = []
        self._last_fatal_log_check = float("-inf")
        self._cached_fatal_log_error = None
        self._last_response_bytes = 0
        self._capabilities_cache = None
        self._optimized_api_available = None
        self._feature_support = {}
        self._legacy_command_results = {}
        self.websocket_max_size = max(
            1024,
            int(self._client_config_value("websocket_max_size", 10 * 1024 * 1024)),
        )
        self.current_tick = None
        self.lock = threading.Lock()

        # Supporting services record state without opening servers.
        self._profile_lock = threading.Lock()
        self.rpc_profile = {
            "rpc_count": 0,
            "request_bytes": 0,
            "response_bytes": 0,
            "json_encode_time": 0.0,
            "json_decode_time": 0.0,
            "socket_wait_time": 0.0,
            "server_tick_time": 0.0,
            "server_ticks": 0,
        }
        # Readable alias for dashboards and lightweight experiment loggers.
        self.profiling = self.rpc_profile
        self.startup_timings = {
            "connection_seconds": None,
            "network_load_seconds": None,
            "fleet_spawn_seconds": None,
        }

        self._initialize_visualization()

        # The same handshake and cache reset serve initial startup and reconnects.
        self._connect(**self._connection_settings)
        register_metsr_client(self)

    # Configuration and simulation step size
    # --------------------------------------

    def _client_config_value(self, name, default=None):
        if self.config is not None:
            value = getattr(self.config, name, None)
            if value is not None:
                return value
        value = self._client_config_values.get(name)
        return default if value is None else value

    def _configured_sim_step_size(self):
        configured = self._client_config_value("sim_step_size", None)
        if configured is not None:
            value = float(configured)
        else:
            value = None
            candidates = []
            if self.sim_folder:
                candidates.extend((
                    os.path.join(self.sim_folder, "data", "Data.properties"),
                    os.path.join(self.sim_folder, "Data.properties"),
                ))
            for properties_path in candidates:
                try:
                    with open(properties_path, "r", encoding="utf-8") as properties:
                        for line in properties:
                            key, separator, raw_value = line.partition("=")
                            if separator and key.strip() == "SIMULATION_STEP_SIZE":
                                value = float(raw_value.strip())
                                break
                except (OSError, TypeError, ValueError):
                    continue
                if value is not None:
                    break
        if value is None:
            raise RuntimeError(
                "Simulation step size is unavailable; pass config/config_json "
                "with sim_step_size or provide sim_folder/data/Data.properties"
            )
        if value <= 0.0:
            raise ValueError("sim_step_size must be positive")
        return value

    def _remember_config(self, config_json=None, config_signature=None, config=None):
        if config_json is not None:
            self.config_json = _normalize_config_json_path(config_json)
        if config_signature is not None or config_json is not None:
            self.config_signature = (
                config_signature
                if config_signature is not None
                else _config_signature_from_path(self.config_json)
            )
        if config is not None:
            self.config = config
            try:
                self._client_config_values = dict(vars(config))
            except TypeError:
                self._client_config_values = {}
        elif self.config_signature:
            try:
                self._client_config_values = json.loads(self.config_signature)
            except (TypeError, ValueError):
                self._client_config_values = {}

    # Connection and readiness
    # ------------------------

    def _connect(
            self,
            max_connection_attempts = 5,
            connection_retry_interval = 0.5,
            connection_open_timeout = 1,
            max_connection_wait = None):
        """Open a socket with bounded retries, then await simulator readiness."""
        self._cached_fatal_log_error = None
        self._last_fatal_log_check = float("-inf")
        self._capabilities_cache = None
        self._optimized_api_available = None
        self._feature_support.clear()
        connection_profile_start = time.perf_counter()
        connection_start = time.time()
        max_connection_wait = (
            max_connection_wait
            if max_connection_wait is not None
            else max_connection_attempts * 10
        )
        failed_attempts = 0
        while True:
            try:
                self.ws = connect(
                    self.uri,
                    max_size = self.websocket_max_size,
                    ping_interval = None,
                    ping_timeout = None,
                    open_timeout = connection_open_timeout,
                )
                self.state = "connected"
                if self.verbose:
                    print(f"Connected to {self.uri}")
                break
            except OSError as exc:
                failed_attempts += 1
                elapsed = time.time() - connection_start
                if elapsed >= max_connection_wait:
                    self.state = "failed"
                    raise RuntimeError(
                        f"Could not connect to METS-R SIM at {self.uri} "
                        f"after {elapsed:.1f} seconds and {failed_attempts} attempts"
                    ) from exc

                sleep_seconds = min(connection_retry_interval, max_connection_wait - elapsed)
                if self.verbose:
                    print(
                        f"Attempt {failed_attempts} to connect to {self.uri} failed. "
                        f"Retrying in {sleep_seconds:.1f} seconds..."
                    )
                time.sleep(sleep_seconds)

        print("Connection established!")
        self._await_simulator_ready()
        self.startup_timings["connection_seconds"] = (
            time.perf_counter() - connection_profile_start
        )
        self.startup_timings.update(read_simulator_startup_timings(self.sim_folder))

        # Record initial startup phases without overwriting them on reconnect.
        handles = getattr(self.config, "launch_handles", None) if self.config else None
        if not handles:
            return
        try:
            handle = handles[self.sim_index]
        except (IndexError, KeyError, TypeError):
            return
        recorder = getattr(handle, "record_timing", None)
        if not callable(recorder):
            return
        existing_timings = getattr(handle, "_timings", None)
        if not isinstance(existing_timings, dict):
            try:
                candidate = getattr(handle, "timings", None)
            except Exception:
                candidate = None
            existing_timings = candidate if isinstance(candidate, dict) else {}
        for phase, key in (
                ("connection", "connection_seconds"),
                ("network_load", "network_load_seconds"),
                ("fleet_spawn", "fleet_spawn_seconds")):
            value = self.startup_timings.get(key)
            if (
                    value is not None
                    and existing_timings.get(phase) is None):
                recorder(phase, value)

    def _await_simulator_ready(self):
        """Wait for startup readiness or a valid late-connection heartbeat."""
        ready_timeout = float(
            self._client_config_value("startup_ready_timeout", self.timeout)
        )
        ready_timeout = max(0.1, ready_timeout)
        try:
            response = self.receive_msg(
                ignore_heartbeats=False,
                waiting_forever=False,
                return_ready=True,
                print_timeout=False,
                timeout=ready_timeout,
            )
            if response is None:
                raise TimeoutError(
                    f"METS-R SIM accepted the WebSocket connection at {self.uri} "
                    f"but did not report ready or step within "
                    f"{ready_timeout:.1f} seconds"
                )
            message_type = response.get("messageType")
            ready = message_type == "ready"
            late_connection_heartbeat = (
                message_type == "step"
                and response.get("status") == "ok"
                and response.get("tick") is not None
            )
            if not ready and not late_connection_heartbeat:
                raise RuntimeError(
                    "Expected METS-R SIM ready response or step heartbeat, received "
                    + str(message_type)
                )
            return response
        except Exception:
            self.state = "failed"
            if getattr(self, "ws", None) is not None:
                self.ws.close()
                self.ws = None
            raise

    # Request / response workflow
    # ---------------------------

    def send_msg(self, msg):
        """Validate and send a simulator request, recording transport statistics."""
        if not isinstance(msg, dict):
            raise TypeError("METS-R SIM requests must be mappings")
        retired = sorted({"TYPE", "DATA", "CODE"}.intersection(msg))
        if retired:
            raise ValueError(
                "METS-R SIM V1 fields are no longer supported: "
                + ", ".join(retired)
            )
        if "schemaVersion" in msg or "schema_version" in msg:
            raise ValueError(
                "METS-R SIM no longer accepts a schemaVersion selector"
            )
        operation = msg.get("messageType")
        if not isinstance(operation, str) or not operation:
            raise ValueError("METS-R SIM requests require a messageType")
        if self.verbose:
            self._logMessage("SENT", msg)
        encode_start = time.perf_counter()
        payload = json.dumps(msg)
        encode_time = time.perf_counter() - encode_start
        request_bytes = len(payload.encode("utf-8"))
        self._profile_add(
            rpc_count=1,
            request_bytes=request_bytes,
            json_encode_time=encode_time,
        )
        self.ws.send(payload)

    def receive_msg(
            self,
            ignore_heartbeats,
            waiting_forever = True,
            return_ready = False,
            print_timeout = True,
            timeout = None,
            return_errors = False):
        """Receive a simulator response, handling heartbeats, errors, and timeouts."""
        timeout = self.timeout if timeout is None else timeout
        start_time = time.time()
        while True:
            fatal_error = self._fatal_log_error()
            if fatal_error:
                raise RuntimeError(fatal_error)
            try:
                socket_wait_start = time.perf_counter()
                try:
                    raw_msg = self.ws.recv(timeout = min(5, max(0.1, timeout)))
                finally:
                    self._profile_add(
                        socket_wait_time=time.perf_counter() - socket_wait_start
                    )

                if isinstance(raw_msg, bytes):
                    response_bytes = len(raw_msg)
                    raw_text = raw_msg.decode("utf-8")
                else:
                    raw_text = str(raw_msg)
                    response_bytes = len(raw_text.encode("utf-8"))
                self._last_response_bytes = response_bytes
                decode_start = time.perf_counter()
                try:
                    msg = json.loads(raw_text)
                finally:
                    self._profile_add(
                        response_bytes=response_bytes,
                        json_decode_time=time.perf_counter() - decode_start,
                    )

                if self.verbose:
                    self._logMessage("RECEIVED", msg)
                if not isinstance(msg, dict):
                    raise RuntimeError("METS-R SIM response must be a JSON object")
                retired = sorted({"TYPE", "DATA", "CODE"}.intersection(msg))
                if retired:
                    raise RuntimeError(
                        "METS-R SIM returned retired V1 fields: "
                        + ", ".join(retired)
                    )
                if "schemaVersion" in msg or "schema_version" in msg:
                    raise RuntimeError(
                        "METS-R SIM returned a retired schemaVersion envelope"
                    )
                msg_type = msg.get("messageType")
                if not isinstance(msg_type, str) or not msg_type:
                    raise RuntimeError(
                        "METS-R SIM response is missing messageType"
                    )
                status = msg.get("status")
                if status not in {"ok", "partial", "error"}:
                    raise RuntimeError(
                        "METS-R SIM response has an invalid or missing status"
                    )

                self._update_current_tick_from_message(msg)

                if msg_type == "ready":
                    if self.current_tick is None:
                        self.current_tick = 0
                    if return_ready:
                        return msg
                    continue

                if msg_type == "error" and not return_errors:
                    print(f"Error: {msg.get('message', 'Unknown METS-R SIM error')}")
                    return None

                if not ignore_heartbeats or msg_type != "step":
                    return msg
            except KeyboardInterrupt:
                raise
            except TimeoutError:
                fatal_error = self._fatal_log_error(force=True)
                if fatal_error:
                    raise RuntimeError(fatal_error)
            except Exception as exc:
                fatal_error = self._fatal_log_error(force=True)
                if fatal_error:
                    raise RuntimeError(fatal_error) from exc
                self.state = "failed"
                raise RuntimeError(
                    f"Error while receiving message from METS-R SIM at "
                    f"{self.uri}: {exc}"
                ) from exc

            if time.time() - start_time > timeout and not waiting_forever:
                if print_timeout:
                    print("Timeout while waiting for message.")
                return None

    def send_receive_msg(
            self,
            msg,
            ignore_heartbeats,
            max_attempts=5,
            timeout=None,
            return_errors=False,
            command_id=None,
            response_matcher=None):
        """Send a request and await a matching response with retries under the client lock."""
        # Only advanceAndSnapshot supports command-ID deduplication. Reuse the
        # same ID and request through all retries without modifying caller data.
        if msg.get("messageType") == "advanceAndSnapshot" and max_attempts != 1:
            msg = dict(msg)
            data = dict(msg.get("data") or {})
            if command_id is None:
                command_id = data.get("commandId")
            if command_id is None:
                command_id = uuid.uuid4().hex
            data.setdefault("commandId", str(command_id))
            msg["data"] = data
        response_timeout = max(0.1, float(self.timeout if timeout is None else timeout))
        bounded = max_attempts > 0
        receive_options = dict(
            ignore_heartbeats=ignore_heartbeats,
            waiting_forever=not bounded,
            timeout=timeout,
            return_errors=return_errors,
        )
        if bounded:
            receive_options["print_timeout"] = False
        with self.lock:
            attempts = 0
            try:
                while True:
                    attempts += 1
                    self.send_msg(msg)
                    deadline = time.monotonic() + response_timeout if bounded else None
                    while True:
                        if bounded:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                break
                            receive_options["timeout"] = remaining
                        response = self.receive_msg(**receive_options)
                        if bounded and response is None:
                            break
                        if (
                                isinstance(response, dict)
                                and response.get("messageType") == "advanceAndSnapshot"
                                and msg.get("messageType") != "advanceAndSnapshot"):
                            continue
                        if response_matcher is not None and not response_matcher(response):
                            continue
                        if response is not None:
                            return response
                        break
                    if bounded and attempts >= max_attempts:
                        raise TimeoutError(
                            f"No response received for '{msg.get('messageType', 'unknown')}' "
                            f"after {max_attempts} attempts; last STEP tick seen was {self.current_tick}"
                        )
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt detected. Stopping the current operation but keeping the server active.")
                return None

    # Simulator restart and client cleanup
    # ------------------------------------

    def reset(self, config_json=None):
        """Reset the simulation, restarting it when a different config is supplied."""
        restart_required = False
        if config_json is not None:
            config_json_path = _normalize_config_json_path(config_json)
            config_signature = _config_signature_from_path(config_json_path)
            config = read_run_config(config_json_path)
            if self.config_signature is not None:
                restart_required = config_signature != self.config_signature
            elif self.config_json is not None:
                restart_required = config_json_path != self.config_json
            else:
                target_host = getattr(config, "metsr_host", self.host)
                target_port = _port_from_config(config, self.sim_index, default=self.port)
                restart_required = (
                    str(target_host) != str(self.host)
                    or (target_port is not None and int(target_port) != int(self.port))
                )

        if not restart_required:
            if config_json is not None:
                self._remember_config(config_json_path, config_signature, config)
            res = self.send_receive_msg(
                {"messageType": "reset"}, ignore_heartbeats=True, max_attempts=-1,
            )
            assert res["messageType"] == "reset", res["messageType"]
            assert res["status"] in {"ok", "partial"}, res["status"]
            self._legacy_command_results.clear()
            self._capabilities_cache = None
            with self.viz_stream_lock:
                self._attack_vehicle_keys.clear()
            if "tick" in res:
                self.current_tick = int(res["tick"])
            else:
                self.current_tick = -1
                self.tick()
                assert self.current_tick == 0
            if self.viz_server is not None:
                self.stop_offline_viz()
                time.sleep(1)
                self.start_offline_viz()
            return

        # Prepare replacement inputs before stopping the existing simulation.
        sim_dirs = prepare_sim_dirs(config)
        target_host = getattr(config, "metsr_host", self.host)
        target_port = _port_from_config(config, self.sim_index, default=self.port)
        if target_port is None:
            raise RuntimeError("Restart config does not define a METS-R websocket port.")

        # Preserve each running visualization's original startup arguments.
        live_kwargs = None
        if self.viz_stream_server is not None:
            live_kwargs = dict(self.viz_stream_start_kwargs or {}) or {
                "server_port": self.viz_stream_port or 8765,
                "host": self.viz_stream_host or "0.0.0.0",
            }
        offline_kwargs = None
        if self.viz_server is not None:
            offline_kwargs = dict(self.offline_viz_start_kwargs or {}) or {
                "server_port": self.viz_port or 8000,
            }
        old_host, old_port = self.host, self.port

        # Close the old socket even when the shutdown request fails.
        if self.ws is not None:
            try:
                res = self.send_receive_msg({"messageType": "end"}, ignore_heartbeats=True)
                if res is not None:
                    assert res["messageType"] == "end", res["messageType"]
                    assert res["status"] in {"ok", "partial"}, res["status"]
            finally:
                if self.ws is not None:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                    self.ws = None
                self.state = "closed"

        if str(old_host) in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}:
            deadline = time.time() + 10
            while time.time() <= deadline:
                try:
                    with socket.create_connection(("127.0.0.1", int(old_port)), timeout=0.25):
                        pass
                except OSError:
                    break
                time.sleep(0.2)

        run_simulation_in_docker(config)
        self._legacy_command_results.clear()
        self.host = target_host
        self.port = int(target_port)
        self.uri = f"ws://{self.host}:{self.port}"
        self.sim_folder = _sim_folder_from_config(
            config, sim_dirs=sim_dirs, sim_index=self.sim_index, default=self.sim_folder,
        )
        self.current_tick = None
        self._remember_config(config_json_path, config_signature, config)
        self._connect(**self._connection_settings)
        with self.viz_stream_lock:
            self._attack_vehicle_keys.clear()
        if live_kwargs is not None:
            self.start_viz(**live_kwargs)
        if offline_kwargs is not None:
            self.start_offline_viz(**offline_kwargs)

    def close(self):
        """Close the simulator connection and stop this client's visualization servers."""
        if self.ws is not None:
            self.ws.close()
            self.ws = None
            self.state = "closed"

        if self.viz_server is not None:
            self.stop_offline_viz()

        if self.viz_stream_server is not None:
            self.stop_viz_stream()
        unregister_metsr_client(self)

    # RPC profiling and startup timings
    # ---------------------------------

    def _profile_add(self, **values):
        lock = getattr(self, "_profile_lock", None)
        profile = getattr(self, "rpc_profile", None)
        if lock is None or profile is None:
            return
        with lock:
            for key, value in values.items():
                profile[key] = profile.get(key, 0) + value

    def get_rpc_profile(self, reset=False):
        """Return a stable snapshot of cumulative client-side RPC timings."""
        with self._profile_lock:
            profile = dict(self.rpc_profile)
            if reset:
                for key in self.rpc_profile:
                    self.rpc_profile[key] = (
                        0.0 if key.endswith("_time") else 0
                    )
        profile["total_bytes"] = profile["request_bytes"] + profile["response_bytes"]
        tick_count = profile["server_ticks"]
        profile["average_server_tick_time"] = (
            profile["server_tick_time"] / tick_count if tick_count else 0.0
        )
        return profile

    def get_profile(self, reset=False):
        """Return RPC profiling together with startup phase timings."""
        profile = self.get_rpc_profile(reset=reset)
        profile["startup_timings"] = dict(self.startup_timings)
        profile.update(self.startup_timings)
        return profile

    def reset_rpc_profile(self):
        """Reset RPC byte, count, JSON, socket, and stepping counters."""
        with self._profile_lock:
            for key in self.rpc_profile:
                self.rpc_profile[key] = 0.0 if key.endswith("_time") else 0

    def _record_server_tick_time(self, started_at, starting_tick):
        if self.current_tick is None:
            return
        advanced = max(0, int(self.current_tick) - int(starting_tick))
        if advanced:
            self._profile_add(
                server_tick_time=max(0.0, time.perf_counter() - started_at),
                server_ticks=advanced,
            )

    # Public API discovery
    # --------------------

    @classmethod
    def list_apis(cls, category=None, topic=None):
        """List public APIs and their descriptions without connecting to a simulator.

        Return an alphabetically sorted list of dictionaries containing ``name``,
        ``signature`` (without self/cls), ``description`` (the full docstring),
        ``category``, ``topic``, and the defining ``module``. Compatibility aliases
        are listed separately. Set ``category`` to ``query``, ``control``, or
        ``client`` and/or ``topic`` to a topic such as ``taxi`` to filter results.
        Unknown topics return an empty list; invalid categories raise ValueError.

        This helper can be called on the class or an existing client instance::

            for api in METSRClient.list_apis(category="control", topic="taxi"):
                print(api["name"] + api["signature"], api["description"])
        """
        if category not in (None, "query", "control", "client"):
            raise ValueError("category must be 'query', 'control', 'client', or None")

        apis = []
        for name, method in inspect.getmembers(cls, predicate=inspect.isroutine):
            if name.startswith("_"):
                continue
            module = method.__module__
            api_category = "client"
            api_topic = "client"
            for candidate in ("query", "control"):
                if module.startswith(f"models.{candidate}_APIs."):
                    api_category = candidate
                    api_topic = module.rsplit(".", 1)[-1].removeprefix(candidate + "_")
                    break
            if name == "reset":
                api_category, api_topic = "control", "simulation"
            if name in VisualizationMixin.__dict__:
                api_topic = "visualization"
            if category is not None and api_category != category:
                continue
            if topic is not None and api_topic != topic:
                continue

            signature = inspect.signature(method)
            parameters = list(signature.parameters.values())
            if parameters and parameters[0].name in ("self", "cls"):
                signature = signature.replace(parameters=parameters[1:])
            apis.append({
                "name": name,
                "signature": str(signature),
                "description": inspect.getdoc(method) or "",
                "category": api_category,
                "topic": api_topic,
                "module": module,
            })
        return apis

    # Response bookkeeping and diagnostics
    # ------------------------------------

    def _update_current_tick_from_message(self, msg):
        msg_type = msg.get("messageType")
        if msg_type not in {
                "step", "tick", "load", "reset",
                "advanceAndSnapshot"}:
            return False
        tick_value = msg.get("tick", msg.get("finalTick"))
        if tick_value is None:
            return False
        server_tick = int(tick_value)
        if msg_type in {"load", "reset"}:
            self.current_tick = server_tick
            return True
        if self.current_tick is None or server_tick > int(self.current_tick):
            self.current_tick = server_tick
            return True
        return False

    def _fatal_log_error(self, force=False):
        """Cache fatal diagnostics and limit log reads while waiting for responses."""
        now = time.monotonic()
        if self._cached_fatal_log_error is not None:
            return self._cached_fatal_log_error
        if not force and now - self._last_fatal_log_check < 1.0:
            return None
        self._last_fatal_log_check = now
        self._cached_fatal_log_error = read_fatal_simulator_error(self.sim_folder)
        return self._cached_fatal_log_error

    def _logMessage(self, direction, msg):
        self._messagesLog.append(
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), direction, tuple(msg.items()))
        )
        print(self._messagesLog[-1])

    def __str__(self):
        return (
            "-----------\n"
            "Client INFO\n"
            "-----------\n"
            f"output folder :\t {self.sim_folder}\n"
            f"address :\t {self.uri}\n"
            f"state :\t {self.state}\n"
        )
