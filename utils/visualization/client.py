"""Client visualization: live frames, saved trajectories, and server lifecycle."""

import json
import logging
import os
import socket
import threading
import time

from utils.primitives import _as_list, _broadcast
from .servers import run_visualization_server, stop_visualization_server
from .trajectories import (
    _configured_trajectory_roots,
    _latest_trajectory_directory,
    _read_trajectory_manifest,
    _resolve_trajectory_root,
    _trajectory_format_name,
    _trajectory_format_score,
    _trajectory_manifest_summary,
)
from .protocol import (
    _VIZ_STREAM_DEFAULT_COORD_SCALE,
    _viz_chunk,
    _viz_first,
    _viz_group_vehicle_records,
    _viz_manifest,
    _viz_resolve_origin,
    _viz_stream_record,
    _viz_vehicle_id,
)


def query_record_ids(query_func, key):
    """Read a road/facility dictionary, tolerating unavailable query APIs."""
    try:
        response = query_func()
    except Exception:
        return []
    return [item for item in (response.get(key) or []) if item is not None]


def query_records(query_func, ids, batch_size=1000):
    """Read records in batches, skipping failed batches and malformed records."""
    ids = [item for item in (ids or []) if item is not None]
    if not ids:
        return []

    records = []
    batch_size = max(1, int(batch_size or 1))
    for start in range(0, len(ids), batch_size):
        batch_ids = ids[start:start + batch_size]
        try:
            response = query_func(id=batch_ids)
        except Exception:
            continue
        if response.get("status") == "error":
            continue
        for record in response.get("data", []):
            if isinstance(record, dict):
                records.append(record)
    return records


class VisualizationMixin:
    """Visualization methods inherited by METSRClient without forwarding wrappers.

    Uses the client's simulation folder and query APIs. State stays on the client
    so control APIs can mark attack vehicles and resets can clear those markers.
    """

    def _initialize_visualization(self):
        """Initialize per-client state without starting servers."""
        self.viz_server = None
        self.viz_event = None
        self.viz_port = None

        self.viz_stream_server = None
        self.viz_stream_servers = []
        self.viz_stream_thread = None
        self.viz_stream_threads = []
        self.viz_stream_stop_event = None
        self.viz_stream_host = None
        self.viz_stream_port = None
        self.viz_stream_manifest = None
        self.viz_stream_options = None
        self.viz_stream_start_kwargs = None
        self.viz_stream_clients = []
        self.viz_stream_lock = threading.Lock()
        self.viz_stream_chunk_counter = 0
        self.viz_stream_last_tick = None
        self.viz_stream_last_active_road_ids = set()
        self._attack_vehicle_keys = set()
        self.offline_viz_start_kwargs = None

    # Visualization state and saved trajectories
    # ------------------------------------------

    def latest_trajectory_output_dir(self, trajectory_output_dir=None, prefer_binary=True, wait_seconds=0):
        """Return the newest trajectory output directory for visualization.

        The latest METS-R SIM writes binary trajectory chunks with a
        ``manifest.json`` file by default. Older runs may still write JSON
        chunks, so this method accepts both formats and prefers binary output
        when available.
        """
        if not self.sim_folder:
            raise ValueError("sim_folder is required to locate trajectory output")
        wait_seconds = max(0.0, float(wait_seconds or 0))

        if trajectory_output_dir is not None:
            roots = [_resolve_trajectory_root(self.sim_folder, trajectory_output_dir)]
        else:
            roots = _configured_trajectory_roots(self.sim_folder)

        deadline = time.time() + wait_seconds
        while True:
            candidates = []
            for root in roots:
                latest = _latest_trajectory_directory(root, prefer_binary=prefer_binary)
                if latest is not None:
                    candidates.append(latest)

            if candidates:
                if prefer_binary:
                    binary_candidates = [
                        directory for directory in candidates
                        if _trajectory_format_score(directory) >= 2
                    ]
                    if binary_candidates:
                        return max(binary_candidates, key=os.path.getmtime)
                return max(candidates, key=os.path.getmtime)

            if wait_seconds <= 0 or time.time() >= deadline:
                roots_text = ", ".join(str(root) for root in roots if root)
                raise FileNotFoundError(
                    "No trajectory output directory found under " + roots_text
                )
            time.sleep(0.5)

    def get_trajectory_manifest(self, trajectory_output_dir=None, prefer_binary=True, wait_seconds=0):
        """Return the manifest for the latest binary trajectory output.

        METS-R SIM binary trajectory format v6 writes zone and charging-station
        frames as sparse deltas. This helper exposes the manifest, including
        ``sparseFrameGroups`` and ``sparseFrameGroupMode``, without making
        callers locate or parse ``manifest.json`` manually.
        """
        latest_directory = self.latest_trajectory_output_dir(
            trajectory_output_dir=trajectory_output_dir,
            prefer_binary=prefer_binary,
            wait_seconds=wait_seconds,
        )
        manifest = _read_trajectory_manifest(latest_directory)
        if manifest is None:
            raise FileNotFoundError(
                "No manifest.json found in trajectory output directory: "
                + latest_directory
            )

        manifest = dict(manifest)
        manifest["_directory"] = latest_directory
        manifest["_manifest_path"] = os.path.join(latest_directory, "manifest.json")
        return manifest

    def get_trajectory_summary(self, trajectory_output_dir=None, prefer_binary=True, wait_seconds=0):
        """Return high-level metadata for the latest trajectory output."""
        latest_directory = self.latest_trajectory_output_dir(
            trajectory_output_dir=trajectory_output_dir,
            prefer_binary=prefer_binary,
            wait_seconds=wait_seconds,
        )
        manifest = _read_trajectory_manifest(latest_directory)
        if manifest is not None:
            return _trajectory_manifest_summary(latest_directory, manifest)

        return {
            "directory": latest_directory,
            "format": _trajectory_format_name(latest_directory),
            "version": None,
            "sparse_frame_groups": [],
            "sparse_frame_group_mode": None,
            "has_sparse_frame_groups": False,
            "has_sparse_zone_frames": False,
            "has_sparse_charging_station_frames": False,
            "has_zone_attributes": False,
            "has_charging_station_attributes": False,
            "has_split_energy_fields": False,
        }

    def start_offline_viz(
            self,
            trajectory_output_dir=None,
            server_port=8000,
            prefer_binary=True,
            wait_seconds=30,
            startup_timeout=3):
        """Start an HTTP visualization server for saved trajectory output."""
        start_kwargs = locals().copy()  # Original arguments for restart.
        del start_kwargs["self"]
        if trajectory_output_dir is not None:
            if os.path.isabs(str(trajectory_output_dir)):
                latest_directory = os.path.normpath(str(trajectory_output_dir))
            elif os.path.isdir(str(trajectory_output_dir)):
                latest_directory = os.path.abspath(str(trajectory_output_dir))
            elif self.sim_folder:
                latest_directory = _resolve_trajectory_root(self.sim_folder, trajectory_output_dir)
            else:
                latest_directory = os.path.abspath(str(trajectory_output_dir))

            if not os.path.isdir(latest_directory):
                raise FileNotFoundError("Trajectory output directory does not exist: " + latest_directory)
            if _read_trajectory_manifest(latest_directory) is None:
                raise FileNotFoundError(
                    "No manifest.json found directly inside trajectory output directory: "
                    + latest_directory
                )
        else:
            latest_directory = self.latest_trajectory_output_dir(
                trajectory_output_dir=None,
                prefer_binary=prefer_binary,
                wait_seconds=wait_seconds,
            )

        if self.viz_server is not None:
            self.stop_offline_viz()

        print(
            f"Starting offline visualization server for {_trajectory_format_name(latest_directory)} "
            f"trajectory output: {latest_directory}"
        )
        self.viz_event, self.viz_server = run_visualization_server(latest_directory, server_port)
        self.viz_port = server_port
        try:
            deadline = time.time() + max(0.1, float(startup_timeout or 0.1))
            last_error = None
            while time.time() <= deadline:
                try:
                    with socket.create_connection(("127.0.0.1", int(server_port)), timeout=0.5):
                        break
                except OSError as exc:
                    last_error = exc
                    time.sleep(0.1)
            else:
                raise RuntimeError(
                    f"Visualization server did not become reachable at http://127.0.0.1:{server_port}/"
                ) from last_error
        except Exception:
            self.stop_offline_viz()
            raise
        self.offline_viz_start_kwargs = start_kwargs
        print(f"Visualization files are available at http://127.0.0.1:{server_port}/")
        return {
            "directory": latest_directory,
            "port": server_port,
            "url": f"http://127.0.0.1:{server_port}/",
            "manifest_url": f"http://127.0.0.1:{server_port}/manifest.json",
            "mode": "offline",
        }

    def stop_offline_viz(self, verbose=True):
        """Stop the HTTP server serving this client's saved trajectory output."""
        if self.viz_server is not None:
            stop_visualization_server(self.viz_event, self.viz_server, self.viz_port or 8000, verbose=verbose)
        self.viz_event = None
        self.viz_server = None
        self.viz_port = None
        self.offline_viz_start_kwargs = None

    def stop_viz(self, verbose=True):
        """Stop both live streaming and offline visualization servers."""
        if self.viz_stream_server is not None:
            self.stop_viz_stream()
        if self.viz_server is not None:
            self.stop_offline_viz(verbose=verbose)

    # Live visualization: query a snapshot, publish frames, and manage listeners
    # --------------------------------------------------------------------------

    def _wait_for_viz_stream_server(self, host, server_port, startup_timeout=3):
        from websockets.sync.client import connect

        connect_host = host if host not in ("", "0.0.0.0", "::") else "127.0.0.1"
        uri = f"ws://{connect_host}:{int(server_port)}"
        deadline = time.time() + max(0.1, float(startup_timeout or 0.1))
        last_error = None
        while time.time() <= deadline:
            try:
                try:
                    websocket = connect(uri, open_timeout=0.5)
                except TypeError:
                    websocket = connect(uri)
                with websocket:
                    return
            except Exception as exc:
                last_error = exc
                time.sleep(0.1)
        raise RuntimeError(
            f"METS-R Vis stream server did not become reachable at {uri}"
        ) from last_error

    def _viz_stream_no_client_error(self, wait_seconds=0):
        connect_host = (
            self.viz_stream_host
            if self.viz_stream_host not in (None, "", "0.0.0.0", "::")
            else "127.0.0.1"
        )
        server_port = self.viz_stream_port if self.viz_stream_port is not None else 8765
        url = f"ws://{connect_host}:{int(server_port)}"
        waited = (
            f" within {wait_seconds:g} seconds"
            if wait_seconds and wait_seconds > 0
            else ""
        )
        return (
            f"No METS-R Vis client connected to the live stream at {url}{waited}, "
            "so render() cannot deliver a frame.\n"
            "Fix: open https://engineering.purdue.edu/HSEES/METSRVis/ in a browser, "
            "click Stream, set the WebSocket URL to the URL printed by start_viz() "
            f"({url}), and then run render() again.\n"
            "If the browser is on another machine, restart the stream with "
            f"start_viz(host='0.0.0.0', server_port={int(self.viz_stream_port or 8765)}) "
            "and connect METS-R Vis to ws://<this-machine-ip>:"
            f"{int(self.viz_stream_port or 8765)}."
        )

    def start_viz(
            self,
            server_port=8765,
            host="0.0.0.0",
            tick_interval=1,
            transform_coords=False,
            include_public=True,
            include_private=True,
            vehicle_ids=None,
            private_veh=False,
            public_vehicle_ids=None,
            private_vehicle_ids=None,
            batch_size=1000,
            coord_scale=_VIZ_STREAM_DEFAULT_COORD_SCALE,
            initial_x=0.0,
            initial_y=0.0,
            link_snapshot_interval=1,
            road_id_dictionary=None,
            zone_dictionary=None,
            charging_station_dictionary=None,
            include_links=True,
            include_zones=False,
            include_charging_stations=False,
            link_batch_size=1000,
            facility_batch_size=1000,
            startup_timeout=3):
        """Start a live METS-R Vis WebSocket stream.

        This only opens the WebSocket server and sends the VIS binary manifest
        to each connected browser. Call :meth:`render` whenever you want to
        query METS-R and push a new frame into METSR_VIS.

        `sim_folder` is optional for live streaming. If `initial_x` and
        `initial_y` are both 0 and `sim_folder` is available, they are read
        from `sim_folder/data/Data.properties`; otherwise the provided origin
        values are used as-is.
        """
        start_kwargs = locals().copy()  # Original arguments for restart.
        del start_kwargs["self"]
        try:
            from websockets.sync.server import serve
        except ImportError as exc:
            raise RuntimeError(
                "start_viz requires the 'websockets' package from requirements.txt."
            ) from exc

        for logger_name in ("websockets", "websockets.server", "websockets.sync.server"):
            logging.getLogger(logger_name).setLevel(logging.CRITICAL)

        tick_interval = max(1, int(tick_interval))
        coord_scale = max(1, int(coord_scale))
        link_snapshot_interval = max(1, int(link_snapshot_interval))
        link_batch_size = max(1, int(link_batch_size or 1))
        facility_batch_size = max(1, int(facility_batch_size or 1))
        include_links = bool(include_links)
        include_zones = bool(include_zones)
        include_charging_stations = bool(include_charging_stations)
        initial_x, initial_y = _viz_resolve_origin(self.sim_folder, initial_x, initial_y)

        dictionaries = []
        for dictionary, enabled, query, key in (
            (road_id_dictionary, include_links, self.query_road, "roadIds"),
            (zone_dictionary, include_zones, self.query_zone, "zoneIds"),
            (charging_station_dictionary, include_charging_stations,
             self.query_chargingStation, "chargingStationIds"),
        ):
            if dictionary == "query" or (dictionary is None and enabled):
                dictionary = query_record_ids(query, key)
                if key == "roadIds":
                    dictionary = sorted(str(road_id) for road_id in dictionary)
            dictionaries.append(list(dictionary or []))
        road_id_dictionary, zone_dictionary, charging_station_dictionary = dictionaries
        manifest = _viz_manifest(
            road_id_dictionary,
            coord_scale,
            initial_x,
            initial_y,
            tick_interval,
            link_snapshot_interval,
            zone_dictionary=zone_dictionary,
            charging_station_dictionary=charging_station_dictionary,
        )

        if self.viz_stream_server is not None:
            self.stop_viz_stream()

        stop_event = threading.Event()
        options = {
            "tick_interval": tick_interval,
            "transform_coords": bool(transform_coords),
            "include_public": bool(include_public),
            "include_private": bool(include_private),
            "vehicle_ids": vehicle_ids,
            "private_veh": private_veh,
            "public_vehicle_ids": public_vehicle_ids,
            "private_vehicle_ids": private_vehicle_ids,
            "batch_size": batch_size,
            "coord_scale": coord_scale,
            "initial_x": float(initial_x),
            "initial_y": float(initial_y),
            "link_snapshot_interval": link_snapshot_interval,
            "include_links": include_links,
            "include_zones": include_zones,
            "include_charging_stations": include_charging_stations,
            "link_batch_size": link_batch_size,
            "facility_batch_size": facility_batch_size,
            "road_id_dictionary": road_id_dictionary,
            "road_id_index": {
                str(road_id): index
                for index, road_id in enumerate(road_id_dictionary)
            },
            "zone_dictionary": zone_dictionary,
            "charging_station_dictionary": charging_station_dictionary,
        }

        def handler(websocket):
            try:
                websocket.send(json.dumps({"type": "manifest", "manifest": manifest}))
                with self.viz_stream_lock:
                    if websocket not in self.viz_stream_clients:
                        self.viz_stream_clients.append(websocket)

                while not stop_event.is_set():
                    try:
                        websocket.recv(timeout=0.5)
                    except TimeoutError:
                        continue
                    except Exception:
                        break
            finally:
                with self.viz_stream_lock:
                    if websocket in self.viz_stream_clients:
                        self.viz_stream_clients.remove(websocket)

        servers = []
        server_threads = []

        def start_server(bind_host):
            server = serve(handler, bind_host, int(server_port))
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()
            servers.append(server)
            server_threads.append(server_thread)
            return server, server_thread

        server, server_thread = start_server(host)
        if host in (None, "", "0.0.0.0", "127.0.0.1", "localhost"):
            try:
                start_server("::1")
            except Exception:
                # Optional compatibility binding for browsers that resolve
                # localhost to IPv6 before IPv4 on Windows.
                pass

        self.viz_stream_server = server
        self.viz_stream_servers = servers
        self.viz_stream_thread = server_thread
        self.viz_stream_threads = server_threads
        self.viz_stream_stop_event = stop_event
        self.viz_stream_host = host
        self.viz_stream_port = int(server_port)
        self.viz_stream_manifest = manifest
        self.viz_stream_options = options
        self.viz_stream_chunk_counter = 0
        self.viz_stream_last_tick = None
        self.viz_stream_last_active_road_ids = set()
        with self.viz_stream_lock:
            self.viz_stream_clients = []

        localhost_reachable = False
        try:
            self._wait_for_viz_stream_server(host, server_port, startup_timeout=startup_timeout)
            try:
                localhost_timeout = max(0.1, min(1.0, float(startup_timeout or 1.0)))
                self._wait_for_viz_stream_server(
                    "localhost",
                    server_port,
                    startup_timeout=localhost_timeout,
                )
                localhost_reachable = True
            except Exception:
                localhost_reachable = False
        except Exception:
            self.stop_viz_stream()
            raise

        connect_host = host if host not in ("", "0.0.0.0", "::") else "127.0.0.1"
        url = f"ws://{connect_host}:{int(server_port)}"
        localhost_url = f"ws://localhost:{int(server_port)}"
        browser_url = localhost_url if localhost_reachable else url
        self.viz_stream_start_kwargs = start_kwargs
        print(f"METS-R Vis live stream is available at {url}; origin=({initial_x}, {initial_y}); call render() to send frames.")
        return {
            "host": host,
            "port": int(server_port),
            "url": url,
            "local_url": url,
            "localhost_url": localhost_url,
            "browser_url": browser_url,
            "localhost_reachable": localhost_reachable,
            "manifest": manifest,
            "initial_x": float(initial_x),
            "initial_y": float(initial_y),
            "mode": "live",
        }

    def query_visualization_vehicles(
            self,
            transform_coords=True,
            include_public=True,
            include_private=True,
            vehicle_ids=None,
            private_veh=False,
            public_vehicle_ids=None,
            private_vehicle_ids=None,
            batch_size=1000,
            road_ids=None):
        """Query live vehicle records for visualization frames and dashboard telemetry."""
        query_groups = []
        for group_ids, private_flag in (
            (private_vehicle_ids, True),
            (public_vehicle_ids, False),
        ):
            if group_ids is not None:
                group_ids = _as_list(group_ids)
                if group_ids:
                    query_groups.append((group_ids, private_flag))
        if vehicle_ids is not None:
            vehicle_ids = _as_list(vehicle_ids)
            private_flags = _broadcast(private_veh, len(vehicle_ids))
            for private_flag in (True, False):
                group_ids = [vehicle_id for vehicle_id, flag in zip(vehicle_ids, private_flags)
                             if bool(flag) == private_flag]
                if group_ids:
                    query_groups.append((group_ids, private_flag))

        automatic_fleet_query = not query_groups
        public_fleet_keys = None

        if not query_groups:
            private_ids = []
            public_ids = []
            try:
                fleet = self.query_on_road_vehicles(roadID=road_ids)
            except Exception:
                fleet = {}
            if isinstance(fleet, dict) and fleet.get("status") != "error":
                private_ids.extend(fleet.get("privateVehicleIds") or [])
                public_ids.extend(fleet.get("publicVehicleIds") or [])
                for road_record in fleet.get("data", []):
                    if isinstance(road_record, dict) and road_record.get("status") != "error":
                        private_ids.extend(road_record.get("privateVehicleIds") or [])
                        public_ids.extend(road_record.get("publicVehicleIds") or [])

            # Vehicles controlled by Scenic can be detached from METS-R's
            # active-road index while they are inside the co-simulation bubble.
            # Merge the authoritative co-sim ownership list so those actors are
            # still represented in live visualization frames.
            try:
                cosim_fleet = self.query_cosim_vehicle()
            except Exception:
                cosim_fleet = {}
            if isinstance(cosim_fleet, dict) and cosim_fleet.get("status") != "error":
                for record in cosim_fleet.get("data", []):
                    if not isinstance(record, dict) or record.get("status") == "error":
                        continue
                    vehicle_id = _viz_vehicle_id(record)
                    if vehicle_id in (None, -1, ""):
                        continue
                    is_private = bool(_viz_first(
                        record,
                        "isPrivate", "privateVehicle", "_viz_private_veh",
                        default=False,
                    ))
                    (private_ids if is_private else public_ids).append(vehicle_id)

            private_ids = list(dict.fromkeys(private_ids))
            public_ids = list(dict.fromkeys(public_ids))
            if include_private and private_ids:
                query_groups.append((private_ids, True))
            if include_public:
                if public_ids:
                    query_groups.append((public_ids, False))
                public_fleet_keys = {
                    (False, str(veh_id)) for veh_id in public_ids
                }

        if automatic_fleet_query and include_public:
            with self.viz_stream_lock:
                self._attack_vehicle_keys = {
                    key
                    for key in self._attack_vehicle_keys
                    if key[0] or key in (public_fleet_keys or set())
                }

        records = []
        with self.viz_stream_lock:
            attack_vehicle_keys = set(self._attack_vehicle_keys)
        batch_size = max(1, int(batch_size or 1)) if query_groups else 1
        for group_ids, private_flag in query_groups:
            for start in range(0, len(group_ids), batch_size):
                batch_ids = group_ids[start:start + batch_size]
                try:
                    response = self.query_vehicle(
                        id=batch_ids,
                        private_veh=[private_flag] * len(batch_ids),
                        transform_coords=transform_coords,
                    )
                except Exception:
                    continue
                if response.get("status") == "error":
                    continue
                for record in response.get("data", []):
                    if _viz_stream_record(record):
                        record = dict(record)
                        record["_viz_private_veh"] = bool(private_flag)
                        record["_viz_attack_vehicle"] = (
                            (bool(private_flag), str(_viz_vehicle_id(record)))
                            in attack_vehicle_keys
                        )
                        records.append(record)
        return records

    def wait_for_visualization_client(self, client_wait_timeout=5):
        """Wait for a browser connection before rendering or starting a dashboard run."""
        try:
            wait_seconds = max(0.0, float(client_wait_timeout or 0.0))
        except (TypeError, ValueError):
            wait_seconds = 0.0
        if wait_seconds != wait_seconds or wait_seconds in (float("inf"), float("-inf")):
            wait_seconds = 0.0
        deadline = time.time() + wait_seconds
        while True:
            with self.viz_stream_lock:
                client_count = len(self.viz_stream_clients)
            if client_count > 0:
                return client_count
            if wait_seconds == 0 or time.time() >= deadline:
                raise RuntimeError(self._viz_stream_no_client_error(wait_seconds))
            time.sleep(min(0.1, max(0.0, deadline - time.time())))

    def render(self, client_wait_timeout=5):
        """Query the current METS-R tick and push one frame to METSR_VIS.

        Raises if no METS-R Vis client connects within client_wait_timeout seconds.
        """
        if self.viz_stream_server is None or self.viz_stream_manifest is None:
            raise RuntimeError("Call start_viz() before render().")

        self.wait_for_visualization_client(client_wait_timeout=client_wait_timeout)
        options = dict(self.viz_stream_options or {})
        tick = int(self.query_tick())

        first_stream_chunk = self.viz_stream_chunk_counter == 0
        include_link_snapshot = (
            options["include_links"]
            and (first_stream_chunk or tick % options["link_snapshot_interval"] == 0)
        )
        # Find active links and vehicles, including Scenic-owned co-sim actors.
        active_road_ids = None
        if options["include_links"]:
            try:
                response = self.query_active_roads()
            except Exception:
                response = {}
            active_road_ids = []
            if response.get("status") != "error":
                ids = (response.get("roadIds") or []) + (response.get("connectorIds") or [])
                if not ids:
                    ids = [_viz_first(record, "segmentId", default=None)
                           for record in response.get("data", []) if isinstance(record, dict)]
                active_road_ids = [str(road_id) for road_id in ids if road_id is not None]
        records = self.query_visualization_vehicles(
            transform_coords=options["transform_coords"],
            include_public=options["include_public"],
            include_private=options["include_private"],
            vehicle_ids=options["vehicle_ids"],
            private_veh=options["private_veh"],
            public_vehicle_ids=options["public_vehicle_ids"],
            private_vehicle_ids=options["private_vehicle_ids"],
            batch_size=options["batch_size"],
            road_ids=active_road_ids,
        )
        vehicle_group_counts = {
            group_key: len(group_records)
            for group_key, group_records in _viz_group_vehicle_records(records).items()
        }
        # Include recently cleared links and the requested facility snapshots.
        link_records = []
        if include_link_snapshot:
            active_ids = set(active_road_ids or [])
            previous_ids = set(self.viz_stream_last_active_road_ids or [])
            link_records = query_records(
                self.query_road, sorted(active_ids | previous_ids), options["link_batch_size"],
            )
            returned_ids = {str(_viz_first(record, "segmentId", default=""))
                            for record in link_records}
            for road_id in sorted(previous_ids - active_ids - returned_ids):
                link_records.append({
                    "segmentId": road_id, "vehicleCount": 0, "speed": 0.0, "flow": 0,
                    "energyConsumed": 0.0, "parkingCapacity": 0, "parkedVehicleCount": 0,
                })
            self.viz_stream_last_active_road_ids = active_ids

        facility_records = []
        for enabled, dictionary, query, key in (
            (options["include_zones"], options.get("zone_dictionary"), self.query_zone, "zoneIds"),
            (options["include_charging_stations"], options.get("charging_station_dictionary"),
             self.query_chargingStation, "chargingStationIds"),
        ):
            if enabled:
                ids = dictionary or query_record_ids(query, key)
                facility_records.append(query_records(query, ids, options["facility_batch_size"]))
            else:
                facility_records.append([])
        zone_records, charging_station_records = facility_records

        # Encode one frame, then send reset/manifest messages if the clock rewound.
        chunk, vehicle_count, link_count, zone_count, charging_station_count = _viz_chunk(
            records,
            tick,
            options["coord_scale"],
            options["initial_x"],
            options["initial_y"],
            options["tick_interval"],
            options["link_snapshot_interval"],
            link_records=link_records,
            road_id_index=options["road_id_index"],
            zone_records=zone_records,
            charging_station_records=charging_station_records,
        )
        self.viz_stream_chunk_counter += 1
        chunk_meta = {
            "file": f"stream-{tick}-{self.viz_stream_chunk_counter}.bin",
            "firstTick": tick,
            "lastTick": tick,
            "tickCount": 1,
            "vehicleCount": vehicle_count,
            "vehicleGroupCounts": vehicle_group_counts,
            "linkCount": link_count,
            "zoneUpdateCount": zone_count,
            "chargingStationUpdateCount": charging_station_count,
            "closed": True,
        }

        messages = []
        if self.viz_stream_last_tick is not None and tick < self.viz_stream_last_tick:
            messages.append(json.dumps({"type": "reset"}))
            messages.append(json.dumps({"type": "manifest", "manifest": self.viz_stream_manifest}))
        messages.append(json.dumps({"type": "chunkMeta", "chunk": chunk_meta}))
        messages.append(chunk)

        with self.viz_stream_lock:
            clients = list(self.viz_stream_clients)

        stale_clients = []
        client_count = 0
        for websocket in clients:
            try:
                for message in messages:
                    websocket.send(message)
                client_count += 1
            except Exception:
                stale_clients.append(websocket)

        if stale_clients:
            with self.viz_stream_lock:
                for websocket in stale_clients:
                    if websocket in self.viz_stream_clients:
                        self.viz_stream_clients.remove(websocket)
        if client_count == 0:
            raise RuntimeError(
                self._viz_stream_no_client_error(0)
                + "\nA client was connected when render() started, but it disconnected before the frame was sent."
            )
        self.viz_stream_last_tick = tick
        return {
            "tick": tick,
            "vehicle_count": vehicle_count,
            "vehicle_group_counts": vehicle_group_counts,
            "link_count": link_count,
            "zone_update_count": zone_count,
            "charging_station_update_count": charging_station_count,
            "client_count": client_count,
            "chunk": chunk_meta,
        }

    def stop_viz_stream(self, join_timeout=2.0):
        """Stop the live visualization servers and clear their stream state."""
        if self.viz_stream_stop_event is not None:
            self.viz_stream_stop_event.set()

        with self.viz_stream_lock:
            clients = list(self.viz_stream_clients)
            self.viz_stream_clients = []
        for websocket in clients:
            close = getattr(websocket, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

        servers = list(getattr(self, "viz_stream_servers", []) or [])
        if self.viz_stream_server is not None and self.viz_stream_server not in servers:
            servers.append(self.viz_stream_server)
        for server in servers:
            shutdown = getattr(server, "shutdown", None)
            close = getattr(server, "close", None)
            try:
                if callable(shutdown):
                    shutdown()
                elif callable(close):
                    close()
            except Exception:
                pass

        threads = list(getattr(self, "viz_stream_threads", []) or [])
        if self.viz_stream_thread is not None and self.viz_stream_thread not in threads:
            threads.append(self.viz_stream_thread)
        for thread in threads:
            thread.join(timeout=join_timeout)

        self.viz_stream_server = None
        self.viz_stream_servers = []
        self.viz_stream_thread = None
        self.viz_stream_threads = []
        self.viz_stream_stop_event = None
        self.viz_stream_host = None
        self.viz_stream_port = None
        self.viz_stream_manifest = None
        self.viz_stream_options = None
        self.viz_stream_start_kwargs = None
        self.viz_stream_chunk_counter = 0
        self.viz_stream_last_tick = None
        self.viz_stream_last_active_road_ids = set()
