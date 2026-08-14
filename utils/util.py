"""Helper functions for METSR-SIM and METSR-HPC."""

import socket
import json
import os
import re
import shlex
import subprocess
import time
import shutil
from os import path
import platform
from contextlib import closing, contextmanager
from types import SimpleNamespace
import sys
import zipfile
import threading
import weakref
from threading import Event
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
_DEFAULT_DOCKER_COMPOSE_DIR = path.abspath(
    path.join(path.dirname(__file__), os.pardir, "docker")
)


def kafka_bootstrap_servers(config=None):
    """Resolve Kafka bootstrap servers from a dict or config namespace."""
    if config is None:
        return DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    if isinstance(config, dict):
        return config.get(
            "kafka_bootstrap_servers",
            config.get("kafka_bootstrap_server", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
        )
    return getattr(
        config,
        "kafka_bootstrap_servers",
        getattr(config, "kafka_bootstrap_server", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
    )


def docker_compose_command():
    """Return an argv prefix for either Compose v1 or the Docker Compose plugin."""
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    if shutil.which("docker"):
        return ["docker", "compose"]
    raise RuntimeError(
        "Docker Compose was not found. Install Docker Desktop or start Kafka "
        f"manually on {DEFAULT_KAFKA_BOOTSTRAP_SERVERS}."
    )


def run_docker_compose(*args, compose_dir=None):
    """Run the repository Docker Compose stack and fail on launch errors."""
    return subprocess.run(
        docker_compose_command() + list(args),
        cwd=compose_dir or _DEFAULT_DOCKER_COMPOSE_DIR,
        check=True,
    )


def wait_for_kafka(bootstrap_servers=DEFAULT_KAFKA_BOOTSTRAP_SERVERS, timeout_s=90):
    """Wait until a Kafka broker responds, raising a useful timeout on failure."""
    from kafka import KafkaAdminClient

    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            admin.close()
            return bootstrap_servers
        except Exception as exc:
            last_error = exc
            time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise RuntimeError(
        f"Kafka broker at {bootstrap_servers!r} did not become ready within {timeout_s} seconds."
    ) from last_error


def start_kafka(config=None, timeout_s=90, compose_dir=None):
    """Start the tutorial Kafka services and wait until the broker is ready."""
    bootstrap_servers = kafka_bootstrap_servers(config)
    run_docker_compose(
        "up", "-d", "zookeeper", "kafka", "init-kafka", compose_dir=compose_dir
    )
    return wait_for_kafka(bootstrap_servers, timeout_s=timeout_s)


# ---------------------------------------------------------------------------
_SERVER_REGISTRY_LOCK = threading.RLock()
_VISUALIZATION_SERVER_REGISTRY = []
_METSR_CLIENT_REGISTRY = weakref.WeakSet()


def register_metsr_client(client):
    """Track a METSRClient so process-local helper servers can be stopped."""
    with _SERVER_REGISTRY_LOCK:
        _METSR_CLIENT_REGISTRY.add(client)
    return client


def unregister_metsr_client(client):
    with _SERVER_REGISTRY_LOCK:
        try:
            _METSR_CLIENT_REGISTRY.discard(client)
        except TypeError:
            pass


def _register_visualization_server(stop_event, server_thread, port=None, directory=None):
    entry = {
        "stop_event": stop_event,
        "server_thread": server_thread,
        "port": int(port) if port is not None else None,
        "directory": os.path.abspath(directory) if directory else None,
    }
    with _SERVER_REGISTRY_LOCK:
        _VISUALIZATION_SERVER_REGISTRY.append(entry)
    return entry


def _unregister_visualization_server(server_thread=None, stop_event=None):
    with _SERVER_REGISTRY_LOCK:
        _VISUALIZATION_SERVER_REGISTRY[:] = [
            entry for entry in _VISUALIZATION_SERVER_REGISTRY
            if not (
                (server_thread is not None and entry.get("server_thread") is server_thread)
                or (stop_event is not None and entry.get("stop_event") is stop_event)
            )
        ]


def stop_all_visualization_servers(verbose=True, join_timeout=2.0):
    """Stop all file/CORS visualization servers started in this Python process."""
    with _SERVER_REGISTRY_LOCK:
        entries = list(_VISUALIZATION_SERVER_REGISTRY)
    stopped = []
    for entry in entries:
        record = {
            "port": entry.get("port"),
            "directory": entry.get("directory"),
            "returncode": 0,
            "stderr": "",
        }
        try:
            stop_visualization_server(
                entry.get("stop_event"),
                entry.get("server_thread"),
                port=entry.get("port") or 8000,
                join_timeout=join_timeout,
                verbose=verbose,
            )
        except Exception as exc:
            record["returncode"] = 1
            record["stderr"] = str(exc).splitlines()[0]
            if verbose:
                print(f"Failed to stop visualization server on port {record['port']}: {record['stderr']}")
        stopped.append(record)
    if verbose and not stopped:
        print("No process-local visualization file servers found.")
    return stopped


def stop_all_metsr_client_servers(verbose=True):
    """Stop live stream and file servers owned by registered METSRClient objects."""
    with _SERVER_REGISTRY_LOCK:
        clients = list(_METSR_CLIENT_REGISTRY)
    stopped = []
    for client in clients:
        record = {
            "client": repr(client),
            "had_stream_server": getattr(client, "viz_stream_server", None) is not None,
            "had_file_server": getattr(client, "viz_server", None) is not None,
            "returncode": 0,
            "stderr": "",
        }
        if not record["had_stream_server"] and not record["had_file_server"]:
            continue
        try:
            stop_viz = getattr(client, "stop_viz", None)
            if callable(stop_viz):
                try:
                    stop_viz(verbose=verbose)
                except TypeError:
                    stop_viz()
            else:
                if record["had_stream_server"] and hasattr(client, "stop_viz_stream"):
                    client.stop_viz_stream()
                if record["had_file_server"] and hasattr(client, "stop_offline_viz"):
                    client.stop_offline_viz()
            if verbose:
                print(
                    "Stopped METS-R client helper servers "
                    f"stream={record['had_stream_server']} file={record['had_file_server']}"
                )
        except Exception as exc:
            record["returncode"] = 1
            record["stderr"] = str(exc).splitlines()[0]
            if verbose:
                print(f"Failed to stop METS-R client helper servers: {record['stderr']}")
        stopped.append(record)
    if verbose and not stopped:
        print("No registered METS-R client helper servers found.")
    return stopped


# Generic helpers
# ---------------------------------------------------------------------------

def check_socket(host, port):
    flag = True
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            flag =  True
        else:
            flag =  False
    time.sleep(1)
    return flag


def str_list_mapper_gen(func):
    def str_list_mapper(str_list):
        return [func(str) for str in str_list]
    return str_list_mapper


def _is_sequence(value):
    return isinstance(value, (list, tuple))


def _as_list(value):
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _broadcast(value, length):
    if length == 1:
        return [value]
    if _is_sequence(value) and len(value) == length:
        return list(value)
    return [value] * length


# ---------------------------------------------------------------------------
# METSRClient payload helpers
# ---------------------------------------------------------------------------

VEHICLE_SENSOR_DSRC = 0
VEHICLE_SENSOR_CV2X = 1
VEHICLE_SENSOR_MOBILE_DEVICE = 2

VEHICLE_SENSOR_TYPES = {
    "dsrc": VEHICLE_SENSOR_DSRC,
    "80211p": VEHICLE_SENSOR_DSRC,
    "cv2x": VEHICLE_SENSOR_CV2X,
    "c-v2x": VEHICLE_SENSOR_CV2X,
    "c_v2x": VEHICLE_SENSOR_CV2X,
    "mobile": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobiledevice": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobile_device": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobile-device": VEHICLE_SENSOR_MOBILE_DEVICE,
}

METS_R_VIS_PURDUE_MAP_ID = 12
METS_R_VIS_PRIVATE_VEHICLE_TYPE = 1
METS_R_VIS_VEHICLE_TYPE_BY_GROUP = {
    "vehicle": 0,
    "ev_private": 1,
    "ev_occupied": 2,
    "ev_relocation": 3,
    "ev_charging": 4,
    "bus": 5,
}


def _has_value(value):
    return value is not None and str(value).strip() != ""


def build_metsr_vis_url(
    viz_url="https://engineering.purdue.edu/HSEES/METSRVis/",
    stream_url=None,
    map_id=METS_R_VIS_PURDUE_MAP_ID,
    vehicle_id=None,
    vehicle_type=METS_R_VIS_PRIVATE_VEHICLE_TYPE,
):
    """Build a hosted METS-R Viz URL with dashboard preload query parameters."""
    import urllib.parse

    parts = urllib.parse.urlsplit(str(viz_url or ""))
    existing = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    managed_keys = {"Map", "StreamURL", "VehicleID", "VehicleType"}
    query_items = [(key, value) for key, value in existing if key not in managed_keys]
    if _has_value(map_id):
        query_items.append(("Map", str(map_id)))
    if _has_value(stream_url):
        query_items.append(("StreamURL", str(stream_url)))
    if _has_value(vehicle_id):
        query_items.append(("VehicleID", str(vehicle_id)))
    if _has_value(vehicle_type):
        query_items.append(("VehicleType", str(vehicle_type)))
    query = urllib.parse.urlencode(query_items)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))

def _request_id_from_record(record):
    if not isinstance(record, dict):
        return record
    return record.get("requestId")


def _request_zone_from_record(record):
    if not isinstance(record, dict):
        return None
    zone_id = record.get("zoneId")
    return record.get("originZoneId") if zone_id is None else zone_id


def _normalize_sensor_type(sensor_type):
    if isinstance(sensor_type, str):
        key = sensor_type.strip().lower()
        compact_key = key.replace(" ", "").replace("_", "").replace("-", "")
        if key in VEHICLE_SENSOR_TYPES:
            return VEHICLE_SENSOR_TYPES[key]
        if compact_key in VEHICLE_SENSOR_TYPES:
            return VEHICLE_SENSOR_TYPES[compact_key]
        raise ValueError(
            "Unknown sensorType. Use 0/'dsrc', 1/'cv2x', or 2/'mobile_device'."
        )
    return sensor_type


def _looks_like_centerline(value):
    if not _is_sequence(value) or len(value) == 0:
        return False
    first_point = value[0]
    if not _is_sequence(first_point) or len(first_point) < 2:
        return False
    return not _is_sequence(first_point[0])


# ---------------------------------------------------------------------------
# Simulation property/config helpers
# ---------------------------------------------------------------------------

_PROPERTY_RE = re.compile(r"^(\s*)([A-Za-z0-9_]+)\s*=\s*(.*?)(\r?\n?)$")
_MISSING = object()

_PROPERTY_OPTION_ALIASES = {
    "SIMULATION_STEP_SIZE": ("sim_step_size",),
    "ENABLE_JSON_WRITE": ("json_output",),
    "NUM_OF_EV": ("num_etaxi",),
    "NUM_OF_BUS": ("num_ebus",),
    "N_THREADS": ("sim_threads", "simulation_threads", "threads"),
    "N_PARTITION": ("sim_partitions", "simulation_partitions", "partitions"),
    "RH_SHARE_PERCENTAGE": ("rh_share_file",),
    "RH_WAITING_TIME": ("rh_wait_file",),
    "BT_STD_FILE": ("bt_event_std_file",),
    "BT_START_HOUR": ("bt_start_hour",),
    "EV_DEMAND_FILE": ("private_ev_demand_file",),
    "GV_DEMAND_FILE": ("private_gv_demand_file",),
    "EV_CHARGING_PREFERENCE": ("private_ev_charging_preference",),
    "ENABLE_INTERSECTION_SWEPT_COLLISION_CHECK": (
        "intersection_swept_collision_check",
        "intersection_collision_avoidance",
    ),
}

# Keep generated runs compatible when the local data template predates a new
# optional SIM property. Values here mirror the upstream Data.properties
# defaults and are only appended when the key is absent from the template.
_REQUIRED_SIM_PROPERTY_DEFAULTS = {
    "ENABLE_INTERSECTION_SWEPT_COLLISION_CHECK": False,
}


def _camel_to_snake(name):
    """Return a config-friendly lowercase form for mixed-case property names."""
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def _config_names_for_property(property_name):
    names = [property_name.lower(), _camel_to_snake(property_name)]
    names.extend(_PROPERTY_OPTION_ALIASES.get(property_name, ()))

    deduped = []
    for name in names:
        if name not in deduped:
            deduped.append(name)
    return deduped


def _get_option(options, name):
    if isinstance(options, dict):
        return options.get(name, _MISSING)
    return getattr(options, name, _MISSING)


def _first_option_value(options, names):
    for name in names:
        value = _get_option(options, name)
        if value is not _MISSING and value is not None:
            return True, value
    return False, None


def _format_property_value(value):
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _property_line(key, value, newline):
    newline = newline or "\n"
    return f"{key} = {_format_property_value(value)}{newline}"


def _ensure_extension(value, extension):
    value = str(value)
    if value.lower().endswith(extension):
        return value
    return value + extension


def _rewrite_data_path(line, src_data_dir):
    if "data/" not in line:
        return line
    src_data_dir = src_data_dir.replace("\\", "/").rstrip("/")
    return line.replace("data/", src_data_dir + "/")


def _property_override_value(key, options, port, instance):
    """Find the value that should be written for a Data.properties key.

    Most properties are now mapped automatically from the lowercase key name
    used in JSON configs, for example CAR_FOLLOWING_MODEL -> car_following_model.
    Existing HPC config names are kept as aliases so old configs still work.
    """
    if key == "NETWORK_LISTEN_PORT":
        return True, port
    if key == "RANDOM_SEED":
        found, seeds = _first_option_value(options, ("random_seeds",))
        if found:
            return True, seeds[instance]
        return False, None
    if key == "STANDALONE":
        return True, False
    if key == "SYNCHRONIZED":
        return True, True
    if key == "AGG_DEFAULT_PATH":
        return True, "agg_output"
    if key == "JSON_DEFAULT_PATH":
        return True, "trajectory_output"

    if key == "ZONES_SHAPEFILE":
        found, value = _first_option_value(options, ("zones_shapefile",))
        if found:
            return True, value
        found, value = _first_option_value(options, ("zone_file",))
        if found:
            return True, _ensure_extension(value, ".shp")
    elif key == "ZONES_CSV":
        found, value = _first_option_value(options, ("zones_csv",))
        if found:
            return True, value
        found, value = _first_option_value(options, ("zone_file",))
        if found:
            return True, _ensure_extension(value, ".csv")
    elif key == "CHARGER_SHAPEFILE":
        found, value = _first_option_value(options, ("charger_shapefile",))
        if found:
            return True, value
        found, value = _first_option_value(options, ("charging_station_file",))
        if found:
            return True, _ensure_extension(value, ".shp")
    elif key == "CHARGER_CSV":
        found, value = _first_option_value(options, ("charger_csv",))
        if found:
            return True, value
        found, value = _first_option_value(options, ("charging_station_file",))
        if found:
            return True, _ensure_extension(value, ".csv")
    elif key == "RH_DEMAND_SHARABLE":
        found, value = _first_option_value(options, _config_names_for_property(key))
        if found:
            return True, value
        found, _ = _first_option_value(options, ("rh_wait_file", "rh_waiting_time"))
        if found:
            return True, True
        return False, None

    found, value = _first_option_value(
        options,
        _config_names_for_property(key),
    )
    if found and key in {"BT_START_HOUR", "N_THREADS", "N_PARTITION"}:
        value = _instance_value(value, instance)
    if found and key == "BT_START_HOUR":
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("bt_start_hour must be a non-negative integer")
    return found, value


# ---------------------------------------------------------------------------
# Simulation file preparation
# ---------------------------------------------------------------------------

def modify_property_file(
    options,
    src_data_dir,
    dest_data_dir,
    port,
    instance,
    template,
    data_path_prefix=None,
):
    fname = src_data_dir + "/Data.properties." + template
    if not path.exists(fname):
        print("ERROR, cannot find the property template file at ", fname)
        sys.exit(-1)

    with open(fname, "r") as property_template:
        lines = property_template.readlines()
    fname = dest_data_dir + "/Data.properties"
    seen_properties = set()
    with open(fname, "w") as generated_properties:
        for line in lines:
            match = _PROPERTY_RE.match(line)
            if match:
                _, key, _, newline = match.groups()
                seen_properties.add(key)
                found, value = _property_override_value(
                    key, options, port, instance
                )
                if found:
                    line = _property_line(key, value, newline)
            line = _rewrite_data_path(line, data_path_prefix or src_data_dir)
            generated_properties.write(line)

        missing_required = [
            (key, default)
            for key, default in _REQUIRED_SIM_PROPERTY_DEFAULTS.items()
            if key not in seen_properties
        ]
        if missing_required and lines and not lines[-1].endswith(("\n", "\r")):
            generated_properties.write("\n")
        for key, default in missing_required:
            found, value = _property_override_value(
                key, options, port, instance
            )
            generated_properties.write(
                _property_line(key, value if found else default, "\n")
            )

def force_copytree(src, dst):
    """
    Recursively copy a directory tree, overwriting the destination directory if it exists.
    """
    # Check if the destination directory exists
    if os.path.exists(dst):
        # Remove the destination directory and all its contents
        shutil.rmtree(dst)
    
    # Copy the source directory to the destination
    shutil.copytree(src, dst)

_THIN_MUTABLE_DATA_ENTRIES = {
    ".metsr_hpc_inputs.json",
    "Data.properties",
}


def _prepare_thin_data_dir(dest_data_dir):
    """Create or safely reuse the mutable data overlay for a thin run."""
    if path.lexists(dest_data_dir):
        if path.islink(dest_data_dir) or not path.isdir(dest_data_dir):
            raise FileExistsError(
                f"Refusing to reuse thin-run data directory {dest_data_dir}: "
                "the path is not a real directory"
            )
    else:
        os.makedirs(dest_data_dir)

    entries = set(os.listdir(dest_data_dir))
    unexpected = sorted(entries - _THIN_MUTABLE_DATA_ENTRIES)
    if unexpected:
        raise FileExistsError(
            f"Refusing to reuse thin-run data directory {dest_data_dir}: "
            f"unexpected existing entries {unexpected}"
        )

    for entry in entries:
        entry_path = path.join(dest_data_dir, entry)
        if path.islink(entry_path) or not path.isfile(entry_path):
            raise FileExistsError(
                f"Refusing to reuse thin-run data directory {dest_data_dir}: "
                f"{entry} is not a regular file"
            )


# Copy necessary files for running the simulation
def prepare_sim_dirs(options):
    preparation_started = time.perf_counter()
    preparation_started_at = time.time()
    src_data_dir = "data"
    thin_run_value = getattr(options, "thin_run", None)
    if thin_run_value is None:
        thin_run_value = os.environ.get("METSR_THIN_RUN")
    thin_run = _coerce_bool(thin_run_value, default=False)
    thin_data_source = os.path.abspath(
        getattr(options, "thin_run_data_source", None) or src_data_dir
    )
    thin_data_target = str(
        (
            getattr(options, "thin_run_data_target", None)
            or os.environ.get("METSR_THIN_RUN_DATA_TARGET")
            or "/opt/metsr-inputs"
        )
    )
    thin_data_target = thin_data_target.replace("\\", "/")
    if thin_data_target != "/":
        thin_data_target = thin_data_target.rstrip("/")
    if thin_run:
        if not os.path.isdir(thin_data_source):
            raise FileNotFoundError(
                f"Thin-run data source does not exist: {thin_data_source}"
            )
        if not str(thin_data_target).startswith("/"):
            raise ValueError("thin_run_data_target must be an absolute container path")
        options.thin_run_data_source = thin_data_source
        options.thin_run_data_target = thin_data_target
        # The selected immutable tree owns both the template and static inputs.
        src_data_dir = thin_data_source
    # check if metsr_port in the NameSpace options
    if hasattr(options, 'metsr_port'):
        # check if metsr_port number is equal to the number of simulations
        if options.num_simulations > len(options.metsr_port):
            print("ERROR , port number is less than the number of simulation instances")
            sys.exit(-1)
        else:
            options.ports = options.metsr_port
    else:
        print("No port number specified, find available ports for simulation instances")
        find_free_ports(options, options.num_simulations)
    if len(options.ports) != options.num_simulations:
        print("ERROR , cannot specify port number for all simulation instances")
        sys.exit(-1)


    dest_data_dirs = []
    preparation_timings = []
    options.sim_dirs = []
    for i in range(options.num_simulations):
        instance_started = time.perf_counter()
        instance_started_at = time.time()
        # make a directory to run the simulator
        dir_name = get_sim_dir(options, i)
        if not path.exists(dir_name):
            os.makedirs(dir_name)
        dest_data_dir = dir_name + "/" + "data"
        if thin_run:
            _prepare_thin_data_dir(dest_data_dir)
        options.sim_dirs.append(dir_name)
        os.makedirs(path.join(dir_name, "logs"), exist_ok=True)
        shutil.copy(src_data_dir+"/log4j.properties", dir_name + "/log4j.properties")
        # copy the simulation config files

        if thin_run:
            manifest_path = path.join(
                dest_data_dir,
                ".metsr_hpc_inputs.json",
            )
            with open(manifest_path, "w", encoding="utf-8") as manifest_file:
                json.dump(
                    {
                        "schema_version": 1,
                        "host_data_source": thin_data_source,
                        "container_data_target": thin_data_target,
                    },
                    manifest_file,
                    indent=2,
                    sort_keys=True,
                )
                manifest_file.write("\n")
        elif (
            not path.exists(dest_data_dir)
            or not path.exists(path.join(dest_data_dir, "Data.properties.Template"))
        ):
            # A normal run owns a complete input copy. The second condition also
            # repairs a same-second directory collision with a prior thin run.
            force_copytree(src_data_dir, dest_data_dir)

        modify_property_file(
            options,
            src_data_dir,
            dest_data_dir,
            options.ports[i],
            i,
            options.template,
            data_path_prefix=thin_data_target if thin_run else None,
        )
        dest_data_dirs.append(dest_data_dir[:-5]) # -5 to remove the "/data" part
        preparation_timings.append(
            {
                "index": i,
                "started_at": instance_started_at,
                "preparation": time.perf_counter() - instance_started,
                "thin_run": thin_run,
            }
        )

    options.preparation_timings = preparation_timings
    options.preparation_time = time.perf_counter() - preparation_started
    options.preparation_started_at = preparation_started_at
    return dest_data_dirs

# Function for getting the file name list of demand scenarios
# def prepare_scenario_dict(options, path):
#     scenarios = os.listdir(path)
#     i = 0
#     scenarios = sorted(scenarios)
#     options.scenarios=[]
#     options.cases = [[] for j in range(len(scenarios))]
#     for scenario in scenarios:
#         options.scenarios.append(scenario)
#         cases = os.listdir(path+"/"+scenario)
#         cases = sorted(cases)
#         for case in cases:
#             options.cases[i].append(case.split("_")[1])
#         i+=1

# ---------------------------------------------------------------------------
# Port and config helpers
# ---------------------------------------------------------------------------

def find_free_ports(options, num_simulations):
    options.ports = []
    while True:
        for i in range(num_simulations):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.bind(('localhost', 0))
                options.ports.append(s.getsockname()[1])
        try:
            for port in options.ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', port))
                s.close()
            break
        except:
            print("The port is not valid anymore, regenerate it")
            continue
    time.sleep(1)
     
# Read json format configuration 
def _load_raw_config(fname):
    """Recursively load a config JSON, merging parent_config fields first."""
    fname = os.path.abspath(fname)
    with open(fname, "r") as f:
        raw = json.load(f)

    if "parent_config" in raw:
        parent_path = os.path.abspath(
            os.path.join(os.path.dirname(fname), raw["parent_config"])
        )
        parent_raw = _load_raw_config(parent_path)
        child_fields = {k: v for k, v in raw.items() if k != "parent_config"}
        return {**parent_raw, **child_fields}

    return raw


def read_run_config(fname):
    merged = _load_raw_config(fname)
    config = SimpleNamespace(**merged)

    if len(config.random_seeds) != config.num_simulations:
       print("ERROR, please specify random seeds for all simulation instances")
       sys.exit(-1)

    return config

# ---------------------------------------------------------------------------
# Java classpath helpers
# ---------------------------------------------------------------------------

def get_classpath(options, includeBin=True, separator=":"):
    
    classpath = ""

    if not path.exists(options.repast_plugin_dir):
        print(f"ERROR , repast plugins not found at {options.repast_plugin_dir}")
        sys.exit(-1)
    
    classpath += options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/lib/*" + separator + \
                 options.sim_dir + "lib/*" + separator    
 
    


    return classpath

def get_classpath2(options, includeBin=True, separator=":"):
    
    classpath = ""

    classpath += options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.batch_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.batch_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.distributed.batch_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.distributed.batch_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.core_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.core_2.7.0/lib/*" + separator + \
                 options.sim_dir + "bin" + separator + \
                 options.sim_dir + "lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.bin_and_src_2.7.0/repast.simphony.bin_and_src.jar" + separator + \
                 options.repast_plugin_dir + "repast.simphony.essentials_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.gis_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.gis_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.sql_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.sql_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.scenario_2.7.0/bin" + separator 

    return classpath

# ---------------------------------------------------------------------------
# Simulation launch helpers
# ---------------------------------------------------------------------------

def _shell_args(value):
    if value in (None, ""):
        return []
    if isinstance(value, str):
        if platform.system() == "Windows":
            lexer = shlex.shlex(value, posix=True)
            lexer.whitespace_split = True
            lexer.commenters = ""
            lexer.escape = ""
            return list(lexer)
        return shlex.split(value)
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


DEFAULT_METSR_SIM_IMAGE = "ennuilei/mets-r_sim:latest"
DEFAULT_METSR_SIM_APPCONTAINER_IMAGE = f"docker://{DEFAULT_METSR_SIM_IMAGE}"

_LAUNCH_TIMING_KEYS = (
    "preparation",
    "launch",
    "connection",
    "network_load",
    "fleet_spawn",
)
_LOG_RELATIVE_TIME_RE = re.compile(r"^\s*(\d+)\s+")
_LOG_PHASE_MARKERS = {
    "connection": ("Connection object created.", "Connected to "),
    "network_load": ("Building subcontexts", "VehicleContext creation"),
    "fleet_spawn": ("VehicleContext creation", "Total EV buses generated"),
}


def _coerce_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled", ""}:
        return False
    return bool(value)


def _launcher_value(options, names, env_name=None, default=None):
    for name in names:
        value = _get_option(options, name)
        if value is not _MISSING and value is not None and value != "":
            return value
    if env_name:
        value = os.environ.get(env_name)
        if value not in (None, ""):
            return value
    return default


def _set_launcher_value(options, name, value):
    if isinstance(options, dict):
        options[name] = value
    else:
        setattr(options, name, value)


def _instance_value(value, index):
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        if index >= len(value):
            raise ValueError(
                f"Launcher option has {len(value)} values for simulation index {index}"
            )
        return value[index]
    return value


def _java_executable(options):
    explicit = _launcher_value(options, ("java_executable",))
    if explicit:
        return str(explicit)
    java_path = str(_launcher_value(options, ("java_path",), default="") or "")
    if not java_path:
        return "java"
    return java_path.rstrip("/\\") + "/java"


def _run_scoped_filename(filename, sim_dir, option_name):
    """Return a POSIX relative path that cannot escape the mounted run directory."""
    run_root = path.realpath(path.abspath(sim_dir))
    raw_filename = os.fspath(filename)
    if path.isabs(raw_filename):
        host_filename = path.realpath(path.abspath(raw_filename))
    else:
        host_relative = raw_filename.replace("\\", os.sep).replace("/", os.sep)
        host_filename = path.realpath(path.abspath(path.join(run_root, host_relative)))
    try:
        common_root = path.commonpath((run_root, host_filename))
    except ValueError as exc:
        raise ValueError(
            f"{option_name} must resolve inside simulation run directory "
            f"{run_root}: {raw_filename}"
        ) from exc
    if path.normcase(common_root) != path.normcase(run_root):
        raise ValueError(
            f"{option_name} must resolve inside simulation run directory "
            f"{run_root}: {raw_filename}"
        )
    relative_filename = path.relpath(host_filename, run_root)
    if relative_filename in ("", "."):
        raise ValueError(f"{option_name} must name a file inside {run_root}")
    host_parent = path.dirname(host_filename)
    if host_parent:
        os.makedirs(host_parent, exist_ok=True)
    return relative_filename.replace("\\", "/")


def _validate_jfr_option_overrides(raw_options):
    message = (
        "jfr_options must not set filename or provide "
        "-XX:StartFlightRecording; use jfr_filename plus recording-option "
        "fragments instead"
    )
    if isinstance(raw_options, dict):
        values = [
            f"{key}={_format_property_value(value)}"
            for key, value in raw_options.items()
            if value is not None
        ]
    elif isinstance(raw_options, (list, tuple)):
        values = [str(item) for item in raw_options if str(item)]
    else:
        values = [str(raw_options or "")]
    for value in values:
        if (
            "-xx:startflightrecording" in value.lower()
            or re.search(
                r"(?<![A-Za-z0-9_])filename\s*=",
                value,
                flags=re.IGNORECASE,
            )
        ):
            raise ValueError(message)

def _jfr_arguments(options, index, sim_dir):
    raw_options = _launcher_value(
        options, ("jfr_options",), env_name="METSR_JFR_OPTIONS"
    )
    enabled = _coerce_bool(
        _launcher_value(
            options,
            ("enable_jfr", "jfr"),
            env_name="METSR_ENABLE_JFR",
            default=raw_options is not None,
        )
    )
    if not enabled:
        return [], {"enabled": False, "filename": None}

    _validate_jfr_option_overrides(raw_options)
    if isinstance(raw_options, (list, tuple)):
        explicit_args = [str(item) for item in raw_options if str(item)]
        option_fragment = ",".join(explicit_args)
    elif isinstance(raw_options, dict):
        option_fragment = ",".join(
            f"{key}={_format_property_value(value)}"
            for key, value in raw_options.items()
            if value is not None
        )
    else:
        option_fragment = str(raw_options or "").strip().strip(",")

    filename = str(
        _launcher_value(
            options,
            ("jfr_filename",),
            env_name="METSR_JFR_FILENAME",
            default=f"profiles/metsr_{index}.jfr",
        )
    ).format(index=index)
    filename = _run_scoped_filename(filename, sim_dir, "jfr_filename")

    recording_options = [f"filename={filename}", "dumponexit=true"]
    for option_name, jfr_name in (
        ("jfr_settings", "settings"),
        ("jfr_duration", "duration"),
        ("jfr_max_age", "maxage"),
        ("jfr_max_size", "maxsize"),
    ):
        value = _launcher_value(options, (option_name,))
        if value not in (None, ""):
            recording_options.append(f"{jfr_name}={value}")
    if option_fragment:
        recording_options.append(option_fragment)

    args = []
    if _coerce_bool(_launcher_value(options, ("jfr_unlock_commercial_features",))):
        args.append("-XX:+UnlockCommercialFeatures")
    args.append("-XX:StartFlightRecording=" + ",".join(recording_options))
    return args, {"enabled": True, "filename": filename}


def _build_batch_java_command(options, index, sim_dir, classpath_separator=":"):
    jvm_options = _launcher_value(
        options,
        ("jvm_options", "java_options"),
        env_name="METSR_JVM_OPTIONS",
        default="",
    )
    extra_options = _launcher_value(
        options, ("jvm_extra_options", "java_extra_options"), default=""
    )
    jfr_args, jfr = _jfr_arguments(options, index, sim_dir)
    scenario_dir = str(options.sim_dir).rstrip("/\\") + "/mets_r.rs"
    command = [
        _java_executable(options),
        *_shell_args(jvm_options),
        *_shell_args(extra_options),
        *jfr_args,
        "-cp",
        get_classpath2(options, False, separator=classpath_separator),
        "repast.simphony.batch.BatchMain",
        "-params",
        scenario_dir + "/batch_params.xml",
        "-interactive",
        scenario_dir,
    ]
    return command, jfr


def _launcher_resources(options, backend, index):
    prefix = "docker" if backend == "docker" else "appcontainer"
    resources = {
        "cpus": _launcher_value(
            options,
            (f"{prefix}_cpus", "container_cpus", "cpu_limit", "cpus"),
            env_name=f"METSR_{prefix.upper()}_CPUS",
        ),
        "memory": _launcher_value(
            options,
            (f"{prefix}_memory", "container_memory", "memory_limit"),
            env_name=f"METSR_{prefix.upper()}_MEMORY",
        ),
        "cpuset_cpus": _launcher_value(
            options, (f"{prefix}_cpuset_cpus", "container_cpuset_cpus")
        ),
        "threads": _launcher_value(
            options, ("n_threads", "sim_threads", "simulation_threads", "threads")
        ),
        "partitions": _launcher_value(
            options,
            ("n_partition", "sim_partitions", "simulation_partitions", "partitions"),
        ),
    }
    return {
        key: _instance_value(value, index)
        for key, value in resources.items()
    }


def _append_resource_arguments(command, resources):
    if resources.get("cpus") not in (None, ""):
        command.extend(["--cpus", str(resources["cpus"])])
    if resources.get("memory") not in (None, ""):
        command.extend(["--memory", str(resources["memory"])])
    if resources.get("cpuset_cpus") not in (None, ""):
        command.extend(["--cpuset-cpus", str(resources["cpuset_cpus"])])


def _thin_run_binding(options):
    enabled = _coerce_bool(
        _launcher_value(
            options, ("thin_run",), env_name="METSR_THIN_RUN", default=False
        )
    )
    if not enabled:
        return None
    source = os.path.abspath(
        _launcher_value(
            options, ("thin_run_data_source",), default=path.abspath("data")
        )
    )
    target = str(
        _launcher_value(
            options,
            ("thin_run_data_target",),
            env_name="METSR_THIN_RUN_DATA_TARGET",
            default="/opt/metsr-inputs",
        )
    ).rstrip("/")
    if not path.isdir(source):
        raise FileNotFoundError(f"Thin-run data source does not exist: {source}")
    if not target.startswith("/"):
        raise ValueError("thin_run_data_target must be an absolute container path")
    return source, target


def _preparation_duration(options, index):
    timings = _launcher_value(options, ("preparation_timings",), default=[])
    if isinstance(timings, (list, tuple)) and index < len(timings):
        record = timings[index]
        if isinstance(record, dict):
            return record.get("preparation")
        if isinstance(record, (int, float)):
            return float(record)
    return None


class SimulationLaunchHandle:
    """A scoped handle for one launched simulator."""

    def __init__(
        self,
        backend,
        command,
        sim_dir,
        *,
        container_id=None,
        process=None,
        launch_result=None,
        docker_executable="docker",
        log_path=None,
        image=None,
        resources=None,
        timings=None,
        launched_at=None,
    ):
        self.backend = backend
        self.command = list(command)
        self.sim_dir = path.abspath(sim_dir)
        self.container_id = container_id
        self.process = process
        self.launch_result = launch_result
        self.docker_executable = docker_executable
        self.log_path = log_path
        self.image = image
        self.resources = dict(resources or {})
        self.launched_at = launched_at or time.time()
        self.cleaned_up = False
        self.cleanup_result = None
        self._phase_starts = {}
        self._log_signature = None
        self._timings = {key: None for key in _LAUNCH_TIMING_KEYS}
        for key, value in (timings or {}).items():
            self.record_timing(key, value)

    @property
    def identifier(self):
        if self.container_id:
            return self.container_id
        return getattr(self.process, "pid", None)

    @property
    def pid(self):
        return getattr(self.process, "pid", None)

    @property
    def timings(self):
        self.refresh_timings()
        return self._timings

    def record_timing(self, phase, seconds):
        key = str(phase).strip().lower().replace("-", "_")
        if seconds is not None:
            self._timings[key] = max(0.0, float(seconds))
        elif key not in self._timings:
            self._timings[key] = None
        return self._timings.get(key)

    def start_phase(self, phase):
        key = str(phase).strip().lower().replace("-", "_")
        self._phase_starts[key] = time.perf_counter()
        return self._phase_starts[key]

    def finish_phase(self, phase):
        key = str(phase).strip().lower().replace("-", "_")
        started = self._phase_starts.pop(key, None)
        if started is None:
            raise RuntimeError(f"Timing phase {phase!r} was not started")
        return self.record_timing(key, time.perf_counter() - started)

    @contextmanager
    def measure_phase(self, phase):
        self.start_phase(phase)
        try:
            yield self
        finally:
            self.finish_phase(phase)

    def refresh_timings(self):
        if not self.log_path:
            return self._timings
        try:
            stat = os.stat(self.log_path)
            signature = (stat.st_size, stat.st_mtime_ns)
            if signature == self._log_signature:
                return self._timings
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as log:
                lines = log.readlines()
            self._log_signature = signature
        except OSError:
            return self._timings

        positions = {
            phase: {"start": None, "end": None}
            for phase in _LOG_PHASE_MARKERS
        }
        for line in lines:
            match = _LOG_RELATIVE_TIME_RE.match(line)
            if match is None:
                continue
            relative_ms = int(match.group(1))
            for phase, (start_marker, end_marker) in _LOG_PHASE_MARKERS.items():
                phase_positions = positions[phase]
                if phase_positions["start"] is None and start_marker in line:
                    phase_positions["start"] = relative_ms
                elif (
                    phase_positions["start"] is not None
                    and phase_positions["end"] is None
                    and end_marker in line
                ):
                    phase_positions["end"] = relative_ms

        for phase, phase_positions in positions.items():
            if (
                self._timings.get(phase) is None
                and phase_positions["start"] is not None
                and phase_positions["end"] is not None
            ):
                self.record_timing(
                    phase,
                    (phase_positions["end"] - phase_positions["start"]) / 1000.0,
                )
        return self._timings

    def cleanup(self, timeout=10):
        """Stop only this container/process; safe to call more than once."""
        if self.cleaned_up:
            return self.cleanup_result
        self.refresh_timings()
        if self.backend == "docker" and self.container_id:
            self.cleanup_result = subprocess.run(
                [
                    self.docker_executable,
                    "stop",
                    "--time",
                    str(max(0, int(timeout))),
                    self.container_id,
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            if self.cleanup_result.returncode != 0:
                error = (
                    self.cleanup_result.stderr
                    or self.cleanup_result.stdout
                    or str(self.cleanup_result.returncode)
                ).strip()
                raise RuntimeError(
                    f"Failed to stop METS-R Docker container "
                    f"{self.container_id}: {error}"
                )
        elif self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.cleanup_result = self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.cleanup_result = self.process.wait(timeout=timeout)
        self.cleaned_up = True
        return self.cleanup_result

    terminate = cleanup
    close = cleanup

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()
        return False


class SimulationLaunchGroup(list):
    """Sequence of launch handles with group-scoped cleanup."""

    @property
    def container_ids(self):
        return [handle.container_id for handle in self if handle.container_id]

    @property
    def processes(self):
        return [handle.process for handle in self if handle.process is not None]

    @property
    def timings(self):
        return [handle.timings for handle in self]

    def refresh_timings(self):
        for handle in self:
            handle.refresh_timings()
        return self.timings

    def cleanup(self, timeout=10):
        results = []
        failures = []
        for handle in reversed(self):
            try:
                results.append(handle.cleanup(timeout=timeout))
            except Exception as exc:
                results.append(None)
                failures.append((handle, exc))
        if failures:
            details = "; ".join(
                f"{getattr(handle, 'identifier', repr(handle))}: {exc}"
                for handle, exc in failures
            )
            error = RuntimeError(
                f"Failed to clean up {len(failures)} METS-R launch "
                f"resource(s): {details}"
            )
            error.failures = failures
            raise error from failures[0][1]
        return results

    terminate = cleanup
    close = cleanup

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()
        return False


LaunchHandle = SimulationLaunchHandle
LaunchGroup = SimulationLaunchGroup


def _attach_launch_group(options, handles):
    _set_launcher_value(options, "launch_handles", handles)
    # Keep references to the live dictionaries so log parsing and client-side
    # phase measurements are reflected without replacing the config field.
    _set_launcher_value(options, "launch_timings", [h._timings for h in handles])
    return handles


def _default_appcontainer_executable():
    for candidate in ("apptainer", "singularity", "appcontainer"):
        if shutil.which(candidate):
            return candidate
    return "apptainer"

def run_simulations(options):
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        if platform.system() == "Windows":
             # go to sim directory
            os.chdir(options.sim_dirs[i])

            # print(get_classpath(options, False, separator = ";"))
            # run the simulation on a new terminal
            sim_command = '"' + options.java_path + 'java"' + " " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    '"' +get_classpath(options, False, separator = ";") + '" '  + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            # print(sim_command)
            if options.verbose: # print the sim output to the console
                subprocess.Popen(sim_command, shell=True)
            else:
                subprocess.Popen(sim_command + " > sim_{}.log 2>&1 &".format(i), shell=True)
        else:
            # go to sim directory
            os.chdir(options.sim_dirs[i])
            # run simulator on new terminal
            sim_command = options.java_path + "java " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    get_classpath(options, False) + " "  + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            if options.verbose:
                os.system(sim_command)
            else:
                os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir(cwd)

def run_simulations_in_background(options):
    """Launch local Java processes and return scoped process handles."""
    if _thin_run_binding(options) is not None:
        raise RuntimeError(
            "thin_run uses container-only input mounts; use Docker/AppContainer "
            "or disable thin_run for a local Java launch"
        )
    handles = SimulationLaunchGroup()
    separator = ";" if platform.system() == "Windows" else ":"
    try:
        for i in range(0, options.num_simulations):
            sim_dir = path.abspath(options.sim_dirs[i])
            command, jfr = _build_batch_java_command(
                options, i, sim_dir, classpath_separator=separator
            )
            log_path = path.join(sim_dir, f"sim_{i}.log")
            log_file = None
            launch_started = time.perf_counter()
            launched_at = time.time()
            try:
                popen_kwargs = {"cwd": sim_dir}
                if not getattr(options, "verbose", False):
                    log_file = open(log_path, "a", encoding="utf-8")
                    popen_kwargs.update(
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                process = subprocess.Popen(command, **popen_kwargs)
            finally:
                if log_file is not None:
                    log_file.close()
            resources = _launcher_resources(options, "appcontainer", i)
            resources["jfr"] = jfr
            handles.append(
                SimulationLaunchHandle(
                    "process",
                    command,
                    sim_dir,
                    process=process,
                    log_path=path.join(sim_dir, "logs", "mets_r.log"),
                    resources=resources,
                    timings={
                        "preparation": _preparation_duration(options, i),
                        "launch": time.perf_counter() - launch_started,
                    },
                    launched_at=launched_at,
                )
            )
            if getattr(options, "verbose", False):
                print("METS-R process ID:", process.pid)
    except Exception:
        handles.cleanup()
        raise
    return _attach_launch_group(options, handles)


def run_simulation_in_docker(options):
    """Launch METS-R SIM containers and return scoped container handles.

    Supported overrides include ``docker_image``, ``docker_cpus``,
    ``docker_memory``, ``docker_cpuset_cpus``, ``jvm_options``, and the JFR
    options accepted by :func:`_jfr_arguments`. All commands are argv lists and
    are executed without a host or in-container shell.
    """
    docker_executable = str(
        _launcher_value(
            options,
            ("docker_executable",),
            env_name="METSR_DOCKER_EXECUTABLE",
            default="docker",
        )
    )
    image = str(
        _launcher_value(
            options,
            ("docker_image",),
            env_name="METSR_DOCKER_IMAGE",
            default=DEFAULT_METSR_SIM_IMAGE,
        )
    )
    cli_args = _shell_args(
        _launcher_value(
            options, ("docker_cli_args",), env_name="METSR_DOCKER_CLI_ARGS"
        )
    )
    runtime_args = _shell_args(
        _launcher_value(options, ("docker_args",), env_name="METSR_DOCKER_ARGS")
    )
    if any(
        argument == "--pull" or argument.startswith("--pull=")
        for argument in runtime_args
    ):
        raise ValueError(
            "docker_args must not override the launcher-managed --pull=always policy"
        )
    bind_target = str(
        _launcher_value(
            options,
            ("docker_bind_target", "container_bind_target"),
            env_name="METSR_DOCKER_BIND_TARGET",
            default="/home/test",
        )
    )
    network = _launcher_value(
        options,
        ("docker_network",),
        env_name="METSR_DOCKER_NETWORK",
        default="host",
    )
    thin_binding = _thin_run_binding(options)
    handles = SimulationLaunchGroup()

    try:
        for i in range(0, options.num_simulations):
            sim_dir = path.abspath(options.sim_dirs[i])
            property_file = path.join(sim_dir, "data", "Data.properties")
            if thin_binding is not None and not path.isfile(property_file):
                raise FileNotFoundError(
                    "Thin-run Data.properties is missing; call prepare_sim_dirs() "
                    f"before launching ({property_file})"
                )
            java_command, jfr = _build_batch_java_command(options, i, sim_dir)
            resources = _launcher_resources(options, "docker", i)
            resources["jfr"] = jfr
            command = [
                docker_executable,
                *cli_args,
                "run",
                "--pull=always",
                "-d",
                "--rm",
                "--label",
                "mets-r.hpc=true",
            ]
            if network not in (None, "", False):
                command.extend(["--network", str(network)])
            _append_resource_arguments(command, resources)
            command.extend(runtime_args)
            command.extend(
                [
                    "--mount",
                    f"type=bind,source={sim_dir},target={bind_target}",
                ]
            )
            if thin_binding is not None:
                thin_source, thin_target = thin_binding
                command.extend(
                    [
                        "--mount",
                        (
                            f"type=bind,source={thin_source},target={thin_target},"
                            "readonly"
                        ),
                    ]
                )
            command.extend(
                [
                    "--workdir",
                    bind_target,
                    image,
                    *java_command,
                ]
            )

            launch_started = time.perf_counter()
            launched_at = time.time()
            try:
                result = subprocess.run(
                    command,
                    text=True,
                    capture_output=True,
                    check=False,
                )
            except FileNotFoundError as exc:
                raise RuntimeError(
                    f"Docker executable {docker_executable!r} was not found. "
                    "Set options.docker_executable or METSR_DOCKER_EXECUTABLE."
                ) from exc
            launch_duration = time.perf_counter() - launch_started
            if result.returncode != 0:
                error = (result.stderr or result.stdout or "").strip()
                raise RuntimeError(
                    f"Failed to launch METS-R Docker container: {error or result.returncode}"
                )
            container_id = (result.stdout or "").strip().splitlines()
            container_id = container_id[-1].strip() if container_id else ""
            if not container_id:
                raise RuntimeError("Docker did not return a METS-R container ID")
            handle = SimulationLaunchHandle(
                "docker",
                command,
                sim_dir,
                container_id=container_id,
                launch_result=result,
                docker_executable=docker_executable,
                log_path=path.join(sim_dir, "logs", "mets_r.log"),
                image=image,
                resources=resources,
                timings={
                    "preparation": _preparation_duration(options, i),
                    "launch": launch_duration,
                },
                launched_at=launched_at,
            )
            handles.append(handle)
            if getattr(options, "verbose", False):
                print("Container ID:", container_id)
    except Exception:
        handles.cleanup()
        raise
    return _attach_launch_group(options, handles)


def run_simulation_in_appcontainer(options):
    """Launch METS-R SIM with Apptainer/Singularity and return process handles.

    Kafka/docker-compose services are not started by this mode. CPU and memory
    limits use the runtime's Docker-compatible cgroup flags when configured.
    """
    note = (
        "NOTE: AppContainer mode starts only METS-R SIM; Kafka/docker-compose "
        "services are not available in this mode."
    )
    if (
        getattr(options, "verbose", False)
        or getattr(options, "kafka_bootstrap_servers", None)
        or getattr(options, "kafka_topics", None)
    ):
        print(note)

    executable = str(
        _launcher_value(
            options,
            ("appcontainer_executable",),
            env_name="METSR_APPCONTAINER_EXECUTABLE",
            default=_default_appcontainer_executable(),
        )
    )
    app_image = _launcher_value(
        options,
        ("appcontainer_image",),
        env_name="METSR_APPCONTAINER_IMAGE",
    )
    if app_image is None:
        docker_image = str(
            _launcher_value(
                options,
                ("docker_image",),
                env_name="METSR_DOCKER_IMAGE",
                default=DEFAULT_METSR_SIM_IMAGE,
            )
        )
        app_image = (
            docker_image
            if "://" in docker_image
            else "docker://" + docker_image
        )
    image = str(app_image)
    runtime_args = _shell_args(
        _launcher_value(
            options,
            ("appcontainer_args",),
            env_name="METSR_APPCONTAINER_ARGS",
        )
    )
    bind_target = str(
        _launcher_value(
            options,
            ("appcontainer_bind_target", "container_bind_target"),
            env_name="METSR_APPCONTAINER_BIND_TARGET",
            default="/home/test",
        )
    )
    command_name = str(
        _launcher_value(
            options,
            ("appcontainer_command",),
            env_name="METSR_APPCONTAINER_COMMAND",
            default="exec",
        )
    )
    workdir_arg = _launcher_value(
        options,
        ("appcontainer_workdir_arg",),
        env_name="METSR_APPCONTAINER_WORKDIR_ARG",
        default="--pwd",
    )
    thin_binding = _thin_run_binding(options)
    handles = SimulationLaunchGroup()

    try:
        for i in range(0, options.num_simulations):
            sim_dir = path.abspath(options.sim_dirs[i])
            property_file = path.join(sim_dir, "data", "Data.properties")
            if thin_binding is not None and not path.isfile(property_file):
                raise FileNotFoundError(
                    "Thin-run Data.properties is missing; call prepare_sim_dirs() "
                    f"before launching ({property_file})"
                )
            java_command, jfr = _build_batch_java_command(options, i, sim_dir)
            resources = _launcher_resources(options, "appcontainer", i)
            resources["jfr"] = jfr
            command = [
                executable,
                command_name,
                *runtime_args,
            ]
            _append_resource_arguments(command, resources)
            command.extend(["--bind", f"{sim_dir}:{bind_target}"])
            if thin_binding is not None:
                thin_source, thin_target = thin_binding
                command.extend(["--bind", f"{thin_source}:{thin_target}:ro"])
            if workdir_arg not in (None, "", False):
                command.extend([str(workdir_arg), bind_target])
            command.extend([image, *java_command])

            log_path = path.join(sim_dir, f"sim_{i}.log")
            log_file = None
            launch_started = time.perf_counter()
            launched_at = time.time()
            try:
                popen_kwargs = {"cwd": sim_dir}
                if not getattr(options, "verbose", False):
                    log_file = open(log_path, "a", encoding="utf-8")
                    popen_kwargs.update(
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                process = subprocess.Popen(command, **popen_kwargs)
            except FileNotFoundError as exc:
                raise RuntimeError(
                    f"AppContainer executable {executable!r} was not found. "
                    "Set options.appcontainer_executable or "
                    "METSR_APPCONTAINER_EXECUTABLE."
                ) from exc
            finally:
                if log_file is not None:
                    log_file.close()

            handles.append(
                SimulationLaunchHandle(
                    "appcontainer",
                    command,
                    sim_dir,
                    process=process,
                    log_path=path.join(sim_dir, "logs", "mets_r.log"),
                    image=image,
                    resources=resources,
                    timings={
                        "preparation": _preparation_duration(options, i),
                        "launch": time.perf_counter() - launch_started,
                    },
                    launched_at=launched_at,
                )
            )
            if getattr(options, "verbose", False):
                print("AppContainer METS-R process ID:", process.pid)
    except Exception:
        handles.cleanup()
        raise
    return _attach_launch_group(options, handles)

def clear_all(patterns=None, docker_executable="docker", verbose=True, stop_servers=True):
    """Stop process-local METS-R helper servers and running METS-R Docker containers.

    This stops live METS-R Vis streams and file/CORS visualization servers that
    were started by METSRClient or utility helpers in the current Python
    process, then stops running Docker containers whose image/name appears to
    belong to METS-R.

    Parameters
    ----------
    patterns : sequence[str], optional
        Case-insensitive substrings to match against container image/name.
        Defaults to METS-R naming variants.
    docker_executable : str, optional
        Docker CLI executable to invoke.
    verbose : bool, optional
        Print a short summary of stopped resources.
    stop_servers : bool, optional
        Stop process-local METS-R client streams and file servers before Docker
        cleanup.

    Returns
    -------
    dict
        ``metsr_clients`` records helper servers stopped through registered
        clients, ``visualization_servers`` records standalone file servers, and
        ``docker_containers`` records stopped Docker containers.
    """
    cleanup = {
        "metsr_clients": [],
        "visualization_servers": [],
        "docker_containers": [],
    }
    if stop_servers:
        cleanup["metsr_clients"] = stop_all_metsr_client_servers(verbose=verbose)
        cleanup["visualization_servers"] = stop_all_visualization_servers(verbose=verbose)

    patterns = tuple(patterns or ("mets-r", "metsr", "mets_r"))
    lowered_patterns = tuple(str(pattern).lower() for pattern in patterns)
    ps_command = [
        docker_executable,
        "ps",
        "--format",
        "{{.ID}}\t{{.Image}}\t{{.Names}}",
    ]
    try:
        ps_result = subprocess.run(
            ps_command,
            text=True,
            capture_output=True,
            check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Docker executable {docker_executable!r} was not found.") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Failed to list running Docker containers: "
            + (exc.stderr or exc.stdout or str(exc)).strip()
        ) from exc

    targets = []
    for line in ps_result.stdout.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        container_id, image, name = parts
        haystack = f"{image} {name}".lower()
        if any(pattern in haystack for pattern in lowered_patterns):
            targets.append({"id": container_id, "image": image, "name": name})

    if not targets:
        if verbose:
            print("No running METS-R Docker containers found.")
        return cleanup

    for target in targets:
        result = subprocess.run(
            [docker_executable, "stop", target["id"]],
            text=True,
            capture_output=True,
        )
        record = dict(target)
        record.update(
            {
                "returncode": result.returncode,
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
            }
        )
        cleanup["docker_containers"].append(record)
        if verbose:
            status = "stopped" if result.returncode == 0 else "failed"
            print(
                f"{status}: {target['id']} "
                f"image={target['image']} name={target['name']}"
            )
            if result.returncode != 0 and result.stderr:
                print(result.stderr.strip())

    return cleanup
# ---------------------------------------------------------------------------
# Visualization server helpers
# ---------------------------------------------------------------------------

class CORSRequestHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def __init__(self, *args, directory=None, **kwargs):
            self.custom_directory = directory
            super().__init__(*args, directory=directory, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        if self.path.rstrip('/').endswith('manifest.json'):
            self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200, "ok")
        self.end_headers()

    def log_message(self, format, *args):
        """Suppress noisy dashboard polling access logs."""
        return


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64


def start_cors_http_server(directory, stop_event, port=8000):
    """Start a CORS-enabled HTTP server for the specified directory."""
    handler_class = lambda *args, **kwargs: CORSRequestHandler(*args, directory=directory, **kwargs)
    server_address = ('', port)
    httpd = ReusableThreadingHTTPServer(server_address, handler_class)
    httpd.timeout = 0.5

    def run_server():
        try:
            httpd.serve_forever(poll_interval=0.2)
        finally:
            httpd.server_close()
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.httpd = httpd
    server_thread.start()
    return server_thread

def run_visualization_server(data_folder, server_port = 8000):
    # store the current work directory
    # workdir = os.getcwd()
    # Ensure the data folder exists
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Created data folder: {data_folder}")
    
    # Start the HTTP server in a separate thread
    # os.chdir(data_folder)  # Change to the specified directory
    stop_event = Event() 
    server_thread = start_cors_http_server(data_folder, stop_event, server_port)
    _register_visualization_server(stop_event, server_thread, port=server_port, directory=data_folder)
    print(f"Serving {data_folder} with CORS enabled on port {server_port}...")

    # recovery the work directory
    # os.chdir(workdir)

    return stop_event, server_thread

def stop_visualization_server(stop_event, server_thread, port=8000, join_timeout=2.0, verbose=True):
    if stop_event is not None:
        stop_event.set()
    httpd = getattr(server_thread, "httpd", None)
    if httpd is not None:
        httpd.shutdown()

    # Send dummy request to unblock handle_request()
    if httpd is None:
        try:
            with socket.create_connection(("localhost", port), timeout=1) as sock:
                sock.sendall(b"HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        except Exception:
            pass

    if server_thread is None:
        _unregister_visualization_server(stop_event=stop_event)
        return
    server_thread.join(timeout=join_timeout)
    _unregister_visualization_server(server_thread=server_thread, stop_event=stop_event)
    if server_thread.is_alive():
        if verbose:
            print(f"Visualization server thread did not stop within {join_timeout:.1f} seconds.")
    else:
        if verbose:
            print("Visualization server stopped.")


def _simulation_folder_from_config(config, sim_index=0, output_root="output"):
    sim_dirs = getattr(config, "sim_dirs", None)
    if sim_dirs:
        try:
            return sim_dirs[sim_index]
        except IndexError as exc:
            raise IndexError(
                f"sim_index {sim_index} is outside config.sim_dirs with {len(sim_dirs)} entries"
            ) from exc

    sim_folder = getattr(config, "sim_folder", None)
    if sim_folder:
        if isinstance(sim_folder, (list, tuple)):
            try:
                return sim_folder[sim_index]
            except IndexError as exc:
                raise IndexError(
                    f"sim_index {sim_index} is outside config.sim_folder with {len(sim_folder)} entries"
                ) from exc
        return sim_folder

    sim_dir = getattr(config, "sim_dir", None)
    if sim_dir:
        sim_dir_candidates = sim_dir if isinstance(sim_dir, (list, tuple)) else [sim_dir]
        try:
            sim_dir_candidate = sim_dir_candidates[sim_index]
        except IndexError as exc:
            raise IndexError(
                f"sim_index {sim_index} is outside config.sim_dir with {len(sim_dir_candidates)} entries"
            ) from exc
        if _folder_has_trajectory_output(sim_dir_candidate):
            return sim_dir_candidate

    run_name = getattr(config, "name", None)
    if not run_name:
        raise ValueError(
            "Cannot infer simulation output folder: config needs sim_dirs, sim_folder, or a name."
        )

    seeds = getattr(config, "random_seeds", None) or []
    seed = seeds[sim_index] if sim_index < len(seeds) else None
    output_root = os.path.abspath(output_root)
    try:
        names = os.listdir(output_root)
    except OSError as exc:
        raise FileNotFoundError(f"Simulation output root does not exist: {output_root}") from exc

    prefix = f"{run_name}_"
    seed_suffix = f"_seed_{seed}" if seed is not None else None
    candidates = []
    for name in names:
        if not name.startswith(prefix):
            continue
        if seed_suffix is not None and not name.endswith(seed_suffix):
            continue
        candidate = os.path.join(output_root, name)
        if os.path.isdir(candidate):
            candidates.append(candidate)

    if not candidates:
        seed_text = f" and seed {seed}" if seed is not None else ""
        raise FileNotFoundError(
            f"No finished simulation output folder found for config name {run_name!r}{seed_text} under {output_root}"
        )

    return max(candidates, key=os.path.getmtime)


def _folder_has_trajectory_output(sim_folder):
    if not sim_folder or not os.path.isdir(sim_folder):
        return False
    for root in _configured_trajectory_roots(sim_folder):
        if _latest_trajectory_directory(root, prefer_binary=False) is not None:
            return True
    return False


def latest_trajectory_output_dir_from_config(
        config,
        sim_index=0,
        trajectory_output_dir=None,
        prefer_binary=True,
        wait_seconds=0,
        output_root="output"):
    sim_folder = _simulation_folder_from_config(
        config,
        sim_index=sim_index,
        output_root=output_root,
    )
    if trajectory_output_dir is not None:
        roots = [_resolve_trajectory_root(sim_folder, trajectory_output_dir)]
    else:
        roots = _configured_trajectory_roots(sim_folder)

    deadline = time.time() + max(0, float(wait_seconds or 0))
    while True:
        for root in roots:
            latest_directory = _latest_trajectory_directory(root, prefer_binary=prefer_binary)
            if latest_directory is not None:
                return latest_directory

        if time.time() >= deadline:
            break
        time.sleep(0.5)

    roots_text = ", ".join(str(root) for root in roots if root)
    raise FileNotFoundError("No trajectory output directory found under " + roots_text)


def start_visualization_server_from_config(
        config,
        sim_index=0,
        trajectory_output_dir=None,
        server_port=8000,
        prefer_binary=True,
        wait_seconds=0,
        output_root="output"):
    latest_directory = latest_trajectory_output_dir_from_config(
        config,
        sim_index=sim_index,
        trajectory_output_dir=trajectory_output_dir,
        prefer_binary=prefer_binary,
        wait_seconds=wait_seconds,
        output_root=output_root,
    )
    print(
        f"Starting visualization server for {_trajectory_format_name(latest_directory)} "
        f"trajectory output: {latest_directory}"
    )
    stop_event, server_thread = run_visualization_server(latest_directory, server_port)

    def stop():
        stop_visualization_server(stop_event, server_thread, server_port)

    return SimpleNamespace(
        directory=latest_directory,
        port=server_port,
        stop_event=stop_event,
        server_thread=server_thread,
        stop=stop,
    )


# ---------------------------------------------------------------------------
# Trajectory output helpers
# ---------------------------------------------------------------------------

def _read_property_values(properties_path):
    values = {}
    try:
        with open(properties_path, "r") as properties_file:
            for raw_line in properties_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


def _read_trajectory_manifest(directory):
    manifest_path = os.path.join(directory, "manifest.json")
    try:
        with open(manifest_path, "r") as manifest_file:
            return json.load(manifest_file)
    except (OSError, json.JSONDecodeError):
        return None


def _resolve_trajectory_root(sim_folder, configured_path):
    if not configured_path:
        return None
    configured_path = os.path.normpath(configured_path)
    if os.path.isabs(configured_path):
        return configured_path
    return os.path.join(sim_folder, configured_path)


def _configured_trajectory_roots(sim_folder):
    properties = _read_property_values(os.path.join(sim_folder, "data", "Data.properties"))
    roots = []
    for key in ("TRAJECTORY_BINARY_DEFAULT_PATH", "JSON_DEFAULT_PATH"):
        root = _resolve_trajectory_root(sim_folder, properties.get(key))
        if root and root not in roots:
            roots.append(root)

    default_root = os.path.join(sim_folder, "trajectory_output")
    if default_root not in roots:
        roots.append(default_root)
    return roots


def _trajectory_format_score(directory):
    try:
        names = os.listdir(directory)
    except OSError:
        return 0

    if "manifest.json" in names:
        return 3
    lowered = [name.lower() for name in names]
    if any(name.endswith(".bin") for name in lowered):
        return 2
    if any(name.endswith(".json") for name in lowered):
        return 1
    return 0


def _trajectory_format_name(directory):
    manifest = _read_trajectory_manifest(directory)
    if manifest is not None:
        output_format = manifest.get("format", "binary")
        version = manifest.get("version")
        sparse_frame_groups = manifest.get("sparseFrameGroups") or []
        sparse_suffix = " sparse" if sparse_frame_groups else ""
        if version is not None:
            return f"{output_format} v{version}{sparse_suffix}"
        return f"{output_format}{sparse_suffix}"

    score = _trajectory_format_score(directory)
    if score >= 2:
        return "binary"
    if score == 1:
        return "JSON"
    return "trajectory"


def _trajectory_manifest_summary(directory, manifest):
    chunks = manifest.get("chunks", [])
    active_chunk = manifest.get("activeChunk", {})
    road_dictionary = manifest.get("roadIdDictionary", [])
    zone_dictionary = manifest.get("zoneDictionary", [])
    charging_station_dictionary = manifest.get("chargingStationDictionary", [])
    schemas = manifest.get("schemas", {})
    frame_groups = manifest.get("frameGroups", [])
    sparse_frame_groups = manifest.get("sparseFrameGroups") or []
    sparse_frame_group_mode = manifest.get("sparseFrameGroupMode")

    return {
        "directory": directory,
        "manifest_path": os.path.join(directory, "manifest.json"),
        "format": manifest.get("format"),
        "version": manifest.get("version"),
        "byte_order": manifest.get("byteOrder"),
        "coord_scale": manifest.get("coordScale"),
        "initial_x": manifest.get("initialX"),
        "initial_y": manifest.get("initialY"),
        "tick_interval": manifest.get("tickInterval"),
        "link_snapshot_interval": manifest.get("linkSnapshotInterval"),
        "chunk_tick_limit": manifest.get("chunkTickLimit"),
        "chunk_count": len(chunks),
        "active_chunk": active_chunk,
        "road_count": len(road_dictionary),
        "zone_count": len(zone_dictionary),
        "charging_station_count": len(charging_station_dictionary),
        "frame_groups": frame_groups,
        "sparse_frame_groups": sparse_frame_groups,
        "sparse_frame_group_mode": sparse_frame_group_mode,
        "has_sparse_frame_groups": bool(sparse_frame_groups),
        "has_sparse_zone_frames": "zone" in sparse_frame_groups,
        "has_sparse_charging_station_frames": "chargingStation" in sparse_frame_groups,
        "schema_names": sorted(schemas.keys()),
        "has_zone_attributes": bool(zone_dictionary) or "zone" in schemas,
        "has_charging_station_attributes": (
            bool(charging_station_dictionary) or "chargingStation" in schemas
        ),
        "has_split_energy_fields": (
            "frameHeader" in schemas
            and any("energyPrivateEV" in field for field in schemas["frameHeader"])
        ),
    }


def _latest_trajectory_directory(root, prefer_binary=True):
    if root is None or not os.path.isdir(root):
        return None

    candidates = []
    root_score = _trajectory_format_score(root)
    if root_score > 0:
        candidates.append((root_score, os.path.getmtime(root), root))

    for name in os.listdir(root):
        candidate = os.path.join(root, name)
        if not os.path.isdir(candidate):
            continue
        score = _trajectory_format_score(candidate)
        if score > 0:
            candidates.append((score, os.path.getmtime(candidate), candidate))

    if not candidates:
        subdirs = [
            os.path.join(root, name)
            for name in os.listdir(root)
            if os.path.isdir(os.path.join(root, name))
        ]
        if not subdirs:
            return None
        return max(subdirs, key=os.path.getmtime)

    if prefer_binary:
        binary_candidates = [candidate for candidate in candidates if candidate[0] >= 2]
        if binary_candidates:
            return max(binary_candidates, key=lambda item: item[1])[2]

    return max(candidates, key=lambda item: item[1])[2]

# ---------------------------------------------------------------------------
# Simulation output path helpers
# ---------------------------------------------------------------------------

def get_sim_dir(options, i):
    sim_dir = "output/"+ options.name + "_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_seed_" + str(options.random_seeds[i])
    return sim_dir

# 
