"""First-layer convenience functions for notebooks and small experiment scripts.

Setup, launch, and visualization entry points are imported from their implementations.
Client connection and request workflows belong in ``clients/METSRClient.py``;
launcher classes and visualization internals belong in their dedicated modules.
"""

import os
from os import path
import shutil
import subprocess
import time

from .configuration import find_free_ports, read_run_config
from .primitives import (
    check_socket,
    str_list_mapper_gen,
    _has_value,
    VEHICLE_SENSOR_DSRC,
    VEHICLE_SENSOR_CV2X,
    VEHICLE_SENSOR_MOBILE_DEVICE,
    VEHICLE_SENSOR_TYPES,
)

from .simulation_commands import (
    DEFAULT_METSR_SIM_IMAGE,
    DEFAULT_METSR_SIM_APPCONTAINER_IMAGE,
    get_classpath,
    get_classpath2,
)
from .simulation_handles import (
    SimulationLaunchHandle,
    SimulationLaunchGroup,
    LaunchHandle,
    LaunchGroup,
)
from .simulation_launch import (
    clear_all,
    run_simulations,
    run_simulations_in_background,
    run_simulation_in_docker,
    run_simulation_in_appcontainer,
)
from .simulation_setup import (
    force_copytree,
    get_sim_dir,
    modify_property_file,
    prepare_sim_dirs,
)
from .visualization.registry import (
    register_metsr_client,
    unregister_metsr_client,
    stop_all_visualization_servers,
    stop_all_metsr_client_servers,
)
from .visualization.servers import (
    CORSRequestHandler,
    ReusableThreadingHTTPServer,
    start_cors_http_server,
    run_visualization_server,
    stop_visualization_server,
    start_visualization_server_from_config,
)
from .visualization.trajectories import latest_trajectory_output_dir_from_config


# Kafka service entry points

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

# Visualization URL parameters

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

__all__ = [
    "check_socket",
    "str_list_mapper_gen",
    "kafka_bootstrap_servers",
    "docker_compose_command",
    "run_docker_compose",
    "wait_for_kafka",
    "start_kafka",
    "build_metsr_vis_url",
    "read_run_config",
    "find_free_ports",
    "modify_property_file",
    "force_copytree",
    "prepare_sim_dirs",
    "get_sim_dir",
    "get_classpath",
    "get_classpath2",
    "run_simulations",
    "run_simulations_in_background",
    "run_simulation_in_docker",
    "run_simulation_in_appcontainer",
    "clear_all",
    "start_cors_http_server",
    "run_visualization_server",
    "stop_visualization_server",
    "latest_trajectory_output_dir_from_config",
    "start_visualization_server_from_config",
    "register_metsr_client",
    "unregister_metsr_client",
    "stop_all_visualization_servers",
    "stop_all_metsr_client_servers",
    "DEFAULT_KAFKA_BOOTSTRAP_SERVERS",
    "DEFAULT_METSR_SIM_IMAGE",
    "DEFAULT_METSR_SIM_APPCONTAINER_IMAGE",
    "SimulationLaunchHandle",
    "SimulationLaunchGroup",
    "LaunchHandle",
    "LaunchGroup",
    "CORSRequestHandler",
    "ReusableThreadingHTTPServer",
    "VEHICLE_SENSOR_DSRC",
    "VEHICLE_SENSOR_CV2X",
    "VEHICLE_SENSOR_MOBILE_DEVICE",
    "VEHICLE_SENSOR_TYPES",
    "METS_R_VIS_PURDUE_MAP_ID",
    "METS_R_VIS_PRIVATE_VEHICLE_TYPE",
    "METS_R_VIS_VEHICLE_TYPE_BY_GROUP",
]
