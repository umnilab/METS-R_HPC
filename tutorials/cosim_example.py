import argparse
import os
import sys
import xml.etree.ElementTree as ET
from collections import deque

# This script lives in tutorials/, but it needs to import from the repo's
# top-level `clients/` and `utils/` packages and resolve relative paths such as
# `configs/...`, `data/...`, and `docker/` against the repo root. Make that work
# regardless of where the script is invoked from.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from clients.METSRClient import METSRClient
from utils.carla_util import (
    CarlaCosimState,
    configure_metsr_cosim_roads,
    open_carla,
    release_ready_cosim_vehicles_from_queue,
    set_overlook_camera,
    step_carla_metsr_cosim,
)
from utils.util import (
    prepare_sim_dirs,
    read_run_config,
    run_simulation_in_docker,
)


# use case: python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v
def get_arguments(argv):
    parser = argparse.ArgumentParser(description="METS-R simulation")
    parser.add_argument(
        "-r",
        "--run_config",
        default="configs/run_cosim_CARLAT5.json",
        help="the folder that contains all the input data",
    )
    parser.set_defaults(display_all=False, all_cosim_roads=True)
    parser.add_argument(
        "-a",
        "--display_all",
        dest="display_all",
        action="store_true",
        help="mirror all METS-R private vehicles into CARLA",
    )
    parser.add_argument(
        "--no_display_all",
        dest="display_all",
        action="store_false",
        help="only show vehicles that are on explicit co-sim roads",
    )
    parser.add_argument(
        "--cosim_roads",
        default=None,
        help="comma-separated SUMO road IDs that CARLA should control; default uses every METS-R road",
    )
    parser.add_argument(
        "--all_cosim_roads",
        action="store_true",
        help="mark every METS-R road as CARLA-controlled",
    )
    parser.add_argument(
        "--no_all_cosim_roads",
        dest="all_cosim_roads",
        action="store_false",
        help="only mark roads supplied by --cosim_roads or by the config",
    )
    parser.add_argument(
        "--vehicles_per_road",
        type=int,
        default=1,
        help="number of private vehicles to generate from each co-sim road",
    )
    parser.add_argument(
        "--start_vid",
        type=int,
        default=0,
        help="first private vehicle ID used by generated co-sim trips",
    )
    parser.add_argument(
        "--route_advance_interval",
        type=int,
        default=25,
        help="ticks between METS-R road-level route advancement calls for CARLA-controlled vehicles",
    )
    parser.add_argument(
        "--reroute_route_threshold",
        type=int,
        default=3,
        help="reroute a vehicle when its remaining METS-R route has this many roads or fewer",
    )
    parser.add_argument(
        "--reroute_min_hops",
        type=int,
        default=8,
        help="prefer replacement destinations at least this many road hops away",
    )
    parser.add_argument(
        "--viz_port",
        type=int,
        default=8000,
        help="port for the trajectory file visualization server",
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="verbose mode")
    args = parser.parse_args(argv)

    config = read_run_config(args.run_config)
    config.run_config = args.run_config
    config.display_all = args.display_all
    config.cosim_roads = args.cosim_roads
    config.all_cosim_roads = args.all_cosim_roads
    config.vehicles_per_road = max(0, args.vehicles_per_road)
    config.start_vid = args.start_vid
    config.route_advance_interval = max(0, args.route_advance_interval)
    config.reroute_route_threshold = max(1, args.reroute_route_threshold)
    config.reroute_min_hops = max(1, args.reroute_min_hops)
    config.viz_port = args.viz_port
    config.verbose = args.verbose

    return config


def get_all_roads(network_file):
    """Return all road IDs from a SUMO net.xml and its paired xodr file.

    metsr_road  - all non-internal SUMO edge IDs (strings, e.g. "-47", "0").
                  Internal junction connector edges (id starts with ":") are excluded.
    carla_road  - all road IDs from the OpenDRIVE xodr file (ints), covering both
                  regular roads (junction="-1") and junction connector roads.
    """
    tree = ET.parse(network_file)
    root = tree.getroot()
    metsr_roads = [
        edge.get("id")
        for edge in root.findall("edge")
        if not edge.get("id", "").startswith(":")
    ]

    xodr_file = network_file.replace(".net.xml", ".xodr")
    carla_roads = []
    if os.path.exists(xodr_file):
        xodr_tree = ET.parse(xodr_file)
        xodr_root = xodr_tree.getroot()
        carla_roads = [int(road.get("id")) for road in xodr_root.findall("road")]
    else:
        print(f"Warning: xodr file not found at {xodr_file}, carla_road will be empty.")

    return metsr_roads, carla_roads


def configured_sim_folder(config):
    if getattr(config, "sim_dirs", None):
        return config.sim_dirs[0]
    return getattr(config, "sim_folder", None)


def open_metsr_client(config):
    return METSRClient(
        host=config.metsr_host,
        port=int(config.ports[0]),
        sim_folder=configured_sim_folder(config),
        timeout=getattr(config, "timeout", 30),
        verbose=getattr(config, "verbose", False),
        config_json=getattr(config, "run_config", None),
        config=config,
    )


def configured_cosim_roads(config, all_metsr_roads):
    if getattr(config, "all_cosim_roads", False):
        print(f"Using all {len(all_metsr_roads)} METS-R roads as CARLA-controlled co-sim roads.")
        return list(all_metsr_roads)

    explicit_roads = getattr(config, "cosim_roads", None)
    if explicit_roads:
        return [road.strip() for road in explicit_roads.split(",") if road.strip()]

    return list(getattr(config, "metsr_road", []) or [])


def query_road_adjacency(metsr, roads, batch_size=25):
    adjacency = {road: [] for road in roads or []}
    if not roads:
        return adjacency

    for batch_start in range(0, len(roads), batch_size):
        batch = roads[batch_start:batch_start + batch_size]
        response = metsr.query_road(id=batch)
        for record in response.get("DATA", []):
            if not isinstance(record, dict):
                continue
            road_id = record.get("ID")
            if road_id is None:
                continue
            adjacency[str(road_id)] = [
                str(downstream)
                for downstream in record.get("down_stream_road", []) or []
            ]

    return adjacency


def downstream_roads(route_graph, road):
    if route_graph is None:
        return []
    if hasattr(route_graph, "successors"):
        return list(route_graph.successors(road))
    return list(route_graph.get(road, []))


def choose_destination_road(route_graph, roads, origin, salt=0, min_hops=8):
    roads = [str(road) for road in roads or []]
    origin = str(origin) if origin is not None else None
    if not roads:
        return None
    if route_graph is not None and origin in route_graph:
        lengths = {}
        queue = deque([(origin, 0)])
        seen = {origin}
        while queue:
            road, depth = queue.popleft()
            lengths[road] = depth
            for downstream in downstream_roads(route_graph, road):
                if downstream not in seen:
                    seen.add(downstream)
                    queue.append((downstream, depth + 1))

        candidates = [
            road
            for road, depth in lengths.items()
            if road != origin and road in roads and depth >= min_hops
        ]
        if not candidates:
            candidates = [
                road
                for road, depth in lengths.items()
                if road != origin and road in roads and depth > 0
            ]
        if candidates:
            candidates = sorted(candidates)
            return candidates[salt % len(candidates)]
        return None

    candidates = [road for road in roads if road != origin]
    if not candidates:
        return None
    return candidates[salt % len(candidates)]


def stable_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return sum(ord(char) for char in str(value))


def seed_vehicle_on_each_cosim_road(metsr, roads, route_graph, vehicles_per_road, start_vid, min_hops, verbose=False):
    veh_ids = []
    origins = []
    destinations = []
    next_vid = int(start_vid)

    for road_index, origin in enumerate(roads):
        for copy_index in range(vehicles_per_road):
            salt = road_index + copy_index * max(1, len(roads) // 2)
            destination = choose_destination_road(
                route_graph,
                roads,
                origin,
                salt=salt,
                min_hops=min_hops,
            )
            if destination is None:
                continue
            veh_ids.append(next_vid)
            origins.append(origin)
            destinations.append(destination)
            next_vid += 1

    if not veh_ids:
        print("No co-sim vehicles generated: no reachable origin/destination road pairs found.")
        return {}

    response = metsr.generate_trip_between_roads(
        vehID=veh_ids,
        origin=origins,
        destination=destinations,
    )
    vehicle_destinations = {}
    ok_count = 0
    for index, (veh_id, origin, destination) in enumerate(zip(veh_ids, origins, destinations)):
        status = response_status(response, index=index)
        if status == "OK":
            ok_count += 1
            vehicle_destinations[veh_id] = destination
            if verbose:
                print(f"Generated co-sim vehicle {veh_id}: {origin} -> {destination}")
        elif verbose:
            print(f"Skipped co-sim vehicle {veh_id}: {origin} -> {destination}, status={status}")

    print(f"Generated {ok_count}/{len(veh_ids)} co-sim private vehicle trip(s).")
    return vehicle_destinations


def response_status(response, index=0, default="KO"):
    if not isinstance(response, dict):
        return default
    data = response.get("DATA", [])
    if index >= len(data):
        return default
    record = data[index]
    if isinstance(record, dict):
        return record.get("STATUS", default)
    if isinstance(record, str):
        return record
    return default


def route_from_response(response):
    data = response.get("DATA", [])
    if not data:
        return []
    record = data[0]
    if not isinstance(record, dict):
        return []
    return record.get("road_list", []) or []


def reroute_vehicle(metsr, veh_id, private_veh, current_road, roads, route_graph, vehicle_destinations, reroute_counts, min_hops, verbose=False):
    if current_road is None:
        return False

    current_road = str(current_road)
    salt = stable_int(veh_id) + reroute_counts.get(veh_id, 0) + len(vehicle_destinations)
    destination = choose_destination_road(
        route_graph,
        roads,
        current_road,
        salt=salt,
        min_hops=min_hops,
    )
    if destination is None or destination == current_road:
        return False

    route = route_from_response(metsr.query_route_between_roads(current_road, destination))
    if len(route) < 2:
        if verbose:
            print(f"No replacement route for vehicle {veh_id}: {current_road} -> {destination}")
        return False

    response = metsr.update_vehicle_route(
        vehID=veh_id,
        route=route,
        private_veh=private_veh,
    )
    status = response_status(response)
    if status != "OK":
        if verbose:
            print(f"Failed to reroute vehicle {veh_id}: {current_road} -> {destination}, status={status}")
        return False

    vehicle_destinations[veh_id] = destination
    reroute_counts[veh_id] = reroute_counts.get(veh_id, 0) + 1
    if verbose:
        print(f"Rerouted vehicle {veh_id}: {current_road} -> {destination} ({len(route)} roads)")
    return True


def manage_active_vehicle_routes(
    metsr,
    step_result,
    roads,
    route_graph,
    vehicle_destinations,
    reroute_counts,
    tick,
    route_threshold,
    route_advance_interval,
    min_hops,
    verbose=False,
):
    cosim_vehicles = step_result.get("cosim_vehicles", [])
    vehicle_states = {
        state.get("ID"): state
        for state in step_result.get("vehicle_states", [])
        if isinstance(state, dict)
    }

    for vehicle in cosim_vehicles:
        veh_id = vehicle.get("ID")
        private_veh = vehicle.get("v_type", True)
        route = vehicle.get("route", []) or []
        state = vehicle_states.get(veh_id, {})
        current_road = state.get("roadID") or (route[0] if route else None)

        if len(route) <= route_threshold:
            reroute_vehicle(
                metsr,
                veh_id,
                private_veh,
                current_road,
                roads,
                route_graph,
                vehicle_destinations,
                reroute_counts,
                min_hops,
                verbose=verbose,
            )
            continue

        if route_advance_interval and tick % route_advance_interval == 0 and len(route) > 1:
            next_road = route[1]
            response = metsr.enter_next_road(
                vehID=veh_id,
                roadID=next_road,
                private_veh=private_veh,
            )
            status = response_status(response)
            if verbose:
                print(f"Advanced vehicle {veh_id} to next METS-R road {next_road}: {status}")


def wait_for_exit_after_completion(sim_minutes):
    try:
        input(
            f"Simulation reached {sim_minutes:g} minutes. "
            "Press Enter to terminate METS-R and the visualization server..."
        )
    except EOFError:
        print("No keyboard input available; terminating METS-R and the visualization server.")


def shutdown_metsr(metsr, timeout=3):
    if metsr is None:
        return

    if getattr(metsr, "viz_server", None) is not None:
        print("Stopping visualization server.")
        try:
            metsr.stop_viz()
        except Exception as exc:
            print(f"Visualization server shutdown failed; continuing cleanup: {exc}")

    original_timeout = getattr(metsr, "timeout", None)
    if original_timeout is not None:
        metsr.timeout = min(float(original_timeout), float(timeout))

    try:
        print("Sending METS-R termination request.")
        metsr.terminate()
        print("METS-R client terminated.")
    except Exception as exc:
        print(f"METS-R termination request failed or timed out; closing client socket: {exc}")
        try:
            metsr.close()
        except Exception as close_exc:
            print(f"METS-R client close failed: {close_exc}")
    finally:
        if original_timeout is not None:
            metsr.timeout = original_timeout


def run_cosimulation(config, carla_client, carla_tm):
    metsr = open_metsr_client(config)
    metsr.start_offline_viz(server_port=getattr(config, "viz_port", 8000), wait_seconds=60)
    configure_metsr_cosim_roads(metsr, getattr(config, "metsr_road", []))
    route_graph = query_road_adjacency(
        metsr,
        getattr(config, "all_metsr_roads", None) or getattr(config, "metsr_road", []),
    )
    vehicle_destinations = seed_vehicle_on_each_cosim_road(
        metsr,
        getattr(config, "metsr_road", []),
        route_graph,
        vehicles_per_road=getattr(config, "vehicles_per_road", 1),
        start_vid=getattr(config, "start_vid", 0),
        min_hops=getattr(config, "reroute_min_hops", 8),
        verbose=getattr(config, "verbose", False),
    )
    reroute_counts = {}
    release_ready_cosim_vehicles_from_queue(metsr, verbose=getattr(config, "verbose", False))

    world = carla_client.get_world()
    set_overlook_camera(world)
    state = CarlaCosimState()
    sim_minutes = 10
    total_ticks = int(sim_minutes * 60 / config.sim_step_size)

    try:
        for tick in range(total_ticks):
            print("Tick:", tick)
            step_result = step_carla_metsr_cosim(
                metsr,
                world,
                carla_tm,
                state=state,
                carla_roads=getattr(config, "carla_road", []),
                metsr_roads=getattr(config, "metsr_road", []),
                display_all=getattr(config, "display_all", False),
                release_ready_queue=bool(getattr(config, "metsr_road", [])),
                verbose=getattr(config, "verbose", False),
            )
            manage_active_vehicle_routes(
                metsr,
                step_result,
                getattr(config, "metsr_road", []),
                route_graph,
                vehicle_destinations,
                reroute_counts,
                tick,
                route_threshold=getattr(config, "reroute_route_threshold", 3),
                route_advance_interval=getattr(config, "route_advance_interval", 25),
                min_hops=getattr(config, "reroute_min_hops", 8),
                verbose=getattr(config, "verbose", False),
            )
        else:
            wait_for_exit_after_completion(sim_minutes)
    except KeyboardInterrupt:
        print("simulation interrupted by user")
    finally:
        print("Terminating METS-R client and visualization server.")
        shutdown_metsr(metsr)


if __name__ == "__main__":
    config = get_arguments(sys.argv[1:])
    os.chdir("docker")
    os.system("docker-compose up -d")
    os.chdir("..")

    prepare_sim_dirs(config)

    metsr_roads, carla_roads = get_all_roads(config.network_file)

    config.all_metsr_roads = metsr_roads
    config.metsr_road = configured_cosim_roads(config, metsr_roads)
    config.carla_road = carla_roads

    carla_client, carla_tm = open_carla(config)

    run_simulation_in_docker(config)
    run_cosimulation(config, carla_client, carla_tm)
