"""Prepare independent simulation run directories and Data.properties files."""
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime
from os import path

from .configuration import (
    _coerce_bool, _first_option_value, _format_property_value, _instance_value, find_free_ports,
)


_PROPERTY_RE = re.compile(r"^(\s*)([A-Za-z0-9_]+)\s*=\s*(.*?)(\r?\n?)$")


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
    "NO_LANECHANGING_LENGTH": ("no_lane_changing_length",),
    "MAX_ROAD_TRAVERSAL_PATIENCE": ("max_stuck_time",),
}


_REQUIRED_SIM_PROPERTY_DEFAULTS = {
    "ENABLE_INTERSECTION_SWEPT_COLLISION_CHECK": False,
    "LANE_CHANGE_LATERAL_SPEED": 1.0,
    "LANE_CHANGE_MIN_DURATION": 1.0,
    # Both names are harmless when unused and let custom/older templates run
    # against either side of the METS-R property rename.
    "MAX_ROAD_TRAVERSAL_PATIENCE": 600,
}


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

    snake_name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", key)
    snake_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake_name).lower()
    names = dict.fromkeys((key.lower(), snake_name, *_PROPERTY_OPTION_ALIASES.get(key, ())))
    found, value = _first_option_value(options, names)
    file_aliases = {
        "ZONES_SHAPEFILE": ("zone_file", ".shp"),
        "ZONES_CSV": ("zone_file", ".csv"),
        "CHARGER_SHAPEFILE": ("charging_station_file", ".shp"),
        "CHARGER_CSV": ("charging_station_file", ".csv"),
    }
    if not found and key in file_aliases:
        alias, extension = file_aliases[key]
        found, value = _first_option_value(options, (alias,))
        if found:
            value = str(value)
            if not value.lower().endswith(extension):
                value += extension
    elif not found and key == "RH_DEMAND_SHARABLE":
        found, _ = _first_option_value(options, ("rh_wait_file", "rh_waiting_time"))
        value = True if found else None
    if found and key in {"BT_START_HOUR", "N_THREADS", "N_PARTITION"}:
        value = _instance_value(value, instance)
    if found and key == "BT_START_HOUR":
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("bt_start_hour must be a non-negative integer")
    return found, value


def modify_property_file(
    options,
    src_data_dir,
    dest_data_dir,
    port,
    instance,
    template,
    data_path_prefix=None,
):
    """Generate one run's properties from the selected immutable template."""
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
                    line = f"{key} = {_format_property_value(value)}" + (newline or "\n")
            if "data/" in line:
                data_root = (data_path_prefix or src_data_dir).replace("\\", "/").rstrip("/")
                line = line.replace("data/", data_root + "/")
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
                f"{key} = {_format_property_value(value if found else default)}\n"
            )


def force_copytree(src, dst):
    """Replace the destination tree with a complete copy of the source."""
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def prepare_sim_dirs(options):
    """Prepare full input copies or thin mutable overlays and record setup timings."""
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
    if hasattr(options, "metsr_port"):
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
            # A thin run owns only its properties and input manifest. Refuse to
            # overwrite a copied input tree or follow a stale directory link.
            if path.lexists(dest_data_dir):
                if path.islink(dest_data_dir) or not path.isdir(dest_data_dir):
                    raise FileExistsError(
                        f"Refusing to reuse thin-run data directory {dest_data_dir}: "
                        "the path is not a real directory"
                    )
            else:
                os.makedirs(dest_data_dir)
            entries = set(os.listdir(dest_data_dir))
            unexpected = sorted(entries - {".metsr_hpc_inputs.json", "Data.properties"})
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
        options.sim_dirs.append(dir_name)
        os.makedirs(path.join(dir_name, "logs"), exist_ok=True)
        shutil.copy(src_data_dir + "/log4j.properties", dir_name + "/log4j.properties")
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
        dest_data_dirs.append(dest_data_dir[:-5])  # Remove the trailing "/data".
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


def get_sim_dir(options, i):
    """Build the timestamped output path for one configured random seed."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return "output/" + options.name + "_" + timestamp + "_seed_" + str(options.random_seeds[i])
