"""Read simulator log diagnostics independently of client connection state."""

import os

from .simulation_handles import _docker_console_tail_from_sim_folder


def read_fatal_simulator_error(sim_folder):
    """Return a fatal JVM log excerpt and known remediation, or None."""
    if not sim_folder:
        return None
    log_path = os.path.join(sim_folder, "logs", "mets_r.log")
    try:
        with open(log_path, "rb") as log_file:
            log_file.seek(0, os.SEEK_END)
            size = log_file.tell()
            log_file.seek(max(0, size - 65536))
            file_tail = log_file.read().decode("utf-8", errors="replace")
    except OSError:
        file_tail = ""

    console_tail = _docker_console_tail_from_sim_folder(sim_folder)
    tail = file_tail
    if console_tail:
        tail = tail + "\n" + console_tail
    fatal_markers = (
        "FATAL JVM ERROR",
        "Unresolved compilation problem",
        'Exception in thread "main"',
        "ExceptionInInitializerError",
    )
    if not any(marker in tail for marker in fatal_markers):
        return None
    file_has_fatal = any(marker in file_tail for marker in fatal_markers)
    console_has_fatal = any(
        marker in console_tail for marker in fatal_markers
    )
    if console_has_fatal and not file_has_fatal:
        fatal_source = (
            f"Docker console for simulation folder {sim_folder}"
        )
    elif console_has_fatal:
        fatal_source = (
            f"{log_path} and the scoped Docker console"
        )
    else:
        fatal_source = log_path

    lines = tail.splitlines()
    marker_index = 0
    for index, line in enumerate(lines):
        if any(marker in line for marker in fatal_markers):
            marker_index = index

    excerpt = "\n".join(lines[marker_index:marker_index + 14])
    guidance = ""
    if "maxWaitingTime cannot be resolved or is not a field" in tail:
        guidance = (
            "\n\nThis matches a METS-R_SIM build issue in the maxWaitingTime "
            "Control API change. Add an Integer maxWaitingTime field to "
            "MessageClass.ZoneIDOrigDestRouteNameNum and rebuild METS-R_SIM."
        )
    elif (
            "validateNetworkConfiguration() is undefined for the type "
            "GlobalVariables" in tail):
        guidance = (
            "\n\nThe current upstream fbce204 Docker image contains "
            "inconsistent compiled classes. Rebuild METS-R_SIM after "
            "defining GlobalVariables.validateNetworkConfiguration(), or "
            "use a corrected upstream image."
        )
    return (
        f"METS-R simulator reported a fatal JVM error in {fatal_source}:\n"
        f"{excerpt}{guidance}"
    )


def read_simulator_startup_timings(sim_folder):
    """Read network and fleet startup durations from the latest simulator run."""
    timings = {
        "network_load_seconds": None,
        "fleet_spawn_seconds": None,
    }
    if not sim_folder:
        return timings
    log_path = os.path.join(sim_folder, "logs", "mets_r.log")
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as log_file:
            lines = log_file.readlines()
    except OSError:
        return timings

    build_ms = None
    vehicle_ms = None
    fleet_end_ms = None
    for line in lines:
        first_token = line.lstrip().split(None, 1)
        if not first_token:
            continue
        token = first_token[0].strip("[]")
        if not token.isdigit():
            continue
        relative_ms = int(token)
        if "Building subcontexts" in line:
            build_ms = relative_ms
            vehicle_ms = None
            fleet_end_ms = None
        elif build_ms is not None and "VehicleContext creation" in line:
            vehicle_ms = relative_ms
        elif vehicle_ms is not None and "Total EV taxis generated" in line:
            fleet_end_ms = relative_ms
        elif vehicle_ms is not None and "Total EV buses generated" in line:
            fleet_end_ms = relative_ms

    if build_ms is not None and vehicle_ms is not None and vehicle_ms >= build_ms:
        timings["network_load_seconds"] = (vehicle_ms - build_ms) / 1000.0
    if vehicle_ms is not None and fleet_end_ms is not None and fleet_end_ms >= vehicle_ms:
        timings["fleet_spawn_seconds"] = (fleet_end_ms - vehicle_ms) / 1000.0
    return timings
