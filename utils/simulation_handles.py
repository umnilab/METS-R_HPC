"""Track individual launches, collect startup timings, and clean up owned resources."""
import os
import re
import subprocess
import time
from contextlib import contextmanager
from os import path

from .configuration import _launcher_value, _set_launcher_value


_METSR_CONTAINER_ID_FILENAME = "mets_r_container_id"


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


def _write_metsr_container_id(sim_dir, container_id):
    """Persist the scoped container ID for client-side startup diagnostics."""
    log_dir = path.join(sim_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    with open(
            path.join(log_dir, _METSR_CONTAINER_ID_FILENAME),
            "w",
            encoding="ascii") as container_file:
        container_file.write(str(container_id).strip() + "\n")


def _docker_console_tail_from_sim_folder(sim_folder, line_count=160):
    """Return bounded Docker console output for a launcher-created run."""
    if not sim_folder:
        return ""
    container_file = path.join(
        os.fspath(sim_folder), "logs", _METSR_CONTAINER_ID_FILENAME
    )
    if not path.isfile(container_file):
        return ""
    try:
        with open(container_file, "r", encoding="ascii") as source:
            container_id = source.read().strip()
    except OSError:
        return ""
    if not isinstance(container_id, str):
        return ""
    if re.fullmatch(r"[0-9a-fA-F]{12,64}", container_id) is None:
        return ""

    docker_executable = os.environ.get(
        "METSR_DOCKER_EXECUTABLE", "docker"
    )
    try:
        result = subprocess.run(
            [
                docker_executable,
                "logs",
                "--tail",
                str(max(1, int(line_count))),
                container_id,
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return ""
    if result.returncode != 0:
        return ""
    return "\n".join(
        part for part in (result.stdout, result.stderr) if part
    )


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
                if "No such container" not in error:
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
