"""Start a blank METS-R SIM instance for external processes.

By default this launches the CARLA Town05 co-simulation config in Docker on
``ws://localhost:4000`` and keeps this launcher alive while other processes
connect separately. Press Ctrl+C to stop the METS-R SIM container this script started.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional


_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
_DEFAULT_CONFIG = _REPO_ROOT / "configs" / "run_cosim_CARLAT5.json"
_DEFAULT_TOWN = "Town05"
_DEFAULT_IMAGE = "ennuilei/mets-r_sim"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a blank METS-R SIM Docker instance.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-c", "--config", default=None, help="METS-R run config JSON. Defaults from --town.")
    parser.add_argument("--town", default=_DEFAULT_TOWN, help="CARLA town config to launch when --config is omitted, e.g. Town06.")
    parser.add_argument("--host", default="localhost", help="METS-R websocket host.")
    parser.add_argument("--port", type=int, default=4000, help="METS-R websocket port.")
    parser.add_argument("--seed", type=int, default=42, help="Simulation random seed.")
    parser.add_argument("--name", default=None, help="Override the output folder run name.")
    parser.add_argument("--image", default=_DEFAULT_IMAGE, help="Docker image to run.")
    parser.add_argument("--sim-folder", default=None, help="Existing/prepared sim folder, useful with an already-running server.")
    parser.add_argument("--wait-seconds", type=float, default=60.0, help="Seconds to wait for METS-R to open the websocket.")
    parser.add_argument("--probe-interval-s", type=float, default=0.5, help="Port probe interval while waiting.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose METS-R config output.")
    parser.add_argument("--clear-all", action="store_true", help="Call clear_all() before starting a new METS-R SIM.")
    parser.add_argument("--clear-wait-seconds", type=float, default=10.0, help="Seconds to wait for the METS-R port to close after --clear-all.")
    parser.add_argument("--attach-existing", action="store_true", help="Attach to an already-running METS-R SIM without verifying its map/config.")
    parser.add_argument("--maintain-interval-s", type=float, default=5.0, help="Port probe interval while keeping the launcher alive.")

    return parser.parse_args()


def as_abs_path(path_value: str | os.PathLike[str]) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (_REPO_ROOT / path).resolve()


def config_for_town(town: str) -> Path:
    text = str(town or _DEFAULT_TOWN).strip()
    key = text.lower().replace("_", "").replace("-", "").replace(" ", "")
    aliases = {
        "purdue": "run_cosim_CARLAPurdue.json",
        "westlafayette": "run_cosim_CARLAPurdue.json",
        "duckietown": "run_cosim_CARLADuckieTown.json",
        "dukietown": "run_cosim_CARLADuckieTown.json",
    }
    if key in aliases:
        candidate = _REPO_ROOT / "configs" / aliases[key]
    else:
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            raise ValueError(f"Cannot infer a METS-R config from --town {town!r}; pass --config explicitly.")
        candidate = _REPO_ROOT / "configs" / f"run_cosim_CARLAT{int(digits)}.json"
    if not candidate.exists():
        raise FileNotFoundError(f"No METS-R run config found for --town {town!r}: {candidate}")
    return candidate.resolve()


def resolve_config_path(args: argparse.Namespace) -> Path:
    if args.config:
        return as_abs_path(args.config)
    return config_for_town(args.town)


def port_reachable(host: str, port: int, timeout_s: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=max(0.1, float(timeout_s))):
            return True
    except OSError:
        return False


def wait_for_port(host: str, port: int, wait_seconds: float, interval_s: float) -> bool:
    deadline = time.time() + max(0.0, float(wait_seconds))
    while time.time() <= deadline:
        if port_reachable(host, int(port), timeout_s=min(1.0, max(0.1, float(interval_s)))):
            return True
        time.sleep(max(0.1, float(interval_s)))
    return False


def wait_for_port_release(host: str, port: int, wait_seconds: float, interval_s: float) -> bool:
    deadline = time.time() + max(0.0, float(wait_seconds))
    while True:
        if not port_reachable(host, int(port), timeout_s=min(1.0, max(0.1, float(interval_s)))):
            return True
        if time.time() >= deadline:
            return False
        time.sleep(max(0.1, float(interval_s)))


def check_docker_ready() -> None:
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker CLI was not found. Install/start Docker Desktop first.") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Timed out checking Docker. Make sure Docker Desktop is running.") from exc

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown Docker error").strip()
        raise RuntimeError(f"Docker is not ready: {detail}")


def prepare_blank_config(args: argparse.Namespace) -> tuple[Any, str]:
    from utils.util import prepare_sim_dirs, read_run_config

    config_path = resolve_config_path(args)
    config = read_run_config(str(config_path))
    config.num_simulations = 1
    config.metsr_host = args.host
    config.metsr_port = [int(args.port)]
    config.random_seeds = [int(args.seed)]
    config.verbose = bool(args.verbose)
    if args.name:
        config.name = str(args.name)

    original_cwd = os.getcwd()
    try:
        os.chdir(_REPO_ROOT)
        prepare_sim_dirs(config)
    finally:
        os.chdir(original_cwd)

    sim_folder = as_abs_path(config.sim_dirs[0])
    config.sim_dirs = [str(sim_folder)]
    return config, str(sim_folder)


def start_metsr_docker_container(config: Any, image: str) -> Optional[str]:
    from utils.util import get_classpath2

    sim_folder = str(Path(config.sim_dirs[0]).resolve())
    sim_command = (
        f"{config.java_path}java -Xmx16G "
        f"-cp {get_classpath2(config, False)} "
        f"repast.simphony.batch.BatchMain "
        f"-params {config.sim_dir}mets_r.rs/batch_params.xml "
        f"-interactive {config.sim_dir}mets_r.rs"
    )
    docker_args = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--mount",
        f"src={sim_folder},target=/home/test,type=bind",
        "--net=host",
        str(image),
        "/bin/bash",
        "-c",
        f"cd /home/test && {sim_command}",
    ]
    result = subprocess.run(docker_args, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown Docker error").strip()
        raise RuntimeError(f"Failed to start METS-R Docker container: {detail}")
    return result.stdout.strip() or None

def stop_metsr_docker_container(container_id: Optional[str]) -> None:
    if not container_id:
        return
    print(f"Stopping METS-R Docker container: {container_id}")
    try:
        result = subprocess.run(["docker", "stop", container_id], capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        print("Docker CLI was not found while stopping METS-R SIM; container may still be running.")
        return
    except subprocess.TimeoutExpired:
        print("Timed out stopping METS-R SIM container; container may still be running.")
        return

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown Docker error").strip()
        print(f"Could not stop METS-R SIM container {container_id}: {detail}")
    else:
        print(f"Stopped METS-R Docker container: {container_id}")


def maintain_launcher(host: str, port: int, interval_s: float, container_id: Optional[str]) -> int:
    interval = max(0.5, float(interval_s))
    if container_id:
        print(
            "Maintaining blank METS-R SIM launcher. "
            "Press Ctrl+C to stop this launcher and the METS-R SIM container it started."
        )
    else:
        print(
            "Maintaining blank METS-R SIM launcher for an already-running METS-R SIM. "
            "Press Ctrl+C to exit this launcher; the external METS-R SIM will not be stopped."
        )
    try:
        while True:
            if not port_reachable(host, int(port), timeout_s=min(1.0, interval)):
                print(f"METS-R websocket ws://{host}:{int(port)} is no longer reachable; exiting launcher.")
                return 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nLauncher stopped by user.")
        return 0
    finally:
        stop_metsr_docker_container(container_id)

def main() -> int:
    args = parse_args()
    sim_folder = as_abs_path(args.sim_folder) if args.sim_folder else None
    config = None
    container_id = None

    if args.clear_all:
        from utils.util import clear_all

        print("Clearing existing METS-R SIM containers and helper servers before startup.")
        clear_all(verbose=True)
        if not wait_for_port_release(args.host, int(args.port), args.clear_wait_seconds, args.probe_interval_s):
            raise RuntimeError(
                f"clear_all() returned, but ws://{args.host}:{int(args.port)} is still accepting TCP connections. "
                "Another process or a non-matching container is still holding the METS-R port; stop it or choose a different --port."
            )

    if port_reachable(args.host, int(args.port), timeout_s=0.75):
        if not args.attach_existing:
            requested_config = resolve_config_path(args)
            raise RuntimeError(
                f"METS-R SIM is already reachable at ws://{args.host}:{int(args.port)}, but this launcher "
                f"cannot verify that it matches {requested_config}. Run with --clear-all to stop existing "
                "METS-R containers and start the requested blank config, or pass --attach-existing if you "
                "intentionally want to use the server already on this port."
            )
        print(
            f"METS-R SIM is already reachable at ws://{args.host}:{int(args.port)}; "
            "attaching without map/config verification."
        )
    else:
        check_docker_ready()
        config, sim_folder = prepare_blank_config(args)
        print(f"Starting blank METS-R SIM from {resolve_config_path(args)}")
        print(f"METS-R output folder: {sim_folder}")
        container_id = start_metsr_docker_container(config, args.image)
        if container_id:
            print(f"METS-R Docker container ID: {container_id}")
        if not wait_for_port(args.host, int(args.port), args.wait_seconds, args.probe_interval_s):
            stop_metsr_docker_container(container_id)
            raise RuntimeError(
                f"METS-R SIM did not open ws://{args.host}:{int(args.port)} within {float(args.wait_seconds):.1f}s. "
                "Check Docker Desktop host networking and the METS-R logs in the output folder."
            )

    print(f"METS-R websocket: ws://{args.host}:{int(args.port)}")
    if sim_folder:
        print(f"METS-R sim folder: {sim_folder}")

    return maintain_launcher(args.host, int(args.port), args.maintain_interval_s, container_id)


if __name__ == "__main__":
    raise SystemExit(main())
