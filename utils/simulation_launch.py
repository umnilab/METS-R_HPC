"""Launch prepared runs locally, with Docker, or with Apptainer/Singularity."""
import os
import platform
import shutil
import subprocess
import time
from os import path

from .configuration import _launcher_value
from .simulation_commands import (
    DEFAULT_METSR_SIM_IMAGE, _append_resource_arguments, _build_batch_java_command,
    _launcher_resources, _shell_args, _thin_run_binding, get_classpath,
)
from .simulation_handles import (
    SimulationLaunchGroup, SimulationLaunchHandle, _attach_launch_group,
    _preparation_duration, _write_metsr_container_id,
)


def run_simulations(options):
    """Launch the legacy interactive Repast entry point in each prepared directory.

    New headless callers should use run_simulations_in_background, which returns
    scoped process handles and builds commands as argument lists.
    """
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        if platform.system() == "Windows":
            os.chdir(options.sim_dirs[i])
            classpath = get_classpath(options, False, separator=";")
            sim_command = (
                f'"{options.java_path}java" {options.java_options} '
                f'-classpath "{classpath}" repast.simphony.runtime.RepastMain '
                f'{options.sim_dir}mets_r.rs'
            )
            if options.verbose:
                subprocess.Popen(sim_command, shell=True)
            else:
                subprocess.Popen(sim_command + f" > sim_{i}.log 2>&1 &", shell=True)
        else:
            os.chdir(options.sim_dirs[i])
            classpath = get_classpath(options, False)
            sim_command = (
                f"{options.java_path}java {options.java_options} "
                f"-classpath {classpath} repast.simphony.runtime.RepastMain "
                f"{options.sim_dir}mets_r.rs"
            )
            if options.verbose:
                os.system(sim_command)
            else:
                os.system(sim_command + f" > sim_{i}.log 2>&1 &")
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
    options accepted by the Java command builder. All commands are argv lists and
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

            launch_cwd = os.getcwd()
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
            finally:
                # Docker Desktop bind mounts can detach the current Windows-drive
                # mount under WSL. Re-enter its absolute path before later imports
                # (including MKL) or relative-path operations use the stale cwd.
                try:
                    os.getcwd()
                except FileNotFoundError:
                    os.chdir(launch_cwd)
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
            _write_metsr_container_id(sim_dir, container_id)
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

    default_executable = "apptainer"
    for candidate in ("apptainer", "singularity", "appcontainer"):
        if shutil.which(candidate):
            default_executable = candidate
            break
    executable = str(
        _launcher_value(
            options,
            ("appcontainer_executable",),
            env_name="METSR_APPCONTAINER_EXECUTABLE",
            default=default_executable,
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
    from .visualization.registry import (
        stop_all_metsr_client_servers,
        stop_all_visualization_servers,
    )

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
