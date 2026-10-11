"""Build Java, resource, recording, and container-bind arguments without launching."""
import os
import platform
import re
import shlex
import sys
from os import path

from .configuration import (
    _coerce_bool, _format_property_value, _instance_value, _launcher_value,
)


DEFAULT_METSR_SIM_IMAGE = "ennuilei/mets-r_sim:latest"


DEFAULT_METSR_SIM_APPCONTAINER_IMAGE = f"docker://{DEFAULT_METSR_SIM_IMAGE}"


def get_classpath(options, includeBin=True, separator=":"):
    """Return the interactive Repast classpath, retaining the legacy trailing separator."""
    if not path.exists(options.repast_plugin_dir):
        print(f"ERROR , repast plugins not found at {options.repast_plugin_dir}")
        sys.exit(-1)
    entries = [
        options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/bin",
        options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/lib/*",
        options.sim_dir + "lib/*",
    ]
    return separator.join(entries) + separator


def get_classpath2(options, includeBin=True, separator=":"):
    """Return the batch Repast classpath in its established search order."""
    plugin_root = options.repast_plugin_dir
    entries = [
        plugin_root + "repast.simphony.runtime_2.7.0/bin",
        plugin_root + "repast.simphony.runtime_2.7.0/lib/*",
        plugin_root + "repast.simphony.batch_2.7.0/bin",
        plugin_root + "repast.simphony.batch_2.7.0/lib/*",
        plugin_root + "repast.simphony.distributed.batch_2.7.0/bin",
        plugin_root + "repast.simphony.distributed.batch_2.7.0/lib/*",
        plugin_root + "repast.simphony.core_2.7.0/bin",
        plugin_root + "repast.simphony.core_2.7.0/lib/*",
        options.sim_dir + "bin",
        options.sim_dir + "lib/*",
        plugin_root + "repast.simphony.bin_and_src_2.7.0/repast.simphony.bin_and_src.jar",
        plugin_root + "repast.simphony.essentials_2.7.0/bin",
        plugin_root + "repast.simphony.gis_2.7.0/bin",
        plugin_root + "repast.simphony.gis_2.7.0/lib/*",
        plugin_root + "repast.simphony.sql_2.7.0/bin",
        plugin_root + "repast.simphony.sql_2.7.0/lib/*",
        plugin_root + "repast.simphony.scenario_2.7.0/bin",
    ]
    return separator.join(entries) + separator


def _shell_args(value):
    """Normalize string/list arguments while preserving legacy Windows paths."""
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


def _build_batch_java_command(options, index, sim_dir, classpath_separator=":"):
    """Build an argv command and optional scoped JFR recording without starting Java."""
    jvm_options = _launcher_value(
        options,
        ("jvm_options", "java_options"),
        env_name="METSR_JVM_OPTIONS",
        default="",
    )
    extra_options = _launcher_value(
        options, ("jvm_extra_options", "java_extra_options"), default=""
    )

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
    jfr_args = []
    filename = None
    if enabled:
        if isinstance(raw_options, dict):
            fragments = [
                f"{key}={_format_property_value(value)}"
                for key, value in raw_options.items()
                if value is not None
            ]
        elif isinstance(raw_options, (list, tuple)):
            fragments = [str(item) for item in raw_options if str(item)]
        else:
            fragments = [str(raw_options or "").strip().strip(",")]
        for fragment in fragments:
            if (
                "-xx:startflightrecording" in fragment.lower()
                or re.search(r"(?<![A-Za-z0-9_])filename\s*=", fragment, re.IGNORECASE)
            ):
                raise ValueError(
                    "jfr_options must not set filename or provide "
                    "-XX:StartFlightRecording; use jfr_filename plus recording-option "
                    "fragments instead"
                )

        # Keep the recording inside the mounted run directory, even when the
        # requested filename is absolute or contains symbolic links.
        raw_filename = str(
            _launcher_value(
                options,
                ("jfr_filename",),
                env_name="METSR_JFR_FILENAME",
                default=f"profiles/metsr_{index}.jfr",
            )
        ).format(index=index)
        run_root = path.realpath(path.abspath(sim_dir))
        if path.isabs(raw_filename):
            host_filename = path.realpath(path.abspath(raw_filename))
        else:
            host_relative = raw_filename.replace("\\", os.sep).replace("/", os.sep)
            host_filename = path.realpath(path.abspath(path.join(run_root, host_relative)))
        scope_error = (
            "jfr_filename must resolve inside simulation run directory "
            f"{run_root}: {raw_filename}"
        )
        try:
            common_root = path.commonpath((run_root, host_filename))
        except ValueError as exc:
            raise ValueError(scope_error) from exc
        if path.normcase(common_root) != path.normcase(run_root):
            raise ValueError(scope_error)
        filename = path.relpath(host_filename, run_root)
        if filename in ("", "."):
            raise ValueError(f"jfr_filename must name a file inside {run_root}")
        os.makedirs(path.dirname(host_filename), exist_ok=True)
        filename = filename.replace("\\", "/")

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
        option_fragment = ",".join(fragments)
        if option_fragment:
            recording_options.append(option_fragment)
        if _coerce_bool(_launcher_value(options, ("jfr_unlock_commercial_features",))):
            jfr_args.append("-XX:+UnlockCommercialFeatures")
        jfr_args.append("-XX:StartFlightRecording=" + ",".join(recording_options))

    java_executable = _launcher_value(options, ("java_executable",))
    if not java_executable:
        java_path = str(_launcher_value(options, ("java_path",), default="") or "")
        java_executable = java_path.rstrip("/\\") + "/java" if java_path else "java"
    scenario_dir = str(options.sim_dir).rstrip("/\\") + "/mets_r.rs"
    command = [
        str(java_executable),
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
    return command, {"enabled": enabled, "filename": filename}


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


