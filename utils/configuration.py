"""Run configuration loading, shared option lookup, and port allocation."""
import json
import os
import socket
import sys
import time
from collections.abc import Mapping
from contextlib import closing
from types import SimpleNamespace


def config_value(config, name, default=None):
    """Read a mapping key or object attribute, retaining explicitly set None."""
    if config is None:
        return default
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _first_option_value(options, names):
    """Return the first explicitly configured, non-None alias."""
    for name in names:
        value = options.get(name) if isinstance(options, dict) else getattr(options, name, None)
        if value is not None:
            return True, value
    return False, None


def _format_property_value(value):
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _coerce_bool(value, default=False):
    """Interpret the boolean spellings accepted by JSON/environment options."""
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
    """Resolve option aliases, then an environment override, then the default."""
    for name in names:
        value = options.get(name) if isinstance(options, dict) else getattr(options, name, None)
        if value is not None and value != "":
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
    """Select a per-run list value, or use a scalar unchanged for every run."""
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        if index >= len(value):
            raise ValueError(
                f"Launcher option has {len(value)} values for simulation index {index}"
            )
        return value[index]
    return value


def find_free_ports(options, num_simulations):
    """Store candidate TCP ports on the options namespace before launch."""
    options.ports = []
    while True:
        for _ in range(num_simulations):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
                probe.bind(("localhost", 0))
                options.ports.append(probe.getsockname()[1])
        try:
            for port in options.ports:
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe.bind(("", port))
                probe.close()
            break
        except:
            print("The port is not valid anymore, regenerate it")
            continue
    time.sleep(1)


def _load_raw_config(fname):
    """Recursively load a config JSON, merging parent_config fields first."""
    fname = os.path.abspath(fname)
    with open(fname, "r") as source:
        raw = json.load(source)

    if "parent_config" in raw:
        parent_path = os.path.abspath(
            os.path.join(os.path.dirname(fname), raw["parent_config"])
        )
        parent_raw = _load_raw_config(parent_path)
        child_fields = {key: value for key, value in raw.items() if key != "parent_config"}
        return {**parent_raw, **child_fields}

    return raw


def read_run_config(fname):
    """Load inherited JSON into a namespace and validate the per-run seed count."""
    merged = _load_raw_config(fname)
    config = SimpleNamespace(**merged)

    if len(config.random_seeds) != config.num_simulations:
        print("ERROR, please specify random seeds for all simulation instances")
        sys.exit(-1)

    return config


def _read_property_values(properties_path):
    """Read uncommented key=value properties; a missing/unreadable file is empty."""
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


def _normalize_config_json_path(config_json):
    """Resolve an optional configuration path to its absolute filesystem path."""
    if config_json is None:
        return None
    return os.path.abspath(os.fspath(config_json))


def _config_signature_from_path(config_json):
    """Serialize inherited configuration values for restart comparisons."""
    if config_json is None:
        return None
    raw_config = _load_raw_config(config_json)
    return json.dumps(raw_config, sort_keys=True, separators=(",", ":"), default=str)


def _port_from_config(config, sim_index=0, default=None):
    """Select the configured port for one simulation, accepting scalar ports."""
    if config is None:
        return default

    for attr_name in ("ports", "metsr_port"):
        ports = getattr(config, attr_name, None)
        if ports is None:
            continue
        if isinstance(ports, (list, tuple)):
            if not ports:
                continue
            return int(ports[int(sim_index)])
        return int(ports)
    return default


def _sim_folder_from_config(config, sim_dirs=None, sim_index=0, default=None):
    """Select a run folder, preferring explicit directories over configuration."""
    candidates = sim_dirs if sim_dirs is not None else getattr(config, "sim_dirs", None)
    if candidates:
        return candidates[int(sim_index)]

    sim_folder = getattr(config, "sim_folder", None)
    if isinstance(sim_folder, (list, tuple)):
        return sim_folder[int(sim_index)]
    if sim_folder:
        return sim_folder
    return default
