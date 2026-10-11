"""Isolated PCLA benchmark runs for demo2 (no optional imports at module load)."""
from __future__ import annotations

import ast
import csv
import functools
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import types
from contextlib import contextmanager
from dataclasses import fields, replace
from typing import Any

# PCLA's registry stores path prefixes for these trained model seeds.
_MODEL_SEEDS = {
    "tfv4_l6": range(3), "tfv4_lav": range(3),
    "tfv4_wp": range(3), "tfv4_aim": range(3),
    "carl_carl": range(2), "carl_roach": range(5),
    "carl_plant": range(5), "plant2_plant2": range(3),
}
CSV_FIELDS = (
    "agent", "run", "seed", "status", "scenario", "town", "num_commuters",
    "requested_seconds", "timestep_s", "simulation_steps", "simulation_seconds",
    "loop_wall_s", "steps_per_wall_second", "realtime_factor",
    "metsr_s", "carla_s", "scenic_s", "pcla_agent_s",
    "pcla_setup_s", "pcla_action_s", "pcla_action_calls", "pcla_clamped_action_calls",
    "metsr_tick_calls", "carla_tick_calls", "compile_s", "scene_generation_s",
    "simulator_setup_s", "simulate_call_s", "termination_reason", "error",
)


def agent_catalog(root: Path) -> dict[str, dict[str, Any]]:
    """Expand the installed registry without importing any agents or models."""
    registry = json.loads((root / "agents.json").read_text(encoding="utf-8"))
    result = {}
    for family, variants in registry.items():
        if family.lower() == "autoware":
            continue  # Requires a separately managed ROS 2 / Docker stack.
        for variant, entry in variants.items():
            name = f"{family}_{variant}"
            seeds = entry.get("seeds", _MODEL_SEEDS.get(name, (None,)))
            for seed in seeds:
                selector = name if seed is None else f"{name}_{seed}"
                # Match give_path(): insert the seed before the config extension.
                base, extension = os.path.splitext(entry["config"])
                config = entry["config"] if seed is None else f"{base}{seed}{extension}"
                result[selector] = {"agent": entry["agent"], "config": config}
    return dict(sorted(result.items()))


def unavailable_assets(root: Path, entry: dict[str, Any]) -> str:
    missing = [str(root / entry[key]) for key in ("agent", "config")
               if not (root / entry[key]).exists()]
    return "Missing PCLA assets: " + ", ".join(missing) if missing else ""


class _AgentImportFinder(importlib.abc.MetaPathFinder):
    """Prefer an agent's namespace packages over unrelated top-level modules.

    sys.path ordering alone does not do this: Python prefers a later util.py
    over an earlier util/ directory without __init__.py (as shipped by PlanT).
    """
    def __init__(self, directories):
        self.directories = [str(directory) for directory in directories]

    def find_spec(self, fullname, path=None, target=None):
        if path is not None:
            return None
        for directory in self.directories:
            spec = importlib.machinery.PathFinder.find_spec(fullname, [directory])
            if spec is not None:
                return spec
        return None


@contextmanager
def agent_imports(*directories):
    finder = _AgentImportFinder(directories)
    previous_path = sys.path[:]
    sys.path[:0] = finder.directories
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.path[:] = previous_path
        sys.meta_path.remove(finder)


def selector_entry(root: Path, selector: str) -> dict[str, Any]:
    """Resolve the registry's default variant and optional model seed."""
    registry = json.loads((root / "agents.json").read_text(encoding="utf-8"))
    parts = selector.split("_")
    variants = registry[parts[0]]
    variant = parts[1] if len(parts) > 1 else next(iter(variants))
    entry = dict(variants[variant])
    if len(parts) > 2:
        base, extension = os.path.splitext(entry["config"])
        entry["config"] = f"{base}{parts[2]}{extension}"
    return entry


@contextmanager
def map_helper_imports(bird_dir: Path):
    """Support PlanT 2's mix of standalone and package-relative BEV imports."""
    if bird_dir.parent.name != "carla_garage":
        yield
        return
    package_name = "_demo2_bev_helpers"
    def selected(name):
        return name == "traffic_light" or name == package_name or name.startswith(package_name + ".")
    previous = {name: module for name, module in sys.modules.copy().items() if selected(name)}
    package = types.ModuleType(package_name)
    package.__path__ = [str(bird_dir)]
    sys.modules[package_name] = package
    try:
        sys.modules["traffic_light"] = importlib.import_module(package_name + ".traffic_light")
        yield
    finally:
        for name in list(sys.modules):
            if selected(name):
                del sys.modules[name]
        sys.modules.update(previous)


def prepare_agent_map(root: Path, selector: str, world_map: Any, cache_root: Path) -> Path | None:
    """Build missing CaRL/Roach/PlanT 2 BEV assets from the loaded map, outside timing.

    Only PCLA's MapImage library is used: its standalone CLI launches/kills
    servers and must never be run here. Generated files stay in our cache.
    """
    entry = selector_entry(root, selector)
    agent_file = root / entry["agent"]
    bird_dir = agent_file.parent / "birds_eye_view"
    if agent_file.parent.name == "plant2":
        bird_dir = agent_file.parent / "carla_garage" / "birds_eye_view"
        config_file = bird_dir.parent / "config.py"
        spec = importlib.util.spec_from_file_location("_demo2_plant2_map_config", config_file)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        config = dict(map_folder="maps_2ppm_cv",
                      pixels_per_meter=config_module.GlobalConfig().pixels_per_meter_collection)
    else:
        config_file = root / entry["config"] / "config.json"
        if agent_file.parent.name != "carl" or not config_file.is_file():
            return None
        config = json.loads(config_file.read_text(encoding="utf-8"))
    if "map_folder" not in config or "pixels_per_meter" not in config:
        return None
    town = world_map.name.rsplit("/", 1)[-1]
    supplied_map = bird_dir / config["map_folder"] / f"{town}.h5"
    if supplied_map.is_file():
        return None
    import h5py

    generator_file = bird_dir / "birdview_map_opencv.py"
    ppm = float(config["pixels_per_meter"])
    digest = hashlib.sha256(world_map.to_opendrive().encode("utf-8"))
    digest.update(generator_file.read_bytes())
    digest.update(str(ppm).encode("ascii"))
    cache_dir = cache_root / f"{digest.hexdigest()[:20]}_{ppm:g}ppm"
    cache_file = cache_dir / f"{town}.h5"
    if cache_file.is_file():
        return cache_dir
    print(f"Preparing {town} bird's-eye-view map at {ppm:g} pixels/m: {cache_file}", flush=True)
    with agent_imports(bird_dir, agent_file.parent), map_helper_imports(bird_dir):
        spec = importlib.util.spec_from_file_location("_demo2_bev_map_builder", generator_file)
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        masks = generator.MapImage.draw_map_image(world_map, ppm)
    cache_dir.mkdir(parents=True, exist_ok=True)
    temporary = cache_file.with_suffix(f".{os.getpid()}.tmp")
    try:
        with h5py.File(temporary, "w") as output:
            output.attrs["pixels_per_meter"] = ppm
            output.attrs["world_offset_in_meters"] = masks["world_offset"]
            output.attrs["width_in_meters"] = masks["width_in_meters"]
            output.attrs["width_in_pixels"] = masks["width_in_pixels"]
            for name, mask in masks.items():
                if name not in {"world_offset", "pixels_per_meter", "width_in_meters", "width_in_pixels"}:
                    output.create_dataset(name, data=mask, compression="gzip", compression_opts=9)
        temporary.replace(cache_file)
    finally:
        temporary.unlink(missing_ok=True)
    return cache_dir


def module_override(module, **overrides):
    """Override dependencies for one adapter without changing shared modules."""
    proxy = types.ModuleType(module.__name__)
    proxy.__dict__.update(vars(module))
    proxy.__dict__.update(overrides)
    return proxy


def prepare_agent_speed_map(root: Path, selector: str, world: Any, cache_root: Path) -> Path | None:
    """Run PlanT's existing speed-limit algorithm against the current world."""
    entry = selector_entry(root, selector)
    agent_dir = (root / entry["agent"]).parent
    speed_dir = agent_dir / "carla_garage" / "speed_limits"
    generator_file = speed_dir / "compute_speed_limits.py"
    if not generator_file.is_file():
        return None
    world_map = world.get_map()
    town = world_map.name.rsplit("/", 1)[-1]
    filename = f"{town}_speed_limits.npy"
    if (speed_dir / filename).is_file():
        return None
    source = generator_file.read_bytes()
    digest = hashlib.sha256(world_map.to_opendrive().encode("utf-8") + source).hexdigest()[:20]
    cache_dir = cache_root / f"{digest}_speed_limits"
    cache_file = cache_dir / filename
    if cache_file.is_file():
        return cache_file
    print(f"Preparing {town} speed-limit map: {cache_file}", flush=True)
    # This upstream script runs an all-town sweep at import time. Load ONLY its
    # imports and functions; never execute the top-level world-reloading loop.
    tree = ast.parse(source, filename=str(generator_file))
    tree.body = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))]
    generator = types.ModuleType("_demo2_speed_map_builder")
    exec(compile(tree, str(generator_file), "exec"), generator.__dict__)
    client = types.SimpleNamespace(set_timeout=lambda timeout: None, load_world=lambda name: world)
    generator.carla = module_override(generator.carla, Client=lambda *args, **kwargs: client)
    numpy = generator.np
    temporary = cache_file.with_suffix(f".{os.getpid()}.tmp")
    def save_map(name, data):
        if name != filename:
            raise RuntimeError(f"Unexpected speed-limit output: {name}")
        with temporary.open("wb") as output:
            numpy.save(output, data)
    generator.np = module_override(numpy, save=save_map)
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        generator.compute_speed_limit_map(0, town)
        temporary.replace(cache_file)
    finally:
        temporary.unlink(missing_ok=True)
    return cache_file


def redirect_speed_map(costs, agent_dir: Path, cache_file: Path):
    planner = sys.modules.get("carla_garage.privileged_route_planner")
    if planner is None:
        raise RuntimeError("PlanT did not expose its privileged route planner")
    expected = agent_dir / "carla_garage" / "speed_limits" / cache_file.name
    numpy = planner.np
    def load_map(file, *args, **kwargs):
        if isinstance(file, (str, os.PathLike)) and Path(file).resolve() == expected.resolve():
            file = cache_file
        return numpy.load(file, *args, **kwargs)
    costs.patch(planner, "np", module_override(numpy, load=load_map))


def agent_import_directories(agent_dir: Path) -> tuple[Path, ...]:
    directories = [agent_dir]
    if agent_dir.name == "lmdrive":
        # PCLA vendors its own LAVIS/timm forks; do not install a different lavis.
        directories.extend(path for path in (agent_dir / "LAVIS", agent_dir / "vision_encoder")
                           if path.is_dir())
    return tuple(directories)


class _LavisInferenceLoader(importlib.machinery.SourceFileLoader):
    """Exclude optional diffusion registrations from LMDrive's LAVIS imports."""
    def get_code(self, fullname):
        tree = ast.parse(self.get_data(self.path), filename=self.path)
        optional_modules = {"lavis.models.blip_diffusion_models.blip_diffusion",
                            "lavis.processors.blip_diffusion_processors"}
        omitted = {alias.asname or alias.name for node in tree.body
                   if isinstance(node, ast.ImportFrom) and node.module in optional_modules
                   for alias in node.names}
        tree.body = [node for node in tree.body if not (
            isinstance(node, ast.ImportFrom) and node.module in optional_modules)]
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                    isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
                if not isinstance(node.value, (ast.List, ast.Tuple)):
                    raise ImportError(f"Unsupported LAVIS exports in {self.path}")
                node.value.elts = [item for item in node.value.elts
                                   if not (isinstance(item, ast.Constant) and item.value in omitted)]
        return compile(tree, self.path, "exec")


class _LavisInferenceFinder(importlib.abc.MetaPathFinder):
    def __init__(self, root: Path):
        self.root = root

    def find_spec(self, fullname, path=None, target=None):
        if fullname not in {"lavis.models", "lavis.processors"}:
            return None
        file = self.root.joinpath(*fullname.split("."), "__init__.py")
        if not file.is_file():
            return None
        return importlib.util.spec_from_file_location(
            fullname, file, loader=_LavisInferenceLoader(fullname, str(file)),
            submodule_search_locations=[str(file.parent)])


@contextmanager
def lmdrive_inference_imports(agent_dir: Path):
    # The driving model does not use BLIP-Diffusion. Its old Diffusers import
    # expects removed Hugging Face APIs; omit only that optional registration.
    # Source files and installed package versions are left untouched.
    if agent_dir.name != "lmdrive" or not (agent_dir / "LAVIS").is_dir():
        yield
        return
    finder = _LavisInferenceFinder(agent_dir / "LAVIS")
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        sys.meta_path.remove(finder)


def resolve_agent_asset_paths(value, root: Path):
    """Resolve PCLA-local config paths, preserving Hub IDs and absolute paths."""
    if isinstance(value, str) and value.startswith(("pcla_agents/", "./pcla_agents/")):
        return str(root / value)
    if isinstance(value, dict):
        for key, item in value.items():
            value[key] = resolve_agent_asset_paths(item, root)
    elif isinstance(value, list):
        value[:] = [resolve_agent_asset_paths(item, root) for item in value]
    elif isinstance(value, tuple):
        value = tuple(resolve_agent_asset_paths(item, root) for item in value)
    return value


def configure_agent_asset_paths(module, family: str, root: Path):
    """Adapt the controller's config reader before its model is constructed."""
    if family == "lmdrive":
        original_load = module.imp.load_source

        def load_config(*args, **kwargs):
            config_module = original_load(*args, **kwargs)
            config_class = config_module.GlobalConfig
            original_init = config_class.__init__

            @functools.wraps(original_init)
            def initialize(config, *args, **kwargs):
                original_init(config, *args, **kwargs)
                for name in ("preception_model_ckpt", "lmdrive_ckpt"):
                    value = getattr(config, name)
                    setattr(config, name, resolve_agent_asset_paths(value, root))

            config_class.__init__ = initialize
            return config_module

        module.imp = module_override(module.imp, load_source=load_config)
    elif family in {"minddrive", "orion"}:
        original_config = module.Config

        class AssetConfig(original_config):
            @staticmethod
            def fromfile(*args, **kwargs):
                config = original_config.fromfile(*args, **kwargs)
                resolve_agent_asset_paths(config._cfg_dict, root)
                return config

        module.Config = AssetConfig


class _LMDriveAgentLoader(importlib.machinery.SourceFileLoader):
    """Fix LMDrive's bounded LiDAR copy without changing its preprocessing."""
    def get_code(self, fullname):
        tree = ast.parse(self.get_data(self.path), filename=self.path)
        broken_copy = ast.dump(ast.parse("lidar[:num_points, :4] = lidar_xyzr").body[0])
        for function in tree.body:
            if isinstance(function, ast.FunctionDef) and function.name == "lidar_to_raw_features":
                for statement in function.body:
                    if ast.dump(statement) == broken_copy:
                        # Keep ego filtering, the seeded shuffle, sanitization,
                        # padding and rotation. Cap only the shuffled source copy.
                        statement.value = ast.copy_location(
                            ast.parse("lidar_xyzr[:num_points, :4]", mode="eval").body,
                            statement.value)
        return compile(ast.fix_missing_locations(tree), self.path, "exec")


class _AgentConfigLoader(importlib.abc.Loader):
    def __init__(self, loader, family: str, root: Path):
        self.loader, self.family, self.root = loader, family, root

    def create_module(self, spec):
        return self.loader.create_module(spec)

    def exec_module(self, module):
        self.loader.exec_module(module)
        configure_agent_asset_paths(module, self.family, self.root)


@contextmanager
def agent_config_paths(pcla_cls, root: Path, agent_file: Path):
    # PCLA uses spec_from_file_location rather than normal imports. Override its
    # module-local importlib reference only; never change the process cwd or the
    # shared importlib/imp modules while Scenic and sensor threads are running.
    family = agent_file.parent.name
    if family not in {"lmdrive", "minddrive", "orion"}:
        yield
        return
    module = sys.modules[pcla_cls.__module__]
    original = module.importlib

    def spec_from_file_location(name, location, *args, **kwargs):
        spec = original.util.spec_from_file_location(name, location, *args, **kwargs)
        if spec is not None and Path(location).resolve() == agent_file.resolve():
            if family == "lmdrive" and isinstance(spec.loader, importlib.machinery.SourceFileLoader):
                spec.loader = _LMDriveAgentLoader(name, str(location))
            spec.loader = _AgentConfigLoader(spec.loader, family, root.resolve())
        return spec

    module.importlib = module_override(original, util=module_override(
        original.util, spec_from_file_location=spec_from_file_location))
    try:
        yield
    finally:
        module.importlib = original


def unavailable_optional_dependencies(root: Path, selector: str) -> str:
    # These larger stacks are optional in the installed PCLA distribution.
    # Probe imports before compiling Scenic or loading a world/model. An absent
    # transitive package or incompatible package API is also an unavailable stack.
    packages = {"lmdrive": ("lavis",),
                "minddrive": ("nuscenes", "similaritymeasures", "diffusers.schedulers"),
                "orion": ("flash_attn", "similaritymeasures", "diffusers.schedulers")}
    required = packages.get(selector.split("_")[0], ())
    if not required:
        return ""
    entry = selector_entry(root, selector)
    agent_dir = (root / entry["agent"]).parent
    try:
        with agent_imports(*agent_import_directories(agent_dir)), lmdrive_inference_imports(agent_dir):
            for name in required:
                importlib.import_module(name)
    except ImportError as exc:
        return (f"Optional controller {selector} is unavailable: {format_error(exc)}. "
                f"Install a compatible dependency stack for {agent_dir} to enable it.")
    return ""


@contextmanager
def retain_registered_modules(pcla_cls, prefixes):
    """Keep registry-owning libraries paired with their cached registrations."""
    pcla_module = sys.modules.get(pcla_cls.__module__)
    original = getattr(pcla_module, "clear_agent_modules", None)
    if original is None or not prefixes:
        yield
        return

    def clear_modules(*args, **kwargs):
        preserved = {name: module for name, module in sys.modules.copy().items()
                     if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)}
        try:
            return original(*args, **kwargs)
        finally:
            sys.modules.update(preserved)

    pcla_module.clear_agent_modules = clear_modules
    try:
        yield
    finally:
        pcla_module.clear_agent_modules = original


def redirect_plant2_map(costs, cache_dir: Path):
    observer_module = sys.modules.get("carla_garage.birds_eye_view.chauffeurnet")
    if observer_module is None:
        raise RuntimeError("PlanT 2 did not expose its BEV observation manager")
    observer = observer_module.ObsManager
    original = observer.__init__

    @functools.wraps(original)
    def initialize(instance, obs_configs, *args, **kwargs):
        # DataAgent builds a fresh config dict on its first action, after setup.
        # Override that dict for both its real and augmented ego observers.
        return original(instance, dict(obs_configs, map_folder=str(cache_dir)), *args, **kwargs)

    costs.patch(observer, "__init__", initialize)


def normalize_vehicle_control(control) -> bool:
    """Saturate finite PCLA actuator commands to Scenic/CARLA's valid ranges."""
    values = {}
    changed = False
    for name, low, high in (("throttle", 0.0, 1.0), ("brake", 0.0, 1.0), ("steer", -1.0, 1.0)):
        value = float(getattr(control, name))
        if not math.isfinite(value):
            raise ValueError(f"PCLA returned non-finite {name}: {value}")
        values[name] = max(low, min(high, value))
        changed |= values[name] != value
    for name, value in values.items():
        setattr(control, name, value)
    return changed


def install_agent_compatibility(costs, pcla_cls, root: Path):
    if not hasattr(pcla_cls, "setup_agent"):
        return
    original = pcla_cls.setup_agent
    @functools.wraps(original)
    def setup_agent(instance, selector, *args, **kwargs):
        # Scenic localPath returns Path; InterFuser/LMDrive put this in os.environ.
        if isinstance(getattr(instance, "routePath", None), os.PathLike):
            instance.routePath = os.fspath(instance.routePath)
        entry = selector_entry(root, selector)
        agent_dir = (root / entry["agent"]).parent
        # ThinkTwice's mmcv registers classes in mmengine. Reimporting mmcv
        # while keeping mmengine's registry raises duplicate-registration errors.
        retained = ("mmcv",) if selector.split("_")[0] == "tt" else ()
        with agent_imports(*agent_import_directories(agent_dir)), lmdrive_inference_imports(agent_dir), \
                retain_registered_modules(pcla_cls, retained), agent_config_paths(pcla_cls, root, root / entry["agent"]):
            try:
                result = original(instance, selector, *args, **kwargs)
            except KeyboardInterrupt:
                watchdog = getattr(instance, "_watchdog", None)
                if watchdog is not None and watchdog.get_status() is False:
                    timeout = getattr(watchdog, "_timeout", "unknown")
                    raise TimeoutError(
                        f"PCLA setup watchdog timed out for {selector} after {timeout} seconds. "
                        "Model downloads/loading exceeded the setup deadline; cache the model "
                        "weights first or increase PCLA_WATCHDOG_SEC. The sweep will continue."
                    ) from None
                raise  # Preserve a real Ctrl+C as an interruption of the suite.
        map_dir = getattr(costs, "agent_map_dir", None)
        if map_dir is not None:
            if agent_dir.name == "plant2":
                redirect_plant2_map(costs, map_dir)
            else:
                instance.agent_instance.config.map_folder = str(map_dir)
        speed_map = getattr(costs, "agent_speed_map", None)
        if speed_map is not None:
            redirect_speed_map(costs, agent_dir, speed_map)
        return result
    costs.patch(pcla_cls, "setup_agent", setup_agent)
    if hasattr(pcla_cls, "get_action"):
        original_action = pcla_cls.get_action

        @functools.wraps(original_action)
        def get_action(instance, *args, **kwargs):
            control = original_action(instance, *args, **kwargs)
            if normalize_vehicle_control(control):
                costs.counts["pcla_clamped_action_calls"] += 1
            return control

        costs.patch(pcla_cls, "get_action", get_action)


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    """Preserve worker-added columns and publish only a fully written CSV."""
    # Workers can import a newer module than a long-running suite parent.
    # Retain their additional metrics when updating failure/interruption rows.
    fieldnames = dict.fromkeys(CSV_FIELDS)
    for row in rows:
        fieldnames.update(dict.fromkeys(row))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="", dir=path.parent,
                prefix=f".{path.name}.", suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            writer = csv.DictWriter(output, fieldnames=list(fieldnames))
            writer.writeheader()
            writer.writerows(rows)
        # Close first for Windows; the same-directory replacement is atomic.
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def base_row(args: Any, run_number: int = 0) -> dict[str, Any]:
    seed = int(args.seed) + (max(0, run_number - 1) if args.increment_seed else 0)
    return dict(agent=args.pcla_agent, run=run_number, seed=seed, status="failed",
                scenario=args.scenic_file, town=args.town, num_commuters=args.num_commuters,
                requested_seconds=args.length, timestep_s=args.timestep, error="")


def worker_command(args: Any, csv_path: Path) -> list[str]:
    from tutorials.dashboard_demos import demo2

    command = [sys.executable, str(Path(demo2.__file__).resolve())]
    for field in fields(args):
        value = getattr(args, field.name)
        if value is None:
            continue
        option = field.name.replace("_", "-")
        if isinstance(value, bool):
            command.append("--" + ("" if value else "no-") + option)
        else:
            command.extend(("--" + option, str(value)))
    command.extend(("--benchmark-worker-csv", str(csv_path)))
    return command


def format_error(exc: BaseException) -> str:
    """Keep the underlying RPC error when Scenic wraps world-load failures."""
    details = []
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        details.append(f"{type(exc).__name__}: {str(exc).strip() or repr(exc)}")
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
    return "; caused by: ".join(details)


def check_carla_ready(args: Any) -> None:
    """Read-only checks, outside worker imports and all throughput timing."""
    endpoint = f"{args.address}:{args.carla_port}"
    try:
        import carla
        client = carla.Client(args.address, int(args.carla_port))
        client.set_timeout(min(5.0, float(args.carla_timeout_s)))
        client.get_server_version()
        maps = client.get_available_maps()
        client.get_world().get_map()
    except Exception as exc:
        raise RuntimeError(
            f"CARLA readiness check failed at {endpoint}. Check the host/port and "
            "ensure only one CARLA instance owns the RPC, streaming, and secondary "
            "ports (normally 2000-2002). Restart CARLA if it is unresponsive."
        ) from exc
    town = str(args.town).replace("\\", "/").rsplit("/", 1)[-1]
    available = {str(name).replace("\\", "/").rsplit("/", 1)[-1] for name in maps}
    if town not in available:
        raise RuntimeError(
            f"CARLA at {endpoint} does not provide world {args.town!r}. "
            f"Available maps: {', '.join(sorted(available))}. "
            "Install the requested CARLA map or select an available --town."
        )


def run_suite(args: Any) -> int:
    from tutorials.dashboard_demos import demo2

    args = replace(args, benchmark_agent=True)
    root = demo2.resolve_pcla_root(args)
    catalog = agent_catalog(root) if args.pcla_agent == "all" else {}
    selectors = list(catalog) if args.pcla_agent == "all" else [args.pcla_agent]
    if not selectors:
        raise ValueError("PCLA agents.json contains no native controllers to benchmark")
    session = f"{time.strftime('%Y%m%d_%H%M%S')}_{time.time_ns()}"
    directory = Path(args.export_folder) / "benchmarks" / session
    directory.mkdir(parents=True)
    print(f"Benchmark output: {directory}", flush=True)
    print("Using identical scenario parameters and seed sequences; dashboard rendering disabled.", flush=True)
    if args.pcla_agent == "all":
        print(f"Discovered {len(selectors)} native selectors; Autoware excluded.", flush=True)
    completed = failed = skipped = 0
    for index, selector in enumerate(selectors, 1):
        if not selector or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in selector):
            raise ValueError(f"Invalid PCLA selector: {selector!r}")
        child_args = replace(args, pcla_agent=selector,
                             export_folder=str(directory / "artifacts" / selector))
        csv_path = directory / f"{selector}.csv"
        log_path = directory / f"{selector}.log"
        print(f"[{index}/{len(selectors)}] {selector}", flush=True)
        reason = unavailable_assets(root, catalog[selector]) if selector in catalog else ""
        if reason:
            write_rows(csv_path, [dict(base_row(child_args), status="skipped", error=reason)])
            print(f"  skipped: {reason}", flush=True)
            skipped += 1
            continue
        # A shared server outage must not incur a full timeout for every model.
        try:
            check_carla_ready(args)
        except (Exception, KeyboardInterrupt) as exc:
            interrupted = isinstance(exc, KeyboardInterrupt)
            reason = format_error(exc)
            write_rows(csv_path, [dict(base_row(child_args),
                                      status="interrupted" if interrupted else "failed", error=reason)])
            log_path.write_text(traceback.format_exc(), encoding="utf-8")
            print(f"Benchmark stopped before {selector}: {reason}", flush=True)
            print(f"No remaining controllers were started. Results: {directory}", flush=True)
            return 130 if interrupted else 1
        # Guarantee an artifact even if a native extension crashes the worker.
        write_rows(csv_path, [dict(base_row(child_args), status="running")])
        try:
            environment = dict(os.environ, PYTHONHASHSEED=str(int(args.seed)), PYTHONUNBUFFERED="1")
            with log_path.open("w", encoding="utf-8") as log:
                process = subprocess.run(worker_command(child_args, csv_path),
                                         cwd=str(demo2._REPO_ROOT), env=environment,
                                         stdout=log, stderr=subprocess.STDOUT, check=False)
            returncode = process.returncode
        except (OSError, KeyboardInterrupt) as exc:
            returncode = 130 if isinstance(exc, KeyboardInterrupt) else 1
            with csv_path.open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            rows.append(dict(base_row(child_args), status="interrupted" if returncode == 130 else "failed",
                             error=format_error(exc)))
            write_rows(csv_path, rows)
        with csv_path.open(encoding="utf-8", newline="") as source:
            rows = list(csv.DictReader(source))
        if returncode or any(row["status"] == "running" for row in rows):
            for row in rows:
                if row["status"] == "running":
                    row.update(status="interrupted" if returncode == 130 else "failed",
                               error=f"Worker exited with code {returncode}; see {log_path.name}")
            if all(row["status"] == "finished" for row in rows):
                rows.append(dict(base_row(child_args), error=f"Worker exited with code {returncode}; see {log_path.name}"))
            write_rows(csv_path, rows)
        statuses = {row["status"] for row in rows}
        if statuses == {"finished"}:
            completed += 1
        elif statuses == {"skipped"}:
            skipped += 1
        else:
            failed += 1
        print(f"  {', '.join(sorted(statuses))}: {csv_path.name} (log: {log_path.name})", flush=True)
        for detail in dict.fromkeys(row.get("error", "") for row in rows if row.get("error")):
            print(f"    {detail}", flush=True)
        if returncode == 130 or "interrupted" in statuses:
            return 130
    print(f"Benchmark complete: {completed} controllers finished, {skipped} skipped, {failed} failed.", flush=True)
    print(f"CSVs: {directory}", flush=True)
    return 1 if failed or not completed else 0


class RuntimeCosts:
    """Exclusive wall time in the simulation thread, measured only inside _run.

    Wrappers preserve CARLA objects and C++ argument types. Nested API calls are
    subtracted from their parent; all remaining loop time belongs to Scenic.
    Background sensor callbacks are not independently added to wall time.
    """
    def __init__(self, clock=time.perf_counter):
        self.clock = clock
        self.patches = []
        self.active_thread = None
        self.reset()

    def reset(self):
        self.costs = dict(metsr=0.0, carla=0.0, scenic=0.0, pcla_agent=0.0)
        self.counts = dict(pcla_action_calls=0, pcla_clamped_action_calls=0, metsr_tick_calls=0, carla_tick_calls=0)
        self.inclusive = dict(pcla_setup_s=0.0, pcla_action_s=0.0)
        self.stack = []
        self.loop_wall_s = 0.0
        self.steps = 0
        self.simulation_seconds = 0.0

    def call(self, category, method, args, kwargs, count=None, inclusive=None):
        if self.active_thread != threading.get_ident():
            return method(*args, **kwargs)
        start = self.clock()
        frame = [0.0]
        self.stack.append(frame)
        if count:
            self.counts[count] += 1
        try:
            return method(*args, **kwargs)
        finally:
            elapsed = self.clock() - start
            self.stack.pop()
            self.costs[category] += max(0.0, elapsed - frame[0])
            if inclusive:
                self.inclusive[inclusive] += elapsed
            if self.stack:
                self.stack[-1][0] += elapsed

    def patch(self, owner, name, replacement):
        # Record ownership so inherited descriptors can be restored exactly.
        original = vars(owner).get(name)
        owned = name in vars(owner)
        setattr(owner, name, replacement)
        self.patches.append((owner, name, owned, original))

    def wrap(self, owner, name, category, count=None, inclusive=None):
        method = getattr(owner, name)
        @functools.wraps(method)
        def timed(*args, **kwargs):
            return self.call(category, method, args, kwargs, count, inclusive)
        self.patch(owner, name, timed)

    def isolate_agent_randomness(self, pcla_cls, name):
        # PCLA model loading/inference can reseed or consume Python/NumPy's global
        # RNGs. Keep an evolving private stream so that Scenic's later spawns do
        # not change merely because a different model used more random numbers.
        method = getattr(pcla_cls, name)
        @functools.wraps(method)
        def isolated(agent, *args, **kwargs):
            numpy = sys.modules.get("numpy")
            scene_state = (random.getstate(), numpy.random.get_state() if numpy is not None else None)
            agent_state = getattr(agent, "_demo2_random_state", scene_state)
            random.setstate(agent_state[0])
            if numpy is not None and agent_state[1] is not None:
                numpy.random.set_state(agent_state[1])
            try:
                return method(agent, *args, **kwargs)
            finally:
                agent._demo2_random_state = (random.getstate(), numpy.random.get_state() if numpy is not None else None)
                random.setstate(scene_state[0])
                if numpy is not None and scene_state[1] is not None:
                    numpy.random.set_state(scene_state[1])
        self.patch(pcla_cls, name, isolated)

    def install(self, simulation_cls, metsr_cls, carla_module, pcla_cls):
        try:
            # METS-R is a Python client; include serialization and blocking waits.
            for name in dir(metsr_cls):
                if not name.startswith("_") and callable(getattr(metsr_cls, name)):
                    self.wrap(metsr_cls, name, "metsr",
                              count="metsr_tick_calls" if name == "tick" else None)
            # Patch the native API classes, not proxy objects (which break CARLA's
            # Boost.Python signatures and isinstance checks).
            seen = set()
            for class_name in ("Client", "World", "Actor", "Vehicle", "Walker", "Sensor",
                               "TrafficManager", "TrafficLight", "TrafficSign", "WalkerAIController",
                               "Map", "Waypoint", "ActorList", "WorldSnapshot"):
                cls = getattr(carla_module, class_name, None)
                if cls is None or cls in seen:
                    continue
                seen.add(cls)
                for name, descriptor in list(vars(cls).items()):
                    if not name.startswith("_") and callable(descriptor):
                        self.wrap(cls, name, "carla",
                                  count="carla_tick_calls" if class_name == "World" and name == "tick" else None)
            if getattr(carla_module, "World", None) not in seen:
                raise RuntimeError("CARLA World API is unavailable for runtime instrumentation")
            self.isolate_agent_randomness(pcla_cls, "__init__")
            self.isolate_agent_randomness(pcla_cls, "get_action")
            self.wrap(pcla_cls, "__init__", "pcla_agent", inclusive="pcla_setup_s")
            self.wrap(pcla_cls, "get_action", "pcla_agent", count="pcla_action_calls", inclusive="pcla_action_s")
            original_run = simulation_cls._run
            @functools.wraps(original_run)
            def measured_loop(simulation, *args, **kwargs):
                self.active_thread = threading.get_ident()
                try:
                    return self.call("scenic", original_run, (simulation, *args), kwargs)
                finally:
                    self.active_thread = None
                    self.loop_wall_s = sum(self.costs.values())
                    self.steps = int(simulation.currentTime)
                    self.simulation_seconds = self.steps * float(simulation.timestep)
            self.patch(simulation_cls, "_run", measured_loop)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.active_thread = None
        for owner, name, owned, original in reversed(self.patches):
            if owned:
                setattr(owner, name, original)
            else:
                delattr(owner, name)
        self.patches.clear()

    def metrics(self):
        wall = self.loop_wall_s
        return dict(simulation_steps=self.steps, simulation_seconds=self.simulation_seconds,
                    loop_wall_s=wall,
                    steps_per_wall_second=self.steps / wall if wall else 0.0,
                    realtime_factor=self.simulation_seconds / wall if wall else 0.0,
                    **{f"{key}_s": value for key, value in self.costs.items()},
                    **self.counts, **self.inclusive)


def configure_worker_environment(selector: str) -> None:
    # Some agents initialize W&B even for inference. Benchmarks are unattended.
    os.environ["WANDB_MODE"] = "disabled"
    os.environ["WANDB_DISABLED"] = "true"
    if selector.split("_")[0] in {"lmdrive", "minddrive", "orion"}:
        # Large models can exceed PCLA's normal 260-second setup deadline even
        # without inference. Honor an explicit user setting, including a shorter one.
        os.environ.setdefault("PCLA_WATCHDOG_SEC", "900")


def run_worker(args: Any, csv_path: Path) -> int:
    from tutorials.dashboard_demos import demo2

    configure_worker_environment(args.pcla_agent)
    rows = []
    costs = RuntimeCosts()
    row = base_row(args)
    simulator = None
    exit_code = 0
    try:
        root = demo2.resolve_pcla_root(args)
        # Single selectors retain PCLA's own aliases/default-variant support.
        demo2.validate_pcla_setup(args)
        if str(root) not in sys.path:
            sys.path.append(str(root))
        import scenic
        from scenic import setDebuggingOptions
        from scenic.core.utils import setSeed
        from scenic.simulators.cosim import CosimSimulator
        from scenic.simulators.cosim import simulator as cosim_module
        import carla
        from PCLA import PCLA

        reason = unavailable_optional_dependencies(root, args.pcla_agent)
        if reason:
            row.update(status="skipped", error=reason)
            write_rows(csv_path, [row])
            print(reason, flush=True)
            return 0

        demo2.patch_scenic_metsr_ready_compat()
        costs.install(cosim_module.CosimSimulation, cosim_module.METSRClient, carla, PCLA)
        install_agent_compatibility(costs, PCLA, root)
        compile_start = time.perf_counter()
        scenario = demo2.compile_scenario(args, scenic, setDebuggingOptions, setSeed, int(args.seed))
        compile_s = time.perf_counter() - compile_start
        Path(args.export_folder).mkdir(parents=True, exist_ok=True)
        for index in range(int(args.total_simulations)):
            costs.reset()
            row = base_row(args, index + 1)
            row["compile_s"] = compile_s if index == 0 else 0.0
            run_base = demo2.run_data_base_path(args, row["seed"], index + 1)
            rows.append(row)
            row["status"] = "running"
            write_rows(csv_path, rows)
            try:
                started = time.perf_counter()
                simulator = demo2.build_simulator(args, CosimSimulator, run_name=run_base)
                costs.agent_map_dir = None
                costs.agent_speed_map = None
                if args.pcla_agent.split("_")[0] in {"carl", "plant2"}:
                    costs.agent_map_dir = prepare_agent_map(
                        root, args.pcla_agent, simulator.world.get_map(), csv_path.parent / "map_cache")
                    costs.agent_speed_map = prepare_agent_speed_map(
                        root, args.pcla_agent, simulator.world, csv_path.parent / "map_cache")
                row["simulator_setup_s"] = time.perf_counter() - started
                # Reloading the CARLA world and Scenic's setup/reset start each
                # repetition afresh; seed Traffic Manager as well as Scenic.
                simulator.tm.set_random_device_seed(row["seed"])
                torch = sys.modules.get("torch")
                if torch is not None:
                    torch.manual_seed(row["seed"])
                started = time.perf_counter()
                scene, _ = demo2.generate_scene_for_run(scenario, setSeed, row["seed"], run_base)
                row["scene_generation_s"] = time.perf_counter() - started
                started = time.perf_counter()
                try:
                    simulation = simulator.simulate(scene, maxIterations=1)
                finally:
                    row["simulate_call_s"] = time.perf_counter() - started
                if simulation is None:
                    raise RuntimeError("Scenic rejected the simulation (automatic retries disabled for benchmarks)")
                if not costs.counts["pcla_action_calls"]:
                    raise RuntimeError("No PCLA get_action() calls were observed; choose a PCLA-driven scenario")
                row["termination_reason"] = str(simulation.result.terminationReason)
                from scenic_exp.runtime.reporting import write_test_report
                write_test_report(simulation.result, getattr(scenario, "params", {}),
                                  csv_path.with_name(f"{csv_path.stem}_run_{index + 1}_test_events.json"))
                row["status"] = "finished"
            except (Exception, KeyboardInterrupt) as exc:
                row["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
                row["error"] = format_error(exc)
                traceback.print_exc()
                exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
            finally:
                row.update(costs.metrics())
                if simulator is not None:
                    try:
                        simulator.destroy()
                    except Exception as exc:
                        row["error"] += f"; simulator cleanup failed: {format_error(exc)}"
                        if not exit_code:
                            row["status"] = "failed"
                            exit_code = 1
                    simulator = None
                write_rows(csv_path, rows)
            print(f"Run {index + 1}: {row['status']}, {row['steps_per_wall_second']:.3f} steps/s", flush=True)
            if exit_code:
                break
    except (Exception, SystemExit, KeyboardInterrupt) as exc:
        row.update(status="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed",
                   error=format_error(exc))
        if not rows:
            rows.append(row)
        write_rows(csv_path, rows)
        traceback.print_exc()
        exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
    finally:
        costs.close()
    return exit_code
