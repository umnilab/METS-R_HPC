"""Optional controller adapters shared by Scenic experiments and demo runners.

Importing this module does not import PCLA, CARLA, NumPy, or dashboard code.
The caller owns simulation stepping and vehicle lifetime; ``step(tick=...)``
performs at most one inference and control application for that tick.
"""

from __future__ import annotations

import importlib.metadata
import math
import os
from pathlib import Path
import sys
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple


def _import_pcla(pcla_dir: Optional[str]) -> Any:
    pcla_dir = pcla_dir or os.environ.get("PCLA_HOME")
    if pcla_dir:
        root = Path(pcla_dir).expanduser().resolve()
        if not (root / "PCLA.py").exists():
            raise RuntimeError(f"--pcla-dir does not contain PCLA.py: {root}")
        if str(root) not in sys.path:
            sys.path.append(str(root))
    # Fail early with a repair command for incompatible PCLA parser installs.
    try:
        runtime_version = importlib.metadata.version("antlr4-python3-runtime")
    except importlib.metadata.PackageNotFoundError:
        runtime_version = "not installed"

    try:
        import omegaconf  # noqa: F401
    except Exception as exc:
        detail = str(exc).splitlines()[0]
        if "Could not deserialize ATN" not in detail:
            raise
        repair = (
            f'"{sys.executable}" -m pip install --force-reinstall '
            "antlr4-python3-runtime==4.9.3"
        )
        raise RuntimeError(
            "PCLA cannot load OmegaConf because its generated parser is "
            "incompatible with the installed antlr4-python3-runtime "
            f"({runtime_version}). PCLA pins version 4.9.3. Repair this "
            f"Python environment with: {repair}"
        ) from exc
    try:
        import PCLA as pcla_module
    except ImportError as exc:
        raise RuntimeError(
            "Could not import PCLA. Activate the PCLA Conda environment and pass "
            "--pcla-dir or set PCLA_HOME to the PCLA checkout."
        ) from exc
    return pcla_module


def _construct_pcla(pcla_class, agent_type, actor, route, client):
    """Keep the caller's utils modules intact when PCLA loads its own agents."""
    saved_utils = {
        name: module for name, module in sys.modules.items()
        if name == "utils" or name.startswith("utils.")
    }
    try:
        return pcla_class(agent_type, actor, str(route), client)
    finally:
        for name in list(sys.modules):
            if name == "utils" or name.startswith("utils."):
                sys.modules.pop(name, None)
        sys.modules.update(saved_utils)


def create_scenic_pcla(simulation, ego, agent_type, route, route_output):
    """Construct the native Scenic policy, preserving its METS-R route.

    The Scenic behavior owns action application; this function neither applies
    initial controls nor advances the simulator. Imports are deferred until the
    behavior starts, so autopilot scenarios do not require PCLA.
    """
    pcla_home = os.environ.get("PCLA_HOME")
    if pcla_home and pcla_home not in sys.path:
        sys.path.append(pcla_home)
    from PCLA import PCLA
    from pcla_functions.route_maker import route_maker

    if not ego.carlaActor:
        raise RuntimeError("PCLA requires the ego's CARLA actor")
    if route is None:
        if not getattr(ego, "route", None):
            raise RuntimeError("Missing authoritative METS-R ego route")
        locations = simulation.generate_carla_trajectory(route=ego.route, obj=ego)
        waypoints = [simulation.map.get_waypoint(location) for location in locations]
        waypoints = [waypoint for waypoint in waypoints if waypoint is not None]
        if len(waypoints) <= 1:
            raise RuntimeError("PCLA route needs at least two road waypoints")
        route = str(route_output)
        Path(route).parent.mkdir(parents=True, exist_ok=True)
        route_maker(waypoints, savePath=route)
    return _construct_pcla(PCLA, agent_type, ego.carlaActor, route, simulation.carla_client)


def _distance(a: Any, b: Any) -> float:
    return math.sqrt(
        (float(a.x) - float(b.x)) ** 2
        + (float(a.y) - float(b.y)) ** 2
        + (float(a.z) - float(b.z)) ** 2
    )


def _generate_pcla_route(
    pcla_module: Any,
    carla_client: Any,
    ego_actor: Any,
    output_path: Path,
    end_spawn_index: Optional[int],
    min_distance_m: float,
) -> Path:
    world = carla_client.get_world()
    start_location = ego_actor.get_location()
    spawn_points = list(world.get_map().get_spawn_points())
    if len(spawn_points) < 2:
        raise RuntimeError("CARLA map did not expose enough spawn points for a PCLA route")

    if end_spawn_index is not None:
        if not 0 <= int(end_spawn_index) < len(spawn_points):
            raise ValueError(
                f"--route-end-spawn-index must be in [0, {len(spawn_points) - 1}]"
            )
        candidates = [spawn_points[int(end_spawn_index)]]
    else:
        candidates = sorted(
            spawn_points,
            key=lambda transform: _distance(start_location, transform.location),
            reverse=True,
        )
        preferred = [
            transform
            for transform in candidates
            if _distance(start_location, transform.location) >= float(min_distance_m)
        ]
        candidates = (preferred or candidates)[:20]

    last_error: Optional[BaseException] = None
    for destination in candidates:
        try:
            waypoints = pcla_module.location_to_waypoint(
                carla_client, start_location, destination.location
            )
            if len(waypoints) <= 1:
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pcla_module.route_maker(waypoints, str(output_path))
            if output_path.exists():
                return output_path
        except Exception as exc:
            last_error = exc
    raise RuntimeError("Could not generate a connected CARLA route for PCLA") from last_error


class PCLAController:
    """Own PCLA sensors and inference while leaving stepping to the simulator.

    ``args`` uses the existing demo arguments (``pcla_dir``, ``pcla_agent``,
    ``pcla_route``, and route generation options). Relative routes resolve against
    the working directory unless a runner supplies ``path_resolver``. A dashboard
    may supply ``tap_factory``; headless experiments need no camera display tap.
    """

    def __init__(
        self,
        args: Any,
        carla_module: Any,
        carla_client: Any,
        ego_actor: Any,
        *,
        tap_factory: Optional[Callable[[Any], Any]] = None,
        path_resolver: Optional[Callable[[str], Path]] = None,
    ) -> None:
        self.ego_actor = ego_actor
        self.last_control: Any = None
        self.tap: Any = None
        self.pcla: Any = None
        self._last_tick: Any = None
        resolve_path = path_resolver or (lambda path: Path(path).expanduser().resolve())

        try:
            ego_actor.set_autopilot(False)
        except Exception:
            pass
        try:
            ego_actor.set_simulate_physics(True)
        except Exception:
            pass
        ego_actor.apply_control(carla_module.VehicleControl(throttle=0.0, brake=1.0))

        pcla_module = _import_pcla(args.pcla_dir)
        if args.pcla_route:
            route_path = resolve_path(args.pcla_route)
            if not route_path.exists():
                raise FileNotFoundError(f"PCLA route does not exist: {route_path}")
        else:
            route_path = _generate_pcla_route(
                pcla_module,
                carla_client,
                ego_actor,
                resolve_path(args.route_output),
                args.route_end_spawn_index,
                args.route_min_distance_m,
            )
        self.route_path = route_path

        self.pcla = _construct_pcla(
            pcla_module.PCLA, args.pcla_agent, ego_actor, route_path, carla_client,
        )
        if tap_factory is not None:
            try:
                self.tap = tap_factory(self.pcla)
            except Exception:
                # Constructor failure must not leak PCLA sensors into the next run.
                self.close(destroy_vehicle=False)
                raise

    @property
    def camera_specs(self) -> Sequence[Mapping[str, Any]]:
        if self.tap is not None:
            return self.tap.camera_specs
        agent = getattr(self.pcla, "agent_instance", None)
        return [
            dict(spec)
            for spec in agent.sensors()
            if str(spec.get("type", "")).startswith("sensor.camera.rgb")
        ] if agent is not None else []

    def get_action(self) -> Any:
        """Infer a control for a Scenic behavior which owns control application."""
        if self.pcla is None:
            raise RuntimeError("PCLA controller is closed")
        return self.pcla.get_action()

    def step(self, tick: Any = None) -> Any:
        """Infer and apply one control; repeated calls for a tick are idempotent."""
        if self.pcla is None:
            raise RuntimeError("PCLA controller is closed")
        if tick is not None and tick == self._last_tick:
            return self.last_control
        control = self.get_action()
        self.ego_actor.apply_control(control)
        self.last_control = control
        self._last_tick = tick
        return control

    def camera_snapshots(self) -> Dict[str, Tuple[str, bytes, Any]]:
        return {} if self.tap is None else self.tap.snapshots()

    def close(self, *, destroy_vehicle: bool = False) -> None:
        """Close sensors once, preserving the simulator-owned vehicle by default."""
        if self.tap is not None:
            tap, self.tap = self.tap, None
            try:
                tap.close()
            except Exception as exc:
                print(f"PCLA camera tap cleanup warning: {str(exc).splitlines()[0]}")
        if self.pcla is not None:
            pcla, self.pcla = self.pcla, None
            try:
                if destroy_vehicle:
                    pcla.cleanup()
                else:
                    pcla.cleanup(destroy_vehicle=False)
            except Exception as exc:
                print(f"PCLA cleanup warning: {str(exc).splitlines()[0]}")
