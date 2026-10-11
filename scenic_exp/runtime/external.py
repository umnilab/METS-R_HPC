"""Reusable external PCLA controller for a selected stop-sign trial.

Runners choose candidates/seeds and observe results. Controller construction,
sensor effects, control application, and cleanup are independent of any UI.
"""
from pathlib import Path
from types import SimpleNamespace

from .config import resolve_test_parameters
from .controllers import PCLAController, _import_pcla
from .lifecycle import register_pre_actor_teardown
from .scenario import ScenarioRuntime
from .stop_sign import generate_route_via_stop


def stop_sign_test_parameters(args, *, index=0, enabled=True):
    """Translate the legacy demo options into the shared, validated schema."""
    return resolve_test_parameters({
        "trigger": {"start_s": 0},
        # Cover the entire trial, including a final fractional Scenic tick.
        "event": {"duration_s": float(args.length) + float(args.timestep)},
        "stop_sign": {
            "index": index, "enabled": enabled,
            "max_distance_m": args.attack_max_distance_m,
            "minimum_red": args.attack_min_red,
            "red_dominance": args.attack_red_dominance,
            "roi_scale": args.attack_roi_scale,
            "min_patch_pixels": args.attack_min_patch_pixels,
        },
    })


class StopSignControllerRuntime:
    """One trial's controller and patch; use with ``install_runtime_hook``.

    ``spec`` supplies candidate, attack_enabled, run_number and phase. ``args``
    supplies PCLA/route paths and legacy patch options. Passing ``test_parameters``
    uses the resolved Scenic configuration as the source of truth for effects.
    Subclasses may observe ``after_step`` without changing control timing.
    """

    def __init__(self, args, spec, *, test_parameters=None):
        self.args, self.spec = args, spec
        self.test_parameters = test_parameters
        self.simulation = None
        self.controller = None
        self.effects = None
        self.tap = None
        self.route_path = None
        self.last_control = None
        self.closed = False
        self._release_teardown = lambda: None

    def attach(self, simulation):
        self.simulation = simulation
        self._release_teardown = register_pre_actor_teardown(simulation, self.close)

    def _ego(self):
        ego = getattr(self.simulation, "ego", None)
        if ego is None:
            objects = list(getattr(self.simulation, "objects", ()) or ())
            ego = objects[0] if objects else None
        actor = getattr(ego, "carlaActor", None)
        if actor is None:
            raise RuntimeError("Scenic did not expose the CARLA ego actor for PCLA")
        return ego, actor

    @staticmethod
    def _carla_module():
        try:
            import carla
        except ImportError as exc:
            raise RuntimeError("The matching CARLA Python API is required") from exc
        return carla

    @staticmethod
    def _release_constant_velocity(actor):
        release = getattr(actor, "disable_constant_velocity", None)
        if callable(release):
            try:
                release()
            except Exception:
                pass

    def _ensure_controller(self):
        if self.controller is not None:
            return
        if self.closed:
            raise RuntimeError("Create a new StopSignControllerRuntime for each trial")
        config = self.test_parameters
        if config is None:
            config = stop_sign_test_parameters(
                self.args, index=self.spec.candidate.index, enabled=self.spec.attack_enabled,
            )
        config = resolve_test_parameters(config)
        if (config["stop_sign"]["index"] != self.spec.candidate.index
                or config["stop_sign"]["enabled"] != self.spec.attack_enabled):
            raise ValueError("Scenic stop_sign settings disagree with the selected trial")
        if config["trigger"]["mode"] != "time":
            raise ValueError("External stop-sign trials have no safety hazard; use a time trigger")
        ego, actor = self._ego()
        self._release_constant_velocity(actor)
        if self.args.pcla_route:
            route_path = Path(self.args.pcla_route)
        else:
            route_path = Path(self.args.route_output_dir) / (
                f"run_{self.spec.run_number:02d}_stop_{self.spec.candidate.index}_{self.spec.phase}.xml"
            )
            route_path = generate_route_via_stop(
                _import_pcla(self.args.pcla_dir), self.simulation.carla_client,
                actor, self.spec.candidate, route_path, self.args.route_min_distance_m,
            )
        self.route_path = route_path
        controller_args = SimpleNamespace(
            pcla_dir=self.args.pcla_dir, pcla_agent=self.args.pcla_agent,
            pcla_route=str(route_path), route_output=str(route_path),
            route_end_spawn_index=None, route_min_distance_m=self.args.route_min_distance_m,
        )
        try:
            self.controller = PCLAController(
                controller_args, self._carla_module(), self.simulation.carla_client, actor,
            )
            self.effects = ScenarioRuntime(
                self.simulation, ego, "none", "stop_sign_patch", config,
            )
            # Make the shared Scenic records report this runtime's actual effect.
            ego.test_runtime = self.effects
            self.tap = self.effects.bind_pcla(
                self.controller.pcla, stop_candidate=self.spec.candidate,
            )
            self.controller.tap = self.tap
        except BaseException:
            self.close()
            raise

    def before_step(self):
        self._ensure_controller()
        self.effects.before_step()

    def apply_control(self):
        self._ensure_controller()
        _, actor = self._ego()
        self._release_constant_velocity(actor)
        self.last_control = self.controller.step()

    def after_step(self):
        """Observer hook; subclasses may update dashboards or collect results."""

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.effects is not None:
                self.effects.close()
        finally:
            try:
                if self.controller is not None:
                    self.controller.close(destroy_vehicle=False)
                    self.controller = None
            finally:
                self._release_teardown()
