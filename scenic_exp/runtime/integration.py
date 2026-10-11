"""Instance-scoped integration for externally controlled Scenic simulations.

The Scenic behavior calls ``require_external_runtime`` before its first yield.
The creator supplies the runtime with ``install_runtime_hook``. A ContextVar
bridges forks which execute a simulation inside its constructor; no simulator
class is patched and a different simulator cannot inherit the pending runtime.
"""
from contextvars import ContextVar
from functools import wraps

from .lifecycle import register_pre_actor_teardown


_pending_runtime = ContextVar("metsr_scenic_runtime", default=None)


def require_external_runtime(simulation):
    """Attach the runner's runtime or fail before silently leaving ego idle."""
    existing = getattr(simulation, "_metsr_experiment_runtime", None)
    if existing is not None:
        return existing
    pending = _pending_runtime.get()
    if pending is None:
        raise RuntimeError(
            "ego_controller='external' requires an attached experiment runtime. "
            "Use scenic_exp.runtime.integration.install_runtime_hook with a "
            "runtime provider (demo4 supplies one), or select ego_controller='pcla'."
        )
    owner, runtime = pending
    if owner[0] is not None and owner[0] is not simulation:
        raise RuntimeError("An external runtime cannot control two simulations")
    owner[0] = simulation
    attach_runtime(simulation, runtime)
    return runtime


def attach_runtime(simulation, runtime):
    """Run one external controller after each completed co-simulation step.

    Actors may transfer during the original step. Applying control afterwards keeps
    the external controller as the final writer for the next tick, as in demo4.
    Cleanup runs before actor destruction, including when inference fails.
    """
    existing = getattr(simulation, "_metsr_experiment_runtime", None)
    if existing is not None:
        if existing is not runtime:
            raise RuntimeError("Simulation already has an external controller")
        return
    if getattr(runtime, "simulation", None) not in (None, simulation):
        raise RuntimeError("Create a fresh experiment runtime for each simulation")
    had_step = "step" in vars(simulation)
    previous_step = vars(simulation).get("step")
    original_step = simulation.step
    closed = False
    release_teardown = lambda: None

    def close():
        nonlocal closed
        if closed:
            return
        closed = True
        try:
            runtime.close()
        finally:
            release_teardown()
            if vars(simulation).get("step") is step_with_runtime:
                if had_step:
                    simulation.step = previous_step
                else:
                    del simulation.step
            if getattr(simulation, "_metsr_experiment_runtime", None) is runtime:
                del simulation._metsr_experiment_runtime
            if getattr(simulation, "_metsr_experiment_close", None) is close:
                del simulation._metsr_experiment_close

    @wraps(original_step)
    def step_with_runtime(*args, **kwargs):
        try:
            runtime.before_step()
            result = original_step(*args, **kwargs)
            runtime.apply_control()
            runtime.after_step()
            return result
        except BaseException:
            try:
                close()
            except Exception:
                pass  # Keep the original simulation/inference exception.
            raise

    simulation._metsr_experiment_runtime = runtime
    simulation._metsr_experiment_close = close
    try:
        runtime.attach(simulation)
        release_teardown = register_pre_actor_teardown(simulation, close)
        simulation.step = step_with_runtime
    except BaseException:
        try:
            close()
        except Exception:
            pass
        raise


def install_runtime_hook(simulator, holder):
    """Supply ``holder.current`` to externally controlled scenarios.

    Returns an idempotent uninstall function. Only this simulator instance's
    factory is wrapped. The scenario's external behavior performs attachment
    before the first step, including on forks which run inside the constructor.
    """
    if getattr(simulator, "_metsr_runtime_installer", None) is not None:
        raise RuntimeError("An external runtime provider is already installed")
    original_create = simulator.createSimulation
    had_create = "createSimulation" in vars(simulator)
    previous_create = vars(simulator).get("createSimulation")

    @wraps(original_create)
    def create_with_runtime(scene, *args, **kwargs):
        runtime = holder.current
        if runtime is None:
            raise RuntimeError("Configure holder.current before simulating the scene")
        owner = [None]
        token = _pending_runtime.set((owner, runtime))
        try:
            simulation = original_create(scene, *args, **kwargs)
            if owner[0] is None:
                # Also support factories which return before running the scene.
                attach_runtime(simulation, runtime)
            return simulation
        except BaseException:
            # Constructor/setup failures may bypass simulator teardown.
            simulation = getattr(runtime, "simulation", None)
            close = getattr(simulation, "_metsr_experiment_close", runtime.close)
            try:
                close()
            except Exception:
                pass
            raise
        finally:
            _pending_runtime.reset(token)

    def uninstall():
        if vars(simulator).get("createSimulation") is create_with_runtime:
            if had_create:
                simulator.createSimulation = previous_create
            else:
                del simulator.createSimulation
        if getattr(simulator, "_metsr_runtime_installer", None) is uninstall:
            del simulator._metsr_runtime_installer

    simulator.createSimulation = create_with_runtime
    simulator._metsr_runtime_installer = uninstall
    return uninstall
