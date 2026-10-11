"""Register experiment cleanup before the simulator destroys parent actors."""


def register_pre_actor_teardown(simulation, callback):
    """Run ``callback`` once before actor teardown; return a release function.

    Current co-simulation backends expose a lifecycle callback. Older backends
    and offline simulators instead need a temporary instance ``destroy`` hook.
    Releasing a native registration disables its callback without assuming an
    unregister API, while releasing the fallback restores the previous method.
    """
    active = True

    def invoke():
        nonlocal active
        if active:
            active = False
            callback()

    register = getattr(simulation, "add_pre_actor_teardown_callback", None)
    if callable(register):
        register(invoke)

        def release():
            nonlocal active
            active = False

        return release

    original_destroy = simulation.destroy
    had_override = "destroy" in vars(simulation)
    previous_destroy = vars(simulation).get("destroy")

    def release():
        nonlocal active
        active = False
        # Another hook may have been installed since ours. Do not remove it.
        if vars(simulation).get("destroy") is destroy:
            if had_override:
                simulation.destroy = previous_destroy
            else:
                del simulation.destroy

    def destroy(*args, **kwargs):
        try:
            invoke()
        finally:
            release()
            original_destroy(*args, **kwargs)

    simulation.destroy = destroy
    return release
