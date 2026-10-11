"""Experiment runtime, independent of dashboard entry points.

Start with ``config`` for parameter validation and ``scenario.ScenarioRuntime``
for effect ownership. ``safety``, ``timing``, and ``sensors`` contain the geometry,
event rules, and observation transforms. ``stop_sign`` groups the camera patch's
target selection, projection, sensor tap, and routing. ``controllers``,
``integration``, and ``lifecycle`` connect these pieces to the simulator.

Importing this package does not connect to simulators or load PCLA. ``testing``
remains a compatibility facade; new code imports from the owning modules.
"""
