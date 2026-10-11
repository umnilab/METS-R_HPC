"""Stop-sign experiment components with stable imports for existing runners.

Implementation is grouped by responsibility:
- ``targets``: candidate/settings records, discovery, and approach selection.
- ``geometry``: world-to-camera projection and overhead target annotation.
- ``patch``: sensor-ingress recoloring, counters, and optional image snapshots.
- ``routing``: continuous route construction and validation through a stop lane.

Public functions and shared helpers are imported directly from their owning modules.
No simulator, PCLA, or dashboard modules are imported when this package loads.
"""

from .geometry import (
    _distance,
    _horizontal_distance,
    _transform_matrix,
    _actor_transform_matrix,
    annotate_overhead_target,
    project_stop_sign,
)
from .targets import (
    StopSignPatchSettings,
    StopSignCandidate,
    _environment_location,
    _match_stop_actors_to_signs,
    _stop_trigger_location,
    _find_stop_approach,
    discover_stop_signs,
    select_candidates,
)
from .patch import (
    _encode_png,
    StopSignColorPatchTap,
)
from .routing import (
    _waypoint_location,
    _route_through_stop_quality,
    generate_route_via_stop,
)

__all__ = [
    "annotate_overhead_target",
    "project_stop_sign",
    "StopSignPatchSettings",
    "StopSignCandidate",
    "discover_stop_signs",
    "select_candidates",
    "StopSignColorPatchTap",
    "generate_route_via_stop",
]
