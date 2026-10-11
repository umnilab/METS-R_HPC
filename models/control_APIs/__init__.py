"""Topic-based control APIs composed into METSRClient."""

from .control_simulation import ControlSimulationMixin
from .control_vehicles import ControlVehiclesMixin
from .control_cosim import ControlCosimMixin
from .control_taxi import ControlTaxiMixin
from .control_bus import ControlBusMixin
from .control_roads import ControlRoadsMixin
from .control_signals import ControlSignalsMixin
from .control_zones import ControlZonesMixin
from .control_charging_stations import ControlChargingStationsMixin


class ControlAPIs(
    ControlSimulationMixin,
    ControlVehiclesMixin,
    ControlCosimMixin,
    ControlTaxiMixin,
    ControlBusMixin,
    ControlRoadsMixin,
    ControlSignalsMixin,
    ControlZonesMixin,
    ControlChargingStationsMixin,
):
    """All simulator control methods, preserving the client interface."""
