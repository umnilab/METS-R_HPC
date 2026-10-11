"""Topic-based query APIs composed into METSRClient."""

from .query_simulation import QuerySimulationMixin
from .query_vehicles import QueryVehiclesMixin
from .query_public_vehicles import QueryPublicVehiclesMixin
from .query_roads import QueryRoadsMixin
from .query_zones import QueryZonesMixin
from .query_requests import QueryRequestsMixin
from .query_signals import QuerySignalsMixin
from .query_charging_stations import QueryChargingStationsMixin
from .query_cosim import QueryCosimMixin
from .query_routing import QueryRoutingMixin
from .query_ride_hailing import QueryRideHailingMixin


class QueryAPIs(
    QuerySimulationMixin,
    QueryVehiclesMixin,
    QueryPublicVehiclesMixin,
    QueryRoadsMixin,
    QueryZonesMixin,
    QueryRequestsMixin,
    QuerySignalsMixin,
    QueryChargingStationsMixin,
    QueryCosimMixin,
    QueryRoutingMixin,
    QueryRideHailingMixin,
):
    """All simulator query methods, preserving the client interface."""
