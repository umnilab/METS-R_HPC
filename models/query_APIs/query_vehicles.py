"""Query APIs for vehicles."""

from models._api_utils import _batch_field_values
from utils.primitives import _as_list


class QueryVehiclesMixin:
    """Query APIs for vehicles."""

    # By default query public vehicles
    def query_vehicle(self, id = None, private_veh = False, transform_coords = False):
        """Query the full kinematic state of one or more vehicles.

        Without ``id`` the server returns two lists::

            {
              'publicVehicleIds':  [...],   # IDs of public vehicles (taxis + buses)
              'privateVehicleIds': [...],   # IDs of private vehicles (EV / GV)
              'messageType': 'vehicle'
            }

        With ``id`` each matched vehicle produces::

            {
              'vehicleId': <int> bridge-visible private ID for EV/GV;
                                  internal vehicle ID for taxi/bus,
              'vehicleClass': <int> vehicle class:
                                   0 = GV  (private gasoline vehicle)
                                   1 = ETAXI
                                   2 = EBUS
                                   3 = EV  (private electric vehicle),
              'state':   <int>   trip state (see Vehicle.java):
                                  -1 = NONE_OF_THE_ABOVE (not on network / idle)
                                   0 = PARKING
                                   1 = OCCUPIED_TRIP
                                   2 = INACCESSIBLE_RELOCATION_TRIP
                                   3 = BUS_TRIP
                                   4 = CHARGING_TRIP
                                   5 = CRUISING_TRIP
                                   6 = PICKUP_TRIP
                                   7 = ACCESSIBLE_RELOCATION_TRIP
                                   8 = PRIVATE_TRIP,
              'x':       <float> SIM/internal x coordinate; projected/local
                                  SUMO x if transform_coords=True,
              'y':       <float> SIM/internal y coordinate; projected/local
                                  SUMO y if transform_coords=True,
              'z':       <float> elevation,
              'bearing': <float> heading in degrees (0 = north, clockwise),
              'acceleration':     <float> current longitudinal acceleration (m/sÂ²),
              'speed':   <float> current speed (m/s),
              'originZoneId': <int> current trip origin zone ID,
              'destinationZoneId': <int> current trip destination zone ID,
              'originRoadId': <str> original road ID for the trip origin road,
              'destinationRoadId': <str> original road ID for the trip destination road,
              'battery': <float> EV battery energy (kWh, EV classes only),
              'totalEnergyConsumed': <float> EV cumulative energy use (kWh),
              'onRoad':      <bool> whether the vehicle occupies a road-like facility,
              'onConnector': <bool> whether that facility is an intersection connector,
              'segmentId': <str> physical road or connector ID,
                                 an intersection connector,
               'laneIndex': <int | None> compact 0-based physical-road lane
                                  index; None on a connector,
              'distanceToSegmentEnd': <float> distance to the next junction on a physical
                                 lane, or remaining connector distance (m),
              'sourceRoadId': <str> connector source road when on a connector,
              'targetRoadId': <str> connector target road when on a connector,
              'intersectionId': <int> connector intersection ID,
              'intersectionCollision': <bool> current conflict state,
              'currentParkingRoadId': <str | None> original road ID where the
                                      vehicle is parked or has reserved parking
            }

        Parameters
        ----------
        id : int | list[int] | None
            Vehicle ID(s) to query. Pass ``None`` to get all fleet IDs.
        private_veh : bool | list[bool]
            ``True`` for private vehicles (EV/GV), ``False`` for public
            (taxi/bus). Must match the length of ``id`` if both are lists.
        transform_coords : bool | list[bool]
            ``False`` returns SIM/internal coordinates (WGS-84 lon/lat for
            georeferenced networks). ``True`` returns projected/local SUMO
            coordinates, which can be passed to the CARLA coordinate helpers.
        """
        msg = {"messageType": "vehicle"}
        if id is not None:
            msg["data"] = []
            if not isinstance(id, list):
                id = [id]
            if not isinstance(private_veh, list):
                private_veh = [private_veh] * len(id)
            if not isinstance(transform_coords, list):
                transform_coords = [transform_coords] * len(id)
            for veh_id, prv, tran in zip(id, private_veh, transform_coords):
                msg["data"].append({"vehicleId": veh_id, "isPrivate": prv, "transformCoordinates": tran})

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "vehicle", res["messageType"]
        return res

    def query_vehicle_route(self, id = None, private_veh = False):
        """Query the currently assigned remaining route of one or more vehicles.

        This reads the route already assigned by METS-R; it does not calculate
        or change a route. Without ``id`` the server returns the public and
        private vehicle ID indexes. With IDs, each record contains physical
        ``roadIds``, ``connectorIds``, an interleaved ``segmentIds`` path,
        connector-aware remaining distance, and mean/P90 travel-time metrics.

        Parameters
        ----------
        id : int | list[int] | None
            Vehicle ID(s) to query, or ``None`` for the fleet ID indexes.
        private_veh : bool | list[bool]
            ``True`` for private vehicles (EV/GV), ``False`` for public
            vehicles (taxi/bus). A scalar is broadcast across a batch.
        """
        msg = {"messageType": "vehicleRoute"}
        if id is not None:
            vehicle_ids = _as_list(id)
            private_flags = _batch_field_values(
                private_veh, len(vehicle_ids), "private_veh", batch_name="id"
            )
            msg["data"] = [
                {"vehicleId": vehicle_id, "isPrivate": private_flag}
                for vehicle_id, private_flag in zip(vehicle_ids, private_flags)
            ]

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "vehicleRoute", res["messageType"]
        return res

    def query_on_road_vehicles(self, roadID=None):
        """Query IDs for vehicles on active physical or connector roads.

        Connector IDs use ``<sourceRoadID>_<targetRoadID>``. Per-road records
        identify them with ``isConnector=True`` and ``laneIndex=None``.
        """
        msg = {"messageType": "onRoadVehicles"}
        if roadID is not None:
            msg["data"] = roadID
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "onRoadVehicles", res["messageType"]
        return res
