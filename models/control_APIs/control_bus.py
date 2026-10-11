"""Control APIs for bus."""

from models._api_utils import _batch_field_values, _optional_vehicle_lengths
from utils.primitives import _as_list


class ControlBusMixin:
    """Control APIs for bus."""

    # assign bus
    def add_bus_route(self, routeName, zone, road, paths = None):
        """Create named bus routes with stop zones, stop roads, and optional paths."""
        has_paths = paths is not None
        if not has_paths:
            msg = {
                    "messageType": "addBusRoute",
                    "data": []
                    }
        else:
            msg = {
                    "messageType": "addBusRouteWithPath",
                    "data": []
                    }
        if not isinstance(routeName, list):
            routeName = [routeName]
            zone = [zone]
            road = [road]
            if has_paths:
                paths = [paths]
        elif has_paths and not isinstance(paths, list):
            paths = [paths] * len(routeName)

        if not has_paths:
            for routeName, zone, road in zip(routeName, zone, road):
                msg["data"].append({"routeName": routeName, "stopZoneIds": zone, "stopRoadIds": road})
        else:
            for routeName, zone, road, paths in zip(routeName, zone, road, paths):
                msg["data"].append({"routeName": routeName, "stopZoneIds": zone, "stopRoadIds": road, "pathRoadIds": paths})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        if not has_paths:
            assert res["messageType"] == "addBusRoute", res["messageType"]
        else:
            assert res["messageType"] == "addBusRouteWithPath", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def add_bus_run(self, routeName, departTime):
        """Schedule runs on named bus routes at the given departure ticks."""
        msg = {
                "messageType": "addBusRun",
                "data": []
                }
        if not isinstance(routeName, list):
            routeName = [routeName]
            departTime = [departTime]

        for routeName, departTime in zip(routeName, departTime):
            msg["data"].append({"routeName": routeName, "departureTicks": departTime})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addBusRun", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def insert_bus_stop(self, busID, routeName, zoneID, roadName, stopIndex):
        """Insert a zone and road stop at a specified index on a bus route."""
        msg = {
                "messageType": "insertStopToRoute",
                "data": []
                }
        if not isinstance(busID, list):
            busID = [busID]
            routeName = [routeName] * len(busID)
            zoneID = [zoneID] * len(busID)
            roadName = [roadName] * len(busID)
            stopIndex = [stopIndex] * len(busID)

        for busID, routeName, zoneID, roadName, stopIndex in zip(busID, routeName, zoneID, roadName, stopIndex):
            msg["data"].append({"busId": busID, "routeName": routeName, "stopZoneId": zoneID, "stopRoadId": roadName, "stopIndex": stopIndex})

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "insertStopToRoute", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def remove_bus_stop(self, busID, routeName, stopIndex):
        """Remove the stop at a specified index from a bus route."""
        msg = {
                "messageType": "removeStopFromRoute",
                "data": []
                }
        if not isinstance(busID, list):
            busID = [busID]
            routeName = [routeName] * len(busID)
            stopIndex = [stopIndex] * len(busID)

        for busID, routeName, stopIndex in zip(busID, routeName, stopIndex):
            msg["data"].append({"busId": busID, "routeName": routeName, "stopIndex": stopIndex})

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "removeStopFromRoute", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def assign_request_to_bus(self, busID, reqID):
        """Match existing pending bus request(s) to bus(es).

        The latest METS-R SIM Control API separates bus-request creation from
        assignment. Use ``add_bus_requests`` first, read the returned
        ``reqID``, then pass ``busID`` and ``reqID`` here.
        """
        msg = {
                "messageType": "assignRequestToBus",
                "data": []
                }
        if not isinstance(busID, list):
            busID = [busID]
        if not isinstance(reqID, list):
            reqID = [reqID] * len(busID)
        assert len(busID) == len(reqID), "busID and reqID must have the same length"

        for bus_id, req_id in zip(busID, reqID):
            msg["data"].append({"busId": bus_id, "requestId": req_id})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "assignRequestToBus", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def add_bus_requests(self, zoneID, dest, routeName, num, max_waiting_time = None, maxWaitingTime = None):
        """Add one or more pending bus requests.

        ``max_waiting_time`` is optional and uses simulation ticks. It maps to
        the API field ``maxWaitTicks``. When it is positive, the simulator
        overrides the request's default waiting tolerance.
        """
        if max_waiting_time is None and maxWaitingTime is not None:
            max_waiting_time = maxWaitingTime

        msg = {
                "messageType": "addBusRequests",
                "data": []
                }
        if not isinstance(zoneID, list):
            zoneID = [zoneID]
        if not isinstance(dest, list):
            dest = [dest] * len(zoneID)
        if not isinstance(num, list):
            num = [num] * len(zoneID)
        if not isinstance(routeName, list):
            routeName = [routeName] * len(zoneID)
        if max_waiting_time is None:
            max_waiting_time = [None] * len(zoneID)
        elif not isinstance(max_waiting_time, list):
            max_waiting_time = [max_waiting_time] * len(zoneID)
        assert len(zoneID) == len(dest) == len(num) == len(routeName) == len(max_waiting_time), \
            "zoneID, dest, num, routeName, and max_waiting_time must have the same length"

        for zoneID, dest, num, routeName, max_wait in zip(zoneID, dest, num, routeName, max_waiting_time):
            record = {"originZoneId": zoneID, "destinationZoneId": dest, "passengerCount": num}
            if max_wait is not None:
                record["maxWaitTicks"] = max_wait
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addBusRequests", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # Spawn e-buses on existing named route(s).
    # routeName: route name or list of route names; num: buses to spawn per route.
    # Returns spawned vehicle IDs grouped by route.
    def add_bus(self, routeName, num, length = None):
        """Spawn e-buses, optionally with a custom length in meters.

        ``length`` may be a scalar or one value per route. When omitted, the
        simulator uses ``DEFAULT_VEHICLE_LENGTH``. Each successful record
        returns the applied ``length`` together with the spawned ``IDs``.
        """
        msg = {"messageType": "addBus", "data": []}
        route_names = _as_list(routeName)
        counts = _batch_field_values(
            num, len(route_names), "num", batch_name="routeName"
        )
        lengths = _optional_vehicle_lengths(length, len(route_names), "routeName")
        for route_name, count, vehicle_length in zip(
                route_names, counts, lengths):
            record = {"routeName": route_name, "vehicleCount": count}
            if vehicle_length is not None:
                record["vehicleLength"] = vehicle_length
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addBus", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
