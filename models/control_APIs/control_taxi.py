"""Control APIs for taxi."""

from models._api_utils import _batch_field_values, _optional_vehicle_lengths
from utils.primitives import (
    _as_list,
    _is_sequence,
    _request_id_from_record,
    _request_zone_from_record,
)


class ControlTaxiMixin:
    """Control APIs for taxi."""

    # Match available taxi(s) to existing pending request(s).
    def dispatch_taxi(
            self,
            vehID,
            reqID,
            origin_zone_id=None,
            include_state=False,
            command_id=None):
        """Dispatch taxi(s) to serve already-pending request(s).

        The METS-R SIM Control API separates request creation from dispatching.
        Use ``add_taxi_requests`` or ``add_taxi_requests_between_roads`` first,
        read the returned ``requestId``, then pass it with the taxi ID here.

        Recent METS-R SIM versions can release a parked taxi from parking,
        queue the request after an unfinished passenger-free trip, and return
        fields such as ``remainingCapacity``, ``requestPassengers``, and
        ``parkingReservationReleased`` in each response record.

        ``origin_zone_id`` activates the indexed request lookup. ``include_state``
        requests only the taxi ``state`` field; when it is absent, the client
        queries the taxi once after dispatch to fill it in. A
        ``command_id`` is sent as stable retry metadata, but SIM f1818b2 does
        not deduplicate dispatch commands; exactly-once behavior is guaranteed
        only by :meth:`advance_and_snapshot`.
        """
        msg = {"messageType": "dispatchTaxi", "data": []}
        if not isinstance(vehID, list):
            vehID = [vehID]
        if not isinstance(reqID, list):
            reqID = [reqID] * len(vehID)
        if origin_zone_id is None:
            origin_zone_id = [None] * len(vehID)
        elif not isinstance(origin_zone_id, list):
            origin_zone_id = [origin_zone_id] * len(vehID)
        assert len(vehID) == len(reqID) == len(origin_zone_id), \
            "vehID, reqID, and origin_zone_id must have the same length"

        for veh_id, req_id, zone_id in zip(vehID, reqID, origin_zone_id):
            record = {"vehicleId": veh_id, "requestId": req_id}
            if zone_id is not None:
                record["originZoneId"] = zone_id
            msg["data"].append(record)
        if include_state:
            msg["fieldMask"] = ["state"]
        res = self.send_receive_msg(
            msg,
            ignore_heartbeats=True,
            command_id=command_id,
        )
        assert res["messageType"] == "dispatchTaxi", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]

        if include_state:
            response_records = [
                record for record in res.get("data", []) if isinstance(record, dict)
            ]
            for index, record in enumerate(response_records):
                if record.get("vehicleId") is None and index < len(vehID):
                    record["vehicleId"] = vehID[index]
            missing_ids = [
                record.get("vehicleId")
                for record in response_records
                if record.get("vehicleId") is not None and "state" not in record
            ]
            if missing_ids:
                taxi_response = self.query_taxi(missing_ids)
                state_by_id = {
                    record.get("taxiId"): record.get("state")
                    for record in taxi_response.get("data", [])
                    if isinstance(record, dict) and "state" in record
                }
                for record in response_records:
                    taxi_id = record.get("vehicleId")
                    if "state" not in record and taxi_id in state_by_id:
                        record["state"] = state_by_id[taxi_id]
                res["legacyFallback"] = True
        return res

    def cancel_requests(self, reqID, zoneID=None, rematch=False):
        """Cancel one or more taxi/bus requests.

        The METS-R SIM control API uses ``cancelRequests`` and
        requires the request's origin zone for each record. ``reqID`` may be a
        scalar request ID, a list of request IDs, a request record, or a list
        of request records containing request ID and origin-zone fields. When
        ``zoneID`` is omitted, the client attempts to infer it with
        :meth:`query_request`.

        ``rematch`` defaults to ``False`` (normal cancellation). When ``True``,
        matched taxi requests that have not been picked up return to their
        origin queue, preserving their ID, generation time, waiting limit,
        and activity plan; pending taxi requests remain unchanged. Occupied
        taxi requests cannot be cancelled, and bus cancellation is unchanged.
        Pass a boolean for all requests or a list/tuple aligned with ``reqID``.
        A request record's non-None ``rematch`` value overrides this argument.

        The returned ``data`` list contains per-request ``status`` and
        ``message`` details; record errors make the top-level status ``partial``.
        Successful taxi rematches report ``action="requeued"`` or
        ``action="unchanged"`` for requests that were already pending.
        """
        if zoneID is None and isinstance(reqID, dict):
            request_records = [reqID]
        elif (
            zoneID is None
            and _is_sequence(reqID)
            and all(isinstance(record, dict) for record in reqID)
        ):
            request_records = list(reqID)
        else:
            source_records = _as_list(reqID)
            request_ids = [_request_id_from_record(record) for record in source_records]
            if zoneID is None:
                zone_ids = [None] * len(request_ids)
            elif _is_sequence(zoneID):
                zone_ids = list(zoneID)
            else:
                zone_ids = [zoneID] * len(request_ids)
            assert len(request_ids) == len(zone_ids), \
                "reqID and zoneID must have the same length"
            request_records = [
                dict(record, requestId=rid, zoneId=zid)
                if isinstance(record, dict) else {"requestId": rid, "zoneId": zid}
                for record, rid, zid in zip(source_records, request_ids, zone_ids)
            ]

        rematch_flags = _batch_field_values(
            rematch, len(request_records), "rematch", batch_name="reqID"
        )
        msg = {"messageType": "cancelRequests", "data": []}
        missing_zone_ids = []
        for record, rematch_flag in zip(request_records, rematch_flags):
            rid = _request_id_from_record(record)
            zid = _request_zone_from_record(record)
            if rid is None:
                raise ValueError("reqID is required for cancel_requests")
            if zid is None:
                zid = self._infer_request_zone_id(rid)
            if zid is None:
                missing_zone_ids.append(rid)
                continue
            record_rematch = record.get("rematch")
            msg["data"].append({
                "requestId": rid,
                "zoneId": zid,
                "rematch": rematch_flag if record_rematch is None else record_rematch,
            })

        if missing_zone_ids:
            raise ValueError(
                "zoneID is required for cancel_requests; could not infer it "
                "for reqID(s): {}".format(missing_zone_ids)
            )

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "cancelRequests", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    cancel_request = cancel_requests

    cancelRequests = cancel_requests

    def reposition_taxi(self, vehID, zoneID=None, roadID=None):
        """Reposition idle/cruising taxi(s) to destination zone(s) or road(s).
        If the taxi was already traveling to reserved parking, the simulator can
        release that reservation and report ``parkingReservationReleased`` in
        the response record.
        """
        msg = {"messageType": "repositionTaxi", "data": []}
        vehID = _as_list(vehID)
        if zoneID is None:
            zoneID = [None] * len(vehID)
        elif not _is_sequence(zoneID):
            zoneID = [zoneID] * len(vehID)
        else:
            zoneID = list(zoneID)
        if roadID is None:
            roadID = [None] * len(vehID)
        elif not _is_sequence(roadID):
            roadID = [roadID] * len(vehID)
        else:
            roadID = list(roadID)
        assert len(vehID) == len(zoneID) == len(roadID), \
            "vehID, zoneID, and roadID must have the same length"
        
        for vid, zid, rid in zip(vehID, zoneID, roadID):
            if zid is None and rid is None:
                raise ValueError("zoneID or roadID is required for reposition_taxi")
            record = {"vehicleId": vid}
            if zid is not None:
                record["zoneId"] = zid
            if rid is not None:
                record["roadId"] = rid
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "repositionTaxi", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def go_parking(self, vehID, zoneID=None, roadID=None):
        """Send idle taxi(s) to park at target zone(s) or road(s).

        Provide ``zoneID``, ``roadID``, or both. If only ``zoneID`` is supplied,
        METS-R SIM samples a parking road in that zone. If only ``roadID`` is
        supplied, the zone is inferred from the road. When both are supplied,
        the road must belong to the zone and have available parking capacity.
        """
        msg = {"messageType": "goParking", "data": []}
        vehID = _as_list(vehID)
        if zoneID is None:
            zoneID = [None] * len(vehID)
        elif not _is_sequence(zoneID):
            zoneID = [zoneID] * len(vehID)
        else:
            zoneID = list(zoneID)
        if roadID is None:
            roadID = [None] * len(vehID)
        elif not _is_sequence(roadID):
            roadID = [roadID] * len(vehID)
        else:
            roadID = list(roadID)
        assert len(vehID) == len(zoneID) == len(roadID), \
            "vehID, zoneID, and roadID must have the same length"

        for vid, zid, rid in zip(vehID, zoneID, roadID):
            if zid is None and rid is None:
                raise ValueError("zoneID or roadID is required for go_parking")
            record = {"vehicleId": vid}
            if zid is not None:
                record["zoneId"] = zid
            if rid is not None:
                record["roadId"] = rid
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "goParking", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def add_taxi_requests(self, zoneID, dest, num, max_waiting_time = None, maxWaitingTime = None):
        """Add one or more pending taxi requests.

        ``max_waiting_time`` is optional and uses simulation ticks. It maps to
        the API field ``maxWaitTicks``. When it is positive, the simulator
        overrides the request's default waiting tolerance; omitted, ``None``,
        or non-positive values keep the simulator default.
        """
        if max_waiting_time is None and maxWaitingTime is not None:
            max_waiting_time = maxWaitingTime

        msg = {
                "messageType": "addTaxiRequests",
                "data": []
                }
        if not isinstance(zoneID, list):
            zoneID = [zoneID]
        if not isinstance(dest, list):
            dest = [dest] * len(zoneID)
        if not isinstance(num, list):
            num = [num] * len(zoneID)
        if max_waiting_time is None:
            max_waiting_time = [None] * len(zoneID)
        elif not isinstance(max_waiting_time, list):
            max_waiting_time = [max_waiting_time] * len(zoneID)
        assert len(zoneID) == len(dest) == len(num) == len(max_waiting_time), \
            "zoneID, dest, num, and max_waiting_time must have the same length"

        for zoneID, dest, num, max_wait in zip(zoneID, dest, num, max_waiting_time):
            record = {"originZoneId": zoneID, "destinationZoneId": dest, "passengerCount": num}
            if max_wait is not None:
                record["maxWaitTicks"] = max_wait
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addTaxiRequests", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def add_taxi_requests_between_roads(self, orig, dest, num):
        """Create taxi requests between origin and destination roads."""
        msg = {
                "messageType": "addTaxiReqBwRoads",
                "data": []
                }
        if not isinstance(orig, list):
            orig = [orig]
        if not isinstance(dest, list):
            dest = [dest] * len(orig)
        if not isinstance(num, list):
            num = [num] * len(orig)

        for orig, dest, num in zip(orig, dest, num):
            msg["data"].append({"originRoadId": orig, "destinationRoadId": dest, "passengerCount": num})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addTaxiReqBwRoads", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # Spawn e-taxis parked at given zone(s).
    # zoneID: zone ID or list of zone IDs; num: number of taxis to spawn per zone.
    # Returns spawned vehicle IDs grouped by zone.
    def add_taxi(self, zoneID, num, length = None):
        """Spawn parked e-taxis, optionally with a custom length in meters.

        ``length`` may be a scalar or one value per zone. When omitted, the
        simulator uses ``DEFAULT_VEHICLE_LENGTH``. Each successful record
        returns the applied ``length`` together with the spawned ``IDs``.
        """
        msg = {"messageType": "addTaxi", "data": []}
        zone_ids = _as_list(zoneID)
        counts = _batch_field_values(num, len(zone_ids), "num", batch_name="zoneId")
        lengths = _optional_vehicle_lengths(length, len(zone_ids), "zoneId")
        for zone_id, count, vehicle_length in zip(zone_ids, counts, lengths):
            record = {"zoneId": zone_id, "vehicleCount": count}
            if vehicle_length is not None:
                record["vehicleLength"] = vehicle_length
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addTaxi", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
