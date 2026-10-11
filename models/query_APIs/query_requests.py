"""Query APIs for requests."""

from utils.primitives import _as_list, _request_zone_from_record


class QueryRequestsMixin:
    """Query APIs for requests."""

    def query_pending_requests(self, zoneID = None):
        """Query pending taxi and bus requests.

        Without ``zoneID`` returns pending requests across all zones. With a
        zone ID, returns pending requests in that zone. Request records include
        ``requestId`` (used by ``dispatch_taxi``), origin/destination
        zones and roads, party size, waiting-time fields, and a queue status.
        """
        msg = {"messageType": "pendingRequests"}
        if zoneID is not None:
            msg["data"] = zoneID
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "pendingRequests", res["messageType"]
        return res

    def query_request(self, reqID):
        """Query one or more ride-hailing or bus request records by ID."""
        msg = {"messageType": "request", "data": []}
        if not isinstance(reqID, list):
            reqID = [reqID]
        for rid in reqID:
            msg["data"].append(rid)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "request", res["messageType"]
        return res

    def _infer_request_zone_id(self, reqID):
        response = self.query_request(reqID)
        for record in response.get("data", []):
            zone_id = _request_zone_from_record(record)
            if zone_id is not None:
                return zone_id
        return None

    def query_pickup_taxi_info(self, reqID=None):
        """Query taxi requests that have been matched but not picked up.

        METS-R SIM added this endpoint with the cancellation API. Passing
        ``reqID`` filters the result to one or more request IDs.
        """
        msg = {"messageType": "pickupTaxiInfo"}
        if reqID is not None:
            msg["data"] = _as_list(reqID)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "pickupTaxiInfo", res["messageType"]
        return res

    def query_occupied_taxi_info(self, reqID=None):
        """Query taxi requests that are already on board a taxi.

        Passing ``reqID`` filters the result to one or more request IDs.
        Cancellation eligibility is reported by the simulator in the response
        to :meth:`cancel_requests`.
        """
        msg = {"messageType": "occupiedTaxiInfo"}
        if reqID is not None:
            msg["data"] = _as_list(reqID)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "occupiedTaxiInfo", res["messageType"]
        return res
