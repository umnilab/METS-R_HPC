"""Control APIs for roads."""

from models._api_utils import _batch_field_values
from utils.primitives import _as_list, _broadcast, _is_sequence, _looks_like_centerline


class ControlRoadsMixin:
    """Control APIs for roads."""

    # update road weights in the routing map
    def update_road_weights(self, roadID, weight):
        """Override routing-only costs for one or more roads.

        This control does not change physical lane speeds, road speed limits,
        or measured travel times. Use :meth:`update_target_speed` when vehicle
        behavior and the corresponding free-flow routing cost should both
        change. The latest server accepts original or internal road IDs,
        rejects non-finite weights, clamps finite values below ``1e-3``, and
        reports the applied ``weight`` or a per-record ``WARN``.
        """
        msg = {"messageType": "updateEdgeWeight", "data": []}
        road_ids = _as_list(roadID)
        weights = _batch_field_values(
            weight, len(road_ids), "weight", batch_name="roadID"
        )
        for road_id, weight_value in zip(road_ids, weights):
            msg["data"].append({"roadId": road_id, "routingWeight": weight_value})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateEdgeWeight", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def update_target_speed(
            self,
            roadID,
            target_speed = None,
            targetSpeed = None,
            speed = None):
        """Set physical target speeds for one or more roads, in m/s.

        Unlike :meth:`update_road_weights`, this updates lane/vehicle behavior,
        the reported road speed limit, and the road's free-flow routing cost.
        ``target_speed`` is the preferred Python argument; ``targetSpeed`` and
        ``speed`` mirror aliases accepted by METS-R SIM. Scalars are broadcast
        across a batched ``roadID`` argument.

        The simulator requires each speed to be finite and positive. It returns
        one result per road with ``status`` and, on success, ``targetSpeed``,
        ``speedUnit``, ``speedLimit``, ``travelTime``, and ``routingWeight``.
        Record errors make the top-level status ``partial``.
        """
        supplied_speeds = [
            (name, value)
            for name, value in (
                ("target_speed", target_speed),
                ("targetSpeed", targetSpeed),
                ("speed", speed),
            )
            if value is not None
        ]
        if not supplied_speeds:
            raise ValueError("target_speed is required")
        if len(supplied_speeds) > 1:
            names = ", ".join(name for name, _ in supplied_speeds)
            raise ValueError(f"Use only one target-speed argument, received: {names}")
        target_speed = supplied_speeds[0][1]

        road_ids = _as_list(roadID)
        target_speeds = _batch_field_values(
            target_speed,
            len(road_ids),
            "target_speed",
            batch_name="roadID",
        )
        msg = {"messageType": "updateTargetSpeed", "data": []}
        for road_id, speed_value in zip(road_ids, target_speeds):
            msg["data"].append({
                "roadId": road_id,
                "targetSpeed": speed_value,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateTargetSpeed", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def update_road_parking_capacity(
            self, roadID, parking_capacity=None, parkingCapacity=None, capacity=None):
        """Update parking capacity for one or more roads.

        ``parking_capacity`` is the preferred Python argument. ``parkingCapacity``
        and ``capacity`` are accepted to mirror the METS-R SIM Control API
        aliases.
        """
        if parking_capacity is None:
            parking_capacity = parkingCapacity
        if parking_capacity is None:
            parking_capacity = capacity
        if parking_capacity is None:
            raise ValueError("parking_capacity is required")

        msg = {"messageType": "updateRoadParkingCapacity", "data": []}
        roadID = _as_list(roadID)
        if not _is_sequence(parking_capacity):
            parking_capacity = [parking_capacity] * len(roadID)
        else:
            parking_capacity = list(parking_capacity)
        assert len(roadID) == len(parking_capacity), \
            "roadID and parking_capacity must have the same length"

        for rid, cap in zip(roadID, parking_capacity):
            msg["data"].append({"roadId": rid, "parkingCapacity": cap})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateRoadParkingCapacity", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # Dynamically add one or more roads and generated lanes.
    # centerline: [[x, y], ...] or [[x, y, z], ...] per road.
    # upstream_road/downstream_road: road orig_id string or list of orig_id strings.
    # parking_capacity: optional road-level parking slots.
    # Pass roads=[{...}] to send fully formed simulator records directly.
    def add_roads(self, centerline=None, upstream_road=None, downstream_road=None,
                  orig_id=None, road_type=None, control_type=None,
                  upstream_control_type=None, downstream_control_type=None,
                  num_lanes=1, lane_width=None, transform_coord=False, roads=None,
                  parking_capacity=None):
        """Create roads and lanes from centerlines or complete road records."""
        msg = {"messageType": "addRoads", "data": []}

        if roads is not None:
            msg["data"] = _as_list(roads)
        else:
            if centerline is None:
                raise ValueError("centerline is required when roads is not provided")
            if upstream_road is None or downstream_road is None:
                raise ValueError("upstream_road and downstream_road are required")

            centerlines = [centerline] if _looks_like_centerline(centerline) else list(centerline)
            if len(centerlines) == 0:
                raise ValueError("At least one road centerline is required")

            count = len(centerlines)
            orig_ids = _broadcast(orig_id, count)
            upstream_roads = _broadcast(upstream_road, count)
            downstream_roads = _broadcast(downstream_road, count)
            road_types = _broadcast(road_type, count)
            control_types = _broadcast(control_type, count)
            upstream_control_types = _broadcast(upstream_control_type, count)
            downstream_control_types = _broadcast(downstream_control_type, count)
            lane_counts = _broadcast(num_lanes, count)
            lane_widths = _broadcast(lane_width, count)
            transform_coords = _broadcast(transform_coord, count)
            parking_capacities = _broadcast(parking_capacity, count)

            for cl, oid, up, down, r_type, c_type, up_control, down_control, lane_count, width, tc, pcap in zip(
                    centerlines, orig_ids, upstream_roads, downstream_roads, road_types,
                    control_types, upstream_control_types, downstream_control_types,
                    lane_counts, lane_widths, transform_coords, parking_capacities):
                record = {
                    "centerline": cl,
                    "laneCount": lane_count,
                    "transformCoordinates": tc,
                    "upstreamRoadIds": _as_list(up),
                    "downstreamRoadIds": _as_list(down),
                }
                if oid is not None:
                    record["roadId"] = oid
                if r_type is not None:
                    record["roadType"] = r_type
                if c_type is not None:
                    record["controlMode"] = (
                        "cosim" if c_type == 1 else "native" if c_type == 0
                        else str(c_type)
                    )
                if up_control is not None:
                    record["upstreamControlMode"] = (
                        "cosim" if up_control == 1 else "native" if up_control == 0
                        else str(up_control)
                    )
                if down_control is not None:
                    record["downstreamControlMode"] = (
                        "cosim" if down_control == 1 else "native" if down_control == 0
                        else str(down_control)
                    )
                if width is not None:
                    record["laneWidth"] = width
                if pcap is not None:
                    record["parkingCapacity"] = pcap
                msg["data"].append(record)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addRoads", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def remove_road(self, roadID):
        """Dynamically remove one or more roads by SUMO/original road ID."""
        msg = {"messageType": "removeRoad", "data": _as_list(roadID)}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "removeRoad", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
