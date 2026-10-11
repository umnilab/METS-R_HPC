"""Control APIs for cosim."""

import math

from models._api_utils import _batch_field_values, _optional_vehicle_lengths
from utils.primitives import _as_list, _broadcast, _is_sequence


class ControlCosimMixin:
    """Control APIs for cosim."""

    # set the road for co-simulation
    def set_cosim_road(self, roadID):
        """Hand one or more roads from native simulation to co-simulation.

        The latest simulator validates every vehicle on a road before freezing
        native control. Per-road failures use ``status='error'``,
        ``errorCode='FREEZE_BLOCKED'``, and ``retryable``; the top-level status
        becomes ``partial``. Successful records include ``connectorIds``.
        Callers should inspect every returned ``data`` record before treating a
        road as externally controlled.
        """
        msg = {
                "messageType": "setCoSimRoad",
                "data": []
              }
        for i in _as_list(roadID):
            msg['data'].append(i)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "setCoSimRoad", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # release the road for co-simulation
    def release_cosim_road(self, roadID):
        """Return one or more roads to native METS-R SIM control.

        A release that cannot yet place its vehicles safely returns a per-road
        ``status='error'`` record with ``errorCode`` and ``retryable``; the
        top-level status becomes ``partial``. Successful records include
        ``releasedConnectorIds`` and ``connectorIds``. Retry transient records
        after advancing the simulation.
        """
        msg = {
                "messageType": "releaseCoSimRoad",
                "data": []
              }
        if not isinstance(roadID, list):
            roadID = [roadID]
        for i in roadID:
            msg['data'].append(i)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "releaseCoSimRoad", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def enter_road_from_queue(self, vehID = None, roadID = None, private_veh = None,
                              internal_vehicle_id = None, requests = None):
        """Release queued vehicle(s) onto co-simulation road(s).

        The latest METS-R SIM keeps vehicles from automatically spawning onto
        roads marked ``Road.COSIM``. Query the queue with
        :meth:`query_entering_vehicle_queue` or
        :meth:`query_cosim_entering_vehicle_queue`, then call this method when
        the external simulator is ready to spawn a queued vehicle.

        Parameters
        ----------
        vehID : int | list[int] | None
            Visible vehicle ID. For private EV/GV this is the private/external
            ID returned by the co-sim bridge; for taxis/buses it is the internal
            vehicle ID. If omitted with ``roadID``, the road queue head is
            released.
        roadID : str | list[str] | None
            Optional SUMO/original road ID. If omitted, all co-sim entering
            queues are searched for ``vehID``.
        private_veh : bool | list[bool] | None
            Optional vehicle-type filter. ``True`` means private EV/GV,
            ``False`` means public taxi/bus.
        internal_vehicle_id : int | list[int] | None
            Optional internal METS-R vehicle ID, useful when visible private IDs
            are not available.
        requests : dict | list[dict] | None
            Fully formed simulator request record(s). When provided, other
            parameters are ignored.

        Notes
        -----
        A temporarily blocked or occupied entry lane is reported per record
        as ``status='error'`` with ``retryable=True`` and an ``errorCode``.
        """
        msg = {"messageType": "enterRoadFromQueue", "data": []}
        if requests is not None:
            msg["data"] = _as_list(requests)
        else:
            if vehID is None and roadID is None and internal_vehicle_id is None:
                raise ValueError("vehID, roadID, internal_vehicle_id, or requests is required")

            lengths = [
                len(value) for value in (vehID, roadID, private_veh, internal_vehicle_id)
                if _is_sequence(value) and not isinstance(value, str)
            ]
            count = max(lengths) if lengths else 1
            veh_ids = _broadcast(vehID, count)
            road_ids = _broadcast(roadID, count)
            private_flags = _broadcast(private_veh, count)
            internal_ids = _broadcast(internal_vehicle_id, count)

            for vid, rid, prv, internal_id in zip(veh_ids, road_ids, private_flags, internal_ids):
                record = {}
                if vid is not None:
                    record["vehicleId"] = vid
                if internal_id is not None:
                    record["internalVehicleId"] = internal_id
                if prv is not None:
                    record["isPrivate"] = prv
                if rid is not None:
                    record["roadId"] = rid
                msg["data"].append(record)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "enterRoadFromQueue", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def initialize_cosim_vehicle(
            self,
            vehID,
            x,
            y,
            bearing,
            destination_road_id = None,
            speed = 0,
            z = 0.0,
            private_veh = True,
            transform_coords = False,
            length = None,
            segment_id = None,
            road_id = None,
            destinationRoadID = None,
            destRoadID = None,
            segmentID = None,
            roadID = None):
        """Create private EVs directly at authoritative co-simulation poses.

        The latest METS-R SIM map-matches each front-bumper pose against the
        controlled physical roads and intersection connectors. ``segment_id``
        is an optional physical-road or connector hint; ``road_id`` is the
        protocol's equivalent legacy spelling. ``destination_road_id`` must be
        a physical METS-R road with a destination zone. The server rejects
        overlaps instead of shifting the requested pose.

        All arguments accept scalars or batches matching ``vehID``. The camel-
        case keyword aliases mirror the wire protocol. Public fleet vehicles
        are not supported by this command and produce a per-record
        ``UNSUPPORTED_VEHICLE_TYPE`` response.
        """
        if destinationRoadID is not None:
            if destination_road_id is not None:
                raise ValueError(
                    "Use either destination_road_id or destinationRoadID, not both"
                )
            destination_road_id = destinationRoadID
        if destRoadID is not None:
            if destination_road_id is not None:
                raise ValueError(
                    "Use only one of destination_road_id, destinationRoadID, or destRoadID"
                )
            destination_road_id = destRoadID
        if segmentID is not None:
            if segment_id is not None:
                raise ValueError("Use either segment_id or segmentID, not both")
            segment_id = segmentID
        if roadID is not None:
            if road_id is not None:
                raise ValueError("Use either road_id or roadID, not both")
            road_id = roadID
        if destination_road_id is None:
            raise ValueError("destination_road_id is required")

        msg = {"messageType": "initializeCoSimVeh", "data": []}
        veh_ids = _as_list(vehID)
        count = len(veh_ids)
        xs = _batch_field_values(x, count, "x")
        ys = _batch_field_values(y, count, "y")
        zs = _batch_field_values(z, count, "z")
        bearings = _batch_field_values(bearing, count, "bearing")
        speeds = _batch_field_values(speed, count, "speed")
        private_flags = _batch_field_values(private_veh, count, "private_veh")
        transform_flags = _batch_field_values(
            transform_coords, count, "transform_coords"
        )
        vehicle_lengths = _optional_vehicle_lengths(
            length, count, batch_name="vehicleId"
        )
        segment_ids = _batch_field_values(segment_id, count, "segment_id")
        road_ids = _batch_field_values(road_id, count, "road_id")
        destination_ids = _batch_field_values(
            destination_road_id, count, "destination_road_id"
        )

        for (veh_id, x_value, y_value, z_value, bearing_value, speed_value,
             private_flag, transform_flag, vehicle_length, segment_hint,
             road_hint, destination_id) in zip(
                veh_ids, xs, ys, zs, bearings, speeds, private_flags,
                transform_flags, vehicle_lengths, segment_ids, road_ids,
                destination_ids):
            record = {
                "vehicleId": veh_id,
                "isPrivate": private_flag,
                "x": x_value,
                "y": y_value,
                "z": z_value,
                "bearing": bearing_value,
                "speed": speed_value,
                "transformCoordinates": transform_flag,
                "destinationRoadId": destination_id,
            }
            if vehicle_length is not None:
                record["vehicleLength"] = vehicle_length
            if segment_hint is not None:
                record["segmentId"] = segment_hint
            elif road_hint is not None:
                record["segmentId"] = road_hint
            msg["data"].append(record)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "initializeCoSimVeh", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    initialize_coSim_vehicle = initialize_cosim_vehicle

    # Teleport a vehicle to an authoritative pose on a controlled segment.
    def teleport_cosim_vehicle(
            self,
            vehID,
            x,
            y,
            bearing,
            speed = 0,
            z = 0.0,
            private_veh = False,
            transform_coords = False,
            observed_road_id = None,
            observedRoadID = None,
            observed_lane_id = None,
            observedLaneID = None,
            segment_id = None,
            segmentID = None,
            road_id = None,
            roadID = None,
            lane_index = None,
            laneIndex = None,
            connector_path_id = None,
            connectorPathID = None,
            pose_only = False):
        """Apply an authoritative external pose to a co-simulation vehicle.

        Set ``pose_only=True`` to mirror a CARLA-authoritative pose without
        imposing METS-R road, lane, or connector membership. This mode requires
        finite coordinates, bearing, and speed, and cannot be combined with
        segment, lane, or connector-path selectors (including their aliases).
        ``pose_only`` accepts a boolean or one boolean per vehicle. False is the
        legacy behavior and does not add a wire field.

        A successful pose-only response keeps ``controlMode='cosim'`` and
        ``releasedFromCoSim=False``, sets ``externalPoseAuthoritative=True``, and
        reports ``shadowMatched``. Nullable segment/lane/path fields describe
        only the best-effort native shadow, not external vehicle authority.

        Without ``pose_only`` or ``segment_id``, METS-R map-matches only against co-simulation
        roads and connectors. An explicit segment is authoritative.
        ``lane_index`` selects a road lane and ``connector_path_id`` selects a
        connector path; both require a segment and are mutually exclusive.

        An explicit native segment releases a co-simulation-owned vehicle back
        to METS-R. The response reports ``controlMode='native'`` and
        ``releasedFromCoSim=True``; callers should then remove their external
        actor.

        ``road_id``/``observed_road_id`` and ``observed_lane_id`` are deprecated
        Python aliases. The wire payload always uses ``segmentId``,
        ``laneIndex``, and ``connectorPathId``. Record failures use
        ``status='error'`` with ``errorCode``; the top-level status may be
        ``partial``.
        """
        if observedRoadID is not None:
            if observed_road_id is not None:
                raise ValueError(
                    "Use either observed_road_id or observedRoadID, not both"
                )
            observed_road_id = observedRoadID
        if observedLaneID is not None:
            if observed_lane_id is not None:
                raise ValueError(
                    "Use either observed_lane_id or observedLaneID, not both"
                )
            observed_lane_id = observedLaneID
        if segmentID is not None:
            if segment_id is not None:
                raise ValueError("Use either segment_id or segmentID, not both")
            segment_id = segmentID
        if roadID is not None:
            if road_id is not None:
                raise ValueError("Use either road_id or roadID, not both")
            road_id = roadID
        if laneIndex is not None:
            if lane_index is not None:
                raise ValueError("Use either lane_index or laneIndex, not both")
            lane_index = laneIndex
        if observed_lane_id is not None:
            if lane_index is not None:
                raise ValueError(
                    "observed_lane_id is a legacy alias for lane_index; use only one"
                )
            lane_index = observed_lane_id
        if connectorPathID is not None:
            if connector_path_id is not None:
                raise ValueError(
                    "Use either connector_path_id or connectorPathID, not both"
                )
            connector_path_id = connectorPathID
        segment_source_count = sum(
            value is not None
            for value in (segment_id, road_id, observed_road_id)
        )
        if segment_source_count > 1:
            raise ValueError(
                "segment_id, road_id, and observed_road_id are aliases; use only one"
            )

        msg = {
                "messageType": "teleportCoSimVeh",
                "data": []
                }
        veh_ids = _as_list(vehID)
        count = len(veh_ids)
        xs = _batch_field_values(x, count, "x")
        ys = _batch_field_values(y, count, "y")
        zs = _batch_field_values(z, count, "z")
        bearings = _batch_field_values(bearing, count, "bearing")
        speeds = _batch_field_values(speed, count, "speed")
        private_flags = _batch_field_values(private_veh, count, "private_veh")
        transform_flags = _batch_field_values(
            transform_coords, count, "transform_coords"
        )
        observed_road_ids = _batch_field_values(
            observed_road_id, count, "observed_road_id"
        )
        segment_ids = _batch_field_values(segment_id, count, "segment_id")
        road_ids = _batch_field_values(road_id, count, "road_id")
        lane_indices = _batch_field_values(lane_index, count, "lane_index")
        connector_path_ids = _batch_field_values(
            connector_path_id, count, "connector_path_id"
        )
        pose_only_flags = _batch_field_values(pose_only, count, "pose_only")

        for (veh_id, x_value, y_value, z_value, bearing_value, speed_value,
             private_flag, transform_flag, observed_road, segment_hint,
             road_hint, selected_lane, selected_connector_path, pose_only_flag) in zip(
                veh_ids,
                xs,
                ys,
                zs,
                bearings,
                speeds,
                private_flags,
                transform_flags,
                observed_road_ids,
                segment_ids,
                road_ids,
                lane_indices,
                connector_path_ids,
                pose_only_flags):
            record = {
                "vehicleId": veh_id,
                "x": x_value,
                "y": y_value,
                "z": z_value,
                "bearing": bearing_value,
                "speed": speed_value,
                "isPrivate": private_flag,
                "transformCoordinates": transform_flag,
            }
            selected_segment = next(
                (candidate for candidate in
                 (segment_hint, road_hint, observed_road)
                 if candidate is not None),
                None,
            )
            if not isinstance(pose_only_flag, bool):
                raise ValueError("pose_only must be a boolean for each vehicle")
            if pose_only_flag:
                if any(value is not None for value in (
                        selected_segment, selected_lane, selected_connector_path)):
                    raise ValueError(
                        "pose_only cannot be combined with segment, lane, "
                        "or connector-path selectors"
                    )
                for field in ("x", "y", "z", "bearing", "speed"):
                    value = record[field]
                    try:
                        number = float(value)
                    except (TypeError, ValueError, OverflowError):
                        number = math.nan
                    if isinstance(value, bool) or not math.isfinite(number):
                        raise ValueError(f"pose_only {field} must be finite")
                    record[field] = number
                record["poseOnly"] = True
            if selected_lane is not None and selected_connector_path is not None:
                raise ValueError(
                    "lane_index and connector_path_id are mutually exclusive"
                )
            if (selected_lane is not None or selected_connector_path is not None) \
                    and selected_segment is None:
                raise ValueError(
                    "lane_index and connector_path_id require segment_id"
                )
            if selected_segment is not None:
                record["segmentId"] = selected_segment
            if selected_lane is not None:
                record["laneIndex"] = selected_lane
            if selected_connector_path is not None:
                record["connectorPathId"] = selected_connector_path
            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "teleportCoSimVeh", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def teleport_digital_twin_vehicle(
            self,
            vehID,
            segment_id = None,
            lane_index = None,
            distance_to_segment_end = None,
            private_veh = False,
            x = None,
            y = None,
            transform_coords = False,
            z = None,
            connector_path_id = None,
            position_type = None,
            roadID = None,
            laneID = None,
            dist = None,
            segmentID = None,
            laneIndex = None,
            connectorPathID = None):
        """Teleport digital-twin vehicles by segment distance or coordinates.

        Each record uses exactly one position mode. Coordinate mode takes
        ``x``/``y`` and optional ``z`` and lets METS-R match a native road or
        connector. Segment mode takes ``segment_id`` plus
        ``distance_to_segment_end`` and optionally ``lane_index`` for a road or
        ``connector_path_id`` for a connector. Set ``position_type`` explicitly
        or let this client infer it from the supplied fields.

        ``roadID``, ``laneID``, and ``dist`` remain Python-call aliases so old
        code can migrate, but the removed ``roadId`` field is never sent on the
        wire. The server rejects co-simulation targets, co-simulation-owned
        vehicles, invalid geometry, and overlapping placements. Record errors
        make the top-level response ``partial``.
        """
        if segmentID is not None:
            if segment_id is not None:
                raise ValueError("Use either segment_id or segmentID, not both")
            segment_id = segmentID
        if roadID is not None:
            if segment_id is not None:
                raise ValueError("roadID is a legacy alias for segment_id; use only one")
            segment_id = roadID
        if laneIndex is not None:
            if lane_index is not None:
                raise ValueError("Use either lane_index or laneIndex, not both")
            lane_index = laneIndex
        if laneID is not None:
            if lane_index is not None:
                raise ValueError("laneID is a legacy alias for lane_index; use only one")
            lane_index = laneID
        if dist is not None:
            if distance_to_segment_end is not None:
                raise ValueError(
                    "dist is a legacy alias for distance_to_segment_end; use only one"
                )
            distance_to_segment_end = dist
        if connectorPathID is not None:
            if connector_path_id is not None:
                raise ValueError(
                    "Use either connector_path_id or connectorPathID, not both"
                )
            connector_path_id = connectorPathID

        msg = {
                "messageType": "teleportDigitalTwinVeh",
                "data": []
                }
        veh_ids = _as_list(vehID)
        count = len(veh_ids)

        segment_ids = _batch_field_values(segment_id, count, "segment_id")
        lane_indices = _batch_field_values(lane_index, count, "lane_index")
        distances = _batch_field_values(
            distance_to_segment_end, count, "distance_to_segment_end"
        )
        private_flags = _batch_field_values(private_veh, count, "private_veh")
        xs = _batch_field_values(x, count, "x")
        ys = _batch_field_values(y, count, "y")
        zs = _batch_field_values(z, count, "z")
        transform_flags = _batch_field_values(
            transform_coords, count, "transform_coords"
        )
        connector_path_ids = _batch_field_values(
            connector_path_id, count, "connector_path_id"
        )
        position_types = _batch_field_values(
            position_type, count, "position_type"
        )

        for (veh_id, segment, lane, distance, private_flag, x_value, y_value,
             z_value, transform_flag, connector_path, requested_mode) in zip(
                veh_ids, segment_ids, lane_indices, distances, private_flags,
                xs, ys, zs, transform_flags, connector_path_ids,
                position_types):
            if lane == -1:
                lane = None

            has_coordinates = any(
                value is not None for value in (x_value, y_value, z_value)
            )
            has_segment_position = any(
                value is not None
                for value in (segment, lane, connector_path, distance)
            )
            if requested_mode is None:
                if has_coordinates and has_segment_position:
                    raise ValueError(
                        "digital-twin teleport cannot mix coordinate and segment fields"
                    )
                if has_coordinates:
                    mode = "coordinate"
                elif has_segment_position:
                    mode = "segment"
                else:
                    raise ValueError(
                        "digital-twin teleport requires coordinates or a segment position"
                    )
            elif isinstance(requested_mode, str):
                mode = requested_mode.lower()
            else:
                raise ValueError("position_type must be 'coordinate' or 'segment'")

            if mode not in {"coordinate", "segment"}:
                raise ValueError("position_type must be 'coordinate' or 'segment'")

            record = {
                "vehicleId": veh_id,
                "isPrivate": private_flag,
                "positionType": mode,
            }

            if mode == "coordinate":
                if has_segment_position:
                    raise ValueError(
                        "coordinate digital-twin teleport cannot include segment, "
                        "distance, lane, or connector-path fields"
                    )
                if x_value is None or y_value is None:
                    raise ValueError(
                        "coordinate digital-twin teleport requires both x and y"
                    )
                record["x"] = x_value
                record["y"] = y_value
                if z_value is not None:
                    record["z"] = z_value
                record["transformCoordinates"] = transform_flag
            else:
                if has_coordinates:
                    raise ValueError(
                        "segment digital-twin teleport cannot include x, y, or z"
                    )
                if transform_flag:
                    raise ValueError(
                        "transform_coords applies only to coordinate teleport"
                    )
                if segment is None or distance is None:
                    raise ValueError(
                        "segment digital-twin teleport requires segment_id and "
                        "distance_to_segment_end"
                    )
                if lane is not None and connector_path is not None:
                    raise ValueError(
                        "lane_index and connector_path_id are mutually exclusive"
                    )
                record["segmentId"] = segment
                record["distanceToSegmentEnd"] = distance
                if lane is not None:
                    record["laneIndex"] = lane
                if connector_path is not None:
                    record["connectorPathId"] = connector_path

            msg["data"].append(record)
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "teleportDigitalTwinVeh", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # Historical name uses the same digital-twin command.
    teleport_trace_replay_vehicle = teleport_digital_twin_vehicle

    # enter the next road
    def enter_next_road(
            self,
            vehID,
            roadID="",
            private_veh = False,
            laneID = None,
            lane_id = None):
        """Report removal of the legacy explicit road-transition command.

        METS-R SIM commit ``18aa172`` removed ``enterNextRoad``. Connector
        entry and completion are now inferred atomically from authoritative
        poses sent through :meth:`teleport_cosim_vehicle`.
        """
        del vehID, roadID, private_veh, laneID, lane_id
        raise NotImplementedError(
            "enter_next_road is not supported by current METS-R SIM; "
            "send the authoritative connector/road pose with "
            "teleport_cosim_vehicle instead"
        )

    def cancel_cosim_vehicle(self, vehID, private_veh=False):
        """Cancel external vehicle ownership and return the server reply unchanged.

        Vehicle IDs and private-vehicle flags accept scalars or equal-length
        batches. Visualization annotations are cleared only when a matching
        reply explicitly confirms successful cancellation.
        """
        vehicle_ids = _as_list(vehID)
        private_flags = _batch_field_values(
            private_veh, len(vehicle_ids), "private_veh"
        )
        if any(not isinstance(flag, bool) for flag in private_flags):
            raise ValueError("private_veh must be a boolean for each vehicle")
        msg = {
            "messageType": "cancelCoSimVeh",
            "data": [{"vehicleId": vehicle_id, "isPrivate": private_flag}
                     for vehicle_id, private_flag in zip(vehicle_ids, private_flags)],
        }
        response = self.send_receive_msg(msg, ignore_heartbeats=True)
        if (not isinstance(response, dict)
                or response.get("messageType") != "cancelCoSimVeh"
                or response.get("status") not in ("ok", "partial")
                or not isinstance(response.get("data"), (list, tuple))):
            return response

        requested_namespaces = {}
        for vehicle_id, private_flag in zip(vehicle_ids, private_flags):
            requested_namespaces.setdefault(str(vehicle_id), set()).add(private_flag)
        cancelled_keys = set()
        for record in response["data"]:
            if (not isinstance(record, dict) or record.get("vehicleId") is None
                    or record.get("status") != "ok"
                    or record.get("cancelled") is not True
                    or record.get("controlMode") != "native"
                    or record.get("externalPoseAuthoritative") is not False):
                continue
            vehicle_id = str(record["vehicleId"])
            namespaces = requested_namespaces.get(vehicle_id, set())
            if "isPrivate" in record:
                private_flag = record["isPrivate"]
                if isinstance(private_flag, bool) and private_flag in namespaces:
                    cancelled_keys.add((private_flag, vehicle_id))
            elif len(namespaces) == 1:
                # Older replies omit the namespace; only an unambiguous ID
                # can safely identify the annotation to clear.
                cancelled_keys.add((next(iter(namespaces)), vehicle_id))
        if cancelled_keys:
            with self.viz_stream_lock:
                self._attack_vehicle_keys.difference_update(cancelled_keys)
        return response
