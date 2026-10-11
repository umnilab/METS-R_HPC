"""Control APIs for vehicles."""

from models._api_utils import _batch_field_values, _optional_vehicle_lengths
from utils.primitives import _as_list, _is_sequence, _normalize_sensor_type


class ControlVehiclesMixin:
    """Control APIs for vehicles."""

    # generate a vehicle trip between origin and destination zones
    def generate_trip(self, vehID, origin = -1, destination = -1, length = None):
        """Generate private-EV trips between zones.

        ``length`` is an optional vehicle length in meters. It may be a scalar
        or one value per ``vehID`` and must be finite and positive when set.
        METS-R SIM applies it only when that external vehicle ID is first
        created; subsequent trips retain the vehicle's original length. Each
        successful response record reports the effective ``length``.
        """
        msg = {"messageType": "generateTrip", "data": []}
        vehicle_ids = _as_list(vehID)
        origins = _batch_field_values(
            origin, len(vehicle_ids), "origin", batch_name="vehicleId"
        )
        destinations = _batch_field_values(
            destination, len(vehicle_ids), "destination", batch_name="vehicleId"
        )
        lengths = _optional_vehicle_lengths(length, len(vehicle_ids), "vehicleId")

        for vehicle_id, origin_id, destination_id, vehicle_length in zip(
                vehicle_ids, origins, destinations, lengths):
            record = {
                "vehicleId": vehicle_id,
                "originZoneId": origin_id,
                "destinationZoneId": destination_id,
            }
            if vehicle_length is not None:
                record["vehicleLength"] = vehicle_length
            msg["data"].append(record)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["messageType"] == "generateTrip", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # generate a vehicle trip between origin and destination roads
    def generate_trip_between_roads(
            self, vehID, origin, destination, length = None):
        """Generate private-EV trips between original road IDs.

        ``length`` follows :meth:`generate_trip`: it is measured in meters,
        supports scalar broadcasting, and only affects a newly created vehicle.
        """
        msg = {"messageType": "generateTripsByRoad", "data": []}
        vehicle_ids = _as_list(vehID)
        origins = _batch_field_values(
            origin, len(vehicle_ids), "origin", batch_name="vehicleId"
        )
        destinations = _batch_field_values(
            destination, len(vehicle_ids), "destination", batch_name="vehicleId"
        )
        lengths = _optional_vehicle_lengths(length, len(vehicle_ids), "vehicleId")

        for vehicle_id, origin_id, destination_id, vehicle_length in zip(
                vehicle_ids, origins, destinations, lengths):
            record = {
                "vehicleId": vehicle_id,
                "originRoadId": origin_id,
                "destinationRoadId": destination_id,
            }
            if vehicle_length is not None:
                record["vehicleLength"] = vehicle_length
            msg["data"].append(record)

        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["messageType"] == "generateTripsByRoad", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # reach destination
    def reach_dest(self, vehID, private_veh = False):
        """Notify the simulator that vehicles have reached their destinations."""
        msg = {
                "messageType": "reachDest",
                "data": []
                }
        vehicle_ids = _as_list(vehID)
        private_flags = _batch_field_values(
            private_veh, len(vehicle_ids), "private_veh"
        )

        for vehicle_id, private_flag in zip(vehicle_ids, private_flags):
            msg["data"].append({
                "vehicleId": vehicle_id,
                "isPrivate": private_flag,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "reachDest", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        with self.viz_stream_lock:
            for veh_id, private_flag in zip(vehicle_ids, private_flags):
                self._attack_vehicle_keys.discard((bool(private_flag), str(veh_id)))
        return res

    # control vehicle with specified acceleration
    def control_vehicle(self, vehID, acc, private_veh = False):
        """Apply the specified accelerations to private or public vehicles."""
        msg = {
                "messageType": "controlVeh",
                "data": []
                }
        if not isinstance(vehID, list):
            vehID = [vehID]
            acc = [acc]
        if not isinstance(private_veh, list):
            private_veh = [private_veh] * len(vehID)
        for vehID, acc, private_veh in zip(vehID, acc, private_veh):
            msg["data"].append({"vehicleId": vehID, "isPrivate": private_veh, "acceleration": acc})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "controlVeh", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def set_attack_vehicle(self, vehID, is_attack = True, private_veh = False):
        """Mark electric taxi trip(s) for attack visualization.

        The simulator accepts public electric taxis only. private_veh=False
        therefore matches the normal use case; unsupported vehicle types are
        reported with status='error' in the response data list. A successful
        designation lasts for the taxi's current trip and is also cleared by
        passing is_attack=False.

        vehID, is_attack, and private_veh may be scalars or equal-length lists.
        The request records use ``vehicleId``, ``isPrivate``, and
        ``attackEnabled``.
        """
        veh_ids = _as_list(vehID)
        count = len(veh_ids)

        def _field_values(value, name):
            if _is_sequence(value):
                values = list(value)
                if len(values) != count:
                    raise ValueError(f"{name} must have the same length as vehID")
                return values
            return [value] * count

        attack_flags = _field_values(is_attack, "is_attack")
        private_flags = _field_values(private_veh, "private_veh")
        msg = {"messageType": "setAttackVehicle", "data": []}
        for veh_id, private_flag, attack_flag in zip(
                veh_ids, private_flags, attack_flags):
            msg["data"].append({
                "vehicleId": veh_id,
                "isPrivate": bool(private_flag),
                "attackEnabled": bool(attack_flag),
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "setAttackVehicle", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]

        with self.viz_stream_lock:
            for request, status in zip(msg["data"], res.get("data") or []):
                if not isinstance(status, dict) or status.get("status") != "ok":
                    continue
                key = (bool(request["isPrivate"]), str(request["vehicleId"]))
                if request["attackEnabled"]:
                    self._attack_vehicle_keys.add(key)
                else:
                    self._attack_vehicle_keys.discard(key)
        return res

    setAttackVehicle = set_attack_vehicle

    def update_vehicle_sensor_type(self, vehID, sensorType, private_veh = False):
        """Update vehicle sensor type used by V2X/DSRC data collection.

        ``sensorType`` may be the simulator integer code or a readable alias:
        ``0``/``"dsrc"``, ``1``/``"cv2x"``, or
        ``2``/``"mobile_device"``. DSRC and C-V2X vehicles emit BSM-style
        safety records when ``V2X = true``; mobile-device vehicles emit link
        travel-time and energy probe records.
        """
        msg = {
                "messageType": "updateVehicleSensorType",
                "data": []
                }
        vehID = _as_list(vehID)
        if not _is_sequence(private_veh):
            private_veh = [private_veh] * len(vehID)
        else:
            private_veh = list(private_veh)
        if not _is_sequence(sensorType):
            sensorType = [sensorType] * len(vehID)
        else:
            sensorType = list(sensorType)
        assert len(vehID) == len(sensorType) == len(private_veh), \
            "vehID, sensorType, and private_veh must have the same length"
        for vehID, sensorType, private_veh in zip(vehID, sensorType, private_veh):
            msg["data"].append({
                "vehicleId": vehID,
                "sensorType": _normalize_sensor_type(sensorType),
                "isPrivate": private_veh,
            })
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateVehicleSensorType", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    set_vehicle_sensor_type = update_vehicle_sensor_type

    updateVehicleSensorType = update_vehicle_sensor_type

    # update vehicle route
    def update_vehicle_route(self, vehID, route, private_veh = False):
        """Replace vehicle routes with the supplied sequences of road IDs."""
        msg = {
                "messageType": "updateVehicleRoute",
                "data": []
                }
        if not isinstance(vehID, list):
            vehID = [vehID]
            route = [route]
        if not isinstance(private_veh, list):
            private_veh = [private_veh] * len(vehID)

        for vehID, route, private_veh in zip(vehID, route, private_veh):
            msg["data"].append({"vehicleId": vehID, "routeRoadIds": route, "isPrivate": private_veh})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateVehicleRoute", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
