"""Control APIs for zones."""

from utils.primitives import _as_list


class ControlZonesMixin:
    """Control APIs for zones."""

    # Dynamically add one or more zones at given coordinates.
    # x, y: map coordinates; z: elevation (default 0.0);
    # capacity: zone capacity; zone_type: zone type int;
    # transform_coord: set True for projected/local SUMO coordinates.
    # Returns assigned zone IDs.
    def add_zone(self, x, y, capacity, zone_type, z=0.0, transform_coord=False):
        """Create zones at the given locations with specified capacities and types."""
        msg = {"messageType": "addZone", "data": []}
        if not isinstance(x, list):
            x = [x]
            y = [y]
            z = [z]
            capacity = [capacity]
            zone_type = [zone_type]
        if not isinstance(z, list):
            z = [z] * len(x)
        if not isinstance(transform_coord, list):
            transform_coord = [transform_coord] * len(x)
        assert len(x) == len(y) == len(z) == len(capacity) == len(zone_type), \
            "x, y, z, capacity, and zone_type must have the same length"
        for xi, yi, zi, cap, ztype, tc in zip(x, y, z, capacity, zone_type, transform_coord):
            msg["data"].append({"x": xi, "y": yi, "z": zi, "transformCoordinates": tc, "capacity": cap, "zoneType": ztype})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addZone", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def remove_zone(self, zoneID):
        """Dynamically remove one or more zones by internal zone ID."""
        msg = {"messageType": "removeZone", "data": _as_list(zoneID)}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "removeZone", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
