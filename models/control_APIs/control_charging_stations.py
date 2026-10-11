"""Control APIs for charging stations."""

from utils.primitives import _as_list


class ControlChargingStationsMixin:
    """Control APIs for charging stations."""

    # update charging station prices
    def update_charging_prices(self, stationID, stationType, price):
        """Update charging prices for the selected stations and charger levels."""
        msg = {"messageType": "updateChargingPrice", "data": []}
        if not isinstance(stationID, list):
            stationID = [stationID]
            stationType = [stationType]
            price = [price]
        if not isinstance(stationType, list):
            stationType = [stationType] * len(stationID)
        if not isinstance(price, list):
            price = [price] * len(stationID)
        for stationID, stationType, price in zip(stationID, stationType, price):
            msg["data"].append({"chargingStationId": stationID, "chargerLevel": stationType, "price": price})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "updateChargingPrice", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    # Dynamically add one or more charging stations at given coordinates.
    # num_l2/l3/bus: charger counts; price_l2/l3: per-unit prices;
    # z: elevation (default 0.0);
    # transform_coord: set True for projected/local SUMO coordinates.
    # Returns assigned (negative) station IDs.
    def add_charging_station(self, x, y, num_l2, num_l3, num_bus, price_l2, price_l3, z=0.0, transform_coord=False):
        """Create charging stations with the given locations, chargers, and prices."""
        msg = {"messageType": "addChargingStation", "data": []}
        if not isinstance(x, list):
            x = [x]
            y = [y]
            z = [z]
            num_l2 = [num_l2]
            num_l3 = [num_l3]
            num_bus = [num_bus]
            price_l2 = [price_l2]
            price_l3 = [price_l3]
        if not isinstance(z, list):
            z = [z] * len(x)
        if not isinstance(transform_coord, list):
            transform_coord = [transform_coord] * len(x)
        assert len(x) == len(y) == len(z) == len(num_l2) == len(num_l3) == len(num_bus) == len(price_l2) == len(price_l3), \
            "All positional arguments must have the same length"
        for xi, yi, zi, nl2, nl3, nbus, pl2, pl3, tc in zip(x, y, z, num_l2, num_l3, num_bus, price_l2, price_l3, transform_coord):
            msg["data"].append({"x": xi, "y": yi, "z": zi, "transformCoordinates": tc,
                                "level2ChargerCount": nl2, "level3ChargerCount": nl3,
                                "busChargerCount": nbus, "level2Price": pl2,
                                "level3Price": pl3})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "addChargingStation", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    def remove_charging_station(self, stationID):
        """Dynamically remove one or more charging stations by station ID."""
        msg = {"messageType": "removeChargingStation", "data": _as_list(stationID)}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "removeChargingStation", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res

    remove_chargingStation = remove_charging_station

    # Command vehicle(s) to interrupt current activity and go charge.
    # veh_type: True = private EV, False = public taxi.
    # charger_type: ChargingStation.L2 / L3 / BUS.
    # cs_id: 0 = auto-select (nearest for public taxis/buses, cheapest usable
    # station for private EVs); nonzero int = specific station ID.
    # After charging the vehicle returns to its pre-charging destination.
    def go_charging(self, vehID, veh_type, charger_type, cs_id=0):
        """Send vehicles to charge at a chosen or automatically selected station.

        ``veh_type`` identifies private EVs when true and public vehicles
        when false. A zero ``cs_id`` lets the simulator select the station.
        Vehicles resume their prior destinations after charging.
        """
        msg = {"messageType": "goCharging", "data": []}
        if not isinstance(vehID, list):
            vehID = [vehID]
        if not isinstance(veh_type, list):
            veh_type = [veh_type] * len(vehID)
        if not isinstance(charger_type, list):
            charger_type = [charger_type] * len(vehID)
        if not isinstance(cs_id, list):
            cs_id = [cs_id] * len(vehID)
        assert len(vehID) == len(veh_type) == len(charger_type) == len(cs_id), \
            "vehID, veh_type, charger_type, and cs_id must have the same length"
        for vid, vtype, ctype, csid in zip(vehID, veh_type, charger_type, cs_id):
            msg["data"].append({"vehicleId": vid, "isPrivate": vtype, "chargerLevel": ctype, "chargingStationId": csid})
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "goCharging", res["messageType"]
        assert res["status"] in {"ok", "partial"}, res["status"]
        return res
