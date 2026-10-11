"""METS-R Vis binary manifests, frame records, and packet encoding.

Pure encoding functions accept simulator response records and return bytes.
Origin lookup reads optional Data.properties values; no client or server starts
when this module is imported.
"""

import os
import struct

from utils.configuration import _read_property_values
from utils.primitives import _is_sequence


_VIZ_STREAM_MAGIC = b"MRTB"


_VIZ_STREAM_VERSION = 8


_VIZ_STREAM_DEFAULT_COORD_SCALE = 100000


_VIZ_STREAM_FRAME_GROUPS = [
    "vehicle",
    "ev_private",
    "ev_occupied",
    "ev_relocation",
    "ev_charging",
    "ev_attack",
    "bus",
    "link",
    "zone",
    "chargingStation",
]


_VIZ_STREAM_VEHICLE_FRAME_GROUPS = [
    "vehicle",
    "ev_private",
    "ev_occupied",
    "ev_relocation",
    "ev_charging",
    "ev_attack",
    "bus",
]


_VIZ_STREAM_VEHICLE_TYPES = {
    "vehicle": 0,
    "ev_private": 1,
    "ev_occupied": 2,
    "ev_relocation": 3,
    "ev_charging": 4,
    "bus": 5,
    "ev_attack": 6,
}


_INT32_MIN = -(2 ** 31)


_INT32_MAX = 2 ** 31 - 1


def _viz_float(value, default=0.0):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    if number != number or number in (float("inf"), float("-inf")):
        return float(default)
    return number


def _viz_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _viz_pack_int(value, default=0):
    number = _viz_int(value, default=default)
    return struct.pack(">i", max(_INT32_MIN, min(_INT32_MAX, number)))


def _viz_pack_float(value, default=0.0):
    return struct.pack(">f", _viz_float(value, default=default))


def _viz_pack_double(value, default=0.0):
    return struct.pack(">d", _viz_float(value, default=default))


def _viz_first(record, *names, default=None):
    if not isinstance(record, dict):
        return default
    for name in names:
        value = record.get(name)
        if value is not None:
            return value
    return default


def _viz_stream_record(record):
    return (
        isinstance(record, dict)
        and record.get("x") is not None
        and record.get("y") is not None
    )


def _viz_scaled_coord(value, origin, coord_scale):
    scaled = round((_viz_float(value) - float(origin)) * int(coord_scale))
    return max(_INT32_MIN, min(_INT32_MAX, scaled))


def _viz_sum(records, *field_names):
    total = 0
    for record in records:
        total += _viz_int(_viz_first(record, *field_names, default=0))
    return total


def _viz_resolve_origin(sim_folder, initial_x=0.0, initial_y=0.0):
    initial_x = _viz_float(initial_x, default=0.0)
    initial_y = _viz_float(initial_y, default=0.0)
    if sim_folder and initial_x == 0.0 and initial_y == 0.0:
        properties = _read_property_values(os.path.join(sim_folder, "data", "Data.properties"))
        if "INITIAL_X" in properties or "INITIAL_Y" in properties:
            return (
                _viz_float(properties.get("INITIAL_X"), default=0.0),
                _viz_float(properties.get("INITIAL_Y"), default=0.0),
            )

    return initial_x, initial_y


def _viz_manifest(road_id_dictionary, coord_scale, initial_x, initial_y, tick_interval,
                  link_snapshot_interval, zone_dictionary=None,
                  charging_station_dictionary=None):
    return {
        "format": "metsr-trajectory-binary",
        "version": _VIZ_STREAM_VERSION,
        "byteOrder": "bigEndian",
        "chunkMagic": _VIZ_STREAM_MAGIC.decode("ascii"),
        "coordScale": int(coord_scale),
        "initialX": float(initial_x),
        "initialY": float(initial_y),
        "tickInterval": int(tick_interval),
        "linkSnapshotInterval": int(link_snapshot_interval),
        "chunkTickLimit": 1,
        "chunks": [],
        "activeChunk": None,
        "roadIdDictionary": [str(road_id) for road_id in (road_id_dictionary or [])],
        "zoneDictionary": list(zone_dictionary or []),
        "chargingStationDictionary": list(charging_station_dictionary or []),
        "busRouteDictionary": [],
        "vehicleTypes": dict(_VIZ_STREAM_VEHICLE_TYPES),
        "frameGroups": list(_VIZ_STREAM_FRAME_GROUPS),
        "sparseFrameGroups": ["zone", "chargingStation"],
        "sparseFrameGroupMode": "initialFullFrameThenChangedRecordsAndRemovedIds",
    }


def _viz_vehicle_id(record):
    return _viz_first(
        record, "vehicleId", default=-1,
    )


def _viz_group_vehicle_records(records):
    groups = {key: [] for key in _VIZ_STREAM_VEHICLE_FRAME_GROUPS}
    for record in records:
        if not _viz_stream_record(record):
            continue
        vehicle_class = _viz_int(_viz_first(record, "vehicleClass", default=-1), default=-1)
        state = _viz_int(_viz_first(record, "state", default=-1), default=-1)
        private_flag = _viz_first(record, "_viz_private_veh", default=None)
        attack_flag = _viz_first(record, "_viz_attack_vehicle", "attackEnabled", default=False)
        if vehicle_class == 0:
            group_key = "vehicle"
        elif vehicle_class == 2 or state == 3:
            group_key = "bus"
        elif vehicle_class == 1:
            if bool(attack_flag):
                group_key = "ev_attack"
            elif state == 4:
                group_key = "ev_charging"
            elif state == 1:
                group_key = "ev_occupied"
            else:
                group_key = "ev_relocation"
        elif vehicle_class == 3:
            group_key = "ev_charging" if state == 4 else "ev_private"
        elif private_flag is True:
            group_key = "ev_private"
        else:
            continue
        groups[group_key].append(record)
    return groups


def _viz_vehicle_record_bytes(group_key, record, coord_scale, initial_x, initial_y):
    """Encode each vehicle type in wire order, sharing its movement fields."""
    x = _viz_first(record, "x", default=0.0)
    y = _viz_first(record, "y", default=0.0)
    data = bytearray()
    data.extend(_viz_pack_int(_viz_vehicle_id(record), default=-1))
    if group_key == "bus":
        data.extend(_viz_pack_int(_viz_first(record, "routeId", default=-1), default=-1))

    data.extend(_viz_pack_int(_viz_scaled_coord(
        _viz_first(record, "prevX", default=x), initial_x, coord_scale,
    )))
    data.extend(_viz_pack_int(_viz_scaled_coord(
        _viz_first(record, "prevY", default=y), initial_y, coord_scale,
    )))
    data.extend(_viz_pack_int(_viz_scaled_coord(x, initial_x, coord_scale)))
    data.extend(_viz_pack_int(_viz_scaled_coord(y, initial_y, coord_scale)))
    data.extend(_viz_pack_float(_viz_first(record, "bearing", default=0.0)))
    data.extend(_viz_pack_float(_viz_first(record, "speed", default=0.0)))

    if group_key == "vehicle":
        for field, fallback, origin in (
            ("originX", x, initial_x), ("originY", y, initial_y),
            ("destX", x, initial_x), ("destY", y, initial_y),
        ):
            data.extend(_viz_pack_int(_viz_scaled_coord(
                _viz_first(record, field, default=fallback), origin, coord_scale,
            )))
        data.extend(_viz_pack_int(_viz_first(record, "vehicleClass", default=-1), default=-1))
        return data

    if group_key != "bus":
        data.extend(_viz_pack_int(_viz_first(record, "originZoneId", default=-1), default=-1))
        data.extend(_viz_pack_int(_viz_first(record, "destinationZoneId", default=-1), default=-1))
    data.extend(_viz_pack_float(_viz_first(record, "battery", default=0.0)))
    energy_field = "energyConsumed" if group_key == "bus" else "totalEnergyConsumed"
    data.extend(_viz_pack_float(_viz_first(record, energy_field, default=0.0)))

    if group_key == "ev_private":
        data.extend(_viz_pack_int(_viz_first(record, "tripNumber", default=0)))
    else:
        for field in (
            "matchedRequests", "matchedPassengers", "pickupRequests",
            "pickupPassengers", "dropoffRequests", "dropoffPassengers",
        ):
            data.extend(_viz_pack_int(_viz_first(record, field, default=0)))

    if group_key == "bus":
        stop_zones = _viz_first(record, "stopZoneIds", default=[])
        stop_zones = [zone for zone in stop_zones if zone is not None] if _is_sequence(stop_zones) else []
        data.extend(_viz_pack_int(len(stop_zones)))
        for stop_zone in stop_zones:
            data.extend(_viz_pack_int(stop_zone, default=-1))
    return data


def _viz_link_record_bytes(record, road_id_index):
    road_id = _viz_first(record, "segmentId", default=None)
    index = None if road_id is None else road_id_index.get(str(road_id))
    if index is None:
        index = _viz_first(record, "visualizationIndex", default=None)
    if index is None:
        return None

    data = bytearray()
    data.extend(_viz_pack_int(index))
    data.extend(_viz_pack_int(_viz_first(
        record, "vehicleCount",
    )))
    data.extend(_viz_pack_float(_viz_first(record, "speed")))
    data.extend(_viz_pack_int(_viz_first(record, "flow")))
    data.extend(_viz_pack_float(_viz_first(
        record, "energyConsumed",
    )))
    data.extend(_viz_pack_int(_viz_first(record, "parkingCapacity")))
    data.extend(_viz_pack_int(_viz_first(
        record, "parkedVehicleCount",
    )))
    return data


def _viz_zone_record_bytes(record, coord_scale, initial_x, initial_y):
    x = _viz_first(record, "x", default=None)
    y = _viz_first(record, "y", default=None)
    if x is None or y is None:
        return None

    data = bytearray()
    data.extend(_viz_pack_int(_viz_first(
        record, "zoneId", default=-1,
    ), default=-1))
    data.extend(_viz_pack_int(_viz_scaled_coord(x, initial_x, coord_scale)))
    data.extend(_viz_pack_int(_viz_scaled_coord(y, initial_y, coord_scale)))
    data.extend(_viz_pack_int(_viz_first(record, "zoneType")))
    data.extend(_viz_pack_int(_viz_first(record, "capacity")))
    data.extend(_viz_pack_int(_viz_first(record, "vehicleStock")))
    data.extend(_viz_pack_int(_viz_first(
        record, "taxiDemand",
    )))
    data.extend(_viz_pack_int(_viz_first(
        record, "busDemand",
    )))
    data.extend(_viz_pack_int(_viz_first(record, "generatedTaxi")))
    data.extend(_viz_pack_int(_viz_first(record, "generatedBus")))
    data.extend(_viz_pack_int(_viz_first(record, "generatedPrivateEV")))
    data.extend(_viz_pack_int(_viz_first(record, "generatedPrivateGV")))
    data.extend(_viz_pack_int(_viz_first(record, "arrivedPrivateEV")))
    data.extend(_viz_pack_int(_viz_first(record, "arrivedPrivateGV")))
    data.extend(_viz_pack_int(_viz_first(record, "pickupTaxiRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "pickupBusRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "dropoffTaxiRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "dropoffBusRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "leftTaxiRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "leftBusRequests")))
    data.extend(_viz_pack_int(_viz_first(record, "leftTaxiPassengers")))
    data.extend(_viz_pack_int(_viz_first(record, "leftBusPassengers")))
    data.extend(_viz_pack_int(_viz_first(record, "relocatedVehicles")))
    data.extend(_viz_pack_int(_viz_first(record, "futureSupply")))
    data.extend(_viz_pack_float(_viz_first(record, "futureDemand")))
    data.extend(_viz_pack_float(_viz_first(record, "vehicleSurplus")))
    data.extend(_viz_pack_float(_viz_first(record, "vehicleDeficiency")))
    data.extend(_viz_pack_int(_viz_first(record, "taxiDropoffWait")))
    data.extend(_viz_pack_int(_viz_first(record, "busDropoffWait")))
    data.extend(_viz_pack_int(_viz_first(record, "taxiLeftWait")))
    data.extend(_viz_pack_int(_viz_first(record, "busLeftWait")))
    data.extend(_viz_pack_int(_viz_first(record, "taxiParkingTime")))
    data.extend(_viz_pack_int(_viz_first(record, "taxiCruisingTime")))
    return data


def _viz_charging_station_record_bytes(record, coord_scale, initial_x, initial_y):
    x = _viz_first(record, "x", default=None)
    y = _viz_first(record, "y", default=None)
    if x is None or y is None:
        return None

    data = bytearray()
    data.extend(_viz_pack_int(_viz_first(
        record, "chargingStationId", default=-1,
    ), default=-1))
    data.extend(_viz_pack_int(_viz_scaled_coord(x, initial_x, coord_scale)))
    data.extend(_viz_pack_int(_viz_scaled_coord(y, initial_y, coord_scale)))
    data.extend(_viz_pack_int(_viz_first(record, "level2QueueCount")))
    data.extend(_viz_pack_int(_viz_first(record, "level3QueueCount")))
    data.extend(_viz_pack_int(_viz_first(record, "busQueueCount")))
    data.extend(_viz_pack_int(_viz_first(record, "chargingLevel2Count")))
    data.extend(_viz_pack_int(_viz_first(record, "chargingLevel3Count")))
    data.extend(_viz_pack_int(_viz_first(record, "chargingBusCount")))
    data.extend(_viz_pack_int(_viz_first(record, "availableLevel2ChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "availableLevel3ChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "availableBusChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "level2ChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "level3ChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "busChargerCount")))
    data.extend(_viz_pack_int(_viz_first(record, "chargedVehicleCount")))
    data.extend(_viz_pack_int(_viz_first(record, "chargedBusCount")))
    data.extend(_viz_pack_float(_viz_first(record, "level2Price")))
    data.extend(_viz_pack_float(_viz_first(record, "level3Price")))
    data.extend(_viz_pack_float(_viz_first(record, "level2WaitingTime")))
    data.extend(_viz_pack_float(_viz_first(record, "level3WaitingTime")))
    data.extend(_viz_pack_int(_viz_first(record, "active", default=1), default=1))
    return data


def _viz_sparse_group_bytes(records, encode_record, removed_ids=None):
    encoded_records = []
    for record in records or []:
        if not isinstance(record, dict):
            continue
        encoded = encode_record(record)
        if encoded is not None:
            encoded_records.append(encoded)

    data = bytearray()
    data.extend(_viz_pack_int(len(encoded_records)))
    for encoded in encoded_records:
        data.extend(encoded)

    valid_removed_ids = [item for item in (removed_ids or []) if item is not None]
    data.extend(_viz_pack_int(len(valid_removed_ids)))
    for removed_id in valid_removed_ids:
        data.extend(_viz_pack_int(removed_id))
    return data, len(encoded_records), len(valid_removed_ids)


def _viz_chunk(records, tick, coord_scale, initial_x, initial_y, tick_interval,
               link_snapshot_interval, link_records=None, road_id_index=None,
               zone_records=None, charging_station_records=None,
               removed_zone_ids=None, removed_charging_station_ids=None):
    vehicle_groups = _viz_group_vehicle_records(records)
    records = [
        record
        for group_key in _VIZ_STREAM_VEHICLE_FRAME_GROUPS
        for record in vehicle_groups.get(group_key, [])
    ]
    private_ev_energy = etaxi_energy = ebus_energy = 0.0
    for record in records:
        vehicle_class = _viz_int(_viz_first(record, "vehicleClass", default=-1), default=-1)
        energy = _viz_float(_viz_first(
            record,
            "totalEnergyConsumed" if vehicle_class in (1, 2, 3) else "energyConsumed",
            default=0.0,
        ))
        if vehicle_class == 3:
            private_ev_energy += energy
        elif vehicle_class == 1:
            etaxi_energy += energy
        elif vehicle_class == 2:
            ebus_energy += energy
    mean_speed = (
        sum(_viz_float(_viz_first(record, "speed", default=0.0)) for record in records) / len(records)
        if records else 0.0
    )

    data = bytearray()
    data.extend(_VIZ_STREAM_MAGIC)
    data.extend(_viz_pack_int(_VIZ_STREAM_VERSION))
    data.extend(_viz_pack_int(coord_scale))
    data.extend(_viz_pack_double(initial_x))
    data.extend(_viz_pack_double(initial_y))
    data.extend(_viz_pack_int(tick_interval))
    data.extend(_viz_pack_int(link_snapshot_interval))

    data.extend(_viz_pack_int(tick))
    data.extend(_viz_pack_int(_viz_sum(records, "matchedRequests")))
    data.extend(_viz_pack_int(_viz_sum(records, "matchedPassengers")))
    data.extend(_viz_pack_int(_viz_sum(records, "pickupRequests")))
    data.extend(_viz_pack_int(_viz_sum(records, "pickupPassengers")))
    data.extend(_viz_pack_int(_viz_sum(records, "dropoffRequests")))
    data.extend(_viz_pack_int(_viz_sum(records, "dropoffPassengers")))
    data.extend(_viz_pack_int(_viz_sum(records, "leftRequests")))
    data.extend(_viz_pack_int(_viz_sum(records, "leftPassengers")))
    data.extend(_viz_pack_float(private_ev_energy + etaxi_energy + ebus_energy))
    data.extend(_viz_pack_int(len(records)))
    data.extend(_viz_pack_float(mean_speed))
    data.extend(_viz_pack_float(private_ev_energy))
    data.extend(_viz_pack_float(etaxi_energy))
    data.extend(_viz_pack_float(ebus_energy))

    for group_key in _VIZ_STREAM_VEHICLE_FRAME_GROUPS:
        group_records = vehicle_groups.get(group_key, [])
        data.extend(_viz_pack_int(len(group_records)))
        for record in group_records:
            data.extend(_viz_vehicle_record_bytes(
                group_key,
                record,
                coord_scale,
                initial_x,
                initial_y,
            ))

    road_id_index = road_id_index or {}
    link_payloads = []
    for record in link_records or []:
        if not isinstance(record, dict):
            continue
        payload = _viz_link_record_bytes(record, road_id_index)
        if payload is not None:
            link_payloads.append(payload)
    data.extend(_viz_pack_int(len(link_payloads)))
    for payload in link_payloads:
        data.extend(payload)

    zone_data, zone_count, _ = _viz_sparse_group_bytes(
        zone_records,
        lambda record: _viz_zone_record_bytes(record, coord_scale, initial_x, initial_y),
        removed_zone_ids,
    )
    charging_station_data, charging_station_count, _ = _viz_sparse_group_bytes(
        charging_station_records,
        lambda record: _viz_charging_station_record_bytes(record, coord_scale, initial_x, initial_y),
        removed_charging_station_ids,
    )
    data.extend(zone_data)
    data.extend(charging_station_data)
    return bytes(data), len(records), len(link_payloads), zone_count, charging_station_count
