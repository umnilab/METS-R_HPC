"""Small value conversions and socket probes shared by client and setup code.

These functions have no dependency on launchers, clients, or visualization servers.
"""

import socket
import time
from contextlib import closing


def check_socket(host, port):
    """Probe a TCP endpoint, retaining the historical one-second polling delay."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        connected = sock.connect_ex((host, port)) == 0
    time.sleep(1)
    return connected


def str_list_mapper_gen(func):
    """Return a function that converts each item with the supplied callable."""
    def str_list_mapper(str_list):
        return [func(value) for value in str_list]
    return str_list_mapper


def _is_sequence(value):
    return isinstance(value, (list, tuple))


def _as_list(value):
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _broadcast(value, length):
    if length == 1:
        return [value]
    if _is_sequence(value) and len(value) == length:
        return list(value)
    return [value] * length


VEHICLE_SENSOR_DSRC = 0


VEHICLE_SENSOR_CV2X = 1


VEHICLE_SENSOR_MOBILE_DEVICE = 2


VEHICLE_SENSOR_TYPES = {
    "dsrc": VEHICLE_SENSOR_DSRC,
    "80211p": VEHICLE_SENSOR_DSRC,
    "cv2x": VEHICLE_SENSOR_CV2X,
    "c-v2x": VEHICLE_SENSOR_CV2X,
    "c_v2x": VEHICLE_SENSOR_CV2X,
    "mobile": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobiledevice": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobile_device": VEHICLE_SENSOR_MOBILE_DEVICE,
    "mobile-device": VEHICLE_SENSOR_MOBILE_DEVICE,
}


def _has_value(value):
    return value is not None and str(value).strip() != ""


def _request_id_from_record(record):
    if not isinstance(record, dict):
        return record
    return record.get("requestId")


def _request_zone_from_record(record):
    if not isinstance(record, dict):
        return None
    zone_id = record.get("zoneId")
    return record.get("originZoneId") if zone_id is None else zone_id


def _normalize_sensor_type(sensor_type):
    if isinstance(sensor_type, str):
        key = sensor_type.strip().lower()
        compact_key = key.replace(" ", "").replace("_", "").replace("-", "")
        if key in VEHICLE_SENSOR_TYPES:
            return VEHICLE_SENSOR_TYPES[key]
        if compact_key in VEHICLE_SENSOR_TYPES:
            return VEHICLE_SENSOR_TYPES[compact_key]
        raise ValueError(
            "Unknown sensorType. Use 0/'dsrc', 1/'cv2x', or 2/'mobile_device'."
        )
    return sensor_type


def _looks_like_centerline(value):
    if not _is_sequence(value) or len(value) == 0:
        return False
    first_point = value[0]
    if not _is_sequence(first_point) or len(first_point) < 2:
        return False
    return not _is_sequence(first_point[0])
