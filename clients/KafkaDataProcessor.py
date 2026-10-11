# Kafka consumer for METS-R sensor data streams.

import hashlib
import json
import math
from collections.abc import Mapping

from utils.configuration import config_value


DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
DEFAULT_TOPICS = (
    "link_tt",
    "link_energy",
    "bsm",
    "v2x_tx_bsm",
    "v2x_rx_bsm",
    "v2x_link_metrics",
    "v2x_attack_events",
)

SENSOR_TYPE_NAMES = {
    0: "dsrc",
    1: "cv2x",
    2: "mobile_device",
}

BSM_LATITUDE_UNAVAILABLE = 900000001
BSM_LONGITUDE_UNAVAILABLE = 1800000001
BSM_ELEVATION_UNAVAILABLE = -4096
BSM_SPEED_UNAVAILABLE = 8191
BSM_HEADING_UNAVAILABLE = 28800
BSM_STEERING_ANGLE_UNAVAILABLE = 127
BSM_ACCELERATION_UNAVAILABLE = 2001
BSM_VERTICAL_ACCELERATION_UNAVAILABLE = -127

# Tesla Model Y approximate body dimensions, encoded as J2735 VehicleSize cm.
TESLA_MODEL_Y_FALLBACK_SIZE_CM = {
    "width": 192,
    "length": 475,
}

DEFAULT_BSM_POSITIONAL_ACCURACY = {
    "semiMajor": 255,
    "semiMinor": 255,
    "orientation": 65535,
}

DEFAULT_BSM_BRAKES = {
    "wheelBrakes": "unavailable",
    "traction": "unavailable",
    "abs": "unavailable",
    "scs": "unavailable",
    "brakeBoost": "unavailable",
    "auxBrakes": "unavailable",
}

TOPIC_ALIASES = {
    "bsm": "bsm",
    "basic_safety_message": "bsm",
    "v2x_bsm": "v2x_rx_bsm",
    "v2x_tx": "v2x_tx_bsm",
    "v2x_tx_bsm": "v2x_tx_bsm",
    "v2x_rx": "v2x_rx_bsm",
    "v2x_rx_bsm": "v2x_rx_bsm",
    "rx_bsm": "v2x_rx_bsm",
    "delivered_bsm": "v2x_rx_bsm",
    "v2x_link_metric": "v2x_link_metrics",
    "v2x_link_metrics": "v2x_link_metrics",
    "v2x_metrics": "v2x_link_metrics",
    "v2x_attack": "v2x_attack_events",
    "v2x_attack_event": "v2x_attack_events",
    "v2x_attack_events": "v2x_attack_events",
    "link_tt": "link_tt",
    "link_travel_time": "link_tt",
    "travel_time": "link_tt",
    "link_energy": "link_energy",
    "energy": "link_energy",
}


def _as_topics(topics):
    if topics is None:
        return list(DEFAULT_TOPICS)
    if isinstance(topics, str):
        return [topic.strip() for topic in topics.split(",") if topic.strip()]
    return list(topics)


def _canonical_topic(topic):
    if topic is None:
        return None
    return TOPIC_ALIASES.get(str(topic).strip().lower(), topic)


def _json_deserialize(raw):
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def _nested_get(data, *keys):
    current = data
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _setdefault_if_present(data, key, value):
    if value is not None and key not in data:
        data[key] = value


def _first_present(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _as_mapping(value):
    return value if isinstance(value, Mapping) else {}


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_int(value):
    number = _to_float(value)
    if number is None:
        return None
    return int(round(number))


def _clamp_int(value, minimum, maximum, unavailable=None):
    number = _to_int(value)
    if number is None:
        return unavailable
    return max(minimum, min(maximum, number))


def _temporary_id(value):
    if isinstance(value, bytes):
        return value[:4].hex().ljust(8, "0")
    if value is None:
        value = 0
    try:
        return f"{int(value) & 0xFFFFFFFF:08x}"
    except (TypeError, ValueError):
        text = str(value).strip()
        hex_text = text[2:] if text.lower().startswith("0x") else text
        if 0 < len(hex_text) <= 8 and all(char in "0123456789abcdefABCDEF" for char in hex_text):
            return hex_text.lower().zfill(8)
        return hashlib.blake2s(text.encode("utf-8"), digest_size=4).hexdigest()


def _latitude_to_e7(value):
    number = _to_float(value)
    if number is None:
        return BSM_LATITUDE_UNAVAILABLE
    if int(round(number)) == BSM_LATITUDE_UNAVAILABLE:
        return BSM_LATITUDE_UNAVAILABLE
    if -90.0 <= number <= 90.0:
        number *= 10_000_000
    return max(-900000000, min(900000000, int(round(number))))


def _longitude_to_e7(value):
    number = _to_float(value)
    if number is None:
        return BSM_LONGITUDE_UNAVAILABLE
    if int(round(number)) == BSM_LONGITUDE_UNAVAILABLE:
        return BSM_LONGITUDE_UNAVAILABLE
    if -180.0 <= number <= 180.0:
        number *= 10_000_000
    return max(-1799999999, min(1800000000, int(round(number))))


def _e7_to_degrees(value, unavailable):
    encoded = _to_int(value)
    if encoded is None or encoded == unavailable:
        return None
    return encoded / 10_000_000.0


def _elevation_to_dm(value, encoded=False):
    number = _to_float(value)
    if number is None:
        return BSM_ELEVATION_UNAVAILABLE
    if not encoded:
        number *= 10
    return max(-4095, min(61439, int(round(number))))


def _speed_mps_to_bsm(value):
    number = _to_float(value)
    if number is None:
        return BSM_SPEED_UNAVAILABLE
    return max(0, min(8190, int(round(number / 0.02))))


def bsm_core_speed_mps(record):
    core_data = get_bsm_core_data(record)
    speed = _to_int(core_data.get("speed"))
    if speed is None or speed == BSM_SPEED_UNAVAILABLE:
        return None
    return speed * 0.02


def _heading_deg_to_bsm(value):
    number = _to_float(value)
    if number is None:
        return BSM_HEADING_UNAVAILABLE
    encoded = int(round((number % 360.0) / 0.0125))
    return 0 if encoded >= BSM_HEADING_UNAVAILABLE else encoded


def bsm_core_heading_degrees(record):
    core_data = get_bsm_core_data(record)
    heading = _to_int(core_data.get("heading"))
    if heading is None or heading == BSM_HEADING_UNAVAILABLE:
        return None
    return heading * 0.0125


def bsm_core_latitude_degrees(record):
    return _e7_to_degrees(get_bsm_core_data(record).get("lat"), BSM_LATITUDE_UNAVAILABLE)


def bsm_core_longitude_degrees(record):
    return _e7_to_degrees(get_bsm_core_data(record).get("long"), BSM_LONGITUDE_UNAVAILABLE)


def _acceleration_to_bsm(value):
    number = _to_float(value)
    if number is None:
        return BSM_ACCELERATION_UNAVAILABLE
    return max(-2000, min(2000, int(round(number / 0.01))))


def _vertical_acceleration_to_bsm(value):
    number = _to_float(value)
    if number is None:
        return BSM_VERTICAL_ACCELERATION_UNAVAILABLE
    return max(-126, min(126, int(round(number / 0.1962))))


def _yaw_rate_to_bsm(value):
    number = _to_float(value)
    if number is None:
        return 0
    return max(-32767, min(32767, int(round(number / 0.01))))


def _size_centimeters(record, size, field, fallback):
    cm_keys = (f"vehicle_{field}_cm", f"{field}_cm")
    meter_keys = (f"vehicle_{field}_m", f"{field}_m")
    value = _first_present(size.get(field), *(record.get(key) for key in cm_keys))
    meters = False
    if value is None:
        value = _first_present(*(record.get(key) for key in meter_keys))
        meters = value is not None
    if value is None:
        value = record.get(field)
        number = _to_float(value)
        meters = number is not None and 0 < number < 50
    number = _to_float(value)
    if number is None:
        number = fallback
    elif meters:
        number *= 100
    maximum = 1023 if field == "width" else 4095
    return max(0, min(maximum, int(round(number))))


def _bsm_accel_set(record, core_data):
    accel_set = _as_mapping(_first_present(core_data.get("accelSet"), record.get("accelSet")))
    return {
        "long": _clamp_int(
            _first_present(
                accel_set.get("long"),
                accel_set.get("longitudinal"),
                record.get("acceleration_mps2"),
                record.get("acc"),
            )
            if "long" in accel_set or "longitudinal" in accel_set
            else _acceleration_to_bsm(_first_present(record.get("acceleration_mps2"), record.get("acc"))),
            -2000,
            2001,
            BSM_ACCELERATION_UNAVAILABLE,
        ),
        "lat": _clamp_int(
            _first_present(
                accel_set.get("lat"),
                accel_set.get("lateral"),
                _acceleration_to_bsm(record.get("lateral_acceleration_mps2")),
            ),
            -2000,
            2001,
            BSM_ACCELERATION_UNAVAILABLE,
        ),
        "vert": _clamp_int(
            _first_present(
                accel_set.get("vert"),
                accel_set.get("vertical"),
                _vertical_acceleration_to_bsm(record.get("vertical_acceleration_mps2")),
            ),
            -127,
            127,
            BSM_VERTICAL_ACCELERATION_UNAVAILABLE,
        ),
        "yaw": _clamp_int(
            _first_present(
                accel_set.get("yaw"),
                _yaw_rate_to_bsm(record.get("yaw_rate_deg_s")),
            ),
            -32767,
            32767,
            0,
        ),
    }


def _bsm_transmission(record, core_data):
    transmission = _first_present(core_data.get("transmission"), record.get("transmission"))
    if transmission is not None:
        return transmission
    speed = _to_float(
        _first_present(
            core_data.get("speed_mps"),
            record.get("speed_mps"),
            record.get("velocity"),
            record.get("speed"),
        )
    )
    if speed is None:
        encoded_speed = _to_int(core_data.get("speed"))
        if encoded_speed is not None and encoded_speed != BSM_SPEED_UNAVAILABLE:
            speed = encoded_speed * 0.02
    if speed is None:
        return "unavailable"
    return "forwardGears" if speed > 0.05 else "park"


def get_bsm_core_data(record):
    """Return BSM core fields from a flat or layered sensor record."""
    if not isinstance(record, Mapping):
        return {}
    messaging = _as_mapping(record.get("messaging_layer"))
    return _as_mapping(
        _first_present(
            record.get("coreData"),
            record.get("BSMcoreData"),
            record.get("core_data"),
            messaging.get("coreData"),
            messaging.get("BSMcoreData"),
            messaging.get("core_data"),
        )
    )


def _sec_mark(value):
    number = _to_float(value)
    if number is None:
        return 65535
    if number > 65535:
        number %= 60000
    return max(0, min(65535, int(round(number))))


def build_bsm_core_data(record):
    """Return a canonical SAE J2735 ``BSMCoreData`` semantic value.

    Integer units and unavailable sentinels follow J2735. An ASN.1 codec is
    still required before this becomes a standards-conformant wire payload.
    """
    if not isinstance(record, Mapping):
        raise TypeError("A BSM record must be a mapping.")
    core_data = get_bsm_core_data(record)
    message_count = _first_present(
        core_data.get("msgCnt"),
        core_data.get("msg_count"),
        record.get("msgCnt"),
        record.get("message_count"),
        record.get("msg_count"),
        record.get("tick"),
    )
    sec_mark = _first_present(
        core_data.get("secMark"),
        core_data.get("sec_mark"),
        record.get("secMark"),
        record.get("sec_mark"),
        record.get("timestamp_ms"),
        record.get("tick"),
    )
    lat = _first_present(
        core_data.get("lat"),
        core_data.get("latitude_e7"),
        record.get("latitude_e7"),
        record.get("lat_e7"),
        record.get("latitude"),
        record.get("lat"),
    )
    lon = _first_present(
        core_data.get("long"),
        core_data.get("longitude_e7"),
        record.get("longitude_e7"),
        record.get("lon_e7"),
        record.get("longitude"),
        record.get("lon"),
        record.get("long"),
    )
    elevation = _first_present(
        core_data.get("elev"),
        core_data.get("elevation_dm"),
        record.get("elev"),
        record.get("elevation_dm"),
    )
    speed = _first_present(
        core_data.get("speed"),
        record.get("speed_units"),
        record.get("bsm_speed"),
    )
    speed_mps = _first_present(
        core_data.get("speed_mps"),
        record.get("speed_mps"),
        record.get("velocity"),
        record.get("speed"),
    )
    heading = _first_present(
        core_data.get("heading"),
        record.get("heading_units"),
        record.get("bsm_heading"),
    )
    heading_deg = _first_present(
        core_data.get("heading_deg"),
        record.get("heading_deg"),
        record.get("heading"),
        record.get("bearing"),
    )
    accuracy = _as_mapping(_first_present(core_data.get("accuracy"), record.get("accuracy")))
    brakes = _as_mapping(_first_present(core_data.get("brakes"), record.get("brakes")))
    size = _as_mapping(_first_present(core_data.get("size"), record.get("size")))

    return {
        "msgCnt": _clamp_int(message_count, 0, 127, 0),
        "id": _temporary_id(
            _first_present(
                core_data.get("id"),
                core_data.get("temporary_id"),
                record.get("temporary_id"),
                record.get("vehicle_id"),
                record.get("vid"),
                record.get("sender_id"),
            )
        ),
        "secMark": _sec_mark(sec_mark),
        "lat": _latitude_to_e7(lat),
        "long": _longitude_to_e7(lon),
        "elev": _clamp_int(
            elevation,
            -4096,
            61439,
            _elevation_to_dm(_first_present(record.get("elevation"), record.get("z"))),
        ),
        "accuracy": {
            "semiMajor": _clamp_int(
                _first_present(accuracy.get("semiMajor"), accuracy.get("semi_major")),
                0, 255, DEFAULT_BSM_POSITIONAL_ACCURACY["semiMajor"],
            ),
            "semiMinor": _clamp_int(
                _first_present(accuracy.get("semiMinor"), accuracy.get("semi_minor")),
                0, 255, DEFAULT_BSM_POSITIONAL_ACCURACY["semiMinor"],
            ),
            "orientation": _clamp_int(
                accuracy.get("orientation"),
                0, 65535, DEFAULT_BSM_POSITIONAL_ACCURACY["orientation"],
            ),
        },
        "transmission": _bsm_transmission(record, core_data),
        "speed": _clamp_int(
            speed,
            0,
            BSM_SPEED_UNAVAILABLE,
            _speed_mps_to_bsm(speed_mps),
        ),
        "heading": _clamp_int(
            heading,
            0,
            BSM_HEADING_UNAVAILABLE,
            _heading_deg_to_bsm(heading_deg),
        ),
        "angle": _clamp_int(
            _first_present(core_data.get("angle"), record.get("steering_wheel_angle")),
            -126,
            127,
            BSM_STEERING_ANGLE_UNAVAILABLE,
        ),
        "accelSet": _bsm_accel_set(record, core_data),
        "brakes": {key: brakes.get(key, default) for key, default in DEFAULT_BSM_BRAKES.items()},
        "size": {
            field: _size_centimeters(record, size, field, TESLA_MODEL_Y_FALLBACK_SIZE_CM[field])
            for field in ("width", "length")
        },
    }


def normalize_sensor_record(value, topic=None):
    """Return a METS-R Kafka payload with stable aliases for old and new schemas."""
    if not isinstance(value, Mapping):
        return value

    record = dict(value)
    canonical_topic = _canonical_topic(topic or record.get("_topic") or record.get("topic"))
    if canonical_topic is not None:
        record.setdefault("_topic", canonical_topic)

    if canonical_topic in {"v2x_tx_bsm", "v2x_rx_bsm"}:
        _normalize_bsm(record, v2x=True)
    elif canonical_topic == "v2x_link_metrics" or (
        "sender_id" in record and "receiver_id" in record
        and any(key in record for key in (
            "latency_ms", "packet_error_rate", "delivery_probability", "rssi_dbm",
        ))
    ):
        _setdefault_if_present(record, "source_vehicle_id", record.get("sender_id"))
        _setdefault_if_present(record, "target_vehicle_id", record.get("receiver_id"))
        _setdefault_if_present(record, "vehicle_id", record.get("sender_id"))
        _setdefault_if_present(record, "vid", record.get("vehicle_id"))
        _setdefault_if_present(record, "message_name", record.get("message", "BasicSafetyMessage"))
        _setdefault_if_present(record, "message_standard", "SAE J2735-aligned")
        _setdefault_if_present(record, "radio_access", record.get("access_technology"))
        _setdefault_if_present(record, "packet_delivery_ratio", record.get("delivery_probability"))
        _setdefault_if_present(record, "packet_error_rate", record.get("per"))
        _setdefault_if_present(record, "latency_ms", record.get("delay_ms"))
        _setdefault_if_present(record, "rssi_dbm", record.get("rssi"))
        _setdefault_if_present(record, "sinr_db", record.get("sinr"))
        _setdefault_if_present(record, "channel_busy_ratio", record.get("cbr"))
    elif canonical_topic == "v2x_attack_events":
        _setdefault_if_present(record, "source_vehicle_id", record.get("sender_id"))
        _setdefault_if_present(record, "target_vehicle_id", record.get("receiver_id"))
        _setdefault_if_present(record, "vehicle_id", record.get("sender_id", record.get("attacker_id")))
        _setdefault_if_present(record, "vid", record.get("vehicle_id"))
        _setdefault_if_present(record, "attack_type", record.get("type"))
        _setdefault_if_present(record, "effect", record.get("result"))
        _setdefault_if_present(record, "message_name", record.get("message", "BasicSafetyMessage"))
    elif canonical_topic == "bsm" or (
        any(key in record for key in ("coreData", "BSMcoreData", "core_data"))
        or _nested_get(record, "messaging_layer", "coreData") is not None
        or _nested_get(record, "messaging_layer", "core_data") is not None
        or ("latitude" in record and "longitude" in record
            and ("velocity" in record or "heading" in record))
    ):
        _normalize_bsm(record)
    elif canonical_topic == "link_tt" or (
        "travel_time" in record
        or _nested_get(record, "messaging_layer", "probe_data", "travel_time_s") is not None
    ):
        _normalize_probe(record, travel_time=True)
    elif canonical_topic == "link_energy" or (
        "link_energy" in record
        or _nested_get(record, "messaging_layer", "probe_data", "link_energy") is not None
    ):
        _normalize_probe(record, travel_time=False)

    return record


def _normalize_probe(record, travel_time):
    probe = _nested_get(record, "messaging_layer", "probe_data") or {}
    quality = _nested_get(record, "messaging_layer", "quality") or {}
    communication = record.get("communication_layer") or {}
    top_quality = record.get("quality_layer") or {}
    vehicle_id = record.get("vehicle_id", probe.get("vehicle_id"))
    road_id = record.get("road_id", probe.get("road_id"))

    _setdefault_if_present(record, "vid", vehicle_id)
    _setdefault_if_present(record, "vehicle_id", record.get("vid"))
    _setdefault_if_present(record, "veh_type", probe.get("vehicle_type"))
    _setdefault_if_present(record, "vehicle_type", record.get("veh_type"))
    _setdefault_if_present(record, "road_id", road_id)
    _setdefault_if_present(record, "link_id", record.get("road_id"))
    _setdefault_if_present(record, "message_name", _nested_get(record, "messaging_layer", "message_name"))
    _setdefault_if_present(record, "message_standard", _nested_get(record, "messaging_layer", "standard"))
    _setdefault_if_present(record, "source_sensor", quality.get("source_sensor"))
    _setdefault_if_present(record, "radio_access", _nested_get(communication, "phy", "radio_access"))
    _setdefault_if_present(record, "service_channel", _nested_get(communication, "channel", "service_channel"))
    _setdefault_if_present(record, "delivery_probability", _nested_get(communication, "mac", "delivery_probability"))
    _setdefault_if_present(record, "packet_error_rate", _nested_get(communication, "mac", "packet_error_rate"))
    _setdefault_if_present(record, "confidence", top_quality.get("confidence"))
    _setdefault_if_present(record, "latency_ms", top_quality.get("estimated_end_to_end_latency_ms"))

    if travel_time:
        _setdefault_if_present(record, "travel_time", probe.get("travel_time_s"))
        _setdefault_if_present(record, "travel_time_s", record.get("travel_time"))
        _setdefault_if_present(record, "link_travel_time", record.get("travel_time"))
        _setdefault_if_present(record, "length", probe.get("length_m"))
        _setdefault_if_present(record, "length_m", record.get("length"))
        _setdefault_if_present(record, "average_speed", probe.get("average_speed_mps"))
        _setdefault_if_present(record, "average_speed_mps", record.get("average_speed"))
        _setdefault_if_present(record, "average_speed_mph", probe.get("average_speed_mph"))
        _setdefault_if_present(record, "delay_ratio", probe.get("delay_ratio_vs_30mph"))
        _setdefault_if_present(record, "delay_ratio_vs_30mph", record.get("delay_ratio"))
    else:
        _setdefault_if_present(record, "link_energy", probe.get("link_energy"))
        _setdefault_if_present(record, "energy_consumed", record.get("link_energy"))
    _setdefault_if_present(record, "timestamp", record.get("utc_time"))

def _normalize_bsm(record, v2x=False):
    if v2x:
        _setdefault_if_present(record, "sender_id", record.get("vehicle_id", record.get("vid")))
        _setdefault_if_present(record, "source_vehicle_id", record.get("sender_id"))
        _setdefault_if_present(record, "vehicle_id", record.get("sender_id"))
        _setdefault_if_present(record, "vid", record.get("vehicle_id"))
        _setdefault_if_present(record, "target_vehicle_id", record.get("receiver_id"))
        _setdefault_if_present(record, "message_name", "BasicSafetyMessage")
        _setdefault_if_present(record, "message_standard", "SAE J2735-aligned")
    messaging = record.get("messaging_layer") or {}
    communication = record.get("communication_layer") or {}
    quality = record.get("quality_layer") or {}
    core_data = build_bsm_core_data(record)
    record["coreData"] = core_data
    if isinstance(messaging, Mapping):
        messaging = dict(messaging)
        messaging.setdefault("message_name", "BasicSafetyMessage")
        messaging.setdefault("standard", "SAE J2735 BSMcoreData")
        messaging["coreData"] = core_data
        record["messaging_layer"] = messaging

    _setdefault_if_present(record, "vid", record.get("vehicle_id"))
    _setdefault_if_present(record, "vehicle_id", record.get("vid"))
    _setdefault_if_present(record, "speed", record.get("velocity"))
    _setdefault_if_present(record, "velocity", record.get("speed"))
    _setdefault_if_present(record, "timestamp", record.get("utc_time"))
    _setdefault_if_present(record, "sensor_type", record.get("type"))
    sensor_type = record.get("sensor_type")
    if isinstance(sensor_type, str):
        try:
            sensor_type = int(sensor_type)
        except ValueError:
            pass
    sensor_name = sensor_type if isinstance(sensor_type, str) else SENSOR_TYPE_NAMES.get(sensor_type)
    _setdefault_if_present(record, "sensor_type_name", sensor_name)

    _setdefault_if_present(record, "message_name", messaging.get("message_name"))
    _setdefault_if_present(record, "message_standard", messaging.get("standard"))
    _setdefault_if_present(record, "temporary_id", core_data.get("id"))
    _setdefault_if_present(record, "msgCnt", core_data.get("msgCnt"))
    _setdefault_if_present(record, "message_count", core_data.get("msgCnt"))
    _setdefault_if_present(record, "secMark", core_data.get("secMark"))
    _setdefault_if_present(record, "sec_mark", core_data.get("secMark"))
    _setdefault_if_present(record, "latitude_e7", core_data.get("lat"))
    _setdefault_if_present(record, "longitude_e7", core_data.get("long"))
    _setdefault_if_present(record, "elevation_dm", core_data.get("elev"))
    _setdefault_if_present(record, "latitude", bsm_core_latitude_degrees(record))
    _setdefault_if_present(record, "longitude", bsm_core_longitude_degrees(record))
    _setdefault_if_present(record, "speed_mps", bsm_core_speed_mps(record))
    _setdefault_if_present(record, "heading_deg", bsm_core_heading_degrees(record))
    _setdefault_if_present(record, "speed", record.get("speed_mps"))
    _setdefault_if_present(record, "heading", record.get("heading_deg"))
    _setdefault_if_present(record, "size", core_data.get("size"))

    _setdefault_if_present(record, "radio_access", _nested_get(communication, "phy", "radio_access"))
    _setdefault_if_present(record, "service_channel", _nested_get(communication, "channel", "service_channel"))
    _setdefault_if_present(record, "delivery_probability", _nested_get(communication, "mac", "delivery_probability"))
    _setdefault_if_present(record, "packet_error_rate", _nested_get(communication, "mac", "packet_error_rate"))
    _setdefault_if_present(record, "confidence", quality.get("confidence"))
    _setdefault_if_present(record, "latency_ms", quality.get("estimated_end_to_end_latency_ms"))


class KafkaDataProcessor:
    def __init__(self, config=None, topics=None, bootstrap_servers=None):
        self.config = config
        configured_topics = config_value(
            config,
            "kafka_topics",
            config_value(config, "kafka_topic", DEFAULT_TOPICS),
        )
        configured_bootstrap = config_value(
            config,
            "kafka_bootstrap_servers",
            config_value(config, "kafka_bootstrap_server", DEFAULT_BOOTSTRAP_SERVERS),
        )
        self.topics = _as_topics(
            topics
            if topics is not None
            else configured_topics
        )
        self.bootstrap_servers = (
            bootstrap_servers
            or configured_bootstrap
        )
        self.poll_timeout_ms = int(config_value(config, "kafka_poll_timeout_ms", 5))
        self.normalize = bool(config_value(config, "kafka_normalize_records", True))
        self.include_metadata = bool(config_value(config, "kafka_include_metadata", False))

        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable
        except ImportError as exc:
            raise ImportError(
                "KafkaDataProcessor requires the optional 'kafka-python' package. "
                "Install it with `pip install kafka-python` before consuming Kafka streams."
            ) from exc

        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=config_value(config, "kafka_auto_offset_reset", "earliest"),
                enable_auto_commit=bool(config_value(config, "kafka_enable_auto_commit", True)),
                group_id=config_value(config, "kafka_group_id", None),
                value_deserializer=_json_deserialize,
            )
        except NoBrokersAvailable as exc:
            raise ConnectionError(
                f"Kafka broker at {self.bootstrap_servers!r} is unavailable. "
                "Start it with `start_kafka(config)` from `utils.util`, or run "
                "`docker compose -f docker/docker-compose.yml up -d "
                "zookeeper kafka init-kafka`, then retry."
            ) from exc
        self.consumer.subscribe(self.topics)

    def process(
        self,
        timeout_ms=None,
        max_records=None,
        topics=None,
        normalize=None,
        include_metadata=None,
    ):
        messages = self.consumer.poll(
            timeout_ms=self.poll_timeout_ms if timeout_ms is None else timeout_ms,
            max_records=max_records,
        )
        if not messages:
            return None

        selected_topics = None
        if topics is not None:
            selected_topics = {_canonical_topic(topic) for topic in _as_topics(topics)}

        normalize = self.normalize if normalize is None else normalize
        include_metadata = self.include_metadata if include_metadata is None else include_metadata

        records = []
        for topic_partition, consumer_records in messages.items():
            topic = _canonical_topic(getattr(topic_partition, "topic", None))
            if selected_topics is not None and topic not in selected_topics:
                continue

            for record in consumer_records:
                value = record.value
                if normalize:
                    value = normalize_sensor_record(value, topic=record.topic)
                if include_metadata:
                    value = dict(value) if isinstance(value, Mapping) else {"value": value}
                    value.setdefault("_topic", _canonical_topic(record.topic))
                    value["_partition"] = record.partition
                    value["_offset"] = record.offset
                    value["_timestamp"] = record.timestamp
                    if record.key is not None:
                        value["_key"] = (
                            record.key.decode("utf-8") if isinstance(record.key, bytes) else record.key
                        )
                records.append(value)

        return records or None

    def process_bsm(self, **kwargs):
        return self.process(topics=("bsm",), **kwargs)

    def process_v2x_transmitted_bsm(self, **kwargs):
        return self.process(topics=("v2x_tx_bsm",), **kwargs)

    def process_v2x_received_bsm(self, **kwargs):
        return self.process(topics=("v2x_rx_bsm",), **kwargs)

    def process_v2x_link_metrics(self, **kwargs):
        return self.process(topics=("v2x_link_metrics",), **kwargs)

    def process_v2x_attack_events(self, **kwargs):
        return self.process(topics=("v2x_attack_events",), **kwargs)

    def process_link_travel_time(self, **kwargs):
        return self.process(topics=("link_tt",), **kwargs)

    def process_link_energy(self, **kwargs):
        return self.process(topics=("link_energy",), **kwargs)

    def clear(self, max_empty_polls=100, timeout_ms=None):
        count = 0
        while count < max_empty_polls:
            if self.process(
                timeout_ms=self.poll_timeout_ms if timeout_ms is None else timeout_ms,
                normalize=False,
                include_metadata=False,
            ) is not None:
                count = 0
            else:
                count += 1

    def close(self):
        self.consumer.close()

    normalize_record = staticmethod(normalize_sensor_record)
