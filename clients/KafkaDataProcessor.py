# Kafka consumer for METS-R sensor data streams.

import hashlib
import json
import math
from collections.abc import Mapping


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


def _config_get(config, name, default=None):
    if config is None:
        return default
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


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
    if -90.0 <= number <= 90.0:
        number *= 10_000_000
    return max(-900000000, min(900000000, int(round(number))))


def _longitude_to_e7(value):
    number = _to_float(value)
    if number is None:
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


def _bsm_size(record, core_data):
    size = _as_mapping(_first_present(core_data.get("size"), record.get("size")))
    return {
        "width": _size_centimeters(
            record,
            size,
            "width",
            TESLA_MODEL_Y_FALLBACK_SIZE_CM["width"],
        ),
        "length": _size_centimeters(
            record,
            size,
            "length",
            TESLA_MODEL_Y_FALLBACK_SIZE_CM["length"],
        ),
    }


def _bsm_accuracy(record, core_data):
    accuracy = _as_mapping(_first_present(core_data.get("accuracy"), record.get("accuracy")))
    return {
        "semiMajor": _clamp_int(
            _first_present(accuracy.get("semiMajor"), accuracy.get("semi_major")),
            0,
            255,
            DEFAULT_BSM_POSITIONAL_ACCURACY["semiMajor"],
        ),
        "semiMinor": _clamp_int(
            _first_present(accuracy.get("semiMinor"), accuracy.get("semi_minor")),
            0,
            255,
            DEFAULT_BSM_POSITIONAL_ACCURACY["semiMinor"],
        ),
        "orientation": _clamp_int(
            accuracy.get("orientation"),
            0,
            65535,
            DEFAULT_BSM_POSITIONAL_ACCURACY["orientation"],
        ),
    }


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


def _bsm_brakes(record, core_data):
    brakes = _as_mapping(_first_present(core_data.get("brakes"), record.get("brakes")))
    return {
        key: brakes.get(key, DEFAULT_BSM_BRAKES[key])
        for key in DEFAULT_BSM_BRAKES
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


def _bsm_core_source(record):
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


def get_bsm_core_data(record):
    if not isinstance(record, Mapping):
        return {}
    return _bsm_core_source(record)


def _sec_mark(value):
    number = _to_float(value)
    if number is None:
        return 65535
    if number > 65535:
        number %= 60000
    return max(0, min(65535, int(round(number))))


def _build_bsm_core_data(record):
    core_data = _bsm_core_source(record)
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
        "accuracy": _bsm_accuracy(record, core_data),
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
        "brakes": _bsm_brakes(record, core_data),
        "size": _bsm_size(record, core_data),
    }


def _sensor_type_name(sensor_type):
    if isinstance(sensor_type, str):
        try:
            sensor_type = int(sensor_type)
        except ValueError:
            return sensor_type
    return SENSOR_TYPE_NAMES.get(sensor_type)


def _looks_like_bsm(data):
    return (
        "coreData" in data
        or "BSMcoreData" in data
        or "core_data" in data
        or _nested_get(data, "messaging_layer", "coreData") is not None
        or _nested_get(data, "messaging_layer", "core_data") is not None
        or ("latitude" in data and "longitude" in data and (
        "velocity" in data or "heading" in data
        ))
    )


def _looks_like_link_tt(data):
    return "travel_time" in data or _nested_get(
        data, "messaging_layer", "probe_data", "travel_time_s"
    ) is not None


def _looks_like_link_energy(data):
    return "link_energy" in data or _nested_get(
        data, "messaging_layer", "probe_data", "link_energy"
    ) is not None


def _looks_like_v2x_link_metrics(data):
    return (
        "sender_id" in data
        and "receiver_id" in data
        and (
            "latency_ms" in data
            or "packet_error_rate" in data
            or "delivery_probability" in data
            or "rssi_dbm" in data
        )
    )


def normalize_sensor_record(value, topic=None):
    """Return a METS-R Kafka payload with stable aliases for old and new schemas."""
    if not isinstance(value, Mapping):
        return value

    record = dict(value)
    canonical_topic = _canonical_topic(topic or record.get("_topic") or record.get("topic"))
    if canonical_topic is not None:
        record.setdefault("_topic", canonical_topic)

    if canonical_topic in {"v2x_tx_bsm", "v2x_rx_bsm"}:
        _normalize_v2x_bsm(record)
    elif canonical_topic == "v2x_link_metrics" or _looks_like_v2x_link_metrics(record):
        _normalize_v2x_link_metrics(record)
    elif canonical_topic == "v2x_attack_events":
        _normalize_v2x_attack_event(record)
    elif canonical_topic == "bsm" or _looks_like_bsm(record):
        _normalize_bsm(record)
    elif canonical_topic == "link_tt" or _looks_like_link_tt(record):
        _normalize_link_tt(record)
    elif canonical_topic == "link_energy" or _looks_like_link_energy(record):
        _normalize_link_energy(record)

    return record


def _normalize_common_probe_fields(record):
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


def _normalize_link_tt(record):
    probe = _nested_get(record, "messaging_layer", "probe_data") or {}
    _normalize_common_probe_fields(record)

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
    _setdefault_if_present(record, "timestamp", record.get("utc_time"))


def _normalize_link_energy(record):
    probe = _nested_get(record, "messaging_layer", "probe_data") or {}
    _normalize_common_probe_fields(record)

    _setdefault_if_present(record, "link_energy", probe.get("link_energy"))
    _setdefault_if_present(record, "energy_consumed", record.get("link_energy"))
    _setdefault_if_present(record, "timestamp", record.get("utc_time"))


def _normalize_bsm(record):
    messaging = record.get("messaging_layer") or {}
    communication = record.get("communication_layer") or {}
    quality = record.get("quality_layer") or {}
    core_data = _build_bsm_core_data(record)
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
    _setdefault_if_present(record, "sensor_type_name", _sensor_type_name(record.get("sensor_type")))

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


def _normalize_v2x_bsm(record):
    _setdefault_if_present(record, "sender_id", record.get("vehicle_id", record.get("vid")))
    _setdefault_if_present(record, "source_vehicle_id", record.get("sender_id"))
    _setdefault_if_present(record, "vehicle_id", record.get("sender_id"))
    _setdefault_if_present(record, "vid", record.get("vehicle_id"))
    _setdefault_if_present(record, "target_vehicle_id", record.get("receiver_id"))
    _setdefault_if_present(record, "message_name", "BasicSafetyMessage")
    _setdefault_if_present(record, "message_standard", "SAE J2735-aligned")
    _normalize_bsm(record)
    _setdefault_if_present(record, "speed", record.get("speed_mps"))
    _setdefault_if_present(record, "heading", record.get("heading_deg"))


def _normalize_v2x_link_metrics(record):
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


def _normalize_v2x_attack_event(record):
    _setdefault_if_present(record, "source_vehicle_id", record.get("sender_id"))
    _setdefault_if_present(record, "target_vehicle_id", record.get("receiver_id"))
    _setdefault_if_present(record, "vehicle_id", record.get("sender_id", record.get("attacker_id")))
    _setdefault_if_present(record, "vid", record.get("vehicle_id"))
    _setdefault_if_present(record, "attack_type", record.get("type"))
    _setdefault_if_present(record, "effect", record.get("result"))
    _setdefault_if_present(record, "message_name", record.get("message", "BasicSafetyMessage"))


class KafkaDataProcessor:
    def __init__(self, config=None, topics=None, bootstrap_servers=None):
        self.config = config
        configured_topics = _config_get(
            config,
            "kafka_topics",
            _config_get(config, "kafka_topic", DEFAULT_TOPICS),
        )
        configured_bootstrap = _config_get(
            config,
            "kafka_bootstrap_servers",
            _config_get(config, "kafka_bootstrap_server", DEFAULT_BOOTSTRAP_SERVERS),
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
        self.poll_timeout_ms = int(_config_get(config, "kafka_poll_timeout_ms", 5))
        self.normalize = bool(_config_get(config, "kafka_normalize_records", True))
        self.include_metadata = bool(_config_get(config, "kafka_include_metadata", False))

        try:
            from kafka import KafkaConsumer
        except ImportError as exc:
            raise ImportError(
                "KafkaDataProcessor requires the optional 'kafka-python' package. "
                "Install it with `pip install kafka-python` before consuming Kafka streams."
            ) from exc

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=_config_get(config, "kafka_auto_offset_reset", "earliest"),
            enable_auto_commit=bool(_config_get(config, "kafka_enable_auto_commit", True)),
            group_id=_config_get(config, "kafka_group_id", None),
            value_deserializer=_json_deserialize,
        )
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
                    value = self._with_kafka_metadata(value, record)
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

    @staticmethod
    def normalize_record(value, topic=None):
        return normalize_sensor_record(value, topic=topic)

    @staticmethod
    def _with_kafka_metadata(value, record):
        if not isinstance(value, Mapping):
            value = {"value": value}
        else:
            value = dict(value)

        value.setdefault("_topic", _canonical_topic(record.topic))
        value["_partition"] = record.partition
        value["_offset"] = record.offset
        value["_timestamp"] = record.timestamp
        if record.key is not None:
            value["_key"] = record.key.decode("utf-8") if isinstance(record.key, bytes) else record.key
        return value
