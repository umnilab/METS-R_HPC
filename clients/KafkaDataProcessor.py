# Kafka consumer for METS-R sensor data streams.

import json
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


def _sensor_type_name(sensor_type):
    if isinstance(sensor_type, str):
        try:
            sensor_type = int(sensor_type)
        except ValueError:
            return sensor_type
    return SENSOR_TYPE_NAMES.get(sensor_type)


def _looks_like_bsm(data):
    return "latitude" in data and "longitude" in data and (
        "velocity" in data or "heading" in data
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
    core_data = messaging.get("core_data") or {}

    _setdefault_if_present(record, "vid", record.get("vehicle_id"))
    _setdefault_if_present(record, "vehicle_id", record.get("vid"))
    _setdefault_if_present(record, "speed", record.get("velocity"))
    _setdefault_if_present(record, "velocity", record.get("speed"))
    _setdefault_if_present(record, "timestamp", record.get("utc_time"))
    _setdefault_if_present(record, "sensor_type", record.get("type"))
    _setdefault_if_present(record, "sensor_type_name", _sensor_type_name(record.get("sensor_type")))

    _setdefault_if_present(record, "message_name", messaging.get("message_name"))
    _setdefault_if_present(record, "message_standard", messaging.get("standard"))
    _setdefault_if_present(record, "temporary_id", core_data.get("temporary_id"))
    _setdefault_if_present(record, "message_count", core_data.get("msg_count"))
    _setdefault_if_present(record, "sec_mark", core_data.get("sec_mark"))
    _setdefault_if_present(record, "latitude_e7", core_data.get("latitude_e7"))
    _setdefault_if_present(record, "longitude_e7", core_data.get("longitude_e7"))
    _setdefault_if_present(record, "speed_mps", core_data.get("speed_mps"))
    _setdefault_if_present(record, "heading_deg", core_data.get("heading_deg"))

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
