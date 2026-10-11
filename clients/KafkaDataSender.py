# Kafka producer for METS-R sensor and V2X sidecar data.

from kafka import KafkaProducer
import json
from utils.configuration import config_value


DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
ALLOWED_TOPICS = {
    "link_tt",
    "link_energy",
    "bsm",
    "v2x_tx_bsm",
    "v2x_rx_bsm",
    "v2x_link_metrics",
    "v2x_attack_events",
}


class KafkaDataSender:
    def __init__(self, config=None, bootstrap_servers=None):
        self.config = config
        self.bootstrap_servers = (
            bootstrap_servers
            or config_value(
                config,
                "kafka_bootstrap_servers",
                config_value(config, "kafka_bootstrap_server", DEFAULT_BOOTSTRAP_SERVERS),
            )
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, topic, data, flush=True):
        if topic not in ALLOWED_TOPICS:
            raise ValueError(
                "Invalid topic. Choose from: " + ", ".join(sorted(ALLOWED_TOPICS))
            )
        records = data if isinstance(data, list) else [data]
        for record in records:
            self.producer.send(topic, record)
        if flush:
            self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    sender = KafkaDataSender()

    # Example data
    link_tt_data = {"link_id": "L123", "travel_time": 45.5}
    link_energy_data = {"link_id": "L123", "energy_consumed": 3.2}
    bsm_data = {"vehicle_id": "V456", "speed": 65.0, "heading": 120}
    v2x_rx_bsm_data = {
        "sender_id": "V456",
        "receiver_id": "V789",
        "message_name": "BasicSafetyMessage",
        "latency_ms": 35.0,
    }

    # Send to each topic
    sender.send("link_tt", link_tt_data)
    sender.send("link_energy", link_energy_data)
    sender.send("bsm", bsm_data)
    sender.send("v2x_rx_bsm", v2x_rx_bsm_data)

    print("Sample messages sent.")
