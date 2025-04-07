# Kafka producer for linkEnergy, linkTravelTime, and bsm data

from kafka import KafkaProducer
import json
import time

class KafkaDataSender:
    def __init__(self, config):
        self.config = config
        
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, topic, data):
        if topic not in ["link_tt", "link_energy", "bsm"]:
            raise ValueError("Invalid topic. Choose from 'link_tt', 'link_energy', 'bsm'.")
        self.producer.send(topic, data)
        self.producer.flush()

if __name__ == "__main__":
    sender = KafkaDataSender()

    # Example data
    link_tt_data = {"link_id": "L123", "travel_time": 45.5}
    link_energy_data = {"link_id": "L123", "energy_consumed": 3.2}
    bsm_data = {"vehicle_id": "V456", "speed": 65.0, "heading": 120}

    # Send to each topic
    sender.send("link_tt", link_tt_data)
    sender.send("link_energy", link_energy_data)
    sender.send("bsm", bsm_data)

    print("Sample messages sent.")