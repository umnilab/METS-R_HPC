# kafka consumer of linkEnergy daata and linkTravelTime data

from kafka import KafkaConsumer
import json

class KafkaDataProcessor:
      def __init__(self, config):
            self.config = config
      
            # Initialize the kafka consumer
            self.consumer = KafkaConsumer(bootstrap_servers="localhost:29092",
                                                auto_offset_reset='earliest',
                                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
      
            # Subscribe to the topic
            self.consumer.subscribe(["link_tt", "link_energy", "bsm"])

      def process(self):
            messages = self.consumer.poll(timeout_ms=5)

            if not (messages is None or len(messages) == 0):
                  res = []
                  for key, records in messages.items():
                        # go through each consumer record
                        for record in records:
                              res.append(record.value)
                  return res