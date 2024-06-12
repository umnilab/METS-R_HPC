# kafka consumer of linkEnergy daata and linkTravelTime data

from kafka import KafkaConsumer
import json

class MabDataProcessor:
      def __init__(self, config, manager):
            self.config = config
            self.manager = manager
      
            # Initialize the kafka consumer
            self.consumer = KafkaConsumer(bootstrap_servers="localhost:29092",
                                                auto_offset_reset='earliest',
                                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
      
            # Subscribe to the topic
            self.consumer.subscribe(["link_tt", "link_energy"])

      def process(self):
            messages = self.consumer.poll(timeout_ms=5)

            if not (messages is None or len(messages) == 0):
                  for key, records in messages.items():
                        if key.topic == "link_energy":
                              # go through each consumer record
                              for record in records:
                                    hour = int((record.value['utc_time'] * self.manager.args.SIMULATION_STEP_SIZE) // 3600)
                                    if record.value['veh_type'] == 1:
                                          self.manager.mab[hour].updateLinkUCB(record.value['road_id'], record.value['link_energy'])
                                    elif record.value['veh_type'] == 2 and self.config.eco_routing_bus:
                                          self.manager.mabBus[hour].updateLinkUCB(record.value['road_id'], record.value['link_energy'])
      def process_bus(self):
            messages = self.consumer.poll(timeout_ms=5)
            if not (messages is None or len(messages) == 0):
                  for key, records in messages.items():
                        if key.topic == "link_tt":
                              # go through each consumer record
                              for record in records:
                                    hour = int((record.value['utc_time'] * self.manager.args.SIMULATION_STEP_SIZE) // 3600)
                                    self.manager.mabBus[hour].updateShadowBus(record.value['road_id'], record.value['travel_time'], record.value['length'])