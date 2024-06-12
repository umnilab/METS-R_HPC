"""
Helper functions for co-simulation with CARLA
"""
import carla
from utils.carla_util import snap_to_ground
from clients.METSRClient import METSRClient

"""
Implementation of the CoSim Runner

A CoSim runner communicates with one METSRClient and one CARLA client to manage the 
data flow between corresponding simulation instances.
"""

# TODO: transform this into the simulator class in Scenic
# The carla control a submap, and the METS-R SIM control the rest maps
# The visualization is done in the carla simulator
class CoSimRunner(object):
      def __init__(self, config, carla_client, tm_client):
            self.config = config

            self.carla = carla_client.get_world()
            self.carla_client = carla_client
            self.carla_tm = tm_client
            self.set_carla_camera(self.carla, config)

            self.metsr = METSRClient(config.metsr_host, int(config.ports[0]), 0, self)
            self.metsr.start()

            self.carla_agent = [] # id of agent
            self.metsr_agent = []

            self.carla_veh = {} # id of agent and vehicle instance in carla

      def set_carla_camera(self, world, config):
            spectator = world.get_spectator()
            transform = carla.Transform()
            transform.location.x = config.camera_x
            transform.location.y = config.camera_y
            transform.location.z = config.camera_z
            transform.rotation.yaw = config.camera_yaw
            transform.rotation.pitch -= 30
            spectator.set_transform(transform)

      def get_carla_location(self, veh_inform):
            # given x, y, find the corresponding z values and rotation in CARLA
            x, y = veh_inform['x'], veh_inform['y']
            location = carla.Location(x, y, 0)
            location = snap_to_ground(self.carla, location)
            return location
      
      def get_carla_rotation(self, veh_inform):
            # given the heading, find the corresponding rotation in CARLA
            heading = veh_inform['bearing']
            if heading >= 0:
                  if heading >= 90:
                        heading = (heading - 90) % 360
                  else:
                        heading = (heading + 90) % 360
            else:
                  if heading <= -90:
                        heading = (- heading + 90) % 360
                  else:
                        heading = (- heading + 270) % 360
            rotation = carla.Rotation(yaw = heading)
            return rotation


      def is_in_carla_submap(self, x, y):
            # check if the vehicle is in the CARLA submap
            if x > self.config.min_x and x < self.config.max_x and y > self.config.min_y and y < self.config.max_y:
                  return True
            else:
                  return False
            
      # TODO: address the fail to spawn vehicle issue
      def step(self):
            self.carla.tick()
            self.metsr.tick()
            # copy METS-R information to CARLA
            to_remove = []
            for agent in self.metsr_agent:
                  veh_inform = self.metsr.query_vehicle(agent, True, True)
                  # veh_inform is not ""Query failed"
                  if veh_inform != "Query failed":
                        if veh_inform['on_road']:
                              # if in carla submap
                              if agent not in self.carla_veh: # not initializaed yet
                                    tmp_veh = self.carla.try_spawn_actor(self.carla.get_blueprint_library().find('vehicle.audi.tt'), carla.Transform(self.get_carla_location(veh_inform), self.get_carla_rotation(veh_inform)))
                                    if tmp_veh is not None:
                                          self.carla_veh[agent] = tmp_veh
                                    else:
                                          print(f"Failed to spawn vehicle {agent} in CARLA")
                              
                              else: # already initialized
                                    self.carla_veh[agent].set_transform(carla.Transform(self.get_carla_location(veh_inform), self.get_carla_rotation(veh_inform)))
                        else:
                              if agent in self.carla_veh:
                                    self.carla_veh[agent].destroy()
                                    del self.carla_veh[agent]
                                    to_remove.append(agent)

                              # if METS-R agent enter the CARLA submap (defined as a box), remove the agent from METS-R
                              # if self.is_in_carla_submap(veh_inform['x'], veh_inform['y']):
                              #       if agent in self.carla_agent:
                              #             self.carla_veh[agent].set_autopilot(True)
                              #       to_remove.append(agent)


            # if CARLA agent enter the METS-R map, remove the agent from CARLA and update its loc in METS-R
            # to_remove2 = []
            # for agent in self.carla_agent:
            #       veh_inform = self.carla_veh[agent].get_location()
            #       if not self.is_in_carla_submap(veh_inform.x, veh_inform.y):
            #             self.carla_veh[agent].set_autopilot(False)
            #             to_remove2.append(agent)

            # self.metsr_agent = [agent for agent in self.metsr_agent if agent not in to_remove]
            # self.carla_agent = [agent for agent in self.carla_agent if agent not in to_remove2]

            # self.metsr_agent += to_remove2
            # self.carla_agent += to_remove

      def run(self):
            # generate 100 vehicles in METS-R
            for vid in range(100):
                  success = self.metsr.generate_trip(vid)
                  while not success:
                        success = self.metsr.generate_trip(vid) # if the vehicle is not generated successfully, try again
                  self.metsr_agent.append(vid)

            for t in range(int(self.config.sim_minutes * 60 / self.config.sim_step_size)):
                  print(t)
                  self.step()





            
             
    

 