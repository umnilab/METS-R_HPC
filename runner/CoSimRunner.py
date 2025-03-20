"""
Helper functions for co-simulation with CARLA
"""
import numpy as np
import carla
from utils.carla_util import snap_to_ground
from clients.METSRClient import METSRClient

"""
Implementation of the CoSim Runner

A CoSim runner communicates with one METSRClient and one CARLA client to manage the 
data flow between corresponding simulation instances.
"""

# Co-simulation 1: The carla control a submap, and the METS-R SIM control the rest maps
# The visualization of the submap is done in the CARLA simulator
# The visualization of the rest maps is done in the METS-R SIM
class CoSimRunner(object):
      def __init__(self, config, docker_ids, carla_client, tm_client):
            self.config = config

            self.carla = carla_client.get_world()
            self.carla_client = carla_client
            self.carla_tm = tm_client
            # self.set_carla_camera(self.carla, config)
            self.set_overlook_camera(self.carla)

            self.metsr = METSRClient(config.metsr_host, int(config.ports[0]), 0, self, verbose = config.verbose) #docker_ids[0]

            # set the co-sim region
            for road in self.config.cosim_road:
                  self.metsr.set_cosim_road(road)

            self.carla_vehs = {} # id of agent and vehicle instance in carla
            self.carla_veh_lanes = {} # lane information of the vehicle in carla
            self.carla_waiting_vehs = [] # vehicles waiting to enter the other road, should be visited in every 10 ticks

      def set_carla_camera(self, world, config):
            spectator = world.get_spectator()
            transform = carla.Transform()
            transform.location.x = config.camera_x
            transform.location.y = config.camera_y
            transform.location.z = config.camera_z
            transform.rotation.yaw = config.camera_yaw
            transform.rotation.pitch -= 50
            spectator.set_transform(transform)

      def set_overlook_camera(self, world): # set the camera to overlook the whole map
            spectator = world.get_spectator()
            transform = carla.Transform()
            transform.location.x = 0
            transform.location.y = 0
            transform.location.z = 300
            transform.rotation.yaw = -90
            transform.rotation.pitch = -90
            spectator.set_transform(transform)

      def get_carla_location(self, veh_inform):
            # given x, y, find the corresponding z values and rotation in CARLA
            x, y = veh_inform['x'], -veh_inform['y']
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
            return rotation, heading


      def is_in_carla_submap(self, x, y):
            # check if the vehicle is in the CARLA submap
            if x > self.config.min_x and x < self.config.max_x and y > self.config.min_y and y < self.config.max_y:
                  return True
            else:
                  return False
            
      def step(self):
            self.carla.tick()
            self.metsr.tick()

            cosim_agents = self.metsr.query_coSimVehicle()
                  
            metsr_agents = cosim_agents['vid_list']
            metsr_agent_types = cosim_agents['vtype_list']

            # if the agent is in the CARLA sim but not in metsr_agents, remove it from CARLA since this means the agent has reached its destination
            to_remove = []
            for vid in self.carla_vehs.keys():
                  if vid not in metsr_agents:
                        to_remove.append(vid)

            for vid in to_remove:
                  self.carla_vehs[vid].set_autopilot(False)
                  while not self.carla_vehs[vid].destroy():
                        pass
                  self.carla_vehs.pop(vid)
                  self.carla_veh_lanes.pop(vid)
                  if vid in self.carla_waiting_vehs:
                        self.carla_waiting_vehs.remove(vid)

            for (vid, vtype) in zip(metsr_agents, metsr_agent_types): # go through all private vehicle agents in the co-sim region
                  # if the agent is in the CARLA co-sim, let it move in CARLA and update its location in METS-R
                  # if the agent is not in the CARLA co-sim, create it
                  # if CARLA agent enter the METS-R map, remove the agent from CARLA and update its loc in METS-R
                  if (vid not in self.carla_waiting_vehs) or (self.metsr.current_tick % 10 == 0):
                        veh_inform = self.metsr.query_vehicle(vid, private_veh = vtype, transform_coords = True)['DATA'][0] # the return value of query_vehicle is a list of vehicle information
                        # print(veh_inform)
                        self.sync_carla_vehicle(vid, vtype, veh_inform)

            # TODO: synchronize the traffic light status in CARLA using METS-R, in this example, we let all veh ignore CARLA's signal

      def run(self):
            try:
                  for t in range(int(self.config.sim_minutes * 60 / self.config.sim_step_size)):
                        print("Tick:", t)
                        if t == 1500:
                              self.set_carla_camera(self.carla, self.config)
                        if t % 600 == 0:
                              # generate 100 random trips every 1 minute
                              self.generate_random_trips(100, start_vid = int(t // 6))
                              print(f"Generated 100 random trips at time {t * self.config.sim_step_size // 60} minute!")
                        self.step()
            except KeyboardInterrupt:
                  print("simulation interrupted by user")

            finally:
                  self.metsr.terminate()
                  # for container_id in container_ids:
                  #       import os
                  #       os.system(f"docker container stop {container_id}")

      def get_distance(self, x1, y1, x2, y2):
            return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
      
      def sync_carla_vehicle(self, vid, private_veh, veh_inform):
            # if in carla agents
            if vid not in self.carla_vehs: # not initialized yet
                  tmp_rotation, tmp_heading = self.get_carla_rotation(veh_inform)
                  tmp_veh = self.carla.try_spawn_actor(self.carla.get_blueprint_library().find('vehicle.audi.tt'), carla.Transform(self.get_carla_location(veh_inform), tmp_rotation))
                  if tmp_veh is not None:
                        self.carla_vehs[vid] = tmp_veh
                        tmp_veh.set_autopilot(True)

                        self.carla_tm.ignore_lights_percentage(tmp_veh, 100)

                        # set the initial speed to be the same as the METS-R
                        tmp_speed = veh_inform['speed']
                        tmp_speed_x = tmp_speed * np.cos(tmp_heading * np.pi / 180)
                        tmp_speed_y = tmp_speed * np.sin(tmp_heading * np.pi / 180)
                        tmp_veh.set_target_velocity(carla.Vector3D(x = tmp_speed_x, y = tmp_speed_y, z = 0))

                        # get lane info
                        tmp_lane = self.carla.get_map().get_waypoint(tmp_veh.get_location(), project_to_road=True, lane_type=(carla.LaneType.Driving)).lane_id
                        self.carla_veh_lanes[vid] = tmp_lane
                  else:
                        print(f"Failed to spawn vehicle {vid} in CARLA, will try in the next step.")
                  
            
            else: 
                  if "lane" in veh_inform:
                        # already initialized, update its location in METS-R
                        # get the veh
                        carla_veh = self.carla_vehs[vid]

                        # check if carla_veh is still in carla
                        # try:
                        #       carla_veh.get_location()
                        # except:
                        #       print(f"Vehicle {vid} has been removed from CARLA ...")
                        #       self.carla_vehs.pop(vid)
                        #       return

                        # print vehicle info for debugging
                        # print(f"On lane vehicle {vid} in CARLA: {carla_veh.get_location().x}, {carla_veh.get_location().y}; in METS-R: {veh_inform['x']}, {veh_inform['y']}; distance: {veh_inform['dist']}")
                        # case 1: veh still on the co-sim road
                        if self.is_in_carla_submap(carla_veh.get_location().x, carla_veh.get_location().y):
                              # update the location in METS-R
                              # vehID, roadID, laneID, dist, x, y, prv = False):
                              self.metsr.teleport_cosim_vehicle(vid, veh_inform['road'], carla_veh.get_location().x, \
                                                                  -carla_veh.get_location().y, private_veh, transform_coords = True)
                        
                        else:
                              # case 2: veh enter the other road
                              print("Vehicle enters the other road, vid = ", vid)
                              success = self.metsr.enter_next_road(vid, private_veh)['DATA'][0]
                              if success == 'OK':
                                    # signal the veh to enter the next road
                                    # if success, remove the veh from CARLA
                                    carla_veh.set_autopilot(False)
                                    while not carla_veh.destroy():
                                          continue
                                    self.carla_vehs.pop(vid)
                                    self.carla_veh_lanes.pop(vid)
                                    if vid in self.carla_waiting_vehs:
                                          self.carla_waiting_vehs.remove(vid)
                              else:
                                    # if failed, keep the veh in CARLA but set the vehicle to be static
                                    carla_veh.set_autopilot(False)
                                    carla_veh.set_target_velocity(carla.Vector3D(x=0, y=0, z=0))
                                    carla_veh.apply_control(carla.VehicleControl(throttle = 0, brake = 1))
                                    carla_veh.enable_constant_velocity(carla.Vector3D(x=0, y=0, z=0))
                                    if vid not in self.carla_waiting_vehs:
                                          self.carla_waiting_vehs.append(vid)
                  else: 
                        carla_veh = self.carla_vehs[vid]
                        # print(f"Off lane vehicle {vid} in CARLA: {carla_veh.get_location().x}, {carla_veh.get_location().y}; in METS-R: {veh_inform['x']}, {veh_inform['y']}")
                        
                        print("Vehicle enters the other road, vid = ", vid)
                        # veh at the intersection and waiting to enter the next road
                        success = self.metsr.enter_next_road(vid, private_veh)['DATA'][0]
                        if success == 'OK':
                              # signal the veh to enter the next road
                              # if success, remove the veh from CARLA
                              carla_veh.set_autopilot(False)
                              while not carla_veh.destroy():
                                    continue
                              self.carla_vehs.pop(vid)
                              self.carla_veh_lanes.pop(vid)
                              if vid in self.carla_waiting_vehs:
                                    self.carla_waiting_vehs.remove(vid)
                        else:
                              # if failed, keep the veh in CARLA but set the vehicle to be static
                              carla_veh.set_autopilot(False)
                              carla_veh.set_target_velocity(carla.Vector3D(x=0, y=0, z=0))
                              carla_veh.apply_control(carla.VehicleControl(throttle = 0, brake = 1))
                              carla_veh.enable_constant_velocity(carla.Vector3D(x=0, y=0, z=0))
                              if vid not in self.carla_waiting_vehs:
                                    self.carla_waiting_vehs.append(vid)


      def generate_random_trips(self, num_trips, start_vid = 0):
            self.metsr.generate_trip(list(range(start_vid, start_vid+num_trips)))






            
             
    

 