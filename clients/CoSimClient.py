"""
Helper functions for co-simulation with CARLA
"""
import numpy as np
import carla
from utils.carla_util import snap_to_ground
from clients.METSRClient import METSRClient

"""
Implementation of the CoSim Client

A CoSim client communicates with one METSRClient and one CARLA client to manage the 
data flow between corresponding simulation instances.
"""

# Co-simulation 1: The carla control a submap, and the METS-R SIM control the rest maps
# The visualization of the submap is done in the CARLA simulator
# The visualization of the rest maps is done in the METS-R SIM
class CoSimClient(object):
      def __init__(self, config, carla_client, tm_client):
            self.config = config

            self.carla = carla_client.get_world()
            self.carla_client = carla_client
            self.carla_tm = tm_client
            # self.set_carla_camera(self.carla, config)
            self.set_overlook_camera(self.carla)

            self.metsr = METSRClient(config.metsr_host, int(config.ports[0]), 0, self, verbose = config.verbose)

            self.display_all = config.display_all # display all the vehicles in the CARLA map

            # set the co-sim region - default to empty list if not specified
            metsr_roads = getattr(config, 'metsr_road', [])
            for road in metsr_roads:
                  self.metsr.set_cosim_road(road)

            self.carla_vehs = {} # id of agent and vehicle instance in carla
            self.carla_coordMaps = {} # id of agent and the corresponding carla coord map
            self.carla_route = {}
            self.carla_destRoad = {}
            self.carla_entered = {}

            self.other_vehs = {} # id of agent and vehicle controlled by METSR, only used for display all vehicles

            self.carla_waiting_vehs = [] # vehicles waiting to enter the other road, should be visited in every 10 ticks
            self.waypoints = self.carla.get_map().generate_waypoints(2.0) # generate all waypoints at 2-meter intervals

      def set_overlook_camera(self, world): # set the camera to overlook the whole map
            spectator = world.get_spectator()
            transform = carla.Transform()
            transform.location.x = 0
            transform.location.y = 0
            transform.location.z = 300
            transform.rotation.yaw = -90
            transform.rotation.pitch = -90
            spectator.set_transform(transform)

      def set_custom_camera(self, x, y, z):
            spectator = self.carla.get_spectator()
            transform = carla.Transform()
            transform.location.x = x
            transform.location.y = y
            transform.location.z = z
            transform.rotation.yaw = -90
            transform.rotation.pitch = -90
            spectator.set_transform(transform)

      def get_carla_location(self, metsr_x, metsr_y):
            # given x, y, find the corresponding z values and rotation in CARLA
            x, y = metsr_x, -metsr_y
            location = carla.Location(x, y, 0)
            location = snap_to_ground(self.carla, location)
            return location
      
      def get_carla_rotation(self, veh_inform):
            # veh_inform['bearing']: compass heading (° clockwise from north)
            bearing = veh_inform['bearing'] % 360
            carla_yaw = (bearing - 90) % 360
            rotation = carla.Rotation(pitch=0.0, yaw=carla_yaw, roll=0.0)
            return rotation, carla_yaw
      
      def get_metsr_rotation(self, carla_yaw):
            """
            Invert carla_yaw = (bearing - 90) % 360
            to recover the original METSR compass bearing.
            """
            # ensure 0 ≤ yaw < 360
            carla_yaw = carla_yaw % 360
            # invert the shift of -90°
            return (carla_yaw + 90) % 360

      def is_in_carla_submap(self, x, y):
            # project x, y to the nearest road in CARLA and check if the road ID is in the co-sim road
            road_id = self.carla.get_map().get_waypoint(carla.Location(x, y), project_to_road=True, lane_type=(carla.LaneType.Driving)).road_id
            return road_id in self.config.carla_road
            
      def step(self):
            self.carla.tick()
            self.metsr.tick()

            cosim_vehs = self.metsr.query_coSimVehicle()['DATA']

            cosim_ids = [v['ID'] for v in cosim_vehs]
            private_flags = [v['v_type'] for v in cosim_vehs]
            all_data = self.metsr.query_vehicle(cosim_ids, private_flags, transform_coords=True)['DATA']
            # Update co-sim vehicles in CARLA

            for cosim_id, cosim_veh, private_flag, veh_info in zip(cosim_ids, cosim_vehs, private_flags, all_data):
                  if cosim_id in self.carla_vehs:
                        if cosim_id not in self.carla_waiting_vehs:
                              self.sync_carla_vehicle(cosim_id, private_flag, veh_info)
                        else:
                              if self.metsr.current_tick % 10 == 0:
                                    success = self.metsr.enter_next_road(cosim_id, private_flag)['DATA'][0]['STATUS']
                                    if success == 'OK':
                                          print(f"Vehicle {cosim_id} exited co-sim area.")
                                          self.destroy_carla_vehicle(cosim_id)
                                    
                  else:
                        if veh_info['state'] > 0:
                              if cosim_id in self.other_vehs:
                                    # remove the vehicle from the other_vehs if it is in the co-sim area
                                    self.destroy_carla_vehicle(cosim_id)
                              self.spawn_carla_vehicle(cosim_id, private_flag, veh_info, display_only=False)
                              # add carla coordMap to the carla_vehs
                              self.carla_coordMaps[cosim_id] = cosim_veh['coord_map'] # add carla coordMap to the carla_vehs
                              # add carla nextRoad to the carla_vehs
                              self.carla_route[cosim_id] = cosim_veh['route']
                              # add carla destRoad to the carla_vehs
                              self.carla_destRoad[cosim_id] = cosim_veh['route'][-1]
                              self.carla_entered[cosim_id] = False
            if self.display_all:
                  private_agents = self.metsr.query_vehicle()['private_vids']

                  # Process display-only vehicles in batches to avoid blocking
                  batch_size = 10
                  for i in range(0, len(private_agents), batch_size):
                        batch_ids = private_agents[i:i+batch_size]
                        batch_infos = self.metsr.query_vehicle(batch_ids, private_veh=True, transform_coords=True)['DATA']

                        for vid, veh_info in zip(batch_ids, batch_infos):
                              if vid not in self.carla_vehs and vid not in self.other_vehs:
                                    if veh_info['state'] > 0:
                                          self.spawn_carla_vehicle(vid, True, veh_info, display_only=True)
                              elif vid in self.other_vehs:
                                    if veh_info['state'] > 0:
                                          import time
                                          time.sleep(0.0001)  # Add a small delay to avoid blocking
                                          self.update_display_only_vehicle(vid, veh_info)
                                    else:
                                          self.destroy_carla_vehicle(vid)
     
      def run(self):
            try:
                  for t in range(int(self.config.sim_minutes * 60 / self.config.sim_step_size)):
                        print("Tick:", t)
                        if t % 600 == 0:
                              # generate 10 random trips every 1 minute
                              self.generate_random_trips(10, start_vid = int(t // 6))
                              print(f"Generated 10 random trips at time {t * self.config.sim_step_size // 60} minute!")
                        self.step()
            except KeyboardInterrupt:
                  print("simulation interrupted by user")

            finally:
                  self.metsr.terminate()

      def get_distance(self, x1, y1, x2, y2):
            return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
      
      def spawn_carla_vehicle(self, vid, private_veh, veh_inform, display_only=False):
            tmp_rotation, tmp_yaw = self.get_carla_rotation(veh_inform)
            spawn_point = carla.Transform(self.get_carla_location(veh_inform['x'], veh_inform['y']), tmp_rotation)

            blueprint = self.carla.get_blueprint_library().find('vehicle.audi.tt' if private_veh else 'vehicle.tesla.model3')
            tmp_veh = self.carla.try_spawn_actor(blueprint, spawn_point)

            if tmp_veh:
                  tmp_veh.set_autopilot(True)
                  self.carla_tm.ignore_lights_percentage(tmp_veh, 100)
                  tmp_speed = veh_inform['speed']
                  tmp_speed_x = tmp_speed * np.cos(tmp_yaw * np.pi / 180)
                  tmp_speed_y = tmp_speed * np.sin(tmp_yaw * np.pi / 180)
                  tmp_veh.set_target_velocity(carla.Vector3D(x=tmp_speed_x, y=tmp_speed_y, z=0))

                  if display_only:
                        self.other_vehs[vid] = tmp_veh
                  else:
                        self.carla_vehs[vid] = tmp_veh

      def destroy_carla_vehicle(self, vid):
            if vid in self.carla_vehs:
                  self.carla_vehs[vid].set_autopilot(False)
                  try:
                        self.carla_vehs[vid].destroy()
                  except:
                        pass
                  self.carla_vehs.pop(vid, None)
                  self.carla_coordMaps.pop(vid, None)
                  self.carla_route.pop(vid, None)
                  self.carla_destRoad.pop(vid, None)
                  self.carla_entered.pop(vid, None)
                  if vid in self.carla_waiting_vehs:
                        self.carla_waiting_vehs.remove(vid)
            if vid in self.other_vehs:
                  try:
                        self.other_vehs[vid].destroy()
                  except:
                        pass
                  self.other_vehs.pop(vid, None)

      def update_display_only_vehicle(self, vid, veh_inform):
            carla_veh = self.other_vehs.get(vid)
            if carla_veh:
                  target_loc = self.get_carla_location(veh_inform['x'], veh_inform['y'])
                  tmp_rotation, _ = self.get_carla_rotation(veh_inform)
                  carla_veh.set_transform(carla.Transform(target_loc, tmp_rotation))
      
      def sync_carla_vehicle(self, vid, private_veh, veh_inform):
            try:
                  carla_veh = self.carla_vehs[vid]
                  loc = carla_veh.get_location()
            except RuntimeError:
                  # re-add the vehicle if it is removed by CARLA
                  print(f"Vehicle {vid} removed by CARLA, re-adding it.")
                  self.destroy_carla_vehicle(vid)
                  self.spawn_carla_vehicle(vid, private_veh, veh_inform, display_only=False)
                  carla_veh = self.carla_vehs[vid]
                  loc = carla_veh.get_location()
            bearing = self.get_metsr_rotation(carla_veh.get_transform().rotation.yaw)
            self.metsr.teleport_cosim_vehicle(vid, loc.x, -loc.y, bearing, private_veh, transform_coords=True)
            # Now vehicle is considered on co-sim road
            if self.is_in_carla_submap(loc.x, loc.y):
                  if self.carla_entered[vid] == False:
                        self.carla_entered[vid] = True
            else:
                  # case 1: vehicle has not entered the co-sim area yet
                  if self.carla_entered[vid] == False:
                        coord_map = self.carla_coordMaps[vid]
                        if len(coord_map) > 0:
                              next_pos = coord_map[0]
                              dist = self.get_distance(loc.x, loc.y, next_pos[0], -next_pos[1])
                              if dist < 3.0:
                                    # reached this waypoint, go to next
                                    coord_map.pop(0)
                              if len(coord_map) > 0:
                                    # still has waypoints to follow
                                    print("Still has waypoints to follow")
                                    target = coord_map[0]
                                    # calculate tmp_ratation and tmp_yaw from target[0] and target[1]
                                    dx = target[0] - loc.x
                                    dy = -target[1] - loc.y  # CARLA uses left-handed coordinate system
                                    tmp_yaw = np.degrees(np.arctan2(dy, dx))
                                    tmp_rotation = carla.Rotation(pitch=0, yaw=tmp_yaw, roll=0)
                                    # Set transform and velocity
                                    carla_veh.set_transform(
                                          carla.Transform(self.get_carla_location(target[0], target[1]), tmp_rotation)
                                    )
                                    tmp_speed = max(veh_inform['speed'], 0.1) # set a minimum speed to avoid stopping
                                    tmp_speed_x = tmp_speed * np.cos(tmp_yaw * np.pi / 180)
                                    tmp_speed_y = tmp_speed * np.sin(tmp_yaw * np.pi / 180)
                                    carla_veh.set_target_velocity(carla.Vector3D(x=tmp_speed_x, y=tmp_speed_y, z=0))
                        else:
                              # Destroy vehicle
                              success = self.metsr.reach_dest(vid)['DATA'][0]['STATUS']
                              print(f"Vehicle {vid} failed to enter co-sim area; remove it.")
                              assert success == 'OK', f"Vehicle {vid} failed to reach destination."
                              

                  else:
                        print("Vehicle " + str(vid) + " has left the co-sim area.")
                        # case 2: vehicle has entered the co-sim area
                        if self.carla_destRoad[vid] in self.config.metsr_road:
                              # case 2.1: vehicle's destination is within the co-sim area
                              success = self.metsr.reach_dest(vid, private_veh)['DATA'][0]['STATUS']
                              assert success == 'OK', f"Vehicle {vid} failed to reach destination."
                              if success == 'OK':
                                    print(f"Vehicle {vid} reached destination.")
                                    self.destroy_carla_vehicle(vid)
                        else:
                              # case 2.2: vehicle's destination is outside the co-sim area
                              success = self.metsr.exit_cosim_region(vid, loc.x, -loc.y, private_veh, True)['DATA'][0]['STATUS']
                              if success == 'OK':
                                    print(f"Vehicle {vid} exited co-sim area.")
                                    self.destroy_carla_vehicle(vid)
                              else:
                                    carla_veh.set_autopilot(False)
                                    carla_veh.set_target_velocity(carla.Vector3D(x=0, y=0, z=0))
                                    carla_veh.apply_control(carla.VehicleControl(throttle=0, brake=1))
                                    carla_veh.enable_constant_velocity(carla.Vector3D(x=0, y=0, z=0))
                              if vid not in self.carla_waiting_vehs:
                                    self.carla_waiting_vehs.append(vid)


      def generate_random_trips(self, num_trips, start_vid = 0):
            self.metsr.generate_trip(list(range(start_vid, start_vid+num_trips))) 