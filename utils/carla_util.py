import math
import os
import platform
import time
from dataclasses import dataclass, field

import carla


def open_carla(config):
      try:
            client = carla.Client(config.carla_host, config.carla_port)
            client.set_timeout(20.0)
            world = client.load_world(config.carla_map)
      except Exception:
            if platform.system() == "Windows":
                  os.system(f"start {config.carla_dir} -carla-server -carla-rpc-port={config.carla_port} -windowed -ResX=800 -ResY=600")
            else:
                  os.system(f"bash {config.carla_dir} -carla-server -carla-rpc-port={config.carla_port} -windowed -ResX=800 -ResY=600 &")
            time.sleep(10)
            client = carla.Client(config.carla_host, config.carla_port)
            client.set_timeout(20.0)
            world = client.load_world(config.carla_map)
            time.sleep(5)
      time.sleep(5)

      tm = client.get_trafficmanager(8000)

      settings = world.get_settings()
      settings.synchronous_mode = True
      settings.fixed_delta_seconds = config.sim_step_size
      settings.no_rendering_mode = False
      world.apply_settings(settings)
      tm.set_synchronous_mode(True)

      time.sleep(1)

      return client, tm


def snap_to_ground(world, location, z_offset=0.5):
      waypoint = world.get_map().get_waypoint(location)
      location.z = waypoint.transform.location.z + z_offset
      return location


@dataclass
class CarlaCosimState:
      active_vehicles: dict = field(default_factory=dict)
      display_vehicles: dict = field(default_factory=dict)
      coord_maps: dict = field(default_factory=dict)
      routes: dict = field(default_factory=dict)
      dest_roads: dict = field(default_factory=dict)
      entered: dict = field(default_factory=dict)
      waiting_vehicles: set = field(default_factory=set)


def set_overlook_camera(world, x=0.0, y=0.0, z=300.0, yaw=-90.0, pitch=-90.0):
      return set_custom_camera(world, x=x, y=y, z=z, yaw=yaw, pitch=pitch)


def set_custom_camera(world, x, y, z, yaw=-90.0, pitch=-90.0, roll=0.0):
      spectator = world.get_spectator()
      transform = carla.Transform()
      transform.location.x = x
      transform.location.y = y
      transform.location.z = z
      transform.rotation.yaw = yaw
      transform.rotation.pitch = pitch
      transform.rotation.roll = roll
      spectator.set_transform(transform)
      return transform


def metsr_to_carla_location(world, metsr_x, metsr_y, z=0.0, invert_y=True, snap=True, z_offset=0.5):
      x = float(metsr_x)
      y = -float(metsr_y) if invert_y else float(metsr_y)
      location = carla.Location(x, y, float(z or 0.0))
      if snap:
            location = snap_to_ground(world, location, z_offset=z_offset)
      return location


def get_carla_location(world, metsr_x, metsr_y):
      return metsr_to_carla_location(world, metsr_x, metsr_y)


def metsr_bearing_to_carla_yaw(bearing):
      return (float(bearing) - 90.0) % 360.0


def metsr_bearing_to_carla_rotation(bearing):
      yaw = metsr_bearing_to_carla_yaw(bearing)
      return carla.Rotation(pitch=0.0, yaw=yaw, roll=0.0), yaw


def get_carla_rotation(vehicle_state):
      return metsr_bearing_to_carla_rotation(vehicle_state.get("bearing", 0.0))


def carla_yaw_to_metsr_bearing(carla_yaw):
      return (float(carla_yaw) + 90.0) % 360.0


def get_metsr_rotation(carla_yaw):
      return carla_yaw_to_metsr_bearing(carla_yaw)


def _road_id_matches(road_id, road_ids):
      road_values = set(road_ids or [])
      return road_id in road_values or str(road_id) in road_values


def is_location_on_carla_roads(world, x, y, road_ids, lane_type=None):
      lane_type = lane_type or carla.LaneType.Driving
      waypoint = world.get_map().get_waypoint(
            carla.Location(float(x), float(y)),
            project_to_road=True,
            lane_type=lane_type,
      )
      if not road_ids:
            return waypoint is not None
      return waypoint is not None and _road_id_matches(waypoint.road_id, road_ids)


def is_in_carla_submap(world, x, y, carla_roads):
      return is_location_on_carla_roads(world, x, y, carla_roads)


def get_distance(x1, y1, x2, y2):
      return math.hypot(float(x1) - float(x2), float(y1) - float(y2))


def carla_velocity_vector(speed, yaw_degrees):
      speed = float(speed or 0.0)
      yaw_radians = math.radians(float(yaw_degrees))
      return carla.Vector3D(
            x=speed * math.cos(yaw_radians),
            y=speed * math.sin(yaw_radians),
            z=0.0,
      )


def spawn_carla_vehicle(
      world,
      traffic_manager,
      veh_id,
      private_veh,
      vehicle_state,
      actor_store=None,
      private_blueprint="vehicle.audi.tt",
      public_blueprint="vehicle.tesla.model3",
      autopilot=True,
      ignore_lights_percentage=100,
      verbose=False,
):
      rotation, yaw = get_carla_rotation(vehicle_state)
      location = metsr_to_carla_location(world, vehicle_state["x"], vehicle_state["y"])
      spawn_point = carla.Transform(location, rotation)
      blueprint_id = private_blueprint if private_veh else public_blueprint
      blueprint = world.get_blueprint_library().find(blueprint_id)
      actor = world.try_spawn_actor(blueprint, spawn_point)
      if actor is None:
            if verbose:
                  print(
                        f"CARLA failed to spawn vehicle {veh_id} at "
                        f"({location.x:.2f}, {location.y:.2f}, {location.z:.2f}) "
                        f"with blueprint {blueprint_id}."
                  )
            return None

      if autopilot:
            actor.set_autopilot(True)
      if traffic_manager is not None and ignore_lights_percentage is not None:
            traffic_manager.ignore_lights_percentage(actor, ignore_lights_percentage)
      actor.set_target_velocity(carla_velocity_vector(vehicle_state.get("speed", 0.0), yaw))

      if actor_store is not None:
            actor_store[veh_id] = actor
      return actor


def destroy_carla_actor(actor, disable_autopilot=True):
      if actor is None:
            return False
      if disable_autopilot:
            try:
                  actor.set_autopilot(False)
            except Exception:
                  pass
      try:
            return bool(actor.destroy())
      except Exception:
            return False


def destroy_tracked_carla_vehicle(state, veh_id):
      destroyed = 0
      for store in (state.active_vehicles, state.display_vehicles):
            actor = store.pop(veh_id, None)
            if actor is not None:
                  destroyed += int(destroy_carla_actor(actor))

      state.coord_maps.pop(veh_id, None)
      state.routes.pop(veh_id, None)
      state.dest_roads.pop(veh_id, None)
      state.entered.pop(veh_id, None)
      state.waiting_vehicles.discard(veh_id)
      return destroyed


def update_carla_vehicle_from_metsr(world, actor, vehicle_state):
      if actor is None:
            return False
      rotation, _ = get_carla_rotation(vehicle_state)
      location = metsr_to_carla_location(world, vehicle_state["x"], vehicle_state["y"])
      actor.set_transform(carla.Transform(location, rotation))
      return True


def teleport_metsr_vehicle_from_carla(metsr, veh_id, private_veh, actor, transform_coords=True, speed=0.0):
      loc = actor.get_location()
      bearing = carla_yaw_to_metsr_bearing(actor.get_transform().rotation.yaw)
      return metsr.teleport_cosim_vehicle(
            vehID=veh_id,
            x=loc.x,
            y=-loc.y,
            z=loc.z,
            bearing=bearing,
            speed=speed,
            private_veh=private_veh,
            transform_coords=transform_coords,
      )


def drive_actor_toward_metsr_waypoints(
      world,
      actor,
      coord_map,
      vehicle_state,
      waypoint_tolerance=3.0,
      min_speed=0.1,
):
      if actor is None or coord_map is None:
            return False

      loc = actor.get_location()
      if coord_map:
            next_pos = coord_map[0]
            dist = get_distance(loc.x, loc.y, next_pos[0], -next_pos[1])
            if dist < waypoint_tolerance:
                  coord_map.pop(0)

      if not coord_map:
            return False

      target = coord_map[0]
      dx = target[0] - loc.x
      dy = -target[1] - loc.y
      yaw = math.degrees(math.atan2(dy, dx))
      rotation = carla.Rotation(pitch=0.0, yaw=yaw, roll=0.0)
      actor.set_transform(carla.Transform(metsr_to_carla_location(world, target[0], target[1]), rotation))
      speed = max(float(vehicle_state.get("speed", 0.0)), min_speed)
      actor.set_target_velocity(carla_velocity_vector(speed, yaw))
      return True


def stop_carla_vehicle(actor):
      if actor is None:
            return
      actor.set_autopilot(False)
      actor.set_target_velocity(carla.Vector3D(x=0.0, y=0.0, z=0.0))
      actor.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0))
      actor.enable_constant_velocity(carla.Vector3D(x=0.0, y=0.0, z=0.0))


def configure_metsr_cosim_roads(metsr, metsr_roads):
      for road in metsr_roads or []:
            metsr.set_cosim_road(road)


def _queued_vehicle_id(entry):
      for key in ("ID", "vehID", "vehicleID", "visibleID"):
            if key in entry:
                  return entry[key]
      return None


def _queued_vehicle_private_flag(entry):
      for key in ("v_type", "vehType", "private_veh", "privateVeh"):
            if key in entry:
                  return entry[key]
      return None


def _queued_vehicle_internal_id(entry):
      for key in ("internalID", "internalVehicleID", "vehicleID"):
            if key in entry:
                  return entry[key]
      return None


def _queued_vehicle_ready(entry):
      return entry.get("ready", entry.get("isReady", True))


def release_ready_cosim_vehicles_from_queue(metsr, verbose=False):
      if not hasattr(metsr, "query_cosim_entering_vehicle_queue"):
            return []

      queues = metsr.query_cosim_entering_vehicle_queue().get("DATA", [])
      requests = []
      for road_queue in queues:
            road_id = road_queue.get("ID", road_queue.get("roadID"))
            entries = road_queue.get("queue", road_queue.get("vehicles", []))
            for entry in entries:
                  if not _queued_vehicle_ready(entry):
                        continue
                  request = {}
                  veh_id = _queued_vehicle_id(entry)
                  internal_id = _queued_vehicle_internal_id(entry)
                  private_veh = _queued_vehicle_private_flag(entry)

                  if veh_id is not None:
                        request["vehID"] = veh_id
                  if internal_id is not None:
                        request["internalVehicleID"] = internal_id
                  if private_veh is not None:
                        request["vehType"] = private_veh
                  if road_id is not None:
                        request["roadID"] = road_id
                  if request:
                        requests.append(request)

      if not requests:
            return []

      response = metsr.enter_road_from_queue(requests=requests)
      if verbose:
            print(f"Released {len(requests)} ready vehicle(s) from co-sim entering queues.")
      return response.get("DATA", requests)


def _sync_active_carla_vehicle(
      metsr,
      world,
      traffic_manager,
      state,
      veh_id,
      private_veh,
      vehicle_state,
      carla_roads,
      metsr_roads,
      transform_coords=True,
      waypoint_tolerance=3.0,
      min_waypoint_speed=0.1,
      verbose=False,
):
      try:
            actor = state.active_vehicles[veh_id]
            loc = actor.get_location()
      except RuntimeError:
            print(f"Vehicle {veh_id} removed by CARLA, re-adding it.")
            destroy_tracked_carla_vehicle(state, veh_id)
            actor = spawn_carla_vehicle(
                  world,
                  traffic_manager,
                  veh_id,
                  private_veh,
                  vehicle_state,
                  actor_store=state.active_vehicles,
                  verbose=verbose,
            )
            if actor is None:
                  return {"vehID": veh_id, "STATUS": "SPAWN_FAILED"}
            loc = actor.get_location()

      teleport_metsr_vehicle_from_carla(
            metsr,
            veh_id,
            private_veh,
            actor,
            transform_coords=transform_coords,
      )

      if is_location_on_carla_roads(world, loc.x, loc.y, carla_roads):
            if not state.entered.get(veh_id, False):
                  state.entered[veh_id] = True
            return {"vehID": veh_id, "STATUS": "ACTIVE"}

      if not state.entered.get(veh_id, False):
            coord_map = state.coord_maps.get(veh_id, [])
            if drive_actor_toward_metsr_waypoints(
                  world,
                  actor,
                  coord_map,
                  vehicle_state,
                  waypoint_tolerance=waypoint_tolerance,
                  min_speed=min_waypoint_speed,
            ):
                  return {"vehID": veh_id, "STATUS": "APPROACHING_CARLA_ROAD"}

            success = metsr.reach_dest(vehID=veh_id, private_veh=private_veh)["DATA"][0]["STATUS"]
            print(f"Vehicle {veh_id} failed to enter co-sim area; remove it.")
            assert success == "OK", f"Vehicle {veh_id} failed to reach destination."
            destroy_tracked_carla_vehicle(state, veh_id)
            return {"vehID": veh_id, "STATUS": "FAILED_TO_ENTER"}

      print(f"Vehicle {veh_id} has left the co-sim area.")
      if _road_id_matches(state.dest_roads.get(veh_id), metsr_roads):
            success = metsr.reach_dest(vehID=veh_id, private_veh=private_veh)["DATA"][0]["STATUS"]
            assert success == "OK", f"Vehicle {veh_id} failed to reach destination."
            print(f"Vehicle {veh_id} reached destination.")
            destroy_tracked_carla_vehicle(state, veh_id)
            return {"vehID": veh_id, "STATUS": "REACHED_DEST"}

      success = metsr.enter_next_road(vehID=veh_id, private_veh=private_veh)["DATA"][0]["STATUS"]
      if success == "OK":
            print(f"Vehicle {veh_id} exited co-sim area.")
            destroy_tracked_carla_vehicle(state, veh_id)
            return {"vehID": veh_id, "STATUS": "EXITED_CARLA_ROAD"}

      stop_carla_vehicle(actor)
      state.waiting_vehicles.add(veh_id)
      return {"vehID": veh_id, "STATUS": "WAITING_FOR_METS_R_ROAD"}


def step_carla_metsr_cosim(
      metsr,
      world,
      traffic_manager,
      state=None,
      carla_roads=None,
      metsr_roads=None,
      display_all=False,
      transform_coords=True,
      display_batch_size=10,
      waiting_retry_interval=10,
      waypoint_tolerance=3.0,
      min_waypoint_speed=0.1,
      release_ready_queue=True,
      metsr_wait_forever=True,
      metsr_retry_interval=None,
      metsr_max_stalled_seconds=None,
      metsr_poll_timeout=5,
      verbose=False,
):
      state = state or CarlaCosimState()
      world.tick()

      results = []
      if release_ready_queue:
            release_ready_cosim_vehicles_from_queue(metsr, verbose=verbose)

      metsr.tick(
            wait_forever=metsr_wait_forever,
            retry_interval=metsr_retry_interval,
            max_stalled_seconds=metsr_max_stalled_seconds,
            poll_timeout=metsr_poll_timeout,
      )

      if release_ready_queue:
            release_ready_cosim_vehicles_from_queue(metsr, verbose=verbose)

      cosim_vehicles = metsr.query_coSimVehicle().get("DATA", [])
      if verbose and not cosim_vehicles:
            print("No vehicles reported by QUERY_coSimVehicle after this tick.")
      cosim_ids = [vehicle["ID"] for vehicle in cosim_vehicles]
      private_flags = [vehicle["v_type"] for vehicle in cosim_vehicles]
      vehicle_states = []
      if cosim_ids:
            vehicle_states = metsr.query_vehicle(
                  id=cosim_ids,
                  private_veh=private_flags,
                  transform_coords=transform_coords,
            ).get("DATA", [])

      for cosim_id, cosim_vehicle, private_flag, vehicle_state in zip(
            cosim_ids,
            cosim_vehicles,
            private_flags,
            vehicle_states,
      ):
            if cosim_id in state.active_vehicles:
                  if cosim_id not in state.waiting_vehicles:
                        results.append(_sync_active_carla_vehicle(
                              metsr,
                              world,
                              traffic_manager,
                              state,
                              cosim_id,
                              private_flag,
                              vehicle_state,
                              carla_roads or [],
                              metsr_roads or [],
                              transform_coords=transform_coords,
                              waypoint_tolerance=waypoint_tolerance,
                              min_waypoint_speed=min_waypoint_speed,
                              verbose=verbose,
                        ))
                  else:
                        current_tick = getattr(metsr, "current_tick", 0) or 0
                        if waiting_retry_interval and current_tick % waiting_retry_interval == 0:
                              success = metsr.enter_next_road(
                                    vehID=cosim_id,
                                    private_veh=private_flag,
                              )["DATA"][0]["STATUS"]
                              if success == "OK":
                                    print(f"Vehicle {cosim_id} exited co-sim area.")
                                    destroy_tracked_carla_vehicle(state, cosim_id)
                                    results.append({"vehID": cosim_id, "STATUS": "EXITED_CARLA_ROAD"})
                  continue

            if vehicle_state.get("state", 0) > 0:
                  if cosim_id in state.display_vehicles:
                        destroy_tracked_carla_vehicle(state, cosim_id)
                  actor = spawn_carla_vehicle(
                        world,
                        traffic_manager,
                        cosim_id,
                        private_flag,
                        vehicle_state,
                        actor_store=state.active_vehicles,
                        verbose=verbose,
                  )
                  if actor is not None:
                        route = cosim_vehicle.get("route", [])
                        state.coord_maps[cosim_id] = cosim_vehicle.get("coord_map", [])
                        state.routes[cosim_id] = route
                        state.dest_roads[cosim_id] = route[-1] if route else None
                        state.entered[cosim_id] = False
                        results.append({"vehID": cosim_id, "STATUS": "SPAWNED"})

      if display_all:
            private_agents = metsr.query_vehicle().get("private_vids", [])
            batch_size = max(1, int(display_batch_size))
            for idx in range(0, len(private_agents), batch_size):
                  batch_ids = private_agents[idx:idx + batch_size]
                  batch_states = metsr.query_vehicle(
                        id=batch_ids,
                        private_veh=True,
                        transform_coords=transform_coords,
                  ).get("DATA", [])

                  for veh_id, vehicle_state in zip(batch_ids, batch_states):
                        if veh_id not in state.active_vehicles and veh_id not in state.display_vehicles:
                              if vehicle_state.get("state", 0) > 0:
                                    spawn_carla_vehicle(
                                          world,
                                          traffic_manager,
                                          veh_id,
                                          True,
                                          vehicle_state,
                                          actor_store=state.display_vehicles,
                                          verbose=verbose,
                                    )
                        elif veh_id in state.display_vehicles:
                              if vehicle_state.get("state", 0) > 0:
                                    update_carla_vehicle_from_metsr(
                                          world,
                                          state.display_vehicles.get(veh_id),
                                          vehicle_state,
                                    )
                              else:
                                    destroy_tracked_carla_vehicle(state, veh_id)

      return {
            "state": state,
            "vehicles": results,
            "cosim_vehicles": cosim_vehicles,
            "vehicle_states": vehicle_states,
      }
