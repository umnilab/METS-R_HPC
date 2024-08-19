import carla
import os
import time
import platform

def open_carla(config):
      try:
            client = carla.Client(config.carla_host, config.carla_port)
            client.set_timeout(20.0)
            world = client.load_world(config.carla_map)
      except:
            if platform.system() == "Windows":
                os.system(f"start {config.carla_dir} -carla-server -carla-rpc-port={config.carla_port} -windowed -ResX=800 -ResY=600")
            else:
                os.system(f"bash {config.carla_dir} -carla-server -carla-rpc-port={config.carla_port} -windowed -ResX=800 -ResY=600 &")
            time.sleep(10)
            client = carla.Client(config.carla_host, config.carla_port)
            client.set_timeout(20.0)
            time.sleep(5)
            world = client.load_world(config.carla_map)

      time.sleep(5)

      # get traffic manager
      tm = client.get_trafficmanager(8000)

      # apply settings
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
            
