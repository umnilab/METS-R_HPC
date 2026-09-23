"""METS-R/CARLA co-simulation with a map-wide bubble. See README.md."""
param backend = "cosim"
param num_commuters = 500
param length = 300
param bubble_size = 100000
param allow_bubble_spawns = True
param ego_controller = "autopilot"
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
