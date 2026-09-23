"""Autopilot co-simulation; retains the historical 100 m radius. See README.md."""
param backend = "cosim"
param length = 300
param num_commuters = 1000
param allow_bubble_spawns = True
param ego_controller = "autopilot"
param export_folder = localPath("../data_logs/CARLA_06/constant_flow")
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
