"""CARLA-only autopilot flow: no METS-R or PCLA. See README.md."""
param backend = "carla"
param num_commuters = 500
param length = 300
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
