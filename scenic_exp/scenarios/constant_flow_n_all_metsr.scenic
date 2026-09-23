"""METS-R-only demand; configure the road network on the server. See README.md."""
param backend = "metsr"
param num_commuters = 500
param length = 300
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
