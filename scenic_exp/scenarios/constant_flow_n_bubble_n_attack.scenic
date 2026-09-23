"""demo2 PCLA preset; attacks are off by default. See traffic_template.scenic."""
param backend = "cosim"
param num_commuters = 1000
param length = 10
param ego_controller = "pcla"
param export_folder = localPath("../data_logs/CARLA_06/constant_flow")
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
