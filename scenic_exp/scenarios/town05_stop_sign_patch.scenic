"""Paired stop-sign trial preset for the shared external runtime (demo4 runner).

The runtime implementation lives in scenic_exp.runtime, independently of demo4.
Direct runs require a registered external runtime; use traffic_template.scenic
with security_scenario='stop_sign_patch' for a native Scenic PCLA controller.
"""
param backend = "cosim"
param town = "Town05"
param num_commuters = 100
param timestep = 0.05
param length = 60
param seed = 33  # Used in run_name before importing the template.
param ego_controller = "external"
param attack_stop_index = 0
param attack_enabled = True
param security_scenario = "stop_sign_patch"
param test_parameters = {
    "trigger": {"start_s": 0},
    "event": {"duration_s": globalParameters.length + globalParameters.timestep},
    "stop_sign": {"index": globalParameters.attack_stop_index, "enabled": globalParameters.attack_enabled}
}
param initial_x = 0.0
param initial_y = 0.0
param initial_heading = 0.0
param initial_pose = (globalParameters.initial_x, globalParameters.initial_y, globalParameters.initial_heading)
param export_folder = localPath("../data_logs/CARLA_05/stop_sign_patch")
param run_name = f"{globalParameters.export_folder}/stop_{globalParameters.attack_stop_index}_{'attack' if globalParameters.attack_enabled else 'baseline'}_seed_{globalParameters.seed}"
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
