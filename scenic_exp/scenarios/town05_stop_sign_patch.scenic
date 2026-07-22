"""Town05 Scenic search space for the PCLA stop-sign color-patch demo."""

param address = "127.0.0.1"
param bubble_size = 100
param town = "Town05"
param map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr')
param xml_map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.net.xml')
param num_commuters = 100
param timestep = 0.05
param length = 60
param seed = 33
param export_folder = localPath('../data_logs/CARLA_05/stop_sign_patch')
param allow_bubble_spawns = False
param attack_stop_index = 0
param attack_enabled = True
param initial_x = 0.0
param initial_y = 0.0
param initial_heading = 0.0
param run_name = f"{globalParameters.export_folder}/stop_{globalParameters.attack_stop_index}_{'attack' if globalParameters.attack_enabled else 'baseline'}_seed_{globalParameters.seed}"

model scenic.simulators.cosim.model


behavior AwaitExternalPCLA():
    """Leave the ego's CARLA controls available to demo4's PCLA controller."""
    take SetAutoPilotAction(False)
    while True:
        wait


scenario SpawnCar(veh_num):
    if veh_num < globalParameters.num_commuters:
        if not globalParameters.allow_bubble_spawns:
            target = simulation().objects[0]
            spawn_region = workspace.network.drivableRegion.difference(target.bubble)
            new NPCCar with name f"car_{veh_num}", with behavior FollowSingleTrajectoryBehavior(), in spawn_region
        else:
            new NPCCar with name f"car_{veh_num}", with behavior FollowSingleTrajectoryBehavior()
    terminate after 1 steps


scenario ContinuousSpawn():
    setup:
        n_vehicles = 0
    compose:
        while True:
            do SpawnCar(n_vehicles)
            n_vehicles += 1


scenario Main():
    setup:
        ego = new EgoCar at globalParameters.initial_x @ globalParameters.initial_y,
            facing globalParameters.initial_heading,
            with name "ego",
            with behavior AwaitExternalPCLA()
        record globalParameters.attack_stop_index as attack_stop_index
        record globalParameters.attack_enabled as attack_enabled
        record {obj.name: obj.position for obj in simulation().objects} as all_positions
        record {obj.name: [obj.velocity.x, obj.velocity.y, obj.velocity.z] for obj in simulation().objects} as all_velocities
    compose:
        do ContinuousSpawn() for globalParameters.length seconds
