"""IP address for CARLA client communication"""
param address = "169.233.199.10"
""" Size of CoSim region in meters """
param bubble_size = 100
""" Map to be simulated """
param town = 'Town06'
""" Path to OpenDrive map """
param map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr') 
""" Path to SUMO map """
param xml_map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.net.xml') 
""" Target number of commuters to enter the roadway """
param num_commuters = 1000
""" Total simulation step size before synchronization """
param timestep = 0.1
""" Total simulation time in seconds"""
param length = 300
""" Scenic run seed"""
param seed = 33
""" Target export location"""
param export_folder = localPath('../data_logs/CARLA_06/constant_flow')
""" Target csv name for generated data"""
param run_name = f"{globalParameters.export_folder}/vehs_{globalParameters.num_commuters}_simtime_{globalParameters.length}_seed_{globalParameters.seed}"

model scenic.simulators.cosim.model

scenario SpawnCar(veh_num):
    if veh_num < globalParameters.num_commuters:
        new NPCCar with name f"car_{veh_num}", with behavior FollowSingleTrajectoryBehavior()
    terminate after 1 steps

scenario Test():
    setup:
        n_vehicles = 0
    compose:
        while True:
            do SpawnCar(n_vehicles)
            n_vehicles += 1

scenario Main():
    setup:
        ego = new EgoCar with name "ego", with behavior FollowSingleTrajectoryBehavior()
    compose:
        do Test() for globalParameters.length seconds
        


    
        



