"""IP address for CARLA client communication"""
param address = "10.0.0.122"
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
param length = 10
""" Scenic run seed"""
param seed = 33
""" Target export location"""
param export_folder = localPath('../data_logs/CARLA_06/constant_flow')
""" Target csv name for generated data"""
param run_name = f"{globalParameters.export_folder}/vehs_{globalParameters.num_commuters}_simtime_{globalParameters.length}_seed_{globalParameters.seed}"

model scenic.simulators.cosim.model


behavior hard_break(attack_duration):
    """
        Hard Break behavior 
            Actor will initiate full break behavior for [attack_duration] steps
    """
    take SetAutoPilotAction(False)
    self.count_since_attack = 0
    while True:
        self.count_since_attack += 1
        if self.count_since_attack >= attack_duration:
            self.attack = False
        take SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)


behavior FollowRandomTrajectory(attack_duration):
    """
        FollowRandomTrajectoy behavior
            Actor will follow the METSR proposed trajectory via CARLA autopilot for [attack_duration] steps
    """
    if not hasattr(self, "trajectory"):
        self.trajectory = None
    take SetAutoPilotAction(True)
    self.count_since_attack = 0
    while True:
        self.count_since_attack += 1
        if self.count_since_attack > attack_duration:
            self.attack = True
        wait

behavior Adversary(attack_duration = 25):
    """
        Adversary behavior
            Simplistic periodic adversary behavior where attacker will alternate between attacking and reasonable driving.
            Adversary will spend [attack_duration] steps in each state
    """
    self.count_since_attack = 0
    state_map = {False: FollowRandomTrajectory(attack_duration), True: hard_break(attack_duration)}
    self.attack = False
    state_func = lambda: self.attack
    while True:
        curr_state = state_func()
        behavior = state_map[curr_state]
        do behavior until curr_state != state_func()
        

scenario SpawnCar(veh_num):
    """ 
        Scenario SpawnCar:
            Spawns a new NPCCar on the road if the total target vehicles has not yet been generated
    """
    if veh_num < globalParameters.num_commuters:
        veh = new NPCCar with name f"car_{veh_num}", with behavior FollowSingleTrajectoryBehavior()
    terminate after 1 steps

scenario ContinuousSpawn():
    """
        Scenario ContinuousSpawn:
            Contiunously spawns NPCCar'a on the road up to a given threshold
    """
    setup:
        n_vehicles = 0
    compose:
        while True:
            do SpawnCar(n_vehicles)
            n_vehicles += 1

scenario Main():
    """
        Scenario Main:
            Generate an EgoCar with adversarial behavior, the continuously spawns NPC Cars
    """
    setup:
        ego = new EgoCar with name "ego", with behavior DriveAvoidingCollisions()
        record {obj.name: obj.position for obj in (simulation().objects)} as all_positions
        record {obj.name: [obj.velocity.x, obj.velocity.y, obj.velocity.z] for obj in (simulation().objects)} as all_velocities
    compose:
        do ContinuousSpawn() for globalParameters.length seconds
        

"""

modify compiler to pass a list to the do statment

how easy to implement a do statement which spawns off 
another scenario to run
"""
