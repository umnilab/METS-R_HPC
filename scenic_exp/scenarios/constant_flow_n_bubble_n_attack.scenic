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
param length = 10
""" Scenic run seed"""
param seed = 33
""" Target export location"""
param export_folder = localPath('../data_logs/CARLA_06/constant_flow')
""" Target csv name for generated data"""
param run_name = f"{globalParameters.export_folder}/vehs_{globalParameters.num_commuters}_simtime_{globalParameters.length}_seed_{globalParameters.seed}"
"""Whether spawning should be allowed inside the ego's co-simulation bubble."""
param allow_bubble_spawns = False
"""PCLA agent name and optional route used by the ego behavior."""
param pcla_agent = "simlingo_simlingo"
param pcla_route = None

model scenic.simulators.cosim.model

import os
import sys

pcla_home = os.environ.get("PCLA_HOME")
if pcla_home and pcla_home not in sys.path:
    sys.path.append(pcla_home)

from PCLA import PCLA
from pcla_functions.route_maker import route_maker
from pcla_functions.location_to_waypoint import location_to_waypoint

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
        if not globalParameters.allow_bubble_spawns:
            target = simulation().objects[0]
            spawn_region = workspace.network.drivableRegion.difference(target.bubble)
            veh = new NPCCar with name f"car_{veh_num}", with behavior FollowSingleTrajectoryBehavior(), in spawn_region
        else:
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


behavior PCLAAgent(agentType=globalParameters.pcla_agent, route=globalParameters.pcla_route):
    """
        Drive the Scenic ego with a PCLA agent.

        If no route is supplied, PCLA receives a route from the ego's current
        location to a Scenic-sampled CARLA spawn point.
    """
    assert self.carlaActor

    if route is None:
        start_pos = self.carlaActor.get_transform().location
        end_point = Uniform(*simulation().spawn_points).location
        waypoints = location_to_waypoint(simulation().carla_client, start_pos, end_point)
        route = localPath('../helpers/routes/ego_route.xml')
        route_maker(waypoints, savePath=route)

    self.pcla = PCLA(agentType, self.carlaActor, route, simulation().carla_client)

    while True:
        action = self.pcla.get_action()
        take SetBrakeAction(action.brake), SetThrottleAction(action.throttle), SetSteerAction(action.steer)

scenario Main():
    """
        Scenario Main:
            Generate an EgoCar with adversarial behavior, the continuously spawns NPC Cars
    """
    setup:
        ego = new EgoCar with name "ego", with behavior PCLAAgent()
        record {obj.name: obj.position for obj in (simulation().objects)} as all_positions
        record {obj.name: [obj.velocity.x, obj.velocity.y, obj.velocity.z] for obj in (simulation().objects)} as all_velocities
    compose:
        do ContinuousSpawn() for globalParameters.length seconds
        

"""

modify compiler to pass a list to the do statment

how easy to implement a do statement which spawns off 
another scenario to run
"""
