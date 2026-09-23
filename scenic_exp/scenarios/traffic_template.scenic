"""Traffic configuration and implementation for METS-R/CARLA experiments.

Edit this file, or declare parameters in a preset and import Main from it.
All Scenic flow, controller, and backend definitions are included below.
Optional safety/security examples are commented out below. See README.md for
their configuration and runtime requirements.
"""

# 1. MAP / CONNECTION --------------------------------------------------------
param backend = globalParameters.get("backend", "cosim")  # "cosim", "carla", "metsr".
param town = globalParameters.get("town", "Town06")  # Also configure METS-R for this town.
param address = globalParameters.get("address", "127.0.0.1")  # demo2 detects Windows CARLA under WSL.
# Default paired maps: ../../data/CARLA/<town>/facility/road/<town>.{xodr,net.xml}
# Override BOTH map and xml_map for a custom OpenDRIVE/SUMO pair:
# param map = localPath("../../data/CARLA/Town06/facility/road/Town06.xodr")
# param xml_map = localPath("../../data/CARLA/Town06/facility/road/Town06.net.xml")
# Co-simulation bubble or CARLA hybrid-physics radius (None disables hybrid physics).
param bubble_size = globalParameters.get("bubble_size", None if globalParameters.backend == "carla" else 100)

# 2. BACKGROUND VEHICLE FLOW -------------------------------------------------
# Background spawn budget; METS-R-only counts all requested trips.
param num_commuters = globalParameters.get("num_commuters", 100)
param spawn_interval_s = globalParameters.get("spawn_interval_s", 0.5)  # 2 vehicles/s; rounded up to whole ticks.
param allow_bubble_spawns = globalParameters.get("allow_bubble_spawns", False)  # Background arrivals outside ego bubble.
param timestep = globalParameters.get("timestep", 0.1)  # Simulation seconds per control tick.
param length = globalParameters.get("length", 30)  # 60 arrivals fit before this time limit.
param seed = globalParameters.get("seed", 33)  # Same sampling seed for paired comparisons.
# For a 20 Hz PCLA agent, use timestep=0.05 and a matching simulator clock.
# With demo2: --timestep 0.05 --metsr-tick-seconds 0.05, with METS-R also at 0.05.
# A control step of 0.1 is 10 Hz; it is not appropriate for every PCLA agent.

# 3. EGO CONTROLLER ---------------------------------------------------------
param ego_controller = globalParameters.get("ego_controller", "pcla" if globalParameters.backend == "cosim" else "autopilot")
param pcla_agent = globalParameters.get("pcla_agent", "simlingo_simlingo")  # e.g. "lav_fast", "lbc_lb", "tfv5_alltowns".
param pcla_route = globalParameters.get("pcla_route", None)  # Derive from the authoritative METS-R route.
# Set PCLA_HOME or demo2 --pcla-dir; install the chosen weights/dependencies.
# --pcla-agent all is a demo2 benchmark selector, not a Scenic controller.
# "external" is reserved for runner-owned experiments such as demo4.

# 4. OPTIONAL SAFETY ENCOUNTER (disabled) ------------------------------------
# param safety_scenario = "none"
# "none": ordinary traffic; no extra hazard vehicles.
# "vehicle_interception": an adjacent car crosses into/across ego's lane.
# "traffic_blockage": two stationary cars obstruct ego and an adjacent lane.
# "sudden_braking": a lead car brakes, then resumes lane following.
# "cut_in": an adjacent car starts ahead and joins ego's lane.
# "merge": an adjacent car starts behind and joins ego's lane.
# "collision_avoidance": a single stopped lead car; ego must brake or avoid it.
# Main samples a sufficiently long mapped lane (and same-direction neighbor
# where needed). Hazards are extra actors named hazard_0 / hazard_1, independent
# of num_commuters. No guaranteed collision or guaranteed successful avoidance.

# 5. OPTIONAL SECURITY EFFECT (disabled) ------------------------------------
# param security_scenario = "none"
# "sensor_spoofing": bias one speedometer, GNSS, or IMU feed delivered to PCLA.
# "traffic_control_spoofing": force one CARLA light's state during the event.
# "adversarial_perception": overlay a camera patch or black out its RGB channels.
# "adversarial_driving": corrupt ego's executed steering/throttle/brake commands.
# All except traffic-control spoofing require ego_controller="pcla". Choose one
# effect per run; combine it with any safety encounter above for a compound test.
# These are controlled synthetic perturbations, not optimized adversarial attacks.

# 6. OPTIONAL TEST PARAMETERS (uncomment only the groups you need) ------------
# The shared runtime supplies defaults; ordinary traffic needs no test config.
# param test_parameters = {
#     "placement": {"gap_m": 25.0, "road_id": None, "lane_id": None, "site_index": None},
#     "trigger": {"mode": "time", "start_s": 3.0, "distance_m": 20.0, "ttc_s": 2.0},
#     "event": {"duration_s": 2.0, "repeat_s": 0.0},
#     "hazard": {"speed_mps": 8.0, "merge_distance_m": 20.0, "lookahead_m": 5.0},
#     "sensor": {"id": None, "kind": "speedometer", "speed_bias_mps": 5.0,
#                "gnss_offset": [0.0001, 0.0001, 0.0], "imu_offset": [0, 0, 0, 0, 0, 0.1, 0]},
#     "perception": {"camera_id": None, "mode": "patch", "roi": [0.4, 0.4, 0.2, 0.2],
#                    "color_rgb": [255, 0, 0]},
#     "traffic_control": {"actor_id": None, "state": "Green", "radius_m": 80.0},
#     "driving": {"mode": "weave", "steer_bias": 0.15, "weave_amplitude": 0.3,
#                 "weave_period_s": 2.0, "throttle": 0.75}
# }
# Trigger: time, distance, or ttc; start_s is always the earliest allowed onset.
# Distance/TTC use ego/hazard centers, so select a safety encounter for these.
# repeat_s=0: one shot. Nonzero: repeat braking/security for duration_s each period.
# Cut-in/merge/interception change path once. Static obstacles exist from t=0.
# Road ID selects a Scenic road; lane_id selects its signed OpenDRIVE lane ID.
# None sensor/camera ID selects the first matching declared sensor; missing types
# fail explicitly. GNSS offsets: latitude degrees, longitude degrees, altitude m.
# IMU offsets: accel xyz (m/s^2), gyro xyz (rad/s), compass (radians).
# Camera roi: [left, top, width, height] as fractions of the image; color is RGB.
# Traffic-control states: Red / Yellow / Green / Off. None actor_id picks nearest
# light within radius_m; choose an explicit ID for a specific signalized approach.
# Driving modes: weave / steering_bias / brake_check / throttle_burst.
# Via demo2 use --safety-scenario, --security-scenario, --test-config configs/*.json.
# CLI selectors win over JSON; omitted selectors preserve the JSON/file values.

# 7. COMMON CONFIGURATION AND FLOW ------------------------------------------
import math
import os

if globalParameters.backend not in ("cosim", "carla", "metsr"):
    raise ValueError("backend must be 'cosim', 'carla', or 'metsr'")

param attack_behavior = globalParameters.get("attack_behavior", "none")
param attack_target = globalParameters.get("attack_target", "ego")
param attack_start_s = globalParameters.get("attack_start_s", 5.0)
param attack_duration_s = globalParameters.get("attack_duration_s", 1.0)
param attack_period_s = globalParameters.get("attack_period_s", 5.0)

if not isinstance(globalParameters.num_commuters, int) or globalParameters.num_commuters < 0:
    raise ValueError("num_commuters must be a nonnegative integer (excluding ego)")
for key in ("timestep", "length", "spawn_interval_s", "attack_duration_s", "attack_period_s"):
    if not math.isfinite(globalParameters[key]) or globalParameters[key] <= 0:
        raise ValueError(f"{key} must be finite and greater than zero")
if not math.isfinite(globalParameters.attack_start_s) or globalParameters.attack_start_s < 0:
    raise ValueError("attack_start_s must be finite and nonnegative")
if globalParameters.attack_duration_s > globalParameters.attack_period_s:
    raise ValueError("attack_duration_s must not exceed attack_period_s")
if globalParameters.attack_behavior not in ("none", "periodic_brake"):
    raise ValueError("attack_behavior must be 'none' or 'periodic_brake'")
target = globalParameters.attack_target
valid_commuter = (isinstance(target, str) and target.startswith("car_")
    and target[4:].isdigit() and str(int(target[4:])) == target[4:]
    and int(target[4:]) < globalParameters.num_commuters)
if target != "ego" and not valid_commuter:
    raise ValueError("attack_target must be 'ego' or an existing commuter name such as 'car_0'")


def attack_active(actor_name):
    """Periodic simulation-time window, independent of machine/controller speed."""
    if globalParameters.attack_behavior == "none" or actor_name != globalParameters.attack_target:
        return False
    elapsed = simulation().currentTime * simulation().timestep - globalParameters.attack_start_s
    if elapsed < -1e-9:
        return False
    phase = (max(0.0, elapsed) + 1e-9) % globalParameters.attack_period_s
    return phase < globalParameters.attack_duration_s


scenario ConstantFlow(spawn_one):
    """One spawn at t=0, then at each interval; remain alive after the budget ends."""
    setup:
        # Round UP to whole Scenic ticks. Smaller intervals still mean one per tick.
        interval_steps = max(1, math.ceil(globalParameters.spawn_interval_s / globalParameters.timestep - 1e-9))
    compose:
        for index in range(globalParameters.num_commuters):
            do spawn_one(index)  # Each backend's SpawnCar lasts exactly one tick.
            for unused in range(interval_steps - 1):
                wait
        while True:
            wait

from utils.scenic_testing import (
    SAFETY_SCENARIOS, SECURITY_SCENARIOS, resolve_test_parameters,
    validate_test_modes, safety_sites, merge_path, path_control,
    trigger_reached, event_active, ScenarioRuntime, scenario_test_records
)

param safety_scenario = globalParameters.get("safety_scenario", "none")
param security_scenario = globalParameters.get("security_scenario", "none")
param test_parameters = globalParameters.get("test_parameters", {})
test_config = resolve_test_parameters(globalParameters.test_parameters)
if globalParameters.safety_scenario not in SAFETY_SCENARIOS:
    raise ValueError(f"safety_scenario must be one of {SAFETY_SCENARIOS}")
if globalParameters.security_scenario not in SECURITY_SCENARIOS:
    raise ValueError(f"security_scenario must be one of {SECURITY_SCENARIOS}")
if globalParameters.security_scenario != "none" and globalParameters.safety_scenario == "none" and test_config["trigger"]["mode"] != "time":
    raise ValueError("Distance/TTC security triggers require a safety_scenario hazard; use trigger.mode='time' for security-only tests")

# 8. METS-R / CARLA CO-SIMULATION --------------------------------------------
if globalParameters.backend == "cosim":
    param map = globalParameters.get("map", localPath(f"../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr"))
    param xml_map = globalParameters.get("xml_map", localPath(f"../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.net.xml"))
    param local_commuter_poses = globalParameters.get("local_commuter_poses", ())
    param initial_pose = globalParameters.get("initial_pose", None)
    param export_folder = globalParameters.get("export_folder", localPath(f"../data_logs/{globalParameters.town}/constant_flow"))
    param run_name = globalParameters.get("run_name", f"{globalParameters.export_folder}/vehs_{globalParameters.num_commuters}_simtime_{globalParameters.length}_seed_{globalParameters.seed}")

    if globalParameters.ego_controller not in ("pcla", "autopilot", "external"):
        raise ValueError("ego_controller must be 'pcla', 'autopilot', or 'external'")
    if globalParameters.ego_controller == "external" and globalParameters.attack_behavior != "none" and globalParameters.attack_target == "ego":
        raise ValueError("External ego controls/attacks belong to the Python runner; choose attack_behavior='none'")
    if not math.isfinite(globalParameters.bubble_size) or globalParameters.bubble_size <= 0:
        raise ValueError("bubble_size must be finite and greater than zero")

    validate_test_modes(globalParameters.safety_scenario, globalParameters.security_scenario, globalParameters.ego_controller)
    if globalParameters.safety_scenario != "none" and globalParameters.initial_pose is not None:
        raise ValueError("Safety profiles place the ego; use test_parameters.placement to select the site instead of initial_pose")
    if globalParameters.safety_scenario != "none" and globalParameters.bubble_size < test_config["placement"]["gap_m"] + 15:
        raise ValueError("Safety hazards require bubble_size >= placement.gap_m + 15 meters")

    model scenic.simulators.cosim.model

    test_sites = safety_sites(network, metsrMappedLaneKeys, globalParameters.safety_scenario, test_config)

    # The fork's Python CoSimActions module caches its first CARLA Vehicle class.
    # After a town switch, put the freshly compiled map's spatial defaults/lookups
    # first, while retaining the co-simulation classes and their action protocols.
    from scenic.simulators.carla.model import Vehicle as CurrentMapVehicle
    if not issubclass(EgoCar, CurrentMapVehicle):
        class EgoCar(CurrentMapVehicle, EgoCar):
            blueprint: Uniform(*blueprints.carModels)

            @property
            def isCar(self):
                return True

        class NPCCar(CurrentMapVehicle, NPCCar):
            blueprint: Uniform(*blueprints.carModels)
            regionContainedIn: metsrMappedRoad
            position: new Point on metsrMappedRoad

            @property
            def isCar(self):
                return True


    behavior AwaitExternalPCLA():
        """demo4 owns PCLA creation, sensor processing, control and cleanup."""
        take SetAutoPilotAction(False)
        while True:
            wait


    behavior AutopilotWithBrake():
        """Example NPC/ego attack: pause route following while inside CARLA."""
        if not hasattr(self, "trajectory"):
            self.trajectory = None
        while True:
            # METS-R-only commuters retain normal route following until in the bubble.
            if attack_active(self.name) and self.carla_actor_flag and self.carlaActor is not None:
                take SetAutoPilotAction(False), SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)
            else:
                take SetAutoPilotAction(True)


    behavior TrafficDriver():
        if globalParameters.attack_behavior != "none" and self.name == globalParameters.attack_target:
            do AutopilotWithBrake()
        else:
            do FollowSingleTrajectoryBehavior()


    def load_pcla_dependencies():
        # Keep imports in a Python function: this Scenic fork does not bind imports
        # inside a behavior's persistent local namespace. Load only when PCLA starts.
        import os
        import sys
        pcla_home = os.environ.get("PCLA_HOME")
        if pcla_home and pcla_home not in sys.path:
            sys.path.append(pcla_home)
        from PCLA import PCLA
        from pcla_functions.route_maker import route_maker
        return PCLA, route_maker


    behavior PCLAAgent(agentType=globalParameters.pcla_agent, route=globalParameters.pcla_route):
        """Initialize once, keep the METS-R route, and customize the control hook below."""
        PCLA, route_maker = load_pcla_dependencies()
        assert self.carlaActor, "PCLA requires the ego's CARLA actor"
        if route is None:
            assert hasattr(self, "route") and self.route, "Missing authoritative METS-R ego route"
            route_locations = simulation().generate_carla_trajectory(route=self.route, obj=self)
            waypoints = [simulation().map.get_waypoint(location) for location in route_locations]
            waypoints = [waypoint for waypoint in waypoints if waypoint is not None]
            assert len(waypoints) > 1, "PCLA route needs at least two road waypoints"
            route = localPath("../helpers/routes/ego_route.xml")
            os.makedirs(os.path.dirname(route), exist_ok=True)
            route_maker(waypoints, savePath=route)

        # Keep this property: the runner uses obj.pcla for profiling and sensor cleanup.
        self.pcla = PCLA(agentType, self.carlaActor, route, simulation().carla_client)
        self.test_runtime.bind_pcla(self.pcla)
        while True:
            self.test_runtime.before_step()
            action = self.test_runtime.modify_control(self.pcla.get_action())
            # Sensor changes happen in the bound input hook before inference.
            # Legacy braking below takes precedence over the new command perturbation.
            if attack_active(self.name):
                take SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)
            else:
                take SetBrakeAction(action.brake), SetThrottleAction(action.throttle), SetSteerAction(action.steer)


    behavior AutopilotEgo():
        self.trajectory = None
        while True:
            self.test_runtime.before_step()
            if attack_active(self.name) and self.carla_actor_flag and self.carlaActor is not None:
                take SetAutoPilotAction(False), SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)
            else:
                take SetAutoPilotAction(True)


    behavior EgoController():
        runtime = ScenarioRuntime(simulation(), self, globalParameters.safety_scenario,
                                  globalParameters.security_scenario, test_config)
        self.test_runtime = runtime
        try:
            if globalParameters.ego_controller == "pcla":
                do PCLAAgent()
            elif globalParameters.ego_controller == "external":
                do AwaitExternalPCLA()
            else:
                do AutopilotEgo()
        finally:
            runtime.close()


    behavior SafetyHazard():
        """Customize hazard motion here, keeping the ego controller independent.

        These extra actors start inside the bubble. The controller follows a sampled
        lane centerline, then brakes or changes path when its trigger is reached.
        Cut-in/merge/interception change path once; they do not teleport or repeat.
        """
        self.trajectory = None
        self.hazard_onset = None
        self.hazard_active = False
        self.hazard_control_frames = 0
        path = self.test_path
        case = globalParameters.safety_scenario
        while True:
            now = simulation().currentTime * simulation().timestep
            if not self.carla_actor_flag or self.carlaActor is None:
                self.hazard_active = False
                take SetAutoPilotAction(True)
                continue
            if self.hazard_onset is None and (case in ("traffic_blockage", "collision_avoidance") or
                                             trigger_reached(now, simulation().objects[0], [self], test_config)):
                self.hazard_onset = now
                if case in ("cut_in", "merge", "vehicle_interception"):
                    path = merge_path(self.position, self.heading, self.test_target_path,
                                      test_config["hazard"]["merge_distance_m"], case == "vehicle_interception")
            # Static obstacles exist from t=0; braking uses the configured event window.
            braking = case in ("traffic_blockage", "collision_avoidance") or (
                case == "sudden_braking" and event_active(now, self.hazard_onset, test_config))
            self.hazard_active = braking or (case in ("cut_in", "merge", "vehicle_interception") and self.hazard_onset is not None)
            action = path_control(self, path, test_config["hazard"]["speed_mps"], test_config["hazard"]["lookahead_m"])
            self.hazard_control_frames += 1
            if braking:
                take SetAutoPilotAction(False), SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)
            else:
                take SetAutoPilotAction(False), SetBrakeAction(action.brake), SetThrottleAction(action.throttle), SetSteerAction(action.steer)


    scenario SpawnCar(index):
        # Explicit poses take precedence (demo4 uses a matched nearby traffic cohort).
        if index < len(globalParameters.local_commuter_poses):
            pose = globalParameters.local_commuter_poses[index]
            new NPCCar with name f"car_{index}", with behavior TrafficDriver(),
                at pose[0] @ pose[1], facing pose[3]
        elif not globalParameters.allow_bubble_spawns:
            target = simulation().objects[0]
            spawn_region = metsrMappedRoad.difference(target.bubble)
            new NPCCar with name f"car_{index}", with behavior TrafficDriver(), in spawn_region
        else:
            new NPCCar with name f"car_{index}", with behavior TrafficDriver()
        terminate after 1 steps


    scenario Main():
        setup:
            if globalParameters.safety_scenario != "none":
                site = Uniform(*test_sites)
                pose = site["ego_pose"]
                ego = new EgoCar at pose[0] @ pose[1], facing pose[2],
                    with name "ego", with behavior EgoController(), with test_site_id site["id"]
                for index in range(2 if globalParameters.safety_scenario == "traffic_blockage" else 1):
                    hazard = site["hazards"][index]
                    pose = hazard["pose"]
                    new NPCCar at pose[0] @ pose[1], facing pose[2],
                        with name f"hazard_{index}", with behavior SafetyHazard(),
                        with test_path hazard["path"], with test_target_path hazard["target_path"]
            elif globalParameters.initial_pose is None:
                ego = new EgoCar with name "ego", with behavior EgoController()
            else:
                pose = globalParameters.initial_pose  # Scenic (x, y, heading radians).
                ego = new EgoCar at pose[0] @ pose[1], facing pose[2],
                    with name "ego", with behavior EgoController()
            record {obj.name: obj.position for obj in simulation().objects} as all_positions
            record {obj.name: [obj.velocity.x, obj.velocity.y, obj.velocity.z] for obj in simulation().objects} as all_velocities
            record attack_active(globalParameters.attack_target) as attack_window
            record scenario_test_records(simulation()) as test_events
            if "attack_stop_index" in globalParameters:
                record globalParameters.attack_stop_index as attack_stop_index
                record globalParameters.attack_enabled as attack_enabled
        compose:
            do ConstantFlow(SpawnCar) for globalParameters.length seconds

# 9. CARLA ONLY --------------------------------------------------------------
elif globalParameters.backend == "carla":
    if globalParameters.safety_scenario != "none" or globalParameters.security_scenario != "none":
        raise ValueError("Safety/security profiles require the co-simulation traffic_template.scenic backend")

    param map = globalParameters.get("map", localPath(f"../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr"))
    if globalParameters.ego_controller != "autopilot":
        raise ValueError("CARLA-only presets use autopilot; use traffic_template.scenic for PCLA")

    model scenic.simulators.carla.model


    behavior TrafficDriver():
        # CARLA's SetAutopilotAction spelling differs from co-simulation's action.
        was_attacking = None
        while True:
            attacking = attack_active(self.name)
            if attacking:
                take SetAutopilotAction(False), SetBrakeAction(1), SetThrottleAction(0), SetSteerAction(0)
            elif was_attacking is None or was_attacking:
                take SetBrakeAction(0), SetAutopilotAction(True)
            else:
                wait
            was_attacking = attacking


    scenario SpawnCar(index):
        new Car with name f"car_{index}", with behavior TrafficDriver()
        terminate after 1 steps


    scenario Main():
        setup:
            ego = new Car with name "ego", with rolename "hero", with behavior TrafficDriver()
            record {obj.name: obj.position for obj in simulation().objects} as all_positions
            record attack_active(globalParameters.attack_target) as attack_window
        compose:
            do ConstantFlow(SpawnCar) for globalParameters.length seconds

# 10. METS-R ONLY ------------------------------------------------------------
else:
    if globalParameters.safety_scenario != "none" or globalParameters.security_scenario != "none":
        raise ValueError("Safety/security profiles require the co-simulation traffic_template.scenic backend")

    param map = globalParameters.get("map", "Data.properties.CARLA")
    param metsr_host = globalParameters.get("metsr_host", "localhost")
    param metsr_port = globalParameters.get("metsr_port", 4000)
    param simTimestep = globalParameters.get("simTimestep", 0.1)
    param origin = globalParameters.get("origin", -1)
    param destination = globalParameters.get("destination", -1)
    if globalParameters.attack_behavior != "none":
        raise ValueError("The braking control example requires CARLA; METS-R-only supports attack_behavior='none'")

    model scenic.simulators.metsr.model

    # The fork's model hardcodes connection options; expose them in this preset.
    # map_name is metadata in this adapter: it does NOT switch the server's network.
    simulator METSRSimulator(host=globalParameters.metsr_host, port=globalParameters.metsr_port,
        map_name=globalParameters.map, timestep=globalParameters.timestep,
        sim_timestep=globalParameters.simTimestep, verbose=globalParameters.verbose)


    scenario SpawnCar(index):
        new PrivateCar with name f"car_{index}",
            with origin globalParameters.origin, with destination globalParameters.destination
        terminate after 1 steps


    scenario Main():
        compose:
            # No physical ego/PCLA actor in this backend; num_commuters is total demand.
            do ConstantFlow(SpawnCar) for globalParameters.length seconds
