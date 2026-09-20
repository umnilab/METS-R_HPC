"""Runtime support for the reusable Scenic safety/security template.

Scenic owns actor placement and scheduling. This module provides map geometry,
simple hazard controls, copied sensor perturbations and reversible CARLA effects.
No simulator connection or optional controller import occurs at module import.
"""
from copy import deepcopy
import math
from types import SimpleNamespace

SAFETY_SCENARIOS = ("none", "vehicle_interception", "traffic_blockage", "sudden_braking",
                    "cut_in", "merge", "collision_avoidance")
SECURITY_SCENARIOS = ("none", "sensor_spoofing", "traffic_control_spoofing",
                      "adversarial_perception", "adversarial_driving")
DEFAULT_TEST_PARAMETERS = {
    "placement": {"gap_m": 25.0, "road_id": None, "lane_id": None, "site_index": None},
    "trigger": {"mode": "time", "start_s": 3.0, "distance_m": 20.0, "ttc_s": 2.0},
    "event": {"duration_s": 2.0, "repeat_s": 0.0},
    "hazard": {"speed_mps": 8.0, "merge_distance_m": 20.0, "lookahead_m": 5.0},
    "sensor": {"id": None, "kind": "speedometer", "speed_bias_mps": 5.0,
               "gnss_offset": [0.0001, 0.0001, 0.0], "imu_offset": [0, 0, 0, 0, 0, 0.1, 0]},
    "perception": {"camera_id": None, "mode": "patch", "roi": [0.4, 0.4, 0.2, 0.2],
                   "color_rgb": [255, 0, 0]},
    "traffic_control": {"actor_id": None, "state": "Green", "radius_m": 80.0},
    "driving": {"mode": "weave", "steer_bias": 0.15, "weave_amplitude": 0.3,
                "weave_period_s": 2.0, "throttle": 0.75},
}


def resolve_test_parameters(overrides=None):
    """Merge partial groups and reject misspellings/invalid physical values."""
    config = deepcopy(DEFAULT_TEST_PARAMETERS)
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError("test_parameters must be a dictionary")
    for group, values in overrides.items():
        if group not in config or not isinstance(values, dict):
            raise ValueError(f"Unknown or invalid test_parameters group: {group}")
        for key, value in values.items():
            if key not in config[group]:
                raise ValueError(f"Unknown test parameter: {group}.{key}")
            config[group][key] = deepcopy(value)
    choices = {("trigger", "mode"): ("time", "distance", "ttc"),
               ("sensor", "kind"): ("speedometer", "gnss", "imu"),
               ("perception", "mode"): ("patch", "blackout"),
               ("traffic_control", "state"): ("Red", "Yellow", "Green", "Off"),
               ("driving", "mode"): ("weave", "steering_bias", "brake_check", "throttle_burst")}
    for (group, key), allowed in choices.items():
        if config[group][key] not in allowed:
            raise ValueError(f"{group}.{key} must be one of {allowed}")
    for group, key, minimum, maximum in (
        ("placement", "gap_m", 8, None), ("trigger", "start_s", 0, None),
        ("trigger", "distance_m", .01, None), ("trigger", "ttc_s", .01, None),
        ("event", "duration_s", .01, None), ("event", "repeat_s", 0, None),
        ("hazard", "speed_mps", 0, None), ("hazard", "merge_distance_m", 5, None),
        ("hazard", "lookahead_m", 1, None), ("traffic_control", "radius_m", 1, None),
        ("sensor", "speed_bias_mps", None, None), ("driving", "steer_bias", -1, 1),
        ("driving", "weave_amplitude", 0, 1), ("driving", "weave_period_s", .01, None),
        ("driving", "throttle", 0, 1),
    ):
        value = config[group][key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"{group}.{key} must be a finite number")
        if (minimum is not None and value < minimum) or (maximum is not None and value > maximum):
            raise ValueError(f"{group}.{key} is out of range [{minimum}, {maximum}]")
    if 0 < config["event"]["repeat_s"] < config["event"]["duration_s"]:
        raise ValueError("event.repeat_s must be zero (one shot) or at least event.duration_s")
    for group, key, size in (("sensor", "gnss_offset", 3), ("sensor", "imu_offset", 7),
                             ("perception", "roi", 4), ("perception", "color_rgb", 3)):
        values = config[group][key]
        if not isinstance(values, (list, tuple)) or len(values) != size or any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in values
        ):
            raise ValueError(f"{group}.{key} must contain {size} finite numbers")
    x, y, width, height = config["perception"]["roi"]
    if min(x, y) < 0 or min(width, height) <= 0 or x + width > 1 or y + height > 1:
        raise ValueError("perception.roi must be an in-image [x, y, width, height] fraction")
    if any(not 0 <= value <= 255 for value in config["perception"]["color_rgb"]):
        raise ValueError("perception.color_rgb components must be in [0, 255]")
    for group, key in (("placement", "site_index"), ("traffic_control", "actor_id")):
        value = config[group][key]
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"{group}.{key} must be a nonnegative integer or null")
    return config


def validate_test_modes(safety, security, controller):
    if safety not in SAFETY_SCENARIOS:
        raise ValueError(f"safety_scenario must be one of {SAFETY_SCENARIOS}")
    if security not in SECURITY_SCENARIOS:
        raise ValueError(f"security_scenario must be one of {SECURITY_SCENARIOS}")
    if controller == "external" and (safety != "none" or security != "none"):
        raise ValueError("External-controller presets own their placement and attacks; use the PCLA/autopilot template")
    if security in ("sensor_spoofing", "adversarial_perception", "adversarial_driving") and controller != "pcla":
        raise ValueError(f"{security} requires ego_controller='pcla'")


def _line(points):
    from shapely.geometry import LineString
    return LineString([(float(p[0]), float(p[1])) for p in points])


def _pose(line, distance):
    distance = max(0., min(line.length, distance))
    point = line.interpolate(distance)
    before = line.interpolate(max(0., distance - .1))
    after = line.interpolate(min(line.length, distance + .1))
    return (point.x, point.y, math.atan2(-(after.x - before.x), after.y - before.y))


def safety_sites(network, mapped_keys, case, config):
    """Sample valid lane geometry; no hardcoded Town05/Town06 coordinates.

    Interception is a cross-lane encroachment on a multilane road. Cut-in joins
    ahead; merge joins from behind. All initial vehicle centers lie on mapped lanes.
    """
    if case == "none":
        return ()
    from shapely.geometry import Point
    gap = config["placement"]["gap_m"]
    plans = []
    for lane in network.laneSections:
        if f"{lane.road.id}_{lane.lane.id}" not in mapped_keys:
            continue
        if config["placement"]["road_id"] is not None and str(lane.road.id) != str(config["placement"]["road_id"]):
            continue
        if config["placement"]["lane_id"] is not None and str(lane.openDriveID) != str(config["placement"]["lane_id"]):
            continue
        line = _line(lane.centerline.points)
        if line.length < 2 * gap + 35:
            continue
        ego_s = gap + 10
        ego_pose = _pose(line, ego_s)
        same_path = tuple(line.coords)
        adjacent = [other for other in lane.adjacentLanes
                    if other.isForward == lane.isForward and f"{other.road.id}_{other.lane.id}" in mapped_keys]
        if case in ("cut_in", "merge", "vehicle_interception", "traffic_blockage") and not adjacent:
            continue
        hazard_lane = adjacent[0] if case in ("cut_in", "merge", "vehicle_interception") else lane
        hazard_line = _line(hazard_lane.centerline.points)
        anchor = hazard_line.project(Point(ego_pose[:2]))
        offset = -gap * .5 if case == "merge" else gap
        if anchor + offset < 8 or anchor + offset + config["hazard"]["merge_distance_m"] > hazard_line.length - 8:
            continue
        hazard_pose = _pose(hazard_line, anchor + offset)
        hazards = [{"pose": hazard_pose, "path": tuple(hazard_line.coords), "target_path": same_path}]
        if case == "traffic_blockage":
            other_line = _line(adjacent[0].centerline.points)
            other_s = other_line.project(Point(hazard_pose[:2]))
            hazards.append({"pose": _pose(other_line, other_s), "path": tuple(other_line.coords), "target_path": same_path})
        plans.append({"id": f"{lane.road.id}:{lane.openDriveID}:{len(plans)}", "ego_pose": ego_pose,
                      "ego_path": same_path, "hazards": hazards})
    if not plans:
        raise ValueError(f"No mapped lane geometry supports {case}; choose another town/road or reduce placement.gap_m")
    index = config["placement"]["site_index"]
    if index is not None:
        if index >= len(plans):
            raise ValueError(f"placement.site_index={index} exceeds the {len(plans)} available sites")
        plans = [plans[index]]
    return tuple(plans)


def merge_path(position, heading, target_points, distance, cross_lane=False):
    """A cubic path to the target lane; interception continues across its center."""
    from shapely.geometry import Point
    line = _line(target_points)
    end_s = min(line.length, line.project(Point(float(position.x), float(position.y))) + distance)
    end_x, end_y, end_heading = _pose(line, end_s)
    start = (float(position.x), float(position.y))
    end = (end_x, end_y)
    if cross_lane:
        # End one meter beyond the lane center, on the opposite side to the start.
        normal = (math.cos(end_heading), math.sin(end_heading))
        side = (start[0] - end[0]) * normal[0] + (start[1] - end[1]) * normal[1]
        direction = -1 if side > 0 else 1
        end = (end[0] + direction * normal[0], end[1] + direction * normal[1])
        end_heading = math.atan2(-(end[0] - start[0]), end[1] - start[1])
    p1 = (start[0] - math.sin(heading) * distance / 3, start[1] + math.cos(heading) * distance / 3)
    p2 = (end[0] + math.sin(end_heading) * distance / 3, end[1] - math.cos(end_heading) * distance / 3)
    result = []
    for i in range(21):
        t = i / 20
        result.append(tuple((1-t)**3*start[j] + 3*(1-t)**2*t*p1[j] + 3*(1-t)*t*t*p2[j] + t**3*end[j] for j in (0, 1)))
    if not cross_lane:
        result.extend((point.x, point.y) for point in (line.interpolate(s) for s in range(math.ceil(end_s)+1, math.floor(line.length)+1, 2)))
    return result


def path_control(obj, points, speed_mps, lookahead_m):
    """Small pure-pursuit/P controller for the hazard, independent of the ego agent."""
    from shapely.geometry import Point
    line = _line(points)
    progress = line.project(Point(float(obj.position.x), float(obj.position.y)))
    target = line.interpolate(min(line.length, progress + lookahead_m))
    heading = math.atan2(-(target.x - obj.position.x), target.y - obj.position.y)
    error = math.remainder(heading - float(obj.heading), 2 * math.pi)
    steer = max(-1., min(1., -1.5 * error))
    if line.length - progress < 1:
        speed_mps = 0
    speed_error = speed_mps - float(obj.speed or 0)
    return SimpleNamespace(brake=min(1., max(0., -speed_error * .25)),
                           throttle=min(.75, max(0., speed_error * .3)), steer=steer)


def encounter(ego, other):
    dx, dy = other.position.x - ego.position.x, other.position.y - ego.position.y
    distance = math.hypot(dx, dy)
    dvx = other.velocity.x - ego.velocity.x
    dvy = other.velocity.y - ego.velocity.y
    closing = -(dx * dvx + dy * dvy) / max(distance, 1e-9)
    return distance, distance / closing if closing > .01 else None


def trigger_reached(now, ego, hazards, config):
    trigger = config["trigger"]
    if now + 1e-9 < trigger["start_s"]:
        return False
    if trigger["mode"] == "time":
        return True
    pairs = [encounter(ego, obj) for obj in hazards]
    if trigger["mode"] == "distance":
        return any(distance <= trigger["distance_m"] for distance, _ in pairs)
    return any(ttc is not None and ttc <= trigger["ttc_s"] for _, ttc in pairs)


def event_active(now, onset, config):
    if onset is None or now + 1e-9 < onset:
        return False
    elapsed = max(0., now - onset)
    period = config["event"]["repeat_s"]
    phase = (elapsed + 1e-9) % period if period else elapsed + 1e-9
    return phase < config["event"]["duration_s"]


def perturb_sensor_batch(batch, sensor_id, security, config):
    """Copy only the affected frame, preserving its timestamp, shape and channels."""
    import numpy as np
    if sensor_id not in batch:
        raise RuntimeError(f"PCLA sensor batch is missing configured sensor {sensor_id!r}")
    frame, payload = batch[sensor_id]
    if security == "sensor_spoofing":
        kind = config["sensor"]["kind"]
        if kind == "speedometer":
            payload = dict(payload)
            payload["speed"] = max(0., float(payload["speed"]) + config["sensor"]["speed_bias_mps"])
        else:
            payload = np.array(payload, copy=True)
            offset = config["sensor"]["gnss_offset" if kind == "gnss" else "imu_offset"]
            if payload.shape != (len(offset),):
                raise RuntimeError(f"Unexpected {kind} sensor shape: {payload.shape}")
            payload = payload.astype(float) + np.asarray(offset)
    else:
        payload = np.array(payload, copy=True)
        if payload.ndim != 3 or payload.shape[2] not in (3, 4):
            raise RuntimeError(f"Expected an RGB camera's BGR/BGRA frame, got {payload.shape}")
        if config["perception"]["mode"] == "blackout":
            payload[..., :3] = 0
        else:
            height, width = payload.shape[:2]
            x, y, w, h = config["perception"]["roi"]
            x0, y0 = int(x * width), int(y * height)
            x1, y1 = min(width, max(x0+1, math.ceil((x+w)*width))), min(height, max(y0+1, math.ceil((y+h)*height)))
            payload[y0:y1, x0:x1, :3] = config["perception"]["color_rgb"][::-1]
    result = dict(batch)
    result[sensor_id] = (frame, payload)
    return result


class ScenarioRuntime:
    """One owner for a trial's sensor hook, light override, and records."""
    def __init__(self, simulation, ego, safety, security, config):
        self.simulation, self.ego = simulation, ego
        self.safety, self.security, self.config = safety, security, config
        self.onset = None
        self.active = False
        self.closed = False
        self._sensor_restore = None
        self._light = None
        self._light_state = None
        # Scenic restores object proxies before generator finalizers in some forks.
        # Close effects before simulator teardown too, including inference failures.
        self._original_destroy = simulation.destroy
        self._had_destroy_override = "destroy" in vars(simulation)
        self._previous_destroy = vars(simulation).get("destroy")
        def destroy(*args, **kwargs):
            try:
                self.close()
            finally:
                self._original_destroy(*args, **kwargs)
        simulation.destroy = destroy
        self.stats = {"security_active": False, "security_onset_s": None, "sensor_frames_modified": 0,
                      "control_frames_modified": 0, "traffic_light_frames_modified": 0,
                      "traffic_light_id": None, "minimum_center_distance_m": None,
                      "minimum_center_closing_ttc_s": None}

    def bind_pcla(self, pcla):
        if self.security not in ("sensor_spoofing", "adversarial_perception"):
            return
        specs = pcla.agent_instance.sensors()
        expected_type = "sensor.camera.rgb" if self.security == "adversarial_perception" else {
            "speedometer": "sensor.speedometer", "gnss": "sensor.other.gnss", "imu": "sensor.other.imu"
        }[self.config["sensor"]["kind"]]
        requested = self.config["perception"]["camera_id"] if self.security == "adversarial_perception" else self.config["sensor"]["id"]
        matches = [spec["id"] for spec in specs if spec["type"] == expected_type and (requested is None or spec["id"] == requested)]
        if not matches:
            raise ValueError(f"Controller has no matching {expected_type} sensor (requested ID {requested!r})")
        sensor_id = matches[0]
        self.stats["sensor_id"] = sensor_id
        interface = pcla.agent_instance.sensor_interface
        original = interface.get_data
        had_override = "get_data" in vars(interface)
        previous = vars(interface).get("get_data")
        def get_data(*args, **kwargs):
            batch = original(*args, **kwargs)
            if not self.active:
                return batch
            modified = perturb_sensor_batch(batch, sensor_id, self.security, self.config)
            self.stats["sensor_frames_modified"] += 1
            return modified
        interface.get_data = get_data
        def restore():
            if had_override:
                interface.get_data = previous
            else:
                del interface.get_data
        self._sensor_restore = restore

    def before_step(self):
        now = self.simulation.currentTime * self.simulation.timestep
        hazards = [obj for obj in self.simulation.objects if str(getattr(obj, "name", "")).startswith("hazard_")]
        for other in hazards:
            distance, ttc = encounter(self.ego, other)
            for key, value in (("minimum_center_distance_m", distance), ("minimum_center_closing_ttc_s", ttc)):
                old = self.stats[key]
                if value is not None and (old is None or value < old):
                    self.stats[key] = float(value)
        if self.onset is None and self.security != "none" and trigger_reached(now, self.ego, hazards, self.config):
            self.onset = now
            self.stats["security_onset_s"] = now
        self.active = self.security != "none" and event_active(now, self.onset, self.config)
        self.stats["security_active"] = self.active
        if self.security == "traffic_control_spoofing":
            if self.active:
                self._apply_light()
            else:
                self._restore_light()

    def _apply_light(self):
        import carla
        if self._light is None:
            world = self.simulation.carla_world
            actor_id = self.config["traffic_control"]["actor_id"]
            if actor_id is not None:
                light = world.get_actor(actor_id)
                if light is None or not str(light.type_id).startswith("traffic.traffic_light"):
                    raise ValueError(f"No CARLA traffic light with actor ID {actor_id}")
            else:
                location = self.ego.carlaActor.get_location()
                lights = list(world.get_actors().filter("traffic.traffic_light*"))
                lights = [actor for actor in lights if actor.get_location().distance(location) <= self.config["traffic_control"]["radius_m"]]
                if not lights:
                    raise ValueError("No traffic light in traffic_control.radius_m; select a signalized road/site or actor_id")
                light = min(lights, key=lambda actor: (actor.get_location().distance(location), actor.id))
            self._light, self._light_state = light, light.get_state()
            self.stats["traffic_light_id"] = light.id
        # Do not call freeze(): CARLA 0.9.15 freezes ALL lights, not this actor alone.
        self._light.set_state(getattr(carla.TrafficLightState, self.config["traffic_control"]["state"]))
        self.stats["traffic_light_frames_modified"] += 1

    def _restore_light(self):
        if self._light is not None:
            self._light.set_state(self._light_state)
            self._light = self._light_state = None

    def modify_control(self, action):
        if self.security != "adversarial_driving" or not self.active:
            return action
        cfg = self.config["driving"]
        result = SimpleNamespace(brake=float(action.brake), throttle=float(action.throttle), steer=float(action.steer))
        now = self.simulation.currentTime * self.simulation.timestep
        if cfg["mode"] == "brake_check":
            result.brake, result.throttle = 1., 0.
        elif cfg["mode"] == "throttle_burst":
            result.brake, result.throttle = 0., cfg["throttle"]
        else:
            bias = cfg["steer_bias"] if cfg["mode"] == "steering_bias" else cfg["weave_amplitude"] * math.sin(2*math.pi*(now-self.onset)/cfg["weave_period_s"])
            result.steer = max(-1., min(1., result.steer + bias))
        self.stats["control_frames_modified"] += 1
        return result

    def snapshot(self):
        return dict(self.stats)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self._restore_light()
        finally:
            try:
                if self._sensor_restore:
                    self._sensor_restore()
            finally:
                if self._had_destroy_override:
                    self.simulation.destroy = self._previous_destroy
                else:
                    del self.simulation.destroy


def scenario_test_records(simulation):
    """Fresh records, including hazard application rather than just trigger intent."""
    ego = simulation.objects[0]
    runtime = getattr(ego, "test_runtime", None)
    records = runtime.snapshot() if runtime is not None else {}
    records["site_id"] = getattr(ego, "test_site_id", None)
    records["hazards"] = {
        obj.name: {"onset_s": getattr(obj, "hazard_onset", None),
                   "active": getattr(obj, "hazard_active", False),
                   "control_frames": getattr(obj, "hazard_control_frames", 0)}
        for obj in simulation.objects if str(getattr(obj, "name", "")).startswith("hazard_")
    }
    return records


def load_test_config(path):
    """Read a demo2 JSON profile, keeping command-line selectors authoritative."""
    import json
    from pathlib import Path
    with Path(path).open(encoding="utf-8") as stream:
        config = json.load(stream)
    allowed = {"safety_scenario", "security_scenario", "test_parameters"}
    if not isinstance(config, dict) or set(config) - allowed:
        raise ValueError(f"Test config must contain only {sorted(allowed)}")
    for name, choices in (("safety_scenario", SAFETY_SCENARIOS), ("security_scenario", SECURITY_SCENARIOS)):
        if name in config and config[name] not in choices:
            raise ValueError(f"{name} must be one of {choices}")
    if "test_parameters" in config:
        config["test_parameters"] = resolve_test_parameters(config["test_parameters"])
    return config


def write_test_report(result, params, path):
    """Persist profile evidence beside demo2 outputs, outside benchmark timing."""
    if params.get("safety_scenario", "none") == params.get("security_scenario", "none") == "none":
        return
    import json
    from pathlib import Path
    records = result.records.get("test_events", ())
    timestep = float(params["timestep"])
    report = {
        "safety_scenario": params.get("safety_scenario", "none"),
        "security_scenario": params.get("security_scenario", "none"),
        "test_parameters": resolve_test_parameters(params.get("test_parameters")),
        "town": params.get("town"), "seed": params.get("seed"),
        "pcla_agent": params.get("pcla_agent"), "timestep_s": timestep,
        "termination_reason": str(result.terminationReason),
        "samples": [{"step": int(step), "simulation_time_s": step * timestep, **record} for step, record in records],
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
