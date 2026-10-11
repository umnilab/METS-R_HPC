"""Own a trial's effect scheduling, sensor hooks, reversible state, and records."""
from __future__ import annotations

import math
from types import SimpleNamespace
from typing import Any, Mapping

from .lifecycle import register_pre_actor_teardown
from .sensors import perturb_sensor_batch
from .timing import encounter, event_active, trigger_reached


class ScenarioRuntime:
    """Manage one effect for one ego; the caller owns simulation steps and PCLA."""

    def __init__(
        self,
        simulation: Any,
        ego: Any,
        safety: str,
        security: str,
        config: Mapping[str, Any],
    ) -> None:
        self.simulation = simulation
        self.ego = ego
        self.safety = safety
        self.security = security
        self.config = config
        self.onset = None
        self.active = False
        self.closed = False
        self._sensor_restore = None
        self._bound_pcla = None
        self.stop_sign_tap = None
        self.stop_candidate = None
        self._light = None
        self._light_state = None
        # Some Scenic forks restore object proxies before behavior finalizers.
        # Also close effects before actor teardown, including inference failures.
        self._release_teardown = register_pre_actor_teardown(simulation, self.close)
        self.stats = {
            "security_active": False,
            "security_onset_s": None,
            "sensor_frames_modified": 0,
            "control_frames_modified": 0,
            "traffic_light_frames_modified": 0,
            "traffic_light_id": None,
            "minimum_center_distance_m": None,
            "minimum_center_closing_ttc_s": None,
        }

    def bind_pcla(self, pcla: Any, *, stop_candidate: Any = None) -> Any:
        """Install one input hook, optionally using a caller-selected stop target."""
        if self.closed:
            raise RuntimeError("Cannot bind a controller to a closed scenario runtime")
        if self._bound_pcla is not None:
            raise RuntimeError("Scenario runtime already has a bound PCLA controller")
        if self.security == "stop_sign_patch":
            from .stop_sign import StopSignColorPatchTap, StopSignPatchSettings, discover_stop_signs

            stop_config = self.config["stop_sign"]
            if stop_candidate is None:
                import carla

                stop_candidates = discover_stop_signs(
                    self.simulation.carla_world, carla, approach_distance_m=35.0,
                )
                stop_candidate = next(
                    (target for target in stop_candidates if target.index == stop_config["index"]),
                    None,
                )
                if stop_candidate is None:
                    indices = [target.index for target in stop_candidates]
                    raise ValueError(
                        f"No modeled stop sign at stop_sign.index={stop_config['index']}; "
                        f"available indices: {indices}"
                    )
            actor = getattr(self.ego, "carlaActor", None)
            if actor is None:
                raise RuntimeError("Stop-sign patch requires the ego's CARLA actor")
            settings = StopSignPatchSettings(
                attack_max_distance_m=stop_config["max_distance_m"],
                attack_min_red=stop_config["minimum_red"],
                attack_red_dominance=stop_config["red_dominance"],
                attack_roi_scale=stop_config["roi_scale"],
                attack_min_patch_pixels=stop_config["min_patch_pixels"],
            )
            tap = StopSignColorPatchTap(
                pcla, actor, stop_candidate, stop_config["enabled"], settings,
                active=lambda: self.active and not self.closed,
            )
            self.stop_sign_tap = tap
            self.stop_candidate = stop_candidate
            self._sensor_restore = tap.close
            self._bound_pcla = pcla
            return tap
        if self.security not in ("sensor_spoofing", "adversarial_perception"):
            return

        if self.security == "adversarial_perception":
            expected_type = "sensor.camera.rgb"
            requested_id = self.config["perception"]["camera_id"]
        else:
            sensor_types = {
                "speedometer": "sensor.speedometer",
                "gnss": "sensor.other.gnss",
                "imu": "sensor.other.imu",
            }
            expected_type = sensor_types[self.config["sensor"]["kind"]]
            requested_id = self.config["sensor"]["id"]
        specs = pcla.agent_instance.sensors()
        matches = [
            spec["id"] for spec in specs
            if spec["type"] == expected_type
            and (requested_id is None or spec["id"] == requested_id)
        ]
        if not matches:
            raise ValueError(
                f"Controller has no matching {expected_type} sensor "
                f"(requested ID {requested_id!r})"
            )
        sensor_id = matches[0]
        self.stats["sensor_id"] = sensor_id
        interface = pcla.agent_instance.sensor_interface
        original_get_data = interface.get_data
        had_override = "get_data" in vars(interface)
        previous_get_data = vars(interface).get("get_data")

        def get_data(*args, **kwargs):
            batch = original_get_data(*args, **kwargs)
            if not self.active:
                return batch
            modified = perturb_sensor_batch(batch, sensor_id, self.security, self.config)
            self.stats["sensor_frames_modified"] += 1
            return modified

        interface.get_data = get_data

        def restore():
            if had_override:
                interface.get_data = previous_get_data
            else:
                del interface.get_data

        self._sensor_restore = restore
        self._bound_pcla = pcla

    def before_step(self) -> None:
        """Latch event onset, update encounter records, and apply scheduled lights."""
        now = self.simulation.currentTime * self.simulation.timestep
        hazards = [
            obj for obj in self.simulation.objects
            if str(getattr(obj, "name", "")).startswith("hazard_")
        ]
        for hazard in hazards:
            distance, ttc = encounter(self.ego, hazard)
            for key, value in (
                ("minimum_center_distance_m", distance),
                ("minimum_center_closing_ttc_s", ttc),
            ):
                previous_minimum = self.stats[key]
                if value is not None and (previous_minimum is None or value < previous_minimum):
                    self.stats[key] = float(value)
        enabled = self.security != "none" and (
            self.security != "stop_sign_patch" or self.config["stop_sign"]["enabled"]
        )
        if self.onset is None and enabled and trigger_reached(now, self.ego, hazards, self.config):
            self.onset = now
            self.stats["security_onset_s"] = now
        self.active = enabled and event_active(now, self.onset, self.config)
        self.stats["security_active"] = self.active
        if self.security != "traffic_control_spoofing":
            return
        if not self.active:
            self._restore_light()
            return
        import carla

        light_config = self.config["traffic_control"]
        if self._light is None:
            world = self.simulation.carla_world
            actor_id = light_config["actor_id"]
            if actor_id is not None:
                light = world.get_actor(actor_id)
                if light is None or not str(light.type_id).startswith("traffic.traffic_light"):
                    raise ValueError(f"No CARLA traffic light with actor ID {actor_id}")
            else:
                location = self.ego.carlaActor.get_location()
                lights = list(world.get_actors().filter("traffic.traffic_light*"))
                lights = [
                    actor for actor in lights
                    if actor.get_location().distance(location) <= light_config["radius_m"]
                ]
                if not lights:
                    raise ValueError(
                        "No traffic light in traffic_control.radius_m; "
                        "select a signalized road/site or actor_id"
                    )
                light = min(lights, key=lambda actor: (actor.get_location().distance(location), actor.id))
            previous_state = light.get_state()
            self._light = light
            self._light_state = previous_state
            self.stats["traffic_light_id"] = light.id
        # CARLA 0.9.15 freeze() affects all lights, rather than only this actor.
        self._light.set_state(getattr(carla.TrafficLightState, light_config["state"]))
        self.stats["traffic_light_frames_modified"] += 1

    def _restore_light(self) -> None:
        if self._light is not None:
            self._light.set_state(self._light_state)
            self._light = None
            self._light_state = None

    def modify_control(self, action: Any) -> Any:
        """Return an altered copy during command attacks; otherwise return the input."""
        if self.security != "adversarial_driving" or not self.active:
            return action
        driving = self.config["driving"]
        result = SimpleNamespace(
            brake=float(action.brake), throttle=float(action.throttle), steer=float(action.steer),
        )
        now = self.simulation.currentTime * self.simulation.timestep
        if driving["mode"] == "brake_check":
            result.brake = 1.0
            result.throttle = 0.0
        elif driving["mode"] == "throttle_burst":
            result.brake = 0.0
            result.throttle = driving["throttle"]
        else:
            if driving["mode"] == "steering_bias":
                bias = driving["steer_bias"]
            else:
                phase = 2 * math.pi * (now - self.onset) / driving["weave_period_s"]
                bias = driving["weave_amplitude"] * math.sin(phase)
            result.steer = max(-1.0, min(1.0, result.steer + bias))
        self.stats["control_frames_modified"] += 1
        return result

    def snapshot(self) -> dict[str, Any]:
        """Return independent counters, including the selected stop and patch evidence."""
        result = dict(self.stats)
        if self.security == "stop_sign_patch":
            tap = self.stop_sign_tap
            candidate = self.stop_candidate
            stop = {
                "enabled": self.config["stop_sign"]["enabled"],
                "candidate_index": self.config["stop_sign"]["index"] if candidate is None else candidate.index,
            }
            if candidate is not None:
                stop.update(
                    candidate_id=candidate.actor_id,
                    visual_id=candidate.visual_id,
                    target={
                        axis: float(getattr(candidate.visual_location, axis))
                        for axis in ("x", "y", "z")
                    },
                )
            for key in (
                "frames_patched", "pixels_patched", "projected_frames", "meaningful_patched_frames",
                "max_pixels_patched_per_frame",
            ):
                stop[key] = getattr(tap, key, 0)
            for key in (
                "closest_patched_distance_m", "last_target_distance_m", "minimum_target_distance_m",
            ):
                stop[key] = getattr(tap, key, None)
            result["sensor_frames_modified"] = stop["frames_patched"]
            result["stop_sign"] = stop
        return result

    def close(self) -> None:
        """Restore owned sensor/light hooks once, even when restoration raises."""
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
                self._release_teardown()


def scenario_test_records(simulation: Any) -> dict[str, Any]:
    """Return fresh effect and hazard records, including issued-control counts."""
    ego = simulation.objects[0]
    runtime = getattr(ego, "test_runtime", None)
    records = runtime.snapshot() if runtime is not None else {}
    records["site_id"] = getattr(ego, "test_site_id", None)
    records["hazards"] = {
        obj.name: {
            "onset_s": getattr(obj, "hazard_onset", None),
            "active": getattr(obj, "hazard_active", False),
            "control_frames": getattr(obj, "hazard_control_frames", 0),
        }
        for obj in simulation.objects
        if str(getattr(obj, "name", "")).startswith("hazard_")
    }
    return records
