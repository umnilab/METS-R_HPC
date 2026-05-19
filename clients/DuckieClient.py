"""
Co-simulation client for Duckietown and METS-R.

DuckieClient mirrors the orchestration style used by CoSimClient: it keeps a
config object, owns the Duckietown WebSocket/process side, owns or receives a
METSRClient, and exposes plain Python methods for query/control/sync flows.
"""

import base64
import json
import math
import os
import subprocess
import threading
import time
from collections import defaultdict, deque

from websockets.sync.client import connect

from clients.METSRClient import METSRClient


DUCKIETOWN_FUNCTIONS = {
      "lane_following": {
            "launcher": "lane-following.sh",
            "package": "duckietown_demos",
            "launch_file": "lane_following.launch",
            "description": (
                  "Starts image processing, line detection, ground projection, "
                  "lane filtering, lane control, FSM/switching, and wheel output."
            ),
      },
      "apriltag_detection": {
            "launcher": "apriltag-detector.sh",
            "package": "apriltag",
            "launch_file": "apriltag_detector_node.launch",
            "description": (
                  "Detects AprilTags from the camera stream for intersections, "
                  "tag-based turns, and road-sign recognition."
            ),
      },
      "communication_demo": {
            "launcher": "communication.sh",
            "package": "duckietown_demos",
            "launch_file": "communication.launch",
            "description": (
                  "Starts the traffic-light and stop-sign communication stack."
            ),
      },
      "traffic_light_response": {
            "launcher": "communication.sh",
            "package": "duckietown_demos",
            "launch_file": "communication.launch",
            "description": "Runs traffic-light interaction behavior.",
      },
      "stop_sign_negotiation": {
            "launcher": "communication.sh",
            "package": "duckietown_demos",
            "launch_file": "communication.launch",
            "description": "Runs stop-sign intersection negotiation behavior.",
      },
      "led_emitter": {
            "launcher": "led-emitter.sh",
            "package": "led_emitter",
            "launch_file": "led_emitter_node.launch",
            "description": "Starts the LED emitter node for robot state display.",
      },
      "object_detection": {
            "launcher": "object-detector.sh",
            "package": "object_detection",
            "launch_file": "object_detector_node.launch",
            "description": (
                  "Starts object detection for YOLO-style objects, traffic lights, "
                  "or signs."
            ),
      },
}


def _config_get(config, name, default=None):
      if config is None:
            return default
      if isinstance(config, dict):
            return config.get(name, default)
      return getattr(config, name, default)


def _now():
      return time.time()


def _as_list(value):
      if value is None:
            return []
      if isinstance(value, list):
            return value
      if isinstance(value, tuple):
            return list(value)
      return [value]


def _dict_like(value):
      return isinstance(value, dict)


def _nested_get(data, *path, default=None):
      current = data
      for key in path:
            if not isinstance(current, dict) or key not in current:
                  return default
            current = current[key]
      return current


class DuckieClient(object):
      def __init__(
            self,
            config=None,
            robot_id=None,
            host=None,
            port=None,
            websocket_uri=None,
            metsr_client=None,
            metsr_host=None,
            metsr_port=None,
            sim_folder=None,
            auto_connect_metsr=None,
            timeout=5,
            verbose=False,
            auto_connect=False,
            launch_cwd=None,
            launcher_dir=None,
      ):
            self.config = config
            self.robot_id = robot_id or _config_get(config, "robot_id", _config_get(config, "veh", None))
            self.host = host or _config_get(config, "duckie_host", "localhost")
            self.port = port if port is not None else _config_get(config, "duckie_port", None)
            self.websocket_uri = (
                  websocket_uri
                  or _config_get(config, "duckie_websocket_uri", None)
                  or _config_get(config, "websocket_uri", None)
            )
            if self.websocket_uri is None and self.port is not None:
                  self.websocket_uri = f"ws://{self.host}:{int(self.port)}"

            self.timeout = _config_get(config, "duckie_timeout", timeout)
            self.verbose = _config_get(config, "verbose", verbose)
            self.launch_cwd = launch_cwd or _config_get(config, "duckie_launch_cwd", None)
            self.launcher_dir = launcher_dir or _config_get(config, "duckie_launcher_dir", None)
            self.angle_unit = _config_get(config, "duckie_angle_unit", "degree")
            self.yaw_is_bearing = bool(_config_get(config, "duckie_yaw_is_bearing", True))
            self.coord_scale = float(_config_get(config, "duckie_coord_scale", 1.0))
            self.coord_offset = _config_get(config, "duckie_coord_offset", {"x": 0.0, "y": 0.0, "z": 0.0})
            self.invert_y = bool(_config_get(config, "duckie_invert_y", False))

            self.ws = None
            self.state = "disconnected"
            self.lock = threading.Lock()
            self.listen_thread = None
            self.listen_event = threading.Event()

            self.processes = {}
            self.messages = deque(maxlen=int(_config_get(config, "duckie_message_history", 500)))
            self.messages_by_type = defaultdict(lambda: deque(maxlen=100))
            self.latest = {}
            self.apriltags = {}
            self.duckietown_vehicles = {}
            self.vehicle_map = {}
            self.tag_vehicle_map = {}

            self.metsr = metsr_client
            self._init_vehicle_map(_config_get(config, "duckie_vehicle_map", None))
            self._init_tag_vehicle_map(_config_get(config, "duckie_tag_vehicle_map", None))
            self._init_metsr_client(
                  config=config,
                  metsr_host=metsr_host,
                  metsr_port=metsr_port,
                  sim_folder=sim_folder,
                  auto_connect_metsr=auto_connect_metsr,
            )
            self._register_metsr_roads(config)

            if auto_connect and self.websocket_uri:
                  self.connect()

      def __getattr__(self, name):
            metsr = self.__dict__.get("metsr")
            if metsr is not None and hasattr(metsr, name):
                  return getattr(metsr, name)
            raise AttributeError(f"{type(self).__name__!s} object has no attribute {name!r}")

      def _init_metsr_client(self, config, metsr_host, metsr_port, sim_folder, auto_connect_metsr):
            if self.metsr is not None:
                  return

            metsr_host = metsr_host or _config_get(config, "metsr_host", None)
            if metsr_port is None:
                  ports = _config_get(config, "ports", None)
                  metsr_port = _config_get(config, "metsr_port", None)
                  if metsr_port is None and ports:
                        metsr_port = ports[0]
            sim_folder = sim_folder or _config_get(config, "sim_folder", None)
            if sim_folder is None:
                  sim_dirs = _config_get(config, "sim_dirs", None)
                  if sim_dirs:
                        sim_folder = sim_dirs[0]

            if auto_connect_metsr is None:
                  auto_connect_metsr = metsr_host is not None and metsr_port is not None
            if not auto_connect_metsr:
                  return
            if metsr_host is None or metsr_port is None:
                  raise ValueError("metsr_host and metsr_port/ports are required to connect METS-R")

            self.metsr = METSRClient(
                  host=metsr_host,
                  port=int(metsr_port),
                  sim_folder=sim_folder,
                  manager=self,
                  timeout=_config_get(config, "timeout", 30),
                  verbose=self.verbose,
            )

      def _register_metsr_roads(self, config):
            if self.metsr is None:
                  return
            roads = (
                  _config_get(config, "duckie_metsr_road", None)
                  or _config_get(config, "duckie_metsr_roads", None)
                  or _config_get(config, "metsr_road", [])
            )
            for road in _as_list(roads):
                  self.metsr.set_cosim_road(road)

      def _init_vehicle_map(self, vehicle_map):
            if not vehicle_map:
                  return
            if isinstance(vehicle_map, dict):
                  for robot_id, mapping in vehicle_map.items():
                        if isinstance(mapping, dict):
                              self.register_duckie_vehicle(robot_id=robot_id, **mapping)
                        else:
                              self.register_duckie_vehicle(robot_id=robot_id, vehID=mapping)
                  return
            for mapping in _as_list(vehicle_map):
                  self.register_duckie_vehicle(**mapping)

      def _init_tag_vehicle_map(self, tag_vehicle_map):
            if not tag_vehicle_map:
                  return
            for tag_id, robot_id in tag_vehicle_map.items():
                  self.tag_vehicle_map[int(tag_id)] = robot_id

      def connect(self, websocket_uri=None):
            self.websocket_uri = websocket_uri or self.websocket_uri
            if not self.websocket_uri:
                  raise ValueError("Duckietown websocket URI is not configured")
            self.ws = connect(self.websocket_uri, ping_interval=None, ping_timeout=None)
            self.state = "connected"
            if self.verbose:
                  print(f"Connected to Duckietown WebSocket at {self.websocket_uri}")
            return self

      def close(self):
            self.stop_listener()
            if self.ws is not None:
                  try:
                        self.ws.close()
                  except Exception:
                        pass
            self.ws = None
            self.state = "disconnected"

      def terminate(self):
            self.close()
            for name in list(self.processes.keys()):
                  self.stop_function(name)
            if self.metsr is not None and hasattr(self.metsr, "terminate"):
                  self.metsr.terminate()

      def _log(self, direction, msg):
            if self.verbose:
                  print(f"{direction}: {msg}")

      def send_msg(self, msg):
            if self.ws is None:
                  raise RuntimeError("Duckietown WebSocket is not connected")
            self._log("SENT", msg)
            self.ws.send(json.dumps(msg))

      def receive_msg(self, timeout=None, store=True):
            if self.ws is None:
                  raise RuntimeError("Duckietown WebSocket is not connected")
            raw_msg = self.ws.recv(timeout=self.timeout if timeout is None else timeout)
            msg = json.loads(str(raw_msg))
            self._log("RECEIVED", msg)
            if store:
                  self._store_message(msg)
            return msg

      def start_listener(self):
            if self.listen_thread is not None and self.listen_thread.is_alive():
                  return
            if self.ws is None:
                  self.connect()
            self.listen_event.clear()
            self.listen_thread = threading.Thread(target=self._listen_loop, daemon=True)
            self.listen_thread.start()

      def stop_listener(self):
            self.listen_event.set()
            if self.listen_thread is not None and self.listen_thread.is_alive():
                  self.listen_thread.join(timeout=1)
            self.listen_thread = None

      def _listen_loop(self):
            while not self.listen_event.is_set():
                  try:
                        self.receive_msg(timeout=1, store=True)
                  except TimeoutError:
                        continue
                  except Exception as exc:
                        if self.verbose:
                              print(f"Duckietown listener stopped: {exc}")
                        self.state = "failed"
                        break

      def _store_message(self, msg):
            msg_type = msg.get("type") or msg.get("TYPE") or msg.get("op") or "unknown"
            self.messages.append(msg)
            self.messages_by_type[msg_type].append(msg)

            if msg_type == "imu":
                  self.latest["imu"] = msg
            elif msg_type == "camera":
                  self.latest["camera"] = msg
            elif msg_type in {"apriltag", "apriltags", "tag"}:
                  normalized = self.normalize_apriltag_message(msg)
                  self.latest["apriltags"] = normalized
                  for tag in normalized.get("tags", []):
                        if tag["id"] is None:
                              continue
                        self.apriltags[tag["id"]] = tag
                        robot_id = self.tag_vehicle_map.get(tag["id"])
                        if robot_id is not None:
                              state = self.vehicle_state_from_apriltag(tag, robot_id=robot_id)
                              self.duckietown_vehicles[robot_id] = state
                              self.latest["vehicles"] = self.duckietown_vehicles
            elif msg_type in {"vehicle", "vehicle_state", "duckie_vehicle", "pose", "odometry", "localization"}:
                  try:
                        state = self.normalize_vehicle_state(msg)
                        robot_id = state.get("robot_id")
                        if robot_id is not None:
                              self.duckietown_vehicles[robot_id] = state
                              self.latest["vehicles"] = self.duckietown_vehicles
                  except ValueError:
                        self.latest[msg_type] = msg
            elif msg_type == "call_service":
                  self.latest["service_call"] = msg
            else:
                  self.latest[msg_type] = msg

      def get_latest(self, msg_type, default=None):
            return self.latest.get(msg_type, default)

      def get_messages(self, msg_type=None):
            if msg_type is None:
                  return list(self.messages)
            return list(self.messages_by_type.get(msg_type, []))

      def wait_for_message(self, msg_type=None, timeout=None):
            deadline = None if timeout is None else time.time() + timeout
            while True:
                  remaining = None if deadline is None else max(0, deadline - time.time())
                  if deadline is not None and remaining <= 0:
                        return None
                  msg = self.receive_msg(timeout=remaining, store=True)
                  if msg_type is None:
                        return msg
                  actual_type = msg.get("type") or msg.get("TYPE") or msg.get("op")
                  if actual_type == msg_type:
                        return msg

      def query_imu(self):
            return self.get_latest("imu")

      def query_camera(self, decode=False):
            msg = self.get_latest("camera")
            if not decode or msg is None:
                  return msg
            decoded = dict(msg)
            if decoded.get("encoding") == "base64" and decoded.get("data") is not None:
                  decoded["data_bytes"] = base64.b64decode(decoded["data"])
            return decoded

      def query_apriltags(self):
            return self.get_latest("apriltags", {"type": "apriltags", "tags": []})

      def query_apriltag(self, tag_id):
            return self.apriltags.get(tag_id)

      def register_duckie_vehicle(
            self,
            robot_id,
            vehID=None,
            private_veh=False,
            transform_coords=False,
            tag_id=None,
            roadID=None,
            **kwargs,
      ):
            vehID = vehID if vehID is not None else kwargs.get("veh_id", kwargs.get("metsr_vehID"))
            if vehID is None:
                  raise ValueError("vehID is required when registering a Duckietown vehicle")
            if "privateVeh" in kwargs:
                  private_veh = kwargs["privateVeh"]
            if "transformCoord" in kwargs:
                  transform_coords = kwargs["transformCoord"]
            if "tagID" in kwargs:
                  tag_id = kwargs["tagID"]
            self.vehicle_map[robot_id] = {
                  "vehID": vehID,
                  "private_veh": private_veh,
                  "transform_coords": transform_coords,
                  "roadID": roadID,
            }
            if tag_id is not None:
                  self.tag_vehicle_map[int(tag_id)] = robot_id

      def unregister_duckie_vehicle(self, robot_id):
            mapping = self.vehicle_map.pop(robot_id, None)
            self.duckietown_vehicles.pop(robot_id, None)
            if mapping is None:
                  return
            for tag_id, mapped_robot in list(self.tag_vehicle_map.items()):
                  if mapped_robot == robot_id:
                        self.tag_vehicle_map.pop(tag_id, None)

      def query_duckie_vehicle(self, robot_id=None):
            if robot_id is None:
                  return dict(self.duckietown_vehicles)
            return self.duckietown_vehicles.get(robot_id)

      def build_duckie_control_message(
            self,
            robot_id,
            v=None,
            omega=None,
            left=None,
            right=None,
            topic=None,
            command_id=None,
            extra=None,
      ):
            msg = {}
            if v is not None:
                  msg["v"] = v
            if omega is not None:
                  msg["omega"] = omega
            if left is not None:
                  msg["left"] = left
            if right is not None:
                  msg["right"] = right
            if extra:
                  msg.update(extra)
            return {
                  "op": "publish",
                  "topic": topic or f"/{robot_id}/car_cmd_switch_node/cmd",
                  "msg": msg,
                  "id": command_id or f"duckie-control-{robot_id}-{int(time.time() * 1000)}",
            }

      def control_duckie_vehicle(
            self,
            robot_id=None,
            v=None,
            omega=None,
            left=None,
            right=None,
            topic=None,
            command_id=None,
            extra=None,
      ):
            robot_id = robot_id or self.robot_id
            if robot_id is None:
                  raise ValueError("robot_id is required to control a Duckietown vehicle")
            msg = self.build_duckie_control_message(
                  robot_id=robot_id,
                  v=v,
                  omega=omega,
                  left=left,
                  right=right,
                  topic=topic,
                  command_id=command_id,
                  extra=extra,
            )
            self.send_msg(msg)
            return msg

      def stop_duckie_vehicle(self, robot_id=None):
            return self.control_duckie_vehicle(robot_id=robot_id, v=0.0, omega=0.0)

      def normalize_vehicle_state(self, msg):
            robot_id = (
                  msg.get("robot_id")
                  or msg.get("veh")
                  or msg.get("vehicle_id")
                  or msg.get("id")
                  or self.robot_id
            )
            position = msg.get("position", {})
            pose = msg.get("pose", {})
            pose_position = pose.get("position", {}) if _dict_like(pose) else {}

            x = self._first_number(
                  msg.get("x"),
                  position.get("x") if _dict_like(position) else None,
                  pose_position.get("x") if _dict_like(pose_position) else None,
                  _nested_get(msg, "msg", "pose", "pose", "position", "x"),
                  _nested_get(msg, "msg", "pose", "position", "x"),
            )
            y = self._first_number(
                  msg.get("y"),
                  position.get("y") if _dict_like(position) else None,
                  pose_position.get("y") if _dict_like(pose_position) else None,
                  _nested_get(msg, "msg", "pose", "pose", "position", "y"),
                  _nested_get(msg, "msg", "pose", "position", "y"),
            )
            z = self._first_number(
                  msg.get("z"),
                  position.get("z") if _dict_like(position) else None,
                  pose_position.get("z") if _dict_like(pose_position) else None,
                  _nested_get(msg, "msg", "pose", "pose", "position", "z"),
                  _nested_get(msg, "msg", "pose", "position", "z"),
                  0.0,
            )

            yaw = self._first_number(
                  msg.get("yaw"),
                  msg.get("heading"),
                  _nested_get(msg, "orientation", "yaw"),
                  _nested_get(msg, "pose", "orientation", "yaw"),
                  _nested_get(msg, "msg", "pose", "pose", "orientation", "yaw"),
            )
            bearing = self._first_number(msg.get("bearing"), msg.get("heading"))
            if bearing is None and yaw is not None:
                  bearing = self._bearing_from_yaw(yaw)

            speed = self._first_number(
                  msg.get("speed"),
                  msg.get("v"),
                  _nested_get(msg, "velocity", "x"),
                  _nested_get(msg, "twist", "linear", "x"),
                  _nested_get(msg, "msg", "twist", "twist", "linear", "x"),
                  0.0,
            )

            x, y, z = self.duckie_to_metsr_coords(x, y, z)
            return {
                  "type": "vehicle_state",
                  "robot_id": robot_id,
                  "timestamp": msg.get("timestamp", _now()),
                  "x": x,
                  "y": y,
                  "z": z,
                  "bearing": bearing if bearing is not None else 0.0,
                  "speed": speed,
                  "source": msg,
            }

      def vehicle_state_from_apriltag(self, tag, robot_id=None):
            x, y, z = self.duckie_to_metsr_coords(tag.get("x"), tag.get("y"), tag.get("z", 0.0))
            return {
                  "type": "vehicle_state",
                  "robot_id": robot_id or tag.get("robot_id") or self.robot_id,
                  "timestamp": tag.get("timestamp", _now()),
                  "x": x,
                  "y": y,
                  "z": z,
                  "bearing": self._bearing_from_yaw(tag.get("yaw", 0.0)),
                  "speed": tag.get("speed", 0.0),
                  "source": tag,
            }

      def duckie_to_metsr_coords(self, x, y, z=0.0):
            if x is None or y is None:
                  raise ValueError("Duckietown vehicle state requires x and y coordinates")
            offset = self.coord_offset or {}
            x_out = float(x) * self.coord_scale + float(offset.get("x", 0.0))
            y_value = -float(y) if self.invert_y else float(y)
            y_out = y_value * self.coord_scale + float(offset.get("y", 0.0))
            z_out = float(z or 0.0) * self.coord_scale + float(offset.get("z", 0.0))
            return x_out, y_out, z_out

      def _first_number(self, *values):
            for value in values:
                  if value is None:
                        continue
                  try:
                        return float(value)
                  except (TypeError, ValueError):
                        continue
            return None

      def _angle_to_degrees(self, value):
            angle = float(value)
            if str(self.angle_unit).lower() in {"rad", "radian", "radians"}:
                  return math.degrees(angle)
            return angle

      def _bearing_from_yaw(self, yaw):
            yaw_degrees = self._angle_to_degrees(yaw)
            if self.yaw_is_bearing:
                  return yaw_degrees % 360
            return (90 - yaw_degrees) % 360

      def sync_duckietown_to_metsr(self, robot_id=None, states=None):
            if self.metsr is None:
                  raise RuntimeError("METS-R client is not configured")

            if states is None:
                  if robot_id is None:
                        states = list(self.duckietown_vehicles.values())
                  else:
                        state = self.duckietown_vehicles.get(robot_id)
                        states = [] if state is None else [state]
            elif isinstance(states, dict) and "robot_id" in states:
                  states = [states]
            else:
                  states = _as_list(states)

            results = []
            for state in states:
                  rid = state.get("robot_id", robot_id or self.robot_id)
                  mapping = self.vehicle_map.get(rid, {})
                  veh_id = mapping.get("vehID", state.get("vehID"))
                  if veh_id is None:
                        results.append({"robot_id": rid, "STATUS": "SKIPPED", "reason": "missing vehID mapping"})
                        continue

                  res = self.metsr.teleport_cosim_vehicle(
                        vehID=veh_id,
                        x=state["x"],
                        y=state["y"],
                        z=state.get("z", 0.0),
                        bearing=state.get("bearing", 0.0),
                        speed=state.get("speed", 0.0),
                        private_veh=mapping.get("private_veh", state.get("private_veh", False)),
                        transform_coords=mapping.get("transform_coords", state.get("transform_coords", False)),
                  )
                  results.append({"robot_id": rid, "vehID": veh_id, "STATUS": "OK", "response": res})
            return results

      def sync_metsr_to_duckietown(self):
            if self.metsr is None:
                  raise RuntimeError("METS-R client is not configured")
            cosim = self.metsr.query_coSimVehicle()
            vehicles = cosim.get("DATA", [])
            if not vehicles:
                  return []

            ids = [vehicle["ID"] for vehicle in vehicles]
            private_flags = [vehicle["v_type"] for vehicle in vehicles]
            states = self.metsr.query_vehicle(id=ids, private_veh=private_flags, transform_coords=True).get("DATA", [])
            return [
                  {
                        "metsr": metsr_vehicle,
                        "state": state,
                  }
                  for metsr_vehicle, state in zip(vehicles, states)
            ]

      def list_functions(self):
            return sorted(DUCKIETOWN_FUNCTIONS.keys())

      def function_info(self, function_name=None):
            if function_name is not None:
                  if function_name not in DUCKIETOWN_FUNCTIONS:
                        known = ", ".join(self.list_functions())
                        raise ValueError(f"Unknown Duckietown function '{function_name}'. Known functions: {known}")
                  return self._function_info(function_name, DUCKIETOWN_FUNCTIONS[function_name])

            return {
                  name: self._function_info(name, spec)
                  for name, spec in DUCKIETOWN_FUNCTIONS.items()
            }

      def _function_info(self, name, spec):
            return {
                  "function_name": name,
                  "launcher": spec["launcher"],
                  "package": spec["package"],
                  "launch_file": spec["launch_file"],
                  "detail": spec["description"],
                  "how_to_call": f"roslaunch {spec['package']} {spec['launch_file']} veh:=XXXX",
            }

      def build_launch_command(self, function_name, veh=None, extra_args=None, use_launcher=False):
            if function_name not in DUCKIETOWN_FUNCTIONS:
                  known = ", ".join(sorted(DUCKIETOWN_FUNCTIONS))
                  raise ValueError(f"Unknown Duckietown function '{function_name}'. Known functions: {known}")

            spec = DUCKIETOWN_FUNCTIONS[function_name]
            veh = veh or self.robot_id
            if not veh:
                  raise ValueError("veh/robot_id is required to launch a Duckietown function")

            if use_launcher:
                  return self._launcher_command(spec, veh, extra_args)
            return self._roslaunch_command(spec, veh, extra_args)

      def launch_function(self, function_name, veh=None, extra_args=None, use_launcher=False):
            command = self.build_launch_command(function_name, veh, extra_args, use_launcher)
            if self.verbose:
                  print("Launching Duckietown function:", " ".join(command))
            process = subprocess.Popen(command, cwd=self.launch_cwd)
            self.processes[function_name] = process
            return process

      def stop_function(self, function_name):
            process = self.processes.pop(function_name, None)
            if process is None:
                  return
            if process.poll() is None:
                  process.terminate()
                  try:
                        process.wait(timeout=5)
                  except subprocess.TimeoutExpired:
                        process.kill()

      def _roslaunch_command(self, spec, veh, extra_args=None):
            command = ["roslaunch", spec["package"], spec["launch_file"], f"veh:={veh}"]
            command.extend(self._format_ros_args(extra_args))
            return command

      def _launcher_command(self, spec, veh, extra_args=None):
            launcher = spec["launcher"]
            if self.launcher_dir:
                  launcher = os.path.join(self.launcher_dir, launcher)
            command = [launcher, f"veh:={veh}"]
            command.extend(self._format_ros_args(extra_args))
            return command

      def _format_ros_args(self, extra_args=None):
            args = []
            if extra_args is None:
                  return args
            if isinstance(extra_args, dict):
                  for key, value in extra_args.items():
                        args.append(f"{key}:={value}")
            else:
                  args.extend(str(arg) for arg in _as_list(extra_args))
            return args

      def lane_following(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("lane_following", veh, extra_args, use_launcher)

      def apriltag_detection(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("apriltag_detection", veh, extra_args, use_launcher)

      def communication_demo(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("communication_demo", veh, extra_args, use_launcher)

      def traffic_light_response(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("traffic_light_response", veh, extra_args, use_launcher)

      def stop_sign_negotiation(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("stop_sign_negotiation", veh, extra_args, use_launcher)

      def led_emitter(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("led_emitter", veh, extra_args, use_launcher)

      def object_detection(self, veh=None, extra_args=None, use_launcher=False):
            return self.launch_function("object_detection", veh, extra_args, use_launcher)

      def build_imu_message(
            self,
            linear_acceleration,
            angular_velocity,
            orientation,
            robot_id=None,
            timestamp=None,
      ):
            return {
                  "type": "imu",
                  "robot_id": robot_id or self.robot_id,
                  "timestamp": _now() if timestamp is None else timestamp,
                  "linear_acceleration": dict(linear_acceleration),
                  "angular_velocity": dict(angular_velocity),
                  "orientation": dict(orientation),
            }

      def build_camera_message(
            self,
            data,
            robot_id=None,
            timestamp=None,
            topic=None,
            image_format="jpeg",
            encoding="base64",
      ):
            if isinstance(data, bytes):
                  if encoding != "base64":
                        raise ValueError("Binary camera data currently requires base64 encoding")
                  data = base64.b64encode(data).decode("ascii")
            robot_id = robot_id or self.robot_id
            topic = topic or f"/{robot_id}/camera_node/image/compressed"
            return {
                  "type": "camera",
                  "robot_id": robot_id,
                  "timestamp": _now() if timestamp is None else timestamp,
                  "topic": topic,
                  "format": image_format,
                  "encoding": encoding,
                  "data": data,
            }

      def build_apriltag_message(
            self,
            tag_id,
            x,
            y,
            z,
            yaw,
            pitch,
            roll,
            robot_id=None,
            timestamp=None,
      ):
            return {
                  "type": "apriltag",
                  "robot_id": robot_id or self.robot_id,
                  "timestamp": _now() if timestamp is None else timestamp,
                  "tags": [
                        {
                              "id": tag_id,
                              "x": x,
                              "y": y,
                              "z": z,
                              "yaw": yaw,
                              "pitch": pitch,
                              "roll": roll,
                        }
                  ],
            }

      def normalize_apriltag_message(self, msg):
            if "tags" in msg:
                  tags = msg["tags"]
            elif "DATA" in msg:
                  tags = msg["DATA"]
            else:
                  tags = [msg]

            normalized_tags = []
            for tag in tags:
                  tag_id = tag.get("id", tag.get("tag_id", tag.get("ID")))
                  try:
                        tag_id = int(tag_id)
                  except (TypeError, ValueError):
                        pass
                  normalized_tags.append({
                        "id": tag_id,
                        "x": tag.get("x"),
                        "y": tag.get("y"),
                        "z": tag.get("z"),
                        "yaw": tag.get("yaw"),
                        "pitch": tag.get("pitch"),
                        "roll": tag.get("roll"),
                        "timestamp": tag.get("timestamp", msg.get("timestamp")),
                        "robot_id": tag.get("robot_id", msg.get("robot_id", self.robot_id)),
                  })

            return {
                  "type": "apriltags",
                  "robot_id": msg.get("robot_id", self.robot_id),
                  "timestamp": msg.get("timestamp", _now()),
                  "tags": normalized_tags,
            }

      def build_led_pattern(
            self,
            colors,
            color_mask=None,
            frequency=0.0,
            frequency_mask=None,
            frame_id="",
      ):
            colors = _as_list(colors)
            color_mask = color_mask if color_mask is not None else [1] * len(colors)
            frequency_mask = frequency_mask if frequency_mask is not None else [0] * len(colors)
            return {
                  "header": {
                        "seq": 0,
                        "stamp": {"secs": 0, "nsecs": 0},
                        "frame_id": frame_id,
                  },
                  "color_list": colors,
                  "color_mask": color_mask,
                  "frequency": frequency,
                  "frequency_mask": frequency_mask,
            }

      def build_service_call(self, service, args, call_id=None):
            return {
                  "op": "call_service",
                  "service": service,
                  "args": args,
                  "id": call_id or f"duckie-call-{int(time.time() * 1000)}",
            }

      def build_traffic_light_message(
            self,
            traffic_light_id,
            colors,
            call_id=None,
            color_mask=None,
            frequency=0.0,
            frequency_mask=None,
      ):
            pattern = self.build_led_pattern(
                  colors=colors,
                  color_mask=color_mask,
                  frequency=frequency,
                  frequency_mask=frequency_mask,
            )
            return self.build_service_call(
                  service=f"/{traffic_light_id}/led_emitter_node/set_custom_pattern",
                  args={"pattern": pattern},
                  call_id=call_id,
            )

      def set_traffic_light(
            self,
            traffic_light_id,
            colors,
            call_id=None,
            color_mask=None,
            frequency=0.0,
            frequency_mask=None,
      ):
            msg = self.build_traffic_light_message(
                  traffic_light_id=traffic_light_id,
                  colors=colors,
                  call_id=call_id,
                  color_mask=color_mask,
                  frequency=frequency,
                  frequency_mask=frequency_mask,
            )
            self.send_msg(msg)
            return msg

      def set_traffic_light_solid(self, traffic_light_id, color, call_id=None):
            return self.set_traffic_light(
                  traffic_light_id=traffic_light_id,
                  colors=[color] * 5,
                  color_mask=[1, 1, 1, 1, 1],
                  frequency=0.0,
                  frequency_mask=[0, 0, 0, 0, 0],
                  call_id=call_id or f"solid-{color}-{int(time.time() * 1000)}",
            )

      def send_imu(self, linear_acceleration, angular_velocity, orientation, robot_id=None, timestamp=None):
            msg = self.build_imu_message(
                  linear_acceleration=linear_acceleration,
                  angular_velocity=angular_velocity,
                  orientation=orientation,
                  robot_id=robot_id,
                  timestamp=timestamp,
            )
            self.send_msg(msg)
            return msg

      def send_camera(self, data, robot_id=None, timestamp=None, topic=None, image_format="jpeg", encoding="base64"):
            msg = self.build_camera_message(
                  data=data,
                  robot_id=robot_id,
                  timestamp=timestamp,
                  topic=topic,
                  image_format=image_format,
                  encoding=encoding,
            )
            self.send_msg(msg)
            return msg

      def send_apriltag(
            self,
            tag_id,
            x,
            y,
            z,
            yaw,
            pitch,
            roll,
            robot_id=None,
            timestamp=None,
      ):
            msg = self.build_apriltag_message(
                  tag_id=tag_id,
                  x=x,
                  y=y,
                  z=z,
                  yaw=yaw,
                  pitch=pitch,
                  roll=roll,
                  robot_id=robot_id,
                  timestamp=timestamp,
            )
            self.send_msg(msg)
            return msg

      def step(
            self,
            duckie_messages=1,
            receive_timeout=None,
            sync_to_metsr=True,
            metsr_step=1,
            wait_forever=True,
      ):
            """Advance one Duckietown/METS-R co-simulation step."""
            received = []
            for _ in range(max(0, int(duckie_messages))):
                  try:
                        received.append(self.receive_msg(timeout=receive_timeout, store=True))
                  except TimeoutError:
                        break

            sync_results = []
            if sync_to_metsr and self.metsr is not None:
                  sync_results = self.sync_duckietown_to_metsr()

            metsr_tick = None
            if metsr_step and self.metsr is not None:
                  self.metsr.tick(int(metsr_step), wait_forever=wait_forever)
                  metsr_tick = self.metsr.current_tick

            return {
                  "duckietown": received,
                  "sync": sync_results,
                  "metsr_tick": metsr_tick,
            }

      def run(
            self,
            max_steps=None,
            duckie_messages_per_step=1,
            receive_timeout=None,
            sync_to_metsr=True,
            metsr_step=1,
            wait_forever=True,
      ):
            count = 0
            try:
                  while max_steps is None or count < max_steps:
                        self.step(
                              duckie_messages=duckie_messages_per_step,
                              receive_timeout=receive_timeout,
                              sync_to_metsr=sync_to_metsr,
                              metsr_step=metsr_step,
                              wait_forever=wait_forever,
                        )
                        count += 1
            except KeyboardInterrupt:
                  print("Duckietown co-simulation interrupted by user")
            return count
