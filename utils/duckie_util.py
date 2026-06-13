"""
Utility helpers for Duckietown/METS-R integration.

These functions keep Duckietown message construction, normalization, launch
command creation, and METS-R sync logic usable without binding them to one
hardcoded client class or connection lifecycle.
"""

import base64
import json
import math
import os
import subprocess
import time
from collections import defaultdict, deque


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
            "description": "Starts the traffic-light and stop-sign communication stack.",
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


def now():
      return time.time()


def as_list(value):
      if value is None:
            return []
      if isinstance(value, list):
            return value
      if isinstance(value, tuple):
            return list(value)
      return [value]


def nested_get(data, *path, default=None):
      current = data
      for key in path:
            if not isinstance(current, dict) or key not in current:
                  return default
            current = current[key]
      return current


def first_number(*values):
      for value in values:
            if value is None:
                  continue
            try:
                  return float(value)
            except (TypeError, ValueError):
                  continue
      return None


def angle_to_degrees(value, angle_unit="degree"):
      angle = float(value)
      if str(angle_unit).lower() in {"rad", "radian", "radians"}:
            return math.degrees(angle)
      return angle


def bearing_from_yaw(yaw, angle_unit="degree", yaw_is_bearing=True):
      yaw_degrees = angle_to_degrees(yaw, angle_unit=angle_unit)
      if yaw_is_bearing:
            return yaw_degrees % 360.0
      return (90.0 - yaw_degrees) % 360.0


def duckie_to_metsr_coords(
      x,
      y,
      z=0.0,
      coord_scale=1.0,
      coord_offset=None,
      invert_y=False,
):
      if x is None or y is None:
            raise ValueError("Duckietown vehicle state requires x and y coordinates")
      offset = coord_offset or {}
      x_out = float(x) * float(coord_scale) + float(offset.get("x", 0.0))
      y_value = -float(y) if invert_y else float(y)
      y_out = y_value * float(coord_scale) + float(offset.get("y", 0.0))
      z_out = float(z or 0.0) * float(coord_scale) + float(offset.get("z", 0.0))
      return x_out, y_out, z_out


def build_duckie_control_message(
      robot_id,
      v=None,
      omega=None,
      left=None,
      right=None,
      topic=None,
      command_id=None,
      extra=None,
):
      if robot_id is None:
            raise ValueError("robot_id is required to control a Duckietown vehicle")
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


def build_stop_duckie_message(robot_id, topic=None, command_id=None):
      return build_duckie_control_message(
            robot_id=robot_id,
            v=0.0,
            omega=0.0,
            topic=topic,
            command_id=command_id,
      )


def connect_duckietown_websocket(host="localhost", port=None, websocket_uri=None, **kwargs):
      from websockets.sync.client import connect

      uri = websocket_uri
      if uri is None:
            if port is None:
                  raise ValueError("port or websocket_uri is required")
            uri = f"ws://{host}:{int(port)}"
      return connect(uri, **kwargs)


def send_duckietown_message(ws, msg):
      ws.send(json.dumps(msg))
      return msg


def receive_duckietown_message(ws, timeout=None):
      raw_msg = ws.recv(timeout=timeout)
      return json.loads(str(raw_msg))


def build_imu_message(linear_acceleration, angular_velocity, orientation, robot_id=None, timestamp=None):
      return {
            "type": "imu",
            "robot_id": robot_id,
            "timestamp": now() if timestamp is None else timestamp,
            "linear_acceleration": dict(linear_acceleration),
            "angular_velocity": dict(angular_velocity),
            "orientation": dict(orientation),
      }


def build_camera_message(
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
      topic = topic or f"/{robot_id}/camera_node/image/compressed"
      return {
            "type": "camera",
            "robot_id": robot_id,
            "timestamp": now() if timestamp is None else timestamp,
            "topic": topic,
            "format": image_format,
            "encoding": encoding,
            "data": data,
      }


def decode_camera_message(msg):
      decoded = dict(msg)
      if decoded.get("encoding") == "base64" and decoded.get("data") is not None:
            decoded["data_bytes"] = base64.b64decode(decoded["data"])
      return decoded


def build_apriltag_message(tag_id, x, y, z, yaw, pitch, roll, robot_id=None, timestamp=None):
      return {
            "type": "apriltag",
            "robot_id": robot_id,
            "timestamp": now() if timestamp is None else timestamp,
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


def normalize_apriltag_message(msg, default_robot_id=None):
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
                  "robot_id": tag.get("robot_id", msg.get("robot_id", default_robot_id)),
            })

      return {
            "type": "apriltags",
            "robot_id": msg.get("robot_id", default_robot_id),
            "timestamp": msg.get("timestamp", now()),
            "tags": normalized_tags,
      }


def normalize_vehicle_state(
      msg,
      robot_id=None,
      angle_unit="degree",
      yaw_is_bearing=True,
      coord_scale=1.0,
      coord_offset=None,
      invert_y=False,
):
      rid = (
            msg.get("robot_id")
            or msg.get("veh")
            or msg.get("vehicle_id")
            or msg.get("id")
            or robot_id
      )
      position = msg.get("position", {})
      pose = msg.get("pose", {})
      pose_position = pose.get("position", {}) if isinstance(pose, dict) else {}

      x = first_number(
            msg.get("x"),
            position.get("x") if isinstance(position, dict) else None,
            pose_position.get("x") if isinstance(pose_position, dict) else None,
            nested_get(msg, "msg", "pose", "pose", "position", "x"),
            nested_get(msg, "msg", "pose", "position", "x"),
      )
      y = first_number(
            msg.get("y"),
            position.get("y") if isinstance(position, dict) else None,
            pose_position.get("y") if isinstance(pose_position, dict) else None,
            nested_get(msg, "msg", "pose", "pose", "position", "y"),
            nested_get(msg, "msg", "pose", "position", "y"),
      )
      z = first_number(
            msg.get("z"),
            position.get("z") if isinstance(position, dict) else None,
            pose_position.get("z") if isinstance(pose_position, dict) else None,
            nested_get(msg, "msg", "pose", "pose", "position", "z"),
            nested_get(msg, "msg", "pose", "position", "z"),
            0.0,
      )

      yaw = first_number(
            msg.get("yaw"),
            msg.get("heading"),
            nested_get(msg, "orientation", "yaw"),
            nested_get(msg, "pose", "orientation", "yaw"),
            nested_get(msg, "msg", "pose", "pose", "orientation", "yaw"),
      )
      bearing = first_number(msg.get("bearing"), msg.get("heading"))
      if bearing is None and yaw is not None:
            bearing = bearing_from_yaw(yaw, angle_unit=angle_unit, yaw_is_bearing=yaw_is_bearing)

      speed = first_number(
            msg.get("speed"),
            msg.get("v"),
            nested_get(msg, "velocity", "x"),
            nested_get(msg, "twist", "linear", "x"),
            nested_get(msg, "msg", "twist", "twist", "linear", "x"),
            0.0,
      )

      x, y, z = duckie_to_metsr_coords(
            x,
            y,
            z,
            coord_scale=coord_scale,
            coord_offset=coord_offset,
            invert_y=invert_y,
      )
      return {
            "type": "vehicle_state",
            "robot_id": rid,
            "timestamp": msg.get("timestamp", now()),
            "x": x,
            "y": y,
            "z": z,
            "bearing": bearing if bearing is not None else 0.0,
            "speed": speed,
            "source": msg,
      }


def vehicle_state_from_apriltag(
      tag,
      robot_id=None,
      angle_unit="degree",
      yaw_is_bearing=True,
      coord_scale=1.0,
      coord_offset=None,
      invert_y=False,
):
      x, y, z = duckie_to_metsr_coords(
            tag.get("x"),
            tag.get("y"),
            tag.get("z", 0.0),
            coord_scale=coord_scale,
            coord_offset=coord_offset,
            invert_y=invert_y,
      )
      return {
            "type": "vehicle_state",
            "robot_id": robot_id or tag.get("robot_id"),
            "timestamp": tag.get("timestamp", now()),
            "x": x,
            "y": y,
            "z": z,
            "bearing": bearing_from_yaw(
                  tag.get("yaw", 0.0),
                  angle_unit=angle_unit,
                  yaw_is_bearing=yaw_is_bearing,
            ),
            "speed": tag.get("speed", 0.0),
            "source": tag,
      }


def create_duckietown_message_store(message_history=500, per_type_history=100):
      return {
            "messages": deque(maxlen=int(message_history)),
            "messages_by_type": defaultdict(lambda: deque(maxlen=int(per_type_history))),
            "latest": {},
            "apriltags": {},
            "vehicles": {},
      }


def store_duckietown_message(
      msg,
      store,
      tag_vehicle_map=None,
      default_robot_id=None,
      normalizer_kwargs=None,
):
      normalizer_kwargs = normalizer_kwargs or {}
      msg_type = msg.get("type") or msg.get("TYPE") or msg.get("op") or "unknown"
      store["messages"].append(msg)
      store["messages_by_type"][msg_type].append(msg)

      latest = store["latest"]
      if msg_type == "imu":
            latest["imu"] = msg
      elif msg_type == "camera":
            latest["camera"] = msg
      elif msg_type in {"apriltag", "apriltags", "tag"}:
            normalized = normalize_apriltag_message(msg, default_robot_id=default_robot_id)
            latest["apriltags"] = normalized
            for tag in normalized.get("tags", []):
                  if tag["id"] is None:
                        continue
                  store["apriltags"][tag["id"]] = tag
                  robot_id = (tag_vehicle_map or {}).get(tag["id"])
                  if robot_id is not None:
                        state = vehicle_state_from_apriltag(
                              tag,
                              robot_id=robot_id,
                              **normalizer_kwargs,
                        )
                        store["vehicles"][robot_id] = state
                        latest["vehicles"] = store["vehicles"]
      elif msg_type in {"vehicle", "vehicle_state", "duckie_vehicle", "pose", "odometry", "localization"}:
            try:
                  state = normalize_vehicle_state(
                        msg,
                        robot_id=default_robot_id,
                        **normalizer_kwargs,
                  )
                  rid = state.get("robot_id")
                  if rid is not None:
                        store["vehicles"][rid] = state
                        latest["vehicles"] = store["vehicles"]
            except ValueError:
                  latest[msg_type] = msg
      elif msg_type == "call_service":
            latest["service_call"] = msg
      else:
            latest[msg_type] = msg
      return msg_type


def register_duckie_vehicle(
      vehicle_map,
      robot_id,
      vehID=None,
      private_veh=False,
      transform_coords=False,
      tag_id=None,
      roadID=None,
      tag_vehicle_map=None,
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
      vehicle_map[robot_id] = {
            "vehID": vehID,
            "private_veh": private_veh,
            "transform_coords": transform_coords,
            "roadID": roadID,
      }
      if tag_id is not None and tag_vehicle_map is not None:
            tag_vehicle_map[int(tag_id)] = robot_id
      return vehicle_map[robot_id]


def unregister_duckie_vehicle(vehicle_map, robot_id, tag_vehicle_map=None, vehicle_states=None):
      mapping = vehicle_map.pop(robot_id, None)
      if vehicle_states is not None:
            vehicle_states.pop(robot_id, None)
      if mapping is None or tag_vehicle_map is None:
            return mapping
      for tag_id, mapped_robot in list(tag_vehicle_map.items()):
            if mapped_robot == robot_id:
                  tag_vehicle_map.pop(tag_id, None)
      return mapping


def build_duckie_vehicle_map(vehicle_map=None, tag_vehicle_map=None):
      vehicles = {}
      tags = {} if tag_vehicle_map is None else dict(tag_vehicle_map)
      if not vehicle_map:
            return vehicles, tags
      if isinstance(vehicle_map, dict):
            for robot_id, mapping in vehicle_map.items():
                  if isinstance(mapping, dict):
                        register_duckie_vehicle(vehicles, robot_id=robot_id, tag_vehicle_map=tags, **mapping)
                  else:
                        register_duckie_vehicle(vehicles, robot_id=robot_id, vehID=mapping, tag_vehicle_map=tags)
            return vehicles, tags
      for mapping in as_list(vehicle_map):
            register_duckie_vehicle(vehicles, tag_vehicle_map=tags, **mapping)
      return vehicles, tags


def sync_duckietown_to_metsr(metsr, states, vehicle_map=None, robot_id=None):
      if states is None:
            return []
      if isinstance(states, dict) and "robot_id" in states:
            states = [states]
      else:
            states = as_list(states)

      vehicle_map = vehicle_map or {}
      results = []
      for state in states:
            rid = state.get("robot_id", robot_id)
            mapping = vehicle_map.get(rid, {})
            veh_id = mapping.get("vehID", state.get("vehID"))
            if veh_id is None:
                  results.append({"robot_id": rid, "STATUS": "SKIPPED", "reason": "missing vehID mapping"})
                  continue

            res = metsr.teleport_cosim_vehicle(
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


def sync_metsr_to_duckietown(metsr, transform_coords=True):
      cosim = metsr.query_coSimVehicle()
      vehicles = cosim.get("DATA", [])
      if not vehicles:
            return []
      ids = [vehicle["ID"] for vehicle in vehicles]
      private_flags = [vehicle["v_type"] for vehicle in vehicles]
      states = metsr.query_vehicle(
            id=ids,
            private_veh=private_flags,
            transform_coords=transform_coords,
      ).get("DATA", [])
      return [
            {"metsr": metsr_vehicle, "state": state}
            for metsr_vehicle, state in zip(vehicles, states)
      ]


def list_duckietown_functions():
      return sorted(DUCKIETOWN_FUNCTIONS.keys())


def duckietown_function_info(function_name=None):
      if function_name is not None:
            if function_name not in DUCKIETOWN_FUNCTIONS:
                  known = ", ".join(list_duckietown_functions())
                  raise ValueError(f"Unknown Duckietown function '{function_name}'. Known functions: {known}")
            return _duckietown_function_info(function_name, DUCKIETOWN_FUNCTIONS[function_name])
      return {
            name: _duckietown_function_info(name, spec)
            for name, spec in DUCKIETOWN_FUNCTIONS.items()
      }


def _duckietown_function_info(name, spec):
      return {
            "function_name": name,
            "launcher": spec["launcher"],
            "package": spec["package"],
            "launch_file": spec["launch_file"],
            "detail": spec["description"],
            "how_to_call": f"roslaunch {spec['package']} {spec['launch_file']} veh:=XXXX",
      }


def format_ros_args(extra_args=None):
      args = []
      if extra_args is None:
            return args
      if isinstance(extra_args, dict):
            for key, value in extra_args.items():
                  args.append(f"{key}:={value}")
      else:
            args.extend(str(arg) for arg in as_list(extra_args))
      return args


def build_duckietown_launch_command(
      function_name,
      veh,
      extra_args=None,
      use_launcher=False,
      launcher_dir=None,
):
      if function_name not in DUCKIETOWN_FUNCTIONS:
            known = ", ".join(list_duckietown_functions())
            raise ValueError(f"Unknown Duckietown function '{function_name}'. Known functions: {known}")
      if not veh:
            raise ValueError("veh is required to launch a Duckietown function")

      spec = DUCKIETOWN_FUNCTIONS[function_name]
      if use_launcher:
            launcher = spec["launcher"]
            if launcher_dir:
                  launcher = os.path.join(launcher_dir, launcher)
            command = [launcher, f"veh:={veh}"]
      else:
            command = ["roslaunch", spec["package"], spec["launch_file"], f"veh:={veh}"]
      command.extend(format_ros_args(extra_args))
      return command


def launch_duckietown_function(
      function_name,
      veh,
      extra_args=None,
      use_launcher=False,
      launcher_dir=None,
      launch_cwd=None,
      processes=None,
      verbose=False,
):
      command = build_duckietown_launch_command(
            function_name,
            veh,
            extra_args=extra_args,
            use_launcher=use_launcher,
            launcher_dir=launcher_dir,
      )
      if verbose:
            print("Launching Duckietown function:", " ".join(command))
      process = subprocess.Popen(command, cwd=launch_cwd)
      if processes is not None:
            processes[function_name] = process
      return process


def stop_duckietown_process(process, timeout=5):
      if process is None:
            return
      if process.poll() is None:
            process.terminate()
            try:
                  process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                  process.kill()


def build_led_pattern(colors, color_mask=None, frequency=0.0, frequency_mask=None, frame_id=""):
      colors = as_list(colors)
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


def build_service_call(service, args, call_id=None):
      return {
            "op": "call_service",
            "service": service,
            "args": args,
            "id": call_id or f"duckie-call-{int(time.time() * 1000)}",
      }


def build_traffic_light_message(
      traffic_light_id,
      colors,
      call_id=None,
      color_mask=None,
      frequency=0.0,
      frequency_mask=None,
):
      pattern = build_led_pattern(
            colors=colors,
            color_mask=color_mask,
            frequency=frequency,
            frequency_mask=frequency_mask,
      )
      return build_service_call(
            service=f"/{traffic_light_id}/led_emitter_node/set_custom_pattern",
            args={"pattern": pattern},
            call_id=call_id,
      )


def build_solid_traffic_light_message(traffic_light_id, color, call_id=None):
      return build_traffic_light_message(
            traffic_light_id=traffic_light_id,
            colors=[color] * 5,
            color_mask=[1, 1, 1, 1, 1],
            frequency=0.0,
            frequency_mask=[0, 0, 0, 0, 0],
            call_id=call_id or f"solid-{color}-{int(time.time() * 1000)}",
      )
