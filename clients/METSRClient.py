# remote data client interface. Each RDClient runs in a separate thread
# run() method  

import datetime
import json
import time
from websockets.sync.client import connect

from utils.util import *

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)


"""
Implementation of the remote data client

A client directly communicates with a specific METSR-SIM server.

Acknowledgement: Eric Vin for helping with the revision of the code
"""

class METSRClient:

    def __init__(self, host, port, index, manager = None, max_connection_attempts = 5, timeout = 30, verbose = False):
        super().__init__()

        # Websocket config
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"

        self.index = index
        self.state = "connecting"
        self.timeout = timeout  # time out for resending the same message if no response
        self.verbose = verbose
        self._messagesLog = []

        # a pointer to the manager
        self.manager = manager
 
        # Track the tick of the corresponding simulator
        self.current_tick = None

        # Establish connection
        failed_attempts = 0
        while True:
            try:
                self.ws = connect(self.uri)
                self.state = "connected"
                if self.verbose:
                    print(f"Connected to {self.uri}")
                break
            except ConnectionRefusedError:
                print(f"Attempt to connect to {self.uri} failed. "
                      f"Waiting for 10 seconds before trying again... "
                      f"({max_connection_attempts - failed_attempts} attempts remaining)")
                failed_attempts += 1
                if failed_attempts >= max_connection_attempts:
                    self.state = "failed"
                    raise RuntimeError("Could not connect to METS-R Sim")
                time.sleep(10)

        # Ensure server is initialized by waiting to receive an initial packet
        # (could be ANS_ready or a heartbeat)
        self.receive_msg(ignore_heartbeats=False)

    def send_msg(self, msg):
        if self.verbose:
            self._logMessage("SENT", msg)
        self.ws.send(json.dumps(msg))

    def receive_msg(self, ignore_heartbeats):
        while True:
            raw_msg = self.ws.recv(timeout = self.timeout)

            # Decode the json string
            msg = json.loads(str(raw_msg))

            if self.verbose:
                self._logMessage("RECEIVED", msg)
            
            # EVERY decoded msg must have a MST_TYPE field
            assert "TYPE" in msg.keys(), "No type field in received message"
            assert msg["TYPE"].split("_")[0] in {"STEP", "ANS", "CTRL", "ATK"}, "Uknown message type: " + str(msg["TYPE"])

            # Allow tick()
            if msg["TYPE"] in {"ANS_ready"}:
                self.current_tick = -1
                continue

            # Return decoded message, if it's not an ignored heartbeat
            if not ignore_heartbeats or msg["TYPE"] != "STEP":
                return msg
            
    def send_receive_msg(self, msg, ignore_heartbeats):
        self.send_msg(msg)
        return self.receive_msg(ignore_heartbeats=ignore_heartbeats)

    def tick(self):
        assert self.current_tick is not None, "self.current_tick is None. Reset should be called first"
        msg = {"TYPE": "STEP", "TICK": self.current_tick}
        self.send_msg(msg)

        while True:
            # Move through messages until we get to an up to date heartbeat
            res = self.receive_msg(ignore_heartbeats=False)

            assert res["TYPE"] == "STEP", res["TYPE"]
            if res["TICK"] == self.current_tick + 1:
                break

        self.current_tick = res["TICK"]
   
    # QUERY: inspect the state of the simulator
    # By default query public vehicles
    def query_vehicle(self, id = None, private_veh = False, transform_coords = False):
        msg = {"TYPE": "QUERY_vehicle"}
        if id is not None:
            msg["ID"] = id
            msg["PRV"] = private_veh
            msg["TRAN"] = transform_coords

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_vehicle", res["TYPE"]
        return res
 
    # query taxi
    def query_taxi(self, id = None):
        my_msg = {"TYPE": "QUERY_taxi"}
        if id is not None:
            my_msg["ID"] = id

        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_taxi", res["TYPE"]
        return res
        
    # query bus
    def query_bus(self, id = None):
        my_msg = {"TYPE": "QUERY_bus"}
        if id is not None:
            my_msg["ID"] = id      
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_bus", res["TYPE"]
        return res

        
    # query road
    def query_road(self, id = None):
        my_msg = {"TYPE": "QUERY_road"}
        if id is not None:
            my_msg["ID"] = id
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_road", res["TYPE"]
        return res

    # query zone
    def query_zone(self, id = None):
        my_msg = {"TYPE": "QUERY_zone"}
        if id is not None:
            my_msg["ID"] = id     
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_zone", res["TYPE"] 
        return res

    # query signal
    def query_signal(self, id = None):
        my_msg = {"TYPE": "QUERY_signal"}
        if id is not None:
            my_msg["ID"] = id
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_signal", res["TYPE"]
        return res
    
    # query chargingStation
    def query_chargingStation(self, id = None):
        my_msg = {"TYPE": "QUERY_chargingStation"}
        if id is not None:
            my_msg["ID"] = id      
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_chargingStation", res["TYPE"]
        return res
    
    # query vehicleID within the co-sim road
    def query_coSimVehicle(self):
        my_msg = {"TYPE": "QUERY_coSimVehicle"}
        res = self.send_receive_msg(my_msg, ignore_heartbeats=True)
        assert res["TYPE"] == "ANS_coSimVehicle", res["TYPE"]
        return res
        
    # CONTROL: change the state of the simulator
    # set the road for co-simulation
    # generate a vehicle trip
    # TODO: make it work for public vehicle (taxi) as well
    def generate_trip(self, vehID, origin = -1, destination = -1):
        msg = {
                "TYPE": "CTRL_generateTrip",
                "vehID": vehID,
                "origin": origin,
                "destination": destination,
              }

        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["TYPE"] == "CTRL_generateTrip", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]

    def set_cosim_road(self, roadID):
        msg = {
                "TYPE": "CTRL_setCoSimRoad",
                "roadID": roadID
              }
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_setCoSimRoad", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
    
    # release the road for co-simulation
    def release_cosim_road(self, roadID):
        msg = {
                "TYPE": "CTRL_releaseCoSimRoad",
                "roadID": roadID
              }
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_releaseCoSimRoad", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
        
    # teleport vehicle to a target location specified by road, lane, and distance to the downstream junction
    def teleport_vehicle(self, vehID, roadID, laneID, dist, x, y, private_veh = False, transform_coords = False):
        msg = {
                "TYPE": "CTRL_teleportVeh",
                "vehID": vehID,
                "roadID": roadID,
                "laneID": laneID,
                "dist": dist,
                "x": x,
                "y": y,
                "prv": private_veh,
                "TRAN": transform_coords
            }
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_teleportVeh", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
    
    # enter the next road
    def enter_next_road(self, vehID, private_veh = False):
        msg = {
                "TYPE": "CTRL_enterNextRoad",
                "vehID": vehID,
                "prv": private_veh
            }
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_enterNextRoad", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
    
    # control vehicle with specified acceleration  
    def control_vehicle(self, vehID, acc, private_veh = False):
        msg = {
                "TYPE": "CTRL_controlVeh",
                "vehID": vehID,
                "acc": acc,
                "prv": private_veh
            }
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_controlVeh", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
    
    # reset the simulation with a property file
    def reset(self, prop_file):
        msg = {"TYPE": "CTRL_reset", "propertyFile": prop_file}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["TYPE"] == "CTRL_reset", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]

        self.current_tick = -1
        self.tick()
        assert self.current_tick == 0
    
    # reset the simulation with a map name
    def reset_map(self, map_name):
        # find the property file for the map
        if map_name == "CARLA":
            # copy CARLA data in the sim folder
            # source_path = "data/CARLA"
            # specify the property file
            prop_file = "Data.properties.CARLA"
        elif map_name == "NYC":
            # copy NYC data in the sim folder
            # source_path = "data/NYC"
            # specify the property file
            prop_file = "Data.properties.NYC"

        # docker_cp_command = f"docker cp {source_path} {self.docker_id}:/home/test/data/"
        # subprocess.run(docker_cp_command, shell=True, check=True)
            
        # reset the simulation with the property file
        self.reset(prop_file)

    # terminate the simulation
    def terminate(self):
        msg = {"TYPE": "CTRL_end"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["TYPE"] == "CTRL_end", res["TYPE"]
        assert res["CODE"] == "OK", res["CODE"]
        self.close()
    
    # close the client but keep the simulator running
    def close(self):
        if self.ws is not None:
            self.ws.close()
            self.ws = None
            self.state = "closed"
    
    def _logMessage(self, direction, msg):
        self._messagesLog.append(
            (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), direction, tuple(msg.items()))
        )
        print(self._messagesLog[-1])
        
    # override __str__ for logging 
    def __str__(self):
        s = f"-----------\n" \
            f"Client INFO\n" \
            f"-----------\n" \
            f"index :\t {self.index}\n" \
            f"address :\t {self.uri}\n" \
            f"state :\t {self.state}\n" 
        return s
