# remote data client interface. Each RDClient runs in a separate thread
# run() method  

import websocket
import ujson as json # faster json according to https://artem.krylysov.com/blog/2015/09/29/benchmark-python-json-libraries/ 
import threading
from contextlib import closing
from threading import Lock
from utils.util import check_socket, str_list_mapper_gen
import time
import os

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)


"""
Implementation of the remote data client

A client directly communicates with a specific METSR-SIM server.
"""

class METSRClient(threading.Thread):

    def __init__(self, host, port, index, manager = None, retry_threshold = 10, verbose = False):
        super().__init__()

        # Websocket config
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self.index = index
        self.state = "connecting"
        self.retry_threshold = retry_threshold  # time out for resending the same message if no response
        self.verbose = verbose

        # a pointer to the manager
        self.manager = manager
 
        # Track the tick of the corresponding simulator
        self.current_tick = -1
        self.prev_tick = -1
        self.prev_time = time.time()

        # latest message from the server
        self.latest_message = None

        # Create a listener socket
        self.ws = websocket.WebSocketApp(self.uri,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        
        # A flag to indicate whether the simulation is ready
        self.ready = False

        # Data maps can be accessed by both main thread (for ML algorithms)
        # and RDClient class. Therefore synchronization is needed to avoid data races.
        # If the main thread only does reads, the lock can be removed safely
        self.lock = Lock()


    # on_message is automatically called when the sever sends a msg
    def on_message(self, ws, message):
        # for debugging
        if self.verbose:
            print(f"{self.uri} : {message[0:200]}")

        # Decode the json string
        decoded_msg = json.loads(str(message))

        # Every decoded msg must have a MSG_TYPE field
        assert 'TYPE' in decoded_msg.keys(), "No TYPE field in received json string!"

        # handle decoded msg based on MSG_TYPE
        if decoded_msg['TYPE'] == "STEP":
            self.handle_step_message(decoded_msg)
        elif decoded_msg['TYPE'].split("_")[0] == "ANS":
            self.handle_answer_message(ws, decoded_msg)
            self.latest_message = decoded_msg
        elif decoded_msg['TYPE'].split("_")[0] == "ATK":
            self.handle_attack_message(ws, decoded_msg)
        elif decoded_msg['TYPE'].split("_")[0] == "CTRL":
            self.latest_message = decoded_msg
            
    def on_error(self, ws, error):
        self.state = "error"
        print(error)

    def on_close(self, ws, status_code, close_msg):
        self.state = "closed"
        print(f"{self.uri} : connection closed")

    def on_open(self, ws):
        self.state = "connected"
        print(f"{self.uri} : connection opened")

    # run() method implements what RemoteDataClient will be doing during its lifetime
    def run(self):
        print(f"Waiting until the server is up at {self.uri}")

        # all clients are disconnected
        wait_time = 0

        # wait until the server is up, or timeout after 60 seconds
        while not check_socket(self.host, self.port):
            # count the real-world seconds 
            wait_time += 1
            if wait_time > 20:
                print(f"Waiting for the server to be up at {self.uri}.. time out in {60-wait_time} seconds..")
            if wait_time > 60:
                print("Waiting overtime, please check the connection and restart the simulation.")
                # close the connection
                self.ws.close()
                os.chdir("docker")
                os.system("docker-compose down")
                break

        if wait_time <= 60:
            print(f"Sever is active at {self.uri},  running client..")
            self.ws.run_forever()

                    
    # Method for handle messages
    def handle_step_message(self, decoded_msg):
        tick = decoded_msg['TICK']
        if tick > self.current_tick: # tick less than current_tick is ignored
            self.current_tick = tick

    def handle_answer_message(self, ws, decoded_msg):
        if decoded_msg['TYPE'] == "ANS_ready":
            print("SIM is ready!!")
            self.ready = True
        elif decoded_msg['TYPE'] == "ANS_TaxiUCB":
            size = int(decoded_msg['SIZE'])
            candidate_paths = {}
            od = decoded_msg['OD']
            candidate_paths[od] = decoded_msg['road_lists']
            self.manager.mab_manager.initialize(candidate_paths, size, type = 'taxi')
        elif decoded_msg['TYPE'] == "ANS_BusUCB":
            size = int(decoded_msg['SIZE'])
            candidate_paths = {}
            bod = decoded_msg['BOD']
            candidate_paths[bod] = decoded_msg['road_lists']
            self.manager.mab_manager.initialize(candidate_paths, size, type = 'bus')

    def handle_attack_message(self, ws, decoded_msg):
        # placeholder for handling attacker's control
        pass

    def send_step_message(self, tick): # helper function for sending step message
        self.prev_tick = tick
        self.prev_time = time.time()
        msg = {'TYPE': 'STEP', 'TICK': tick}
        self.ws.send(json.dumps(msg))

    def tick(self): # synchronized, wait until the simulator finish the corresponding step
        while self.current_tick <= self.prev_tick:
            time.sleep(0.002)
        self.send_step_message(self.current_tick)

    def send_query_message(self, msg): # asynchronized, other tasks can be done while waiting for the answer
        time.sleep(0.005) # wait for some time to avoid blocking the message pending
        while not self.ready:
            time.sleep(1)

        self.prev_time = time.time()
        self.ws.send(json.dumps(msg))

    def send_control_message(self, msg): # synchronized, wait until receive the answer
        time.sleep(0.005) # wait for some time to avoid blocking the message pending
        while not self.ready:
            time.sleep(1)

        self.ws.send(json.dumps(msg))
        sent_time = time.time()
        # wait until receive the answer or time out
        while(self.latest_message is None or self.latest_message['TYPE'] != msg['TYPE']):
            time.sleep(0.005)
            if time.time() - sent_time > self.retry_threshold:
                return False, f"Control time out, the message is {msg}"
        res = self.latest_message.copy()
        self.latest_message = None 
        if res['CODE'] == "OK":
            return True, res
        else:
            return False,  f"Control failed, the reply is {res}"

    def process_query_message(self, msg):
        ans_type = msg['TYPE'].replace("QUERY", "ANS")
        while(self.latest_message is None or self.latest_message['TYPE'] != ans_type):
            time.sleep(0.001)
            if time.time() - self.prev_time > self.retry_threshold:
                return "Query failed"
        res = self.latest_message.copy()
        self.latest_message = None
        return res
    
    def process_query_message_with_id(self, msg):
        ans_type = msg['TYPE'].replace("QUERY", "ANS")
        while(self.latest_message is None or self.latest_message['TYPE'] != ans_type or "ID" not in self.latest_message or self.latest_message['ID'] != msg['ID']):
            time.sleep(0.001)
            if time.time() - self.prev_time > self.retry_threshold:
                return "Query failed"
        res = self.latest_message.copy()
        self.latest_message = None
        return res
    
   
    # QUERY: inspect the state of the simulator
    # By default query public vehicles
    def query_vehicle(self, id = None, private_veh = False, transform_coords = False):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_vehicle"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id
            my_msg["PRV"] = private_veh
            my_msg["TRAN"] = transform_coords
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)

 
    # query taxi
    def query_taxi(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_taxi"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)
        
    # query bus
    def query_bus(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_bus"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id      
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)

        
    # query road
    def query_road(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_road"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id      
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)

    # query zone
    def query_zone(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_zone"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:

            my_msg["ID"] = id      
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)

    # query signal
    def query_signal(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_signal" 
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id      
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)

    # query chargingStation
    def query_chargingStation(self, id = None):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_chargingStation"
        if id is None:
            self.send_query_message(my_msg)
            return self.process_query_message(my_msg)
        else:
            my_msg["ID"] = id      
            self.send_query_message(my_msg)
            return self.process_query_message_with_id(my_msg)
        
    # query vehicleID within the co-sim road
    def query_coSimVehicle(self):
        my_msg = {}
        my_msg["TYPE"] = "QUERY_coSimVehicle"
        self.send_query_message(my_msg)
        return self.process_query_message(my_msg)
        
    # CONTROL: change the state of the simulator
    # set the road for co-simulation
    def set_cosim_road(self, roadID):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_setCoSimRoad"
        my_msg["roadID"] = roadID
        return self.send_control_message(my_msg)
    
    # release the road for co-simulation
    def release_cosim_road(self, roadID):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_releaseCoSimRoad"
        my_msg["roadID"] = roadID
        return self.send_control_message(my_msg)
        
    # teleport vehicle to a target location specified by road, lane, and distance to the downstream junction
    def teleport_vehicle(self, vehID, roadID, laneID, dist, x, y, private_veh = False, transform_coords = False):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_teleportVeh"
        my_msg["vehID"] = vehID
        my_msg["roadID"] = roadID
        my_msg["laneID"] = laneID
        my_msg["dist"] = dist
        my_msg["prv"] = private_veh
        my_msg["x"] = x
        my_msg["y"] = y
        my_msg["TRAN"] = transform_coords
        return self.send_control_message(my_msg)
    
    # enter the next road
    def enter_next_road(self, vehID, private_veh = False):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_enterNextRoad"
        my_msg["vehID"] = vehID
        my_msg["prv"] = private_veh
        return self.send_control_message(my_msg)
    
    # generate a vehicle trip
    def generate_trip(self, vehID, origin = None, destination = None):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_generateTrip"
        my_msg["vehID"] = vehID # if not exists, the sim will generate a new vehicle with this vehID
        if origin is not None:
            my_msg["origin"] = origin
        else:
            my_msg["origin"] = -1
        if destination is not None:
            my_msg["destination"] = destination
        else:
            my_msg["destination"] = -1
        return self.send_control_message(my_msg)

    
    # control vehicle with specified acceleration  
    def control_vehicle(self, vehID, acc, private_veh = False):
        my_msg = {}
        my_msg["TYPE"] = "CTRL_controlVeh"
        my_msg["vehID"] = vehID
        my_msg["acc"] = acc
        my_msg["prv"] = private_veh
        return self.send_control_message(my_msg)
        
    # override __str__ for logging 
    def __str__(self):
        s = f"-----------\n" \
            f"Client INFO\n" \
            f"-----------\n" \
            f"index :\t {self.index}\n" \
            f"address :\t {self.uri}\n" \
            f"state :\t {self.state}\n" 
        return s
