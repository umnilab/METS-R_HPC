# remote data client interface. Each RDClient runs in a separate thread
# run() method  

import websocket
import json
import threading
from contextlib import closing
from threading import Lock
from util import check_socket, str_list_mapper_gen
import time
import os

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)


"""
Implementation of the remote data client

A RDC directly communicates with a specific METSR-SIM instance.
"""

class RemoteDataClient(threading.Thread):

    def __init__(self, host, port, index, manager):
        super().__init__()

        # Websocket config
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self.index = index
        self.state = "connecting"

        # a pointer to the manager
        self.manager = manager

        # Track the tick of the corresponding simulator
        self.current_tick = -1
        self.prev_tick = -1
        self.prev_time = time.time()

        # Create a listener socket
        self.ws = websocket.WebSocketApp(self.uri,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        # Data maps can be accessed by both main thread (for ML algorithms)
        # and RDClient class. Therefore synchronization is needed to avoid data races.
        # If the main thread only does reads, the lock can be removed safely
        self.lock = Lock()


    # on_message is automatically called when the sever sends a msg
    def on_message(self, ws, message):
        # for debugging
        # print(f"{self.uri} : {message[0:100]}")

        # Decode the json string
        decoded_msg = json.loads(str(message))

        # Every decoded msg must have a MSG_TYPE field
        assert 'TYPE' in decoded_msg.keys(), "No TYPE field in received json string!"

        # handle decoded msg based on MSG_TYPE
        if decoded_msg['TYPE'] == "STEP":
            self.handle_step_message(decoded_msg)
        elif decoded_msg['TYPE'].split("_")[0] == "ANS":
            self.handle_answer_message(ws, decoded_msg)
        elif decoded_msg['TYPE'].split("_")[0] == "ATK":
            self.handle_attack_message(ws, decoded_msg)
            
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
        print(f"waiting until the server is up at {self.uri}")

        # all clients are disconnected
        wait_time = 0

        # wait until the server is up, or timeout after 60 seconds
        while not check_socket(self.host, self.port):
            # count the real-world seconds 
            wait_time += 1
            if wait_time > 20:
                print(f"waiting for the server to be up at {self.uri}.. time out in {30-wait_time} seconds..")
            if wait_time > 30:
                print("Waiting overtime, please check the connection and restart the simulation.")
                # close the connection
                self.ws.close()
                os.chdir("docker")
                os.system("docker-compose down")
                break

        if wait_time <= 30:
            print(f"sever is active at {self.uri},  running client..")
            self.ws.run_forever()

                    
    # Method for handle messages
    def handle_step_message(self, decoded_msg):
        tick = decoded_msg['TICK']
        if tick > self.current_tick: # tick less than current_tick is ignored
            self.current_tick = tick

    def handle_answer_message(self, ws, decoded_msg):
        if decoded_msg['TYPE'] == "ANS_TaxiUCB":
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

    def send_step_message(self, tick):
        self.prev_tick = tick
        self.prev_time = time.time()
        msg = {'TYPE': 'STEP', 'TICK': tick}
        self.ws.send(json.dumps(msg))
        

    def send_query_message(self, msg):
        self.ws.send(json.dumps(msg))

    # override __str__ for logging 
    def __str__(self):
        s = f"-----------\n" \
            f"Client INFO\n" \
            f"-----------\n" \
            f"index :\t {self.index}\n" \
            f"address :\t {self.uri}\n" \
            f"state :\t {self.state}\n" 
        return s
