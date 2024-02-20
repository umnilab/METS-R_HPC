# remote data client interface. Each RDClient runs in a separate thread
# run() method  

import websocket
import json
import threading
from contextlib import closing
from threading import Lock
from util import check_socket, str_list_mapper_gen

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)


"""
Implementation of the RDC (remote data client)

A RDC directly communicates with a specific METSR-SIM instance.
"""

class RDClient(threading.Thread):

    def __init__(self, host, port, index, log_msgs, msg_log_size=50):
        super().__init__()

        # Websocket config
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self.index = index
        self.state = "connecting"

        # Msgs logging flags
        self.msg_log_size = msg_log_size
        self.log_msgs = log_msgs

        # Data maps
        self.hour = -1
        self.prevHour = -1
        self.route_ucb_received = {}
        self.route_ucb_bus_received = {}
        self.link_ucb_received = {}
        self.link_ucb_bus_received = {}
        self.speed_vehicle_received = {}

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
        if(self.log_msgs):
            print(f"{self.uri} : {message[0:self.msg_log_size]}")

        # Decode the json string
        decoded_msg = json.loads(str(message))
        # print(decoded_msg)
        # Every decoded msg must have a MSG_TYPE field
        assert 'MSG_TYPE' in decoded_msg.keys(), "No MSG_TYPE field in received json string!"
        
        # OD pair and ucb route candidates received 
        if decoded_msg['MSG_TYPE'] == "OD_PAIR":
            #print("OD pair start to be updated")
            self.update_route_ucb(decoded_msg)
            #print("OD pair fnished updated")
            
        # bus OD pair and ucb bus route candidates received
        elif decoded_msg['MSG_TYPE'] == "BOD_PAIR":
            self.update_route_ucb_bus(decoded_msg)
              
        # Tick msg received
        elif decoded_msg['MSG_TYPE'] == 'TICK_MSG':
            entries = decoded_msg["entries"]
            for entry in entries:
                # ev energy update
                if entry['TYPE'] == 'E':
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    values=entry['values']
                    self.update_link_ucb(id, values)

                # bus energy update
                elif entry['TYPE'] == 'BE':
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    values=entry['values']
                    self.update_link_ucb_bus(id, values)

                # vehicle speed update
                elif entry['TYPE'] == 'V':
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    values=entry['values']
                    self.update_speed_vehicle(id, values)
                
                # simulation hour update
                elif entry['TYPE'] == 'H':
                    self.hour = int(entry['hour'])
                    
                else:
                    print(f"unknown entry type in json mesage : {entry['TYPE']}")

    # Method for updating the link energy map
    def update_link_ucb(self, id,values):
        if id not in self.link_ucb_received.keys():
            self.link_ucb_received[id] = values        
        else:
            self.link_ucb_received[id].extend(values)

    # Method for updating the link bus energy map
    def update_link_ucb_bus(self, id,  values):
        if id not in self.link_ucb_bus_received.keys():
            self.link_ucb_bus_received[id] = values
        else:
            self.link_ucb_bus_received[id].extend(values)
    
    # Method for updating the vehicle speed map
    def update_speed_vehicle(self, id, values):
        if id not in self.speed_vehicle_received.keys():
            self.speed_vehicle_received[id] = values
        else:
            self.speed_vehicle_received[id].extend(values)

    # Method for updating the UCB routes
    def update_route_ucb(self, json_obj):
        assert json_obj['OD'] not in self.route_ucb_received.keys(), f"WARNING : OD pair {json_obj['OD']} is already in the route_ucb_received map!"
        self.route_ucb_received[json_obj['OD']] = list(map(str_list_to_int_list, json_obj['road_lists']))

    # method for updating the ucb bus routes
    def update_route_ucb_bus(self, json_obj):
        assert json_obj['BOD'] not in self.route_ucb_bus_received.keys(), f"WARNING : BOD pair {json_obj['BOD']} is already in the route_ucb_bus_received map!"

        self.route_ucb_bus_received[json_obj['BOD']] = list(map(str_list_to_int_list, json_obj['road_lists']))

    def on_error(self, ws, error):
        self.state = "error"
        print(error)

    def on_close(self, ws, status_code, close_msg):
        self.state = "closed"
        print(f"{self.uri} : connection closed")

    def on_open(self, ws):
        self.state = "connected"
        print(f"{self.uri} : connection opened")

    # run() method implements what RDClient will be doing during its lifetime
    def run(self):
        print(f"waiting until the server is up at {self.uri}")

        # all clients are disconnected
        wait_time = 0

        while not check_socket(self.host, self.port):
            # count the real-world seconds 
            wait_time += 1
            if wait_time > 20:
                print(f"waiting for the server to be up at {self.uri}.. time out in {30-wait_time} seconds..")
            if wait_time > 30:
                print("Waiting overtime, please check the connection and restart the simulation.")
                # close the connection
                self.ws.close()
                break

        if wait_time <= 60:
            print(f"sever is active at {self.uri},  running client..")

            # listen to the server forever
            self.ws.run_forever()

    # override __str__ for logging 
    def __str__(self):
        s = f"-----------\n" \
            f"Client INFO\n" \
            f"-----------\n" \
            f"index :\n {self.index}\n" \
            f"address :\n {self.uri}\n" \
            f"route_ucb_received keys :\n {self.route_ucb_received.keys()}\n" \
            f"route_ucb_bus_received keys :\n {self.route_ucb_bus_received.keys()}\n" \
            f"link_ucb_received keys :\n {self.link_ucb_received.keys()}\n" \
            f"link_ucb_bus_received keys :\n {self.link_ucb_bus_received.keys()}\n" \
            f"speed_vehicle_received keys :\n {self.speed_vehicle_received.keys()}\n" 
        return s
