# remote data client interface. Each RDClient runs in a separate thread
# run() method  

import websocket
import json
from contextlib import closing
from threading import Lock
from util import check_socket, str_list_mapper_gen

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)

class RDClient(threading.Thread):

    def __init__(self, host, port, index, log_msgs, msg_log_size=50):
        super().__init__()

        # websocket config
        self.host = host
        self.port = port
        self.uri = f"ws://{host}:{port}"
        self.index = index

        # msgs logging flags
        self.msg_log_size = msg_log_size
        self.log_msgs = log_msgs

        # data maps
        self.route_ucb_received = {}
        self.route_ucb_bus_received = {}
        self.link_ucb_received = {}
        self.link_ucb_bus_received = {}
        self.speed_vehicle_received = {}

        # create a listener socket
        self.ws = websocket.WebSocketApp(self.uri,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        # data maps can be accessed by both main thread (for ML algorithms)
        # and RDClient class. Therefore synchronization is needed to avoid data races
        # TODO : if the main thready does reads, the lock can be removed safely?
        self.lock = Lock()


    # on_message is automatically called when the sever sends a msg
    #  this part has not been updated
    def on_message(self, ws, message):
        # print(message)
        if(self.log_msgs):
            print(f"{self.uri} : {message[0:self.msg_log_size]}")

        # decode the json string
        decoded_msg = json.loads(str(message))
        #print("received decoded message")
        #print(decoded_msg['MSG_TYPE'])
        # every decoded msg must have a MSG_TYPE field
        assert 'MSG_TYPE' in decoded_msg.keys(), "No MSG_TYPE field in received json string!"
        
        # OD pair and ucb route candidates received 
        if decoded_msg['MSG_TYPE'] == "OD_PAIR":
            self.update_route_ucb(decoded_msg)
            
        # bus OD pair and ucb bus route candidates received
        elif decoded_msg['MSG_TYPE'] == "BOD_PAIR":
            self.update_route_ucb_bus(decoded_msg)
              
        # tick msg received
        elif decoded_msg['MSG_TYPE'] == 'TICK_MSG':
                 
            entries = decoded_msg["entries"]
            #print("received tick message") 
            #print(entries)
            for entry in entries:
                # energy update 
                if entry['TYPE'] == 'E':
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    #id = int(entry['ID'])
                    #hour_int = int(entry['hour'])
                    #print('id string')
                    #print(id)
                    #values = list(map(str_list_to_float_list, entry['values']))
                    values=entry['values']
                    #print("values")
                    #print(values)
                    #self.link_ucb_received['hour_int'] = int(entry['hour'])
                    self.update_link_ucb(id, values)
                    #print("received value")
                    #print(self.link_ucb_received[id])

                # bus energy update
                elif entry['TYPE'] == 'BE':
                    #id = int(entry['ID'])
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    #values = list(map(str_list_to_float_list, entry['values']))
                    values=entry['values']
                    #print("values")
                    #print(values)
                    #self.link_ucb_bus_received['hour_int'] = int(entry['hour'])
                    self.update_link_ucb_bus(id, values)

                # vehicle speed update
                elif entry['TYPE'] == 'V':
                    #id = int(entry['ID'])
                    id = str(entry['ID'])+";"+str(entry['hour'])
                    #values = list(map(str_list_to_float_list, entry['values']))
                    values=entry['values']
                    #print("values")
                    #print(values)
                    #self.speed_vehicle_received['hour_int'] = int(entry['hour'])
                    self.update_speed_vehicle(id, values)
                else:
                    print(f"unknown entry type in json mesage : {entry['TYPE']}")

    # method for updating the link energy map
    def update_link_ucb(self, id,values):
        with self.lock:
            #self.link_ucb_received['hour_int'] = hour_int
            if id not in self.link_ucb_received.keys():
                self.link_ucb_received[id] = values
        
            else:
                #self.link_ucb_received[id].append(values)
                self.link_ucb_received[id].extend(values)
            #print("updated link_ucb_received")
            #print(self.link_ucb_received[id])
    # method for updating the link bus energy map
    def update_link_ucb_bus(self, id,  values):
        with self.lock:
            if id not in self.link_ucb_bus_received.keys():
                self.link_ucb_bus_received[id] = values
            
            else:
                self.link_ucb_bus_received[id].extend(values)
    
    # method for updating the vehicle speed map
    def update_speed_vehicle(self, id, values):
        with self.lock:
            if id not in self.speed_vehicle_received.keys():
                self.speed_vehicle_received[id] = values
        
            else:
                self.speed_vehicle_received[id].extend(values)

    # method for updating the ucb routes
    def update_route_ucb(self, json_obj):
        with self.lock:
            assert json_obj['OD'] not in self.route_ucb_received.keys(), f"WARNING : OD pair {json_obj['OD']} is already in the route_ucb_received map!"

            self.route_ucb_received[json_obj['OD']] = list(map(str_list_to_int_list, json_obj['road_lists']))

    # method for updating the ucb bus routes
    def update_route_ucb_bus(self, json_obj):
        with self.lock:
            assert json_obj['BOD'] not in self.route_ucb_bus_received.keys(), f"WARNING : BOD pair {json_obj['BOD']} is already in the route_ucb_bus_received map!"

            self.route_ucb_bus_received[json_obj['BOD']] = list(map(str_list_to_int_list, json_obj['road_lists']))


    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print(f"{self.uri} : connection closed")

    def on_open(self, ws):
        print(f"{self.uri} : connection opened")

    # run() method implements what RDClient will be doing during its lifetime
    # RDClient will wait until the simulation socket server is up and 
    # start listening to it until the socket connection is terminated.
    def run(self):
        print(f"waiting until the server is up at {self.uri}")
        while not check_socket(self.host, self.port):
            pass

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
            f"speed_vehicle_received keys :\n {self.speed_vehicle_received.keys()}\n" \

        return s