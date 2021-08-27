import websocket
import json
import sys
import os
import threading
from threading import Lock
import ast
import time


import socket
from contextlib import closing

from eco_routing.MabManager import MABManager
from types import SimpleNamespace as SN

from collections import defaultdict

# utilities
def check_socket(host, port):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            return True
        else:
            return False

def str_list_mapper_gen(func):
    def str_list_mapper(str_list):
        return [func(str) for str in str_list]
    return str_list_mapper

str_list_to_int_list = str_list_mapper_gen(int)
str_list_to_float_list = str_list_mapper_gen(float)


# remote data client interface. Each RDClient runs in a separate thread
# run() method  
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
        # TODO : if the main thread only does reads, the lock can be removed safely?
        self.lock = Lock()


    # on_message is automatically called when the sever sends a msg
    def on_message(self, ws, message):
        print(message)
        if(self.log_msgs):
            print(f"{self.uri} : {message[0:self.msg_log_size]}")

        # decode the json string
        decoded_msg = json.loads(str(message))
        
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

            for entry in entries:
                # energy update 
                if entry['TYPE'] == 'E':
                    id = int(entry['ID'])
                    hour = int(entry['hour'])
                    values = map(str_list_to_float_list, entry['values'])
                    self.update_link_ucb(id, hour, values)

                # bus energy update
                elif entry['TYPE'] == 'BE':
                    id = int(entry['ID'])
                    hour = int(entry['hour'])
                    values = map(str_list_to_float_list, entry['values'])
                    self.update_link_ucb_bus(id, hour, values)

                # vehicle speed update
                elif entry['TYPE'] == 'V':
                    id = int(entry['ID'])
                    hour = int(entry['hour'])
                    values = map(str_list_to_float_list, entry['values'])
                    self.update_speed_vehicle(id, hour, values)
            
                else:
                    print(f"unknown entry type in json mesage : {entry['TYPE']}")

    # method for updating the link energy map
    def update_link_ucb(self, id, hour, values):
        with self.lock: 
            id = str(id)+";"+str(hour)
            if id not in self.link_ucb_received.keys():
                self.link_ucb_received[id] = values
        
            else:
                self.link_ucb_received[id].append(values)
    
    # method for updating the link bus energy map
    def update_link_ucb_bus(self, id, hour, values):
        with self.lock:
            id = str(id)+";"+str(hour)
            if id not in self.link_ucb_bus_received.keys():
                self.link_ucb_bus_received[id] = values
            
            else:
                self.link_ucb_bus_received[id].append(values)
    
    # method for updating the vehicle speed map
    def update_speed_vehicle(self, id, hour, values):
        with self.lock:
            id = str(id)+";"+str(hour)
            if id not in self.speed_vehicle_received.keys():
                self.speed_vehicle_received[id] = values
        
            else:
                self.speed_vehicle_received[id].append(values)

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

# method for intializing and running RDClients according
# to the configurations specified in config
def run_rdcm(num_clients, port_numbers):
    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    # Obtain simulation arguments
    args = {}
    with open(os.path.join(config['evacsim_dir']+'data', 'Data.properties'), "r") as f:
        for line in f:
            if "#" in line:
                continue
            fields = line.replace(" ","").strip().split("=")
            if len(fields) != 2:
                continue
            else:
                print(fields)
                try:
                    args[fields[0]] = ast.literal_eval(fields[1])
                except:
                    args[fields[0]] = fields[1]


    args = SN(**args)

    mabManager= MABManager(config['evacsim_dir'], args)

    currentHour = {}

    rd_clients = []

    # create all clients and start the threads
    for i in range(num_clients):
        ws_client = RDClient("localhost", int(port_numbers[i]), i, False)
        ws_client.start()
        rd_clients.append(ws_client)
        currentHour[port_numbers[i]] = 0

    print("created all clients!")

    # TODO : machine learning stuff can go here in the main thread
    # ---------- ML STUFF GOES HERE ------------------------------
    # NOTES : 
    # 1) you need to acquire a lock if you read or write to rd_client's 
    #   data maps, somthing like,
    #
    #   with rd_client[i].lock:
    #       do somthing here with rd_client data maps
    # 2) all messages are encoded in json format, so route_result must
    #   also be in JSON format, also change the route_result reception side
    #   in the simulator to facilitate this.
    # TODO : just print the content of rd_clients for debugging purposes, remove if not needed
    # initialize UCB data
    routeUCBMap = {}

    i = 0
    while len(routeUCBMap) == 0:
        with rd_clients[i].lock:
            routeUCBMap = rd_clients[i].route_ucb_received
            i += 1
            i = i % num_clients
            time.sleep(0.5)

    print("routeUCBMap received")

    routeUCBMapBus = {}

    i = 0
    while len(routeUCBMapBus) == 0:
        with rd_clients[i].lock:
            routeUCBMap = rd_clients[i].route_ucb_bus_received
            i += 1
            i = i % num_clients
            time.sleep(0.5)

    print("routeUCBMapBus received")

    # initialize mabManager using background data
    mabManager.refreshRouteUCB(routeUCBMap)
    mabManager.refreshRouteUCBBus(routeUCBMapBus)
    mabManager.initializeLinkEnergy1()
    mabManager.initializeLinkEnergy2()
    roadLength = mabManager.roadLengthMap

    # initialize route result
    routeResult = []
    routeResultBus = []
    for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SZIE/3600)):
        oneResult = {}
        for od in routeUCBMap:
            onResult[od]=-1
        routeResult.append(oneResult)
        oneResultBus = {}
        for od in routeUCBMapBus:
            oneResultBus[od]=-1
        routeResultBus.append(oneResultBus)

    # Update the UCB result regularly
    while True:
        print("One loop")
        # # update the routing data
        for i in range(num_clients):
            with rd_clients[i].lock:
                linkUCBMap = rd_clients[i].link_ucb_received
                hour = mabManager.refreshLinkUCB(linkUCBMap)
                currentHour[port_numbers[i]] = hour
                linkUCBMapBus = rd_clients[i].link_ucb_bus_received
                mabManager.refreshLinkUCBBus(linkUCBMapBus)
                speedVehicle = rd_clients[i].speed_vehicle_received
                mabManager.refreshLinkUCBShadow(speedVehicle)
        # list of hour, each hour contains lists of od, each element of the routing result is the routeAction
# turn into a json object with 3 levels: hour , then od; then origin and destination
        # update the routing result
        #  redudant for the od in routeResult? might be same od for this?
        for hour in currentHour:
            for od in routeResult[hour]:
                routeAction = mabManager.ucbRouting(od, hour)
                routeResult[hour][od] = routeAction
            for od in routeResultBus[hour]:
                routeAction = mabManager.ucbRoutingBus(od, hour)
                routeResultBus[hour][od] = routeAction
        # sent back the routing result
        for i in range(num_clients):
            with rd_clients[i].lock:
                hour = currentHour[port_numbers[i]]
                routeResult_json = {}
                routeResultBus_json = {}
                index_od = 0
                routeResult_json['MSG_TYPE'] = 'RR'
                for od in routeResult:
                    routeResult_json[index_od]['OD'] = od
                    routeResult_json[index_od]['result']=routeResult[hour][od]
                    index_od += 1
                index_od = 0
                routeResultBus_json['MSG_TYPE'] = 'BRR'
                for od in routeResultBus:
                    routeResultBus_json[index_od]['OD']=od
                    routeResultBus_json[index_od]['result']=routeResultBus[hour][od]
                    index_od += 1
            routeResult_json_string = json.dumps(routeResult_json)
            routeResultBus_json_string = json.dumps(routeResultBus_json)
            rd_clients[i].ws.send(routeResult_json_string)
            rd_clients[i].ws.send(routeResultBus_json_string)

               # ws_client.ws.send(json.dumps(routeResult[currentHour[port_numbers[i]]]))
              #  ws_client.ws.send(json.dumps(routeResultBus[currentHour[port_numbers[i]]]))
              # ws_client.ws.send(routeResult[currentHour[port_numbers[i]]])
              # ws_client.ws.send(routeResultBus[currentHour[port_numbers[i]]])
        # Note routeResult[currentHour[port_numbers[i]]] is a dictionary, to do things are:
        # Add a tag for MSG_TYPE
        # json.dump
        # send it back to the simulator
        # Simulation side, update Connection.onMessage()
        # list of list route result 
        # for loop add name information
        # is this list or string?
        #  how to turn the list of od into string? transmiting jsonstring directly or other process?

        # ws_client.ws.send(route_result_json)

        time.sleep(0.5) # wait for 0.5 seconds


        # wait until all rd_clients finish their work
        for i in range(num_clients):
            rd_clients[i].join()
    for i in range(num_clients):
        print(rd_clients[i])

    


# main function (used only for debugging)
if __name__ == "__main__":

    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    # Obtain simulation arguments
    num_clients = int(config['num_sim_instances'])
    port_numbers = config['socket_port_numbers']

    run_rdcm(num_clients, port_numbers)


