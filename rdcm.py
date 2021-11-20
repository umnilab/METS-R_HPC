import websocket
import json
import sys
import os
import threading
from threading import Lock
import ast
import time
import pandas as pd

import socket
from contextlib import closing

from eco_routing.MabManager import MABManager
from bus_scheduling.BusPlanningManager import BusPlanningManager

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
                    
    # Loading the demand prediction result
    path_pre = config['evacsim_dir']+args['DEMAND_PREDICTION_RESULTS_PATH']
    files = os.listdir(path_pre)
    demand_pre = {}
    for f in files:
        pre_type = f.split("PCA")[0]
        demand_pre[pre_type] = pd.read_csv(path_pre+"/"+f)
    # print(demand_pre.keys())
    # print(demand_pre['LGA'])
    ## query test for demand prediciton
    #currenthour_query=14
    #index_row=currenthour_query-12
    # will map index based on region 
    #index_col=2
    #query_14=demand_pre['LGA'].iloc[index_row,index_col]
    #print(query_14)
 
    args = SN(**args)
    # print(args)

    # read data for taxi codes 
    
      
    mabManager= MABManager(config['evacsim_dir'], args)
    busPlanningManager = BusPlanningManager(range(1, args.NUM_OF_ZONE+1))
    busPlanningManager.generate_route()
    busPlanningManager.optimize_route()
    
    currentHour = {}
    previousHour = {}

    rd_clients = []

    # create all clients and start the threads
    for i in range(num_clients):
        ws_client = RDClient("localhost", int(port_numbers[i]), i, False)
        ws_client.start()
        rd_clients.append(ws_client)
        currentHour[port_numbers[i]] = 0
        previousHour[port_numbers[i]] = -1

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
            #print(routeUCBMap)

    print("routeUCBMap received")
    #print(routeUCBMap)
    routeUCBMapBus = {}

    i = 0
    while len(routeUCBMapBus) == 0:
        with rd_clients[i].lock:
            routeUCBMapBus = rd_clients[i].route_ucb_bus_received
            #print("routeUCBMapBus received"+str(rd_clients[i].route_ucb_bus_received))
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
    #print(args)
    # initialize route result
    routeResult = []
    routeResultBus = []
    # print(defaultdict)
    print(range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)))
    for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)+1):
        oneResult = {}
        for od in routeUCBMap:
            oneResult[od]=-1 
        routeResult.append(oneResult)
        #oneResultBus = defaultdict(lambda: -1)
        # raw value is simply -1
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
                #print("rd_client is")
                #print(rd_clients[i])
                linkUCBMap = rd_clients[i].link_ucb_received 
                print("link ucb received")
                print(linkUCBMap.keys())
                hour = mabManager.refreshLinkUCB(linkUCBMap)
                currentHour[port_numbers[i]] = hour
                linkUCBMapBus = rd_clients[i].link_ucb_bus_received
                mabManager.refreshLinkUCBBus(linkUCBMapBus)
                speedVehicle = rd_clients[i].speed_vehicle_received
                mabManager.refreshLinkUCBShadow(speedVehicle)
        #print("mabmanager is ")
        #print(mabManager)
        print("current hour is " +str(currentHour))
        #print(routeResult)
        for hour_od in currentHour.values():
            for od in routeResult[hour_od]:
                #print("hour is " +str(hour))
                #print("od is " +str(od))
                routeAction = mabManager.ucbRouting(od, hour_od)
                routeResult[hour_od][od] = routeAction
            for od in routeResultBus[hour_od]:
                routeAction = mabManager.ucbRoutingBus(od, hour_od)
                routeResultBus[hour_od][od] = routeAction
            #print("routeResult_Bus_string")
            #print(json.dumps(routeResultBus[hour_od]))
        # sent back the routing result      
        for i in range(num_clients):
            with rd_clients[i].lock:
                 #  clean the data set after refresh functions     
                 rd_clients[i].link_ucb_received={}
                 rd_clients[i].link_ucb_bus_received={}
                 rd_clients[i].speed_vehicle_received={}
                 hour = currentHour[port_numbers[i]]
                 index_od =0
                 index_od_bus=0
                 od_list=[]
                 result_list=[]
                 bus_od_list=[]
                 bus_result_list=[]
        # generate json objects with keys "origin","dest",and "result"
                 for od in routeResult[hour]:
                    od_list.append(od)
                    result_list.append(routeResult[hour][od])
                    index_od+=1
                 for od in routeResultBus[hour]:
                    bus_od_list.append(od)
                    bus_result_list.append(routeResultBus[hour][od])
                    index_od_bus+=1   
                 routeResult_json_dict={}
                 routeResult_json_dict['MSG_TYPE']="OD_PAIR"
                 routeResult_json_dict['OD']=od_list 
                 routeResult_json_dict['result']=result_list
                 routeResultBus_json_dict={}
                 routeResultBus_json_dict['MSG_TYPE']="BOD_PAIR"
                 routeResultBus_json_dict['OD']=bus_od_list 
                 routeResultBus_json_dict['result']=bus_result_list
                 if index_od==len(routeResult[hour]):
                    #print("length of index"+str(len(routeResult[hour])))
                    #print("length of hour"+str(index_od))
                    routeResult_json_string=json.dumps(routeResult_json_dict)
                    rd_clients[i].ws.send(routeResult_json_string) 
                    #print("routeResult_json_string")
                    #print(routeResult_json_string)
                 if index_od_bus==len(routeResultBus[hour]):
                    routeResultBus_json_string=json.dumps(routeResultBus_json_dict)
                    rd_clients[i].ws.send(routeResultBus_json_string)
                 # generate json message based on the bus planning optimization
                 BusPlanning_json = {}
                 if (((hour%2)==0) and (hour>previousHour[port_numbers[i]])):
                    # mode function and upate the bus planning every 2 hours
                    # only send message when current hour differs from previous hour
                    BusPlanning_json['MSG_TYPE'] = "BUS_SCHEDULE"
                    BusPlanning_json['Bus_num'] = list(busPlanningManager.bus_frequency[0:33])
                    len_json=len(BusPlanning_json['Bus_num'])
                    ## generate dummy gap message 
                    dummygaplist = list(range(1200, 1200+len_json))
                    BusPlanning_json['Bus_gap'] = dummygaplist
                    BusPlanning_json['Bus_route'] = busPlanningManager.bus_route['routes_fromhub']
                    ## generate dummy route name based on hub time and route count
                    list_routename=[]
                    for l in range(0,len_json):
                        # assume the current hub is 134
                        # XXX for hub  XX for hour XX for route 
                        list_routename.append(134*10000+hour*100+l)
                    BusPlanning_json['Bus_routename'] = list_routename
                    BusPlanning_json['Bus_currenthour'] = hour 
                    BusPlanning_json_string = json.dumps(BusPlanning_json)
                    rd_clients[i].ws.send(BusPlanning_json_string)
                    previousHour[port_numbers[i]]=hour
                    print('BusPlanning_json_string')
                    print(BusPlanning_json_string)       
        time.sleep(0.5) # wait for 0.5 seconds
        #print("time sleep 0.5 s")
        # wait until all rd_clients finish their work
    for j in range(num_clients):
        rd_clients[j].join()
        print("join function performed")
    #for i in range(num_clients):
            #print(rd_clients[i]) 

# main function (used only for debugging)
if __name__ == "__main__":

    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    # Obtain simulation arguments
    num_clients = int(config['num_sim_instances'])
    port_numbers = config['socket_port_numbers']

    run_rdcm(num_clients, port_numbers)


