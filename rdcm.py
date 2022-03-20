import sys
import os
import json
import ast
import time
import pandas as pd

from eco_routing.MabManager import MABManager
from bus_scheduling.RouteGeneration20211124 import RouteGeneration
from bus_scheduling.RouteOptimization20211124 import RouteOptimization
from types import SimpleNamespace as SN
from collections import defaultdict
from rdc import RDClient

# main function for remote control client (RDC) manager
# to the configurations specified in config
def run_rdcm(config, num_clients, port_numbers):
    # Obtain simulation arguments
    args = {}
    with open(os.path.join(config.addsevs_dir+'data', 'Data.properties'), "r") as f:
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

    # Track the progress (simulation hours) of each instance using rd_clients
    rd_clients = []
    currentHour = {}
    previousHour = {}
 
    for i in range(num_clients):
        #pending_servers[i].server_close()
        ws_client = RDClient("localhost", int(port_numbers[i]), i, False)
        ws_client.start()
        rd_clients.append(ws_client)
        currentHour[port_numbers[i]] = 0
        previousHour[port_numbers[i]] = -1
    print("Created all clients!")


    # TODO : machine learning stuff can go here in the main thread
    # ---------- ML STUFF GOES HERE ------------------------------
    # NOTES : 
    # 1) you need to acquire a lock if you read or write to rd_client's 
    #   data maps, somthing like,
    #   with rd_client[i].lock:
    #       do somthing here with rd_client data maps
    # 2) all messages are encoded in json format, so route_result must
    #   also be in JSON format, also change the route_result reception side
    #   in the simulator to facilitate this.
    # TODO : just print the content of rd_clients for debugging purposes, remove if not needed
    
    # initialize UCB data
    if (config.eco_routing == 'true'):
        print("Initializing CUCB data!")
        mabManager= MABManager(config.addsevs_dir, args)
        routeUCBMap = {}
        i = 0
        with rd_clients[i].lock:
            while len(routeUCBMap) == 0:
                routeUCBMap = rd_clients[i].route_ucb_received
                i += 1
                i = i % num_clients
                time.sleep(0.5)
            print("routeUCBMap received")
            # print(routeUCBMap)
            # routeUCBMapBus = {}
            # i = 0
            # while len(routeUCBMapBus) == 0:
            #     with rd_clients[i].lock:
            #         routeUCBMapBus = rd_clients[i].route_ucb_bus_received
            #         #print("routeUCBMapBus received"+str(rd_clients[i].route_ucb_bus_received))
            #         i += 1
            #         i = i % num_clients
            #         time.sleep(0.5)
            # print("routeUCBMapBus received")
        time.sleep(10) # wait for processing routeUCBMap        
        # initialize mabManager using background data
        mabManager.refreshRouteUCB(routeUCBMap)
        # mabManager.refreshRouteUCBBus(routeUCBMapBus)
        mabManager.initializeLinkEnergy1()
        mabManager.initializeLinkEnergy2()
        # initialize route result
        routeResult = []
        # routeResultBus = []
        # print(list(routeUCBMap.keys()))
        for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)+1):
            oneResult = {}
            for od in routeUCBMap.keys():
                oneResult[od]=-1 
            routeResult.append(oneResult)
            #oneResultBus = defaultdict(lambda: -1)
            # raw value is simply -1
            # oneResultBus = {}
            # for od in routeUCBMapBus:
            #     oneResultBus[od]=-1 
            # routeResultBus.append(oneResultBus)

    # If enabling bus scheduling, then loading the demand prediction data
    if (config.bus_scheduling == 'true'):
        print("Initializing bus scheduling data")
        date_sim=args.BT_EVENT_FILE.split("speed_")[1].split(".csv")[0]
        scenario_index=args.BT_EVENT_FILE.split("scenario")[1].split("/speed")[0]
        # data for bus scheduling
        #path_pre = "demand_prediction/Modelling/PredictionResults"
        #demand_file_location_from = {}
        #demand_file_location_to = {}
        #for f in ['JFK','LGA','PENN']:
        #    demand_file_location_from[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019fromHub.csv")
        #    demand_file_location_to[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019toHub.csv")
        #taxi_zone_file = "bus_scheduling/input_route_generation/tax_zones_bus_version.gpkg"  
        # use date_sim as the index in demand file
        
        # directly use the cached results as the optimization is much slower than the simulator
        ## comment this block and activate the next block                         
        bus_scheduling_read = "bus_scheduling/bus_ratio_demand_8_80_400/ratio_scenario"+scenario_index+"_speed_"+date_sim+"_bus_scheduling.json" 
        bus_scheduling_read_raw = open(bus_scheduling_read)
        busPlanningResults = json.load(bus_scheduling_read_raw)
        ## generate bus schedules in real time
        #busPlanningResults={}
        #for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)+1):
        #    busPlanningResults[hour] = {}
        #    if (len(bus_scheduling_file[str(hour)].keys())>0):
        #       for f in ['JFK','LGA','PENN']:
        #           busPlanningResults[str(hour)][f]=bus_scheduling_file[str(hour)][f]
                

    # Update the UCB result regularly
    hour = 0
    while True:
        print(currentHour)
        # uncomment this block if you want to generate bus schedules in real time
        # generate json message based on the bus planning optimization
        # if ((hour%2)==0 and hour>previousHour[port_numbers[i]]):
        #         # mode function and upate the bus planning every 2 hours
        #         # only send message when current hour differs from previous hour
        #         for f in ['JFK','LGA','PENN']:
        #             bus_planning_json = {}
        #             hub_type = f
        #             #JFK: 114; LGA: 120; PENN: 164. 
        #             if hub_type=='JFK':
        #                 hub_index = 114  
        #             if hub_type=='LGA':
        #                 hub_index = 120
        #             if hub_type=='PENN':
        #                 hub_index = 164 
        #             if hub_index>=180:
        #                 continue
        #             hour_idx= min(hour + 2, int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600))
        #             max_route = 30
        #             routeGeneration = RouteGeneration(hub_index,bus_ratio_file,demand_file_location_from[f],demand_file_location_to[f],taxi_zone_file,max_route, date_sim, hour_idx)          
        #             routeGeneration.run()
        #             # run() can include route_generate() and route_combine() function you def
        #             mat=routeGeneration.bus_mat
        #             Tlist = [10]     #uncertainty level
        #             Blist = [200]    #fleet size  
        #             routeOptimization=RouteOptimization(mat,Tlist,Blist)
        #             routeOptimization.run()
        #             # run() perform the process
        #             bus_planning_json['Bus_route'] =routeOptimization.Bus_route
        #             len_json=len(bus_planning_json['Bus_route'])
        #             bus_planning_json['Bus_num'] =routeOptimization.Bus_num[:len_json]
        #             # extract the first len_json items
        #             bus_planning_json['Bus_gap'] =routeOptimization.Bus_gap[:len_json]
        #             if sum(bus_planning_json['Bus_num'])==0:
        #                 bus_planning_json['Bus_num'][0]=1
        #                 bus_planning_json['Bus_gap'][0]=routeOptimization.bus_mat["route_trip_time"][0][0]*60
        #             print(bus_planning_json['Bus_route'])
        #             ## organize the json format of output 
        #             bus_planning_json['MSG_TYPE'] = "BUS_SCHEDULE" 
        #             # len_json=len(bus_planning_json['Bus_num']) 
        #             # generate dummy route name based on hub time and route count
        #             list_routename=[]
        #             for l in range(0,len_json): 
        #                 # XXX for hub  XX for hour XX for ro
        #                 list_routename.append(hub_index*10000+hour_idx*100+l)
        #             bus_planning_json['Bus_routename'] = list_routename
        #             bus_planning_json['Bus_currenthour'] = str(hour_idx) 
        #             busPlanningResults[str(hour_idx)][f] = json.dumps(bus_planning_json)
        #             previousHour[port_numbers[i]]=hour
        #             # print('bus_planning_json_str')
        #             # print(busPlanningResults[hour][f]) 
        if(config.eco_routing == 'true'):
            # # update the routing data
            for i in range(num_clients):
                with rd_clients[i].lock:
                    linkUCBMap = rd_clients[i].link_ucb_received 
                    print("link ucb received")
                    # print(linkUCBMap.keys())
                    hour = mabManager.refreshLinkUCB(linkUCBMap)
                    #print("hour is")
                    #print(hour)
                    if(currentHour[port_numbers[i]] < hour):
                        currentHour[port_numbers[i]] = hour
                    # linkUCBMapBus = rd_clients[i].link_ucb_bus_received
                    # mabManager.refreshLinkUCBBus(linkUCBMapBus)
                    # speedVehicle = rd_clients[i].speed_vehicle_received
                    # mabManager.refreshLinkUCBShadow(speedVehicle)
            for hour_od in currentHour.values():
                for od in routeResult[hour_od]:
                    routeAction = mabManager.ucbRouting(od, hour_od)
                    routeResult[hour_od][od] = routeAction
                # for od in routeResultBus[hour_od]:
                #     routeAction = mabManager.ucbRoutingBus(od, hour_od)
                #     routeResultBus[hour_od][od] = routeAction
            # sent back the planning results     
            for i in range(num_clients):
                with rd_clients[i].lock:
                    # clean the data set after refresh functions     
                    rd_clients[i].link_ucb_received={}
                    # rd_clients[i].link_ucb_bus_received={}
                    # rd_clients[i].speed_vehicle_received={}
                    hour = currentHour[port_numbers[i]]
                    index_od =0
                    # index_od_bus=0
                    od_list=[]
                    result_list=[]
                    # bus_od_list=[]
                    # bus_result_list=[]
                    # generate json objects with keys "origin","dest",and "result"
                    for od in routeResult[hour]:
                       od_list.append(od)
                       result_list.append(routeResult[hour][od])
                       index_od+=1
                    # for od in routeResultBus[hour]:
                    #    bus_od_list.append(od)
                    #    bus_result_list.append(routeResultBus[hour][od])
                    #    index_od_bus+=1   
                    routeResult_json_dict={}
                    routeResult_json_dict['MSG_TYPE']="OD_PAIR"
                    routeResult_json_dict['OD']=od_list 
                    routeResult_json_dict['result']=result_list
                    # routeResultBus_json_dict={}
                    # routeResultBus_json_dict['MSG_TYPE']="BOD_PAIR"
                    # routeResultBus_json_dict['OD']=bus_od_list 
                    # routeResultBus_json_dict['result']=bus_result_list
                    if index_od==len(routeResult[hour]):
                        #print("length of index"+str(len(routeResult[hour])))
                        #print("length of hour"+str(index_od))
                        routeResult_json_string=json.dumps(routeResult_json_dict)
                        rd_clients[i].ws.send(routeResult_json_string) 
                        print("routing results sent!") 
                    # if index_od_bus==len(routeResultBus[hour]):
                    #    routeResultBus_json_string=json.dumps(routeResultBus_json_dict)
                    #    rd_clients[i].ws.send(routeResultBus_json_string)
        
        if(config.bus_scheduling == 'true'):
            for i in range(num_clients):
                hour=currentHour[port_numbers[i]]
                if (((hour%2)==0) and (hour>previousHour[port_numbers[i]])):
                    # mode function and upate the bus planning every 2 hours
                    # only send message when current hour differs from previous hour
                    # all results are prepared
                    for f in ['JFK','LGA','PENN']:
                        bus_planning_prepared = True
                        if f not in busPlanningResults[str(hour)]:
                            bus_planning_prepared = False
                    if bus_planning_prepared:
                        print("Send bus scheduling results!")
                        with rd_clients[i].lock:
                            busPlanningResults_combine={}
                            JFK_json=json.loads(busPlanningResults[str(hour)]['JFK'])
                            LGA_json=json.loads(busPlanningResults[str(hour)]['LGA'])
                            PENN_json=json.loads(busPlanningResults[str(hour)]['PENN'])
                            busPlanningResults_combine['Bus_route']=list(JFK_json['Bus_route'])+list(LGA_json['Bus_route'])+list(PENN_json['Bus_route'])
                            busPlanningResults_combine['Bus_num']=list(JFK_json['Bus_num'])+list(LGA_json['Bus_num'])+list(PENN_json['Bus_num'])
                            busPlanningResults_combine['Bus_gap']=list(JFK_json['Bus_gap'])+list(LGA_json['Bus_gap'])+list(PENN_json['Bus_gap'])
                            busPlanningResults_combine['MSG_TYPE']="BUS_SCHEDULE"
                            busPlanningResults_combine['Bus_routename']=list(JFK_json['Bus_routename'])+list(LGA_json['Bus_routename'])+list(PENN_json['Bus_routename'])
                            busPlanningResults_combine['Bus_currenthour']=JFK_json['Bus_currenthour']
                            print(json.dumps(busPlanningResults_combine))
                            rd_clients[i].ws.send(json.dumps(busPlanningResults_combine))
                            #for f in ['JFK','LGA','PENN']:
                            #    print(f)
                            #    print(busPlanningResults[str(hour)][f])
                            #    rd_clients[i].ws.send(busPlanningResults[str(hour)][f])
                            previousHour[port_numbers[i]]=hour
                                   
        time.sleep(0.5) # wait for 0.5 seconds

    # wait until all rd_clients finish their work
    for j in range(num_clients):
        rd_clients[j].join()

# main function (used only for debugging)
if __name__ == "__main__":
    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    # Obtain simulation arguments
    num_clients = int(config.num_sim_instances)
    port_numbers = config.socket_port_numbers
    # pending_servers=
    #run_rdcm(options.num_simulations, options.ports, options.server_sockets)
    run_rdcm(num_clients, port_numbers,index_bus_scheduling)


