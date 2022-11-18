import sys
import signal
import json
import ast
import time
import pandas as pd
import psutil, os

from eco_routing.MabManager import MABManager
from bus_scheduling.RouteGeneration import RouteGeneration
from bus_scheduling.RouteOptimization import RouteOptimization
from types import SimpleNamespace as SN
from collections import defaultdict
from rdc import RDClient

"""
Implementation of the RDCM (remote data client manager)

RDCM communicates with multiple RDCs (remote data clients) to manage the 
data flow between corresponding simulation instances.

Currently we assume that all simulation instances are ran with the same 
configurations, which can be extended to cover instances with different
settings.
"""

def run_rdcm(config, num_clients, port_numbers):
    # Obtain simulation arguments from the configuration file
    args = {}
    with open(os.path.join(config.data_dir, 'Data.properties'), "r") as f:
        for line in f:
            if "#" in line:
                continue
            fields = line.replace(" ","").strip().split("=")
            if len(fields) != 2:
                continue
            else:
                try:
                    args[fields[0]] = ast.literal_eval(fields[1])
                except:
                    args[fields[0]] = fields[1]
    args = SN(**args)

    # Create and maintain a list for RDCs
    rd_clients = []
 
    for i in range(num_clients):
        ws_client = RDClient("localhost", int(port_numbers[i]), i, False)
        ws_client.start()
        rd_clients.append(ws_client)
    print("Created all clients!")


    ''' 
    ---------- OPERATIONAL ALGS START HERE ------------------------------
    Remarks : 
    1) you need to acquire a lock if you read or write to rd_client's 
      data maps, somthing like,
      with rd_client[i].lock:
          do somthing here with rd_client data maps
    2) all messages are encoded in json format, so route_result must
      also be in JSON format, also change the route_result reception side
      in the simulator to facilitate this.
    ''' 
    
    # Initialize UCB data
    print("Initializing operational data!")
    mabManager= MABManager(config.sim_dir, args)
    with rd_clients[i].lock:
        routeUCBMap = {}
        i = 0
        while len(routeUCBMap) == 0:
            routeUCBMap = rd_clients[i].route_ucb_received
            i += 1
            i = i % num_clients
            time.sleep(0.5)
        print("routeUCBMap received")
        # uncomment to enable eco-routing for bus
        # routeUCBMapBus = {}
        # i = 0
        # while len(routeUCBMapBus) == 0:
        #     with rd_clients[i].lock:
        #         routeUCBMapBus = rd_clients[i].route_ucb_bus_received
        #         i += 1
        #         i = i % num_clients
        #         time.sleep(0.5)
        # print("routeUCBMapBus received")
    time.sleep(30) # Wait some time for processing routeUCBMap        
    # Initialize mabManager using background data
    mabManager.refreshRouteUCB(routeUCBMap)
    # mabManager.refreshRouteUCBBus(routeUCBMapBus)
    mabManager.initializeLinkEnergy1()
    mabManager.initializeLinkEnergy2()
    
    # Initialize route result
    routeResult = []
    # routeResultBus = []
    
    totalHour = int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)
    emptyCount = 0
    for hour in range(totalHour+1):
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

    # Initialize bus scheduling data
    if (config.bus_scheduling == 'true'):
        print("Initializing bus scheduling data")
        date_sim=args.BT_EVENT_FILE.split("speed_")[1].split(".csv")[0]
        scenario_index=args.BT_EVENT_FILE.split("scenario")[1].split("/speed")[0]
        # data for bus schedulinge
        #path_pre = "demand_prediction/Modelling/PredictionResults"
        #demand_file_location_from = {}
        #demand_file_location_to = {}
        #for f in ['JFK','LGA','PENN']:
        #    demand_file_location_from[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019fromHub.csv")
        #    demand_file_location_to[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019toHub.csv")
        #taxi_zone_file = "bus_scheduling/input_route_generation/tax_zones_bus_version.gpkg"  
        # use date_sim as the index in demand file
        ## generate bus schedules in real time
        #busPlanningResults={}
        #for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)+1):
        #    busPlanningResults[hour] = {}
        
        # directly use the cached results as the optimization is much slower than the simulator                        
        bus_scheduling_read = "bus_scheduling/offline_cache_cleaned/scenario_"+scenario_index+"_speed_"+date_sim + "_" + str(args.NUM_OF_BUS)+"_bus_scheduling.json"
        print("Using cached bus schedule from: " + bus_scheduling_read)
        bus_scheduling_read_raw = open(bus_scheduling_read)
        busPlanningResults = json.load(bus_scheduling_read_raw)

    # Recurrent data flow
    hour = 0
    continue_flag = True
    while continue_flag:
        try:
            # Uncomment this block if you want to generate bus schedules in real time
            # for i in range(len(rd_clients)):
            #     if ((hour%2)==0 and hour>rd_clients[i].hour):
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
            if(config.eco_routing == 'true'):
                # Training the eco-routing with newly observed energy consumption of traversing links
                for i in range(num_clients):
                    if rd_clients[i].state == "connected":
                        with rd_clients[i].lock:
                            linkUCBMap = rd_clients[i].link_ucb_received
                            if (rd_clients[i].hour >= totalHour):
                                if(len(linkUCBMap.keys()) == 0):
                                    emptyCount += 1
                                else:
                                    emptyCount = 0
                            mabManager.refreshLinkUCB(linkUCBMap)
                            # linkUCBMapBus = rd_clients[i].link_ucb_bus_received
                            # mabManager.refreshLinkUCBBus(linkUCBMapBus)
                            # speedVehicle = rd_clients[i].speed_vehicle_received
                            # mabManager.refreshLinkUCBShadow(speedVehicle)

                # Sending back the eco-routing results 
                for i in range(num_clients):
                    if rd_clients[i].state == "connected":
                        hour_od = rd_clients[i].hour
                        if hour_od < 0:
                            continue
                        for od in routeResult[hour_od]:
                            routeAction = mabManager.ucbRouting(od, hour_od)
                            routeResult[hour_od][od] = routeAction
                        # for od in routeResultBus[hour_od]:
                        #     routeAction = mabManager.ucbRoutingBus(od, hour_od)
                        #     routeResultBus[hour_od][od] = routeAction
                        # Generate resulting json objects 
                        index_od =0
                        od_list = []
                        result_list=[]
                        for od in routeResult[hour]:
                            od_list.append(od)
                            result_list.append(routeResult[hour][od])
                            index_od+=1\
                        
                        # index_od_bus=0
                        # bus_od_list=[]
                        # bus_result_list=[]
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
                            routeResult_json_string=json.dumps(routeResult_json_dict)
                            rd_clients[i].ws.send(routeResult_json_string) 
                        # if index_od_bus==len(routeResultBus[hour]):
                        #    routeResultBus_json_string=json.dumps(routeResultBus_json_dict)
                        #    rd_clients[i].ws.send(routeResultBus_json_string)

            # Clean the data cache in RDC
            for i in range(num_clients):
                if rd_clients[i].state == "connected":
                    with rd_clients[i].lock: 
                        rd_clients[i].link_ucb_received={}
                        # rd_clients[i].link_ucb_bus_received={}
                        # rd_clients[i].speed_vehicle_received={}
                        hour = rd_clients[i].hour
                        if hour < 0:
                            continue

            # Sending back the bus scheduling results 
            if(config.bus_scheduling == 'true'):
                if rd_clients[i].state == "connected":
                    for i in range(num_clients):
                        hour = rd_clients[i].hour
                        if (((hour % 2)==0) and (hour>rd_clients[i].prevHour) and (hour<totalHour)):
                            # Only send message when current hour differs from previous hour
                            for f in ['JFK','LGA','PENN']:
                                bus_planning_prepared = True
                                if f not in busPlanningResults[str(hour)]:
                                    bus_planning_prepared = False
                            if bus_planning_prepared:
                                print("Sending bus scheduling results for hour {}".format(hour))
                                with rd_clients[i].lock:
                                    busPlanningResults_combine={}
                                    # comment the following three lines if the schedules are generated in real time
                                    JFK_json=json.loads(busPlanningResults[str(hour)]['JFK'])
                                    LGA_json=json.loads(busPlanningResults[str(hour)]['LGA'])
                                    PENN_json=json.loads(busPlanningResults[str(hour)]['PENN'])

                                    busPlanningResults_combine['Bus_route']=list(JFK_json['Bus_route'])+list(LGA_json['Bus_route'])+list(PENN_json['Bus_route'])
                                    busPlanningResults_combine['Bus_num']=list(JFK_json['Bus_num'])+list(LGA_json['Bus_num'])+list(PENN_json['Bus_num'])
                                    busPlanningResults_combine['Bus_gap']=list(JFK_json['Bus_gap'])+list(LGA_json['Bus_gap'])+list(PENN_json['Bus_gap'])
                                    busPlanningResults_combine['MSG_TYPE']="BUS_SCHEDULE"
                                    busPlanningResults_combine['Bus_routename']=list(JFK_json['Bus_routename'])+list(LGA_json['Bus_routename'])+list(PENN_json['Bus_routename'])
                                    busPlanningResults_combine['Bus_currenthour']=JFK_json['Bus_currenthour']
                                    rd_clients[i].ws.send(json.dumps(busPlanningResults_combine))
                                    rd_clients[i].prevHour=hour
            
            time.sleep(0.5) # wait for 0.5 seconds
        except:
            pass
        finally:
            continue_flag = False
            for j in range(num_clients):
                if rd_clients[j].state == "connected":
                    continue_flag = True
    # Wait until all rd_clients finish their work
    for j in range(num_clients):
        rd_clients[j].join()

def kill_proc_tree(pid, including_parent=True):    
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()
    gone, still_alive = psutil.wait_procs(children, timeout=5)
    if including_parent:
        parent.kill()
        parent.wait(5)
        
def get_pids_by_script_name():
    pids = []
    for proc in psutil.process_iter():

        try:
            cmdline = proc.cmdline()
            pid = proc.pid
        except psutil.NoSuchProcess:
            continue
        if (len(cmdline)>2 and 'java' in cmdline[0]
            and cmdline[-1].endswith('mets_r.rs')):
            pids.append(pid)

    return pids
        
# main function (used only for debugging)
if __name__ == "__main__":
    # Load simulation arguments
    with open(sys.argv[1], "r") as f:
        config = json.load(f)
    num_clients = int(config.num_sim_instances)
    port_numbers = config.socket_port_numbers

    # Start RDCM
    run_rdcm(num_clients, port_numbers,index_bus_scheduling)


