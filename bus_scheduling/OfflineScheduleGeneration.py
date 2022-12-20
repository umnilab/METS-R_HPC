import sys
import os
import json
import ast
import time
import pandas as pd
import argparse
import numpy as np
from types import SimpleNamespace as SN

from RouteGeneration import RouteGeneration
from RouteOptimization import RouteOptimization


def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METSR cache bus scheduling')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario, values from 0 to 3')
    parser.add_argument('-c','--case_index', type=int, 
                        help='the index within the scenario, take values from 0 to 9')
    parser.add_argument('-bf', '--bus_num', type=int, default=0,
                        help='number of AEV buses initialized')
    args = parser.parse_args(argv)

    return args

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

sim_dir = '/home/bridge/c/lei67/git/METS-R_SIM/METS_R/'
# Specify scenario and index
options = get_arguments(sys.argv[1:])
def prepare_scenario_dict(options, path):
    options.cases = [[],[],[],[]]
    for scenario in os.listdir(path):
        for case in os.listdir(path+"/"+scenario):
            if(case.startswith("speed_std_")):
                options.cases[int(scenario[-1])].append(case.split("speed_std_")[1].split(".csv")[0])
        
prepare_scenario_dict(options, sim_dir+"data/NYC/operation/speed")
options.cases[options.scenario_index] = sorted(options.cases[options.scenario_index])
date_sim=options.cases[options.scenario_index][options.case_index]
scenario_index=options.scenario_index

# Load the data for bus scheduling
path_pre = "../demand_prediction/Modelling/PredictionResults"
taxi_zone_file = "input_route_generation/tax_zones_bus_version.gpkg"
demand_file_location_from = {}
demand_file_location_to = {}

# Note here the bus split ratio is not exact, which is a limitation of using offline cache
ratio_file = "input_route_generation/bus_ratio.json"
with open(ratio_file, 'r') as f:
    bus_ratio_file = json.load(f)
    bus_ratio_file = json.loads(bus_ratio_file)

for f in ['JFK','LGA','PENN']:
    demand_file_location_from[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019fromHub.csv")
    demand_file_location_to[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019toHub.csv")
    
    # Generate and store the bus schedules
busPlanningResults={}
for hour in range(0, 30, 2):
    if ((hour%2)==0):
        busPlanningResults[hour] = {}
        for f in ['JFK','LGA','PENN']:
            bus_planning_json = {}
            hub_type = f
            if hub_type=='JFK':
                hub_index = 114  
            if hub_type=='LGA':
                hub_index = 120 
            if hub_type=='PENN':
                hub_index = 164 
            max_route = 30
            routeGeneration = RouteGeneration(hub_index,bus_ratio_file,demand_file_location_from[f],demand_file_location_to[f],taxi_zone_file,max_route, date_sim, hour-3)
            routeGeneration.run()
            mat=routeGeneration.bus_mat
            print(mat)
            Tlist = [10]     #uncertainty level  
            Blist = [options.bus_num]    #fleet size
            routeOptimization=RouteOptimization(mat,Tlist,Blist)
            routeOptimization.run()      
            bus_planning_json['Bus_route'] =routeOptimization.Bus_route 
            len_json=len(bus_planning_json['Bus_route'])
            bus_planning_json['Bus_num'] =routeOptimization.Bus_num[:len_json]
            bus_planning_json['Bus_gap'] =routeOptimization.Bus_gap[:len_json]
            if sum(bus_planning_json['Bus_num'])==0:
                bus_planning_json['Bus_num'][0]=1
                bus_planning_json['Bus_gap'][0]=routeOptimization.bus_mat["route_trip_time"][0][0]*60
            # print(bus_planning_json['Bus_route'])
            bus_planning_json['MSG_TYPE'] = "BUS_SCHEDULE" 
            list_routename=[]
            for l in range(0,len_json): 
                # XXX for hub  XX for hour XX for ro
                list_routename.append(hub_index*10000+hour*100+l)
            bus_planning_json['Bus_routename'] = list_routename
            bus_planning_json['Bus_currenthour'] = str(hour)
            print(bus_planning_json)
            busPlanningResults[hour][f] = json.dumps(bus_planning_json, cls=NpEncoder)
# directly use the cached results as the optimization is much slower than the simulator
with open("offline_cache/"+"scenario_"+str(scenario_index)+"_speed_"+str(date_sim)+"_" + str(options.bus_num) + "_bus_scheduling.json" , 'w') as f:
    json.dump(busPlanningResults, f)


