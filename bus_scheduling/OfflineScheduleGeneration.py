import sys
import os
import json
import ast
import time
import pandas as pd
import argparse
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

addsevs_dir = '/home/umni2/a/umnilab/projects/DOE_METSR/METSR_FINAL/ADDSAEVS/METSR_SIM/ADDSEVS/'
# Specify scenario and index
options = get_arguments(sys.argv[1:])

def prepare_scenario_dict(options, path):
    options.cases = [[],[],[],[]]
    for case in os.listdir(path):
        options.cases[int(case[14])].append(case.split("speed_")[1].split(".json")[0])
        
prepare_scenario_dict(options, "input_route_generation/bus_ratio")
date_sim=options.cases[options.scenario_index][options.case_index]
scenario_index=options.scenario_index

# Load simulation config
args = {}
with open(os.path.join(addsevs_dir+'data', 'Data.properties'), "r") as f:
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

# Load the data for bus scheduling
path_pre = "../demand_prediction/Modelling/PredictionResults"
taxi_zone_file = "input_route_generation/tax_zones_bus_version.gpkg"
demand_file_location_from = {}
demand_file_location_to = {}

# Note here the bus split ratio is not exact, which is a limitation of using offline cache
ratio_file = "input_route_generation/bus_ratio/ratio_scenario"+str(scenario_index)+"_speed_"+date_sim+".json"
with open(ratio_file, 'r') as f:
    bus_ratio_file = json.load(f)
    bus_ratio_file = json.loads(bus_ratio_file)

for f in ['JFK','LGA','PENN']:
    demand_file_location_from[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019fromHub.csv")
    demand_file_location_to[f] = pd.read_csv(path_pre+"/"+f+"VehicleByHour2019toHub.csv")
    
    # Generate and store the bus schedules
busPlanningResults={}
for hour in range(0, int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600), 2):
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
            max_route = 50
            routeGeneration = RouteGeneration(hub_index,bus_ratio_file,demand_file_location_from[f],demand_file_location_to[f],taxi_zone_file,max_route, date_sim, hour)
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
            busPlanningResults[hour][f] = json.dumps(bus_planning_json)
# directly use the cached results as the optimization is much slower than the simulator
with open("offline_cache/"+"scenario_"+str(scenario_index)+"_speed_"+str(date_sim)+"_" + str(options.bus_num) + "_bus_scheduling.json" , 'w') as f:
    json.dump(busPlanningResults, f)


