import sys
import os
import json
import ast
import time
import pandas as pd
import argparse
import numpy as np
from types import SimpleNamespace as SN

from StaticRouteGeneration import RouteGeneration
from RouteOptimization import RouteOptimization


def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METSR cache bus scheduling')
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
    
    demand_file_location_from[f]['hourOfDay'] = (demand_file_location_from[f]['Hour']+1) % 2
    demand_file_location_to[f]['hourOfDay'] = (demand_file_location_to[f]['Hour']+1) % 2

    demand_file_location_from[f] = demand_file_location_from[f].groupby(['DOLocationID','hourOfDay']).agg({'vehicle_count':np.mean}).reset_index()
    demand_file_location_to[f] = demand_file_location_to[f].groupby(['PULocationID','hourOfDay']).agg({'vehicle_count':np.mean}).reset_index()
    
# Generate and store the bus schedules
bus_planning_json = {}
bus_route = []
bus_num = []
bus_gap = []
bus_name = []
for f in ['JFK','LGA','PENN']:
    hub_type = f
    if hub_type=='JFK':
        hub_index = 114  
    if hub_type=='LGA':
        hub_index = 120 
    if hub_type=='PENN':
        hub_index = 164 
    max_route = 30
    routeGeneration = RouteGeneration(hub_index,bus_ratio_file,demand_file_location_from[f],demand_file_location_to[f],taxi_zone_file,max_route, '2019-1-1', 0)
    routeGeneration.run()
    mat=routeGeneration.bus_mat
    print(mat)
    Tlist = [10]     #uncertainty level  
    Blist = [options.bus_num]    #fleet size
    routeOptimization=RouteOptimization(mat,Tlist,Blist)
    routeOptimization.run()      
    bus_route += routeOptimization.Bus_route 
    len_json=len(routeOptimization.Bus_route)
    bus_num +=routeOptimization.Bus_num[:len_json]
    bus_gap +=routeOptimization.Bus_gap[:len_json]
    # print(bus_planning_json['Bus_route'])
    
    for l in range(0,len_json): 
        # XXX for hub  XX for hour XX for ro
        bus_name.append(hub_index*10000+l)
bus_planning_json['names'] = bus_name
bus_planning_json['routes'] = bus_route
bus_planning_json['nums'] = bus_num
bus_planning_json['gaps'] = bus_gap
# directly use the cached results as the optimization is much slower than the simulator
with open("static_cache/bus_routes" + str(options.bus_num//100) + ".json" , 'w') as f:
    json.dump(bus_planning_json, f, cls=NpEncoder)


