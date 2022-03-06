#BusPlanningManager = RouteGeneration + RouteOptimization

#Author: Jiawei Xue. 
#Time: October 2021

import math
import numpy as np 
from bus_scheduling.RouteGeneration20211123 import RouteGeneration
from bus_scheduling.RouteOptimization20211123 import RouteOptimization
#import RouteGeneration
#import RouteOptimization

class BusPlanningManager(object):
    #1. initialize the travel time, and travel demand.
    #2. initialize the bus route, and bus frequency.
    #3. update the travel time, and travel demand.
    #4. implement the bus route generation
    #5. implement the bus route optimization
    #6. output the bus route, and bus frequency, and store them.
    
    #1,2 initialization. we have N zones, and 3 hubs.
    # travel_time: N*N 
    ### {"i" : {"j1" : 123, "j2" : 456,...}...}  #unit: seconds? the travel time from region i to region j2.
    # travel_demand: 6*N
    ### {"from_JFK" : {"i": 123, "j": 234, ...}, "to_JFK" : {"i": 123, "j": 234, ...}, ... }     #unit: number of people?
    ### keys = {"from_JFK", "to_JFK", "from_LGA", "to_LGA", "from_PENN", "to_PENN"}
    # bus_route = ["123","456",...,"1234"]   each entry represents the zone that the bus goes through
    # bus_frequency  = [12, 23, ..., 35]     each entry represents the assigned number of buses on each route.
    def __init__(self, demand_location):
        #super().__init__(zone_id_list) #the zone id list
        self.travel_time = {}         #the travel time between two different zones in the city
        self.travel_demand = {}       #the travel demand from or to hubs 
        self.bus_mat={}
        self.bus_route = {}
        self.bus_gap = {}
        self.bus_num = {}
        self.bus_routename={}
                    list_routename=[]
                    for l in range(0,len_json):
                        # assume the current hub is 134
                        # XXX for hub  XX for hour XX for route 
                        list_routename.append(134*10000+hour*100+l)
                    BusPlanning_json['Bus_routename'] = list_routename

        self.route_generation = {}     #the route generation model
        self.route_optimization = {}   #the route optimization model
        demand_type_list = ["from_JFK", "to_JFK", "from_LGA", "to_LGA", "from_PENN", "to_PENN"]
        
        #initialize the travel time
        for zone_id_from in zone_id_list:
            self.travel_time[zone_id_from] = {}
            for zone_id_to in zone_id_list:
                self.travel_time[zone_id_from][zone_id_to] = 3000.0   #3000 seconds' travel between two zones
        #initialize the demand
        for demand_type in demand_type_list:
            self.travel_demand[demand_type] = {}
            for zone_id in zone_id_list:
                self.travel_demand[demand_type][zone_id] = 30     #the demand between zone i to the airport is 30.
        #self.bus_route = [zone_id_list[i] for i in range(10)]     #initialize the bus route, a default bus route
        #self.bus_frequency = [3 for i in range(10)]              #initialize the bus frequency
        
        self.route_generation = RouteGeneration(self.travel_time, self.travel_demand)
        self.route_generation.run()
        self.bus_route = self.route_generation.get_route()
        
        self.route_optimization = RouteOptimization(self.travel_time, self.travel_demand, self.bus_route)
        self.route_optimization.run()
        self.bus_frequency = self.route_optimization.get_frequency()
        
    #3 update the travel time, and travel demand
    def update_travel_time_demand(self, travel_time, travel_demand):   
        #to Zhen: please check the format of travel_time and travel_demand
        self.travel_time = travel_time
        self.travel_demand = travel_demand
        self.route_generation.update_time_demand(travel_time, travel_demand)
        self.route_optimization.update_time_demand(travel_time, travel_demand)
        
    #4 implement the bus route design
    def generate_route(self):
        self.route_generation.run()
        self.bus_mat = self.route_generation.get_route()
    
    #5 implement the bus frequency design
    def optimize_route(self):
        self.route_optimization.update_route(self.bus_route)
        self.route_optimization.run()
        self.bus_frequency = self.route_optimization.get_frequency()

    #6 output the bus route, and bus frequency
    def output_route(self):
        return self.bus_route;
    def output_route_frequency(self):
        return self.bus_frequency;






