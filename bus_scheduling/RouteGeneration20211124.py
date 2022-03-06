#!/usr/bin/env python
# coding: utf-8

# # RouteGeneration1123

# In[1]:


#The route design module generates the route
#Author: Jiawei Xue. 
#Time: Oct, Nov, 2021.
#Step 1: Read the (1) shapefile; (2) demand data; (3) travel time/distance data.
#Step 2: Route generation functions.
#Step 3: Route combination functions.
#Step 4: Route generation and optimization implementation.
#Step 5: Save to the mat file.


# In[2]:


import math
import copy
import shapely
import random
import numpy as np
import pandas as pd
import seaborn as sns
import networkx as nx
import geopandas as gp
import scipy.io as sio
from lapsolver import solve_dense
from scipy.spatial import cKDTree 
from shapely.geometry import LineString, shape,Point
import matplotlib.pyplot as plt
from shapely.geometry import Point
from lapsolver import solve_dense
from scipy.io import savemat

 
#function 1
def reachable_tohub(node,distance,threshold,path_length,candidate,potential):
    #giving the deviation threshold, find the set of nodes that are reachable from the current node
    #path_length: current length of the path
    #threshold: the route deviation threshold
    #nodes j are reachable if path_length+distance(node,j)<=threshold*distance(j,j)
    #candidate node is the set of node to choose from
    reach_set=[]
    if len(candidate)==0:
        return reach_set
    for j in candidate:
        if path_length+distance[j, node]<=threshold*distance[j,j] and potential[j]>0:
            reach_set.append(j)
    return reach_set

#function 2
def reachable_fromhub(node,distance,threshold,path_length,candidate,potential):
    #giving the deviation threshold, find the set of nodes that are reachable from the current node
    #path_length: current length of the path
    #threshold: the route deviation threshold
    #nodes j are reachable if path_length+distance(node,j)<=threshold*distance(j,j)
    #candidate node is the set of node to choose from
    reach_set=[]
    if len(candidate)==0:
        return reach_set
    for j in candidate:
        if path_length+distance[node, j]<=threshold*distance[j,j] and potential[j]>0:
            reach_set.append(j)
    return reach_set

#function 3
def get_potential_tohub(node_id,self_potential,reach_set,p,distance,path_length,threshold,visited_nodes):
    #print(reach_set)
    global tohub_lowerbound #this is a global variable for early stopping purpose
    visited_nodes.append(node_id)
    visited_potential=sum([p[i] for i in visited_nodes])
    if visited_potential>tohub_lowerbound:
        tohub_lowerbound=visited_potential  # the maximum potential visited so far
    max_possible_potential=sum([p[i] for i in reach_set]) #maximum possible potential to visit 
    #do early stopping 
    if visited_potential+max_possible_potential<=tohub_lowerbound:
        return ([node_id],self_potential)
    #not early stopping... then continue recursion
    if len(reach_set)==1:
        return ([node_id,reach_set[0]],self_potential+p[reach_set[0]])
    elif len(reach_set)==0:
        return ([node_id],self_potential)
    else:
        return tuple([item1+item2 for item1,item2 in zip([[node_id],self_potential],max([get_potential_tohub(node_id=i,self_potential=p[i],
                                                 reach_set=reachable_tohub(i,distance,threshold,path_length+distance[i,node_id],[k for k in reach_set if k!=i],p), #calculate reachable set
                                                 p=p,distance=distance,path_length=path_length+distance[i,node_id],threshold=threshold,visited_nodes=visited_nodes)
                                   for i in reach_set],key=lambda x:x[1]))])
    
#function 4
def get_potential_fromhub(node_id,self_potential,reach_set,p,distance,path_length,threshold,visited_nodes):
    #print(reach_set)
    global fromhub_lowerbound #this is a global variable for early stopping purpose
    visited_nodes.append(node_id)
    visited_potential=sum([p[i] for i in visited_nodes])
    if visited_potential>fromhub_lowerbound:
        fromhub_lowerbound=visited_potential  # the maximum potential visited so far
    max_possible_potential=sum([p[i] for i in reach_set]) #maximum possible potential to visit 
    #do early stopping 
    if visited_potential+max_possible_potential<=fromhub_lowerbound:
        return ([node_id],self_potential)
    #not early stopping... then continue recursion
    if len(reach_set)==1:
        return ([node_id,reach_set[0]],self_potential+p[reach_set[0]])
    elif len(reach_set)==0:
        return ([node_id],self_potential)
    else:
        #introduce an early stopping mechanism:
        return tuple([item1+item2 for item1,item2 in zip([[node_id],self_potential],max([get_potential_fromhub(node_id=i,self_potential=p[i],
                                                 reach_set=reachable_fromhub(i,distance,threshold,path_length+distance[node_id,i],[k for k in reach_set if k!=i],p), #calculate reachable set
                                                 p=p,distance=distance,path_length=path_length+distance[node_id,i],threshold=threshold,visited_nodes=visited_nodes)
                                   for i in reach_set],key=lambda x:x[1]))])

#function 5
def all_routes_tohub_exact(hub_id,potential_vec,distance,threshold):
    routes=[]
    total_weight=0
    #loop until all potential demands are served
    global tohub_lowerbound
    while sum(potential_vec)>0:
        visited_nodes=[hub_id]
        tohub_lowerbound=0 #reset the global lowerbound
        route,weight=get_potential_tohub(node_id=hub_id,self_potential=0,reach_set=[i for i in range(234) if i!=hub_id and potential_vec[i]>0],p=potential_vec,distance=distance,path_length=0,threshold=threshold,visited_nodes=visited_nodes)
        for node in route:
            potential_vec[node]=0
        routes.append(route)
        total_weight+=weight
#         print(potential_vec,route,weight)
    return routes,total_weight

#function 6
def all_routes_fromhub_exact(hub_id,potential_vec,distance,threshold):
    routes=[]
    total_weight=0
    global fromhub_lowerbound
    while sum(potential_vec)>0:
        visited_nodes=[hub_id]
        fromhub_lowerbound=0
        route,weight=get_potential_fromhub(node_id=hub_id,self_potential=0,reach_set=[i for i in range(234) if i!=hub_id and potential_vec[i]>0],p=potential_vec,distance=distance,path_length=0,threshold=threshold,visited_nodes=visited_nodes)
        for node in route:
            potential_vec[node]=0
        routes.append(route)
        total_weight+=weight
#         print(potential_vec,route,weight)
    return routes,total_weight


# # Step 3: Route combination functions

# In[6]:


#function 1
def process_one_route_tohub(route,Q_to,time,distance):
    new_routes = [route[0]]
    length = 0
    duration = 0
    travel_time = [0]
    weight = 0
    for i in range(len(route)-1):
        new_routes = [route[i+1]]+new_routes
        length += distance[route[i+1], route[i]]
        duration += time[route[i+1], route[i]]
        travel_time= [duration]+travel_time
        weight += Q_to[route[i+1]]
    return new_routes,travel_time, length, duration, weight

#function 2
def process_one_route_fromhub(route,Q_from,time,distance):
    new_routes = [route[0]]
    length = 0
    duration = 0
    travel_time = [0]
    weight = 0
    for i in range(len(route)-1):
        new_routes = new_routes+[route[i+1]]
        length += distance[route[i], route[i+1]]
        duration += time[route[i], route[i+1]]
        travel_time= travel_time+[duration]
        weight += Q_from[route[i+1]]
    return new_routes,travel_time, length, duration, weight

#function 3
def get_threshold(routes,routes_fromhub, Q_tohub,time,distance):
    lengths = []
    for route in routes:
        new_route,travel_time,length,duration, weight = process_one_route_tohub(route,Q_tohub,time,distance)
        lengths.append(length)
    return np.tile(np.array(lengths),(len(routes_fromhub),1))

#function 4
def get_similarity(weight_fromhub, weight_tohub):
    return np.abs(np.tile(weight_fromhub,(len(weight_tohub),1)).T-np.tile(weight_tohub,(len(weight_fromhub),1)))

#function 5
def matching_routes(routes_fromhub, routes_tohub, distance, weight_fromhub, weight_tohub,Q_tohub,time):
    dist_mat = 1e6*np.ones((len(routes_fromhub), len(routes_tohub)))
    for i in range(len(routes_fromhub)):
        for j in range(len(routes_tohub)):
            dist_mat[i][j] = distance[routes_fromhub[i][-1], routes_tohub[j][-1]]
    thres_mat = get_threshold(routes_tohub, routes_fromhub, Q_tohub,time,distance)
    sim_mat = get_similarity(weight_fromhub, weight_tohub)
    #print(sim_mat.shape)
    cost_mat = sim_mat #+dist_mat
    cost_mat[dist_mat>thres_mat]=1e6
    row_ind, col_ind = solve_dense(dist_mat)
    unmatched_row = [i for i in range(len(routes_fromhub)) if i not in row_ind]
    unmatched_col = [i for i in range(len(routes_tohub)) if i not in col_ind]
    return row_ind, col_ind, unmatched_row, unmatched_col

#function 6
def gen_candidate(routes_fromhub,routes_tohub,row_ind,col_ind,unmatched_row,unmatched_col,Q_fromhub,Q_tohub,time, distance,hub_id):
    routes = []
    travel_times = []
    lengths = []
    durations = []
    weights = []
    for ind in range(len(row_ind)):
        new_routes,travel_time, length, duration, weight =         process_one_route_fromhub(routes_fromhub[row_ind[ind]],Q_fromhub,time,distance)
        start_pt = routes_tohub[col_ind[ind]][-1]
        end_pt = routes_fromhub[row_ind[ind]][-1]
        routes.append(new_routes[1:])
        travel_times.append(travel_time[1:])
        lengths.append(length+distance[start_pt][end_pt])
        durations.append(duration+time[start_pt][end_pt])
        weights.append(weight)
    for i in unmatched_row:
        new_routes,travel_time, length, duration, weight =         process_one_route_fromhub(routes_fromhub[i], Q_fromhub,time,distance)
        start_pt = new_routes[-1]
        end_pt = hub_id
        routes.append(new_routes[1:])
        travel_times.append(travel_time[1:])
        lengths.append(length+distance[start_pt][end_pt])
        durations.append(duration+time[start_pt][end_pt])
        weights.append(weight)
    for i in unmatched_col:
        new_routes =[]
        travel_time = []
        length = 0
        duration = 0
        weight = 0
        routes.append(new_routes)
        travel_times.append(travel_time)
        lengths.append(length)
        durations.append(duration)
        weights.append(weight)
    candidate_routes_fromhub=pd.DataFrame({'route':routes, 'travel_time': travel_times,                                            'length': lengths, 'duration': durations,                                           'weights':weights})
    routes = []
    travel_times = []
    lengths = []
    durations = []
    weights = []
    for ind in range(len(col_ind)):
        new_routes,travel_time, length, duration, weight =         process_one_route_tohub(routes_tohub[col_ind[ind]], Q_tohub,time,distance)
        routes.append(new_routes[:-1])
        travel_times.append(travel_time[:-1])
        lengths.append(length)
        durations.append(duration)
        weights.append(weight)
    for i in unmatched_row:
        new_routes =[]
        travel_time = []
        length = 0
        duration = 0
        weight = 0
        routes.append(new_routes)
        travel_times.append(travel_time)
        lengths.append(length)
        durations.append(duration)
        weights.append(weight)
    for i in unmatched_col:
        new_routes,travel_time, length, duration, weight =         process_one_route_tohub(routes_tohub[i],  Q_tohub,time,distance)
        #print("routes_tohub")
        #print(routes_tohub[i])
        #print("new_routes")
        #print(new_routes)
        start_pt = hub_id
        end_pt = new_routes[0]       
        #print(new_routes[:-1]) 
        routes.append(new_routes[:-1])
        travel_times.append(travel_time)
        lengths.append(length+distance[start_pt][end_pt])
        durations.append(duration+time[start_pt][end_pt])
        weights.append(weight)
    candidate_routes_tohub=pd.DataFrame({'route':routes, 'travel_time': travel_times, 'length': lengths, 'duration': durations,
                     'weights':weights})
    return candidate_routes_tohub,candidate_routes_fromhub

#function 7
def convert_from_ROW_INDEX_to_OBJECTID(result, mapping_row_objectid):
    #transforms the zone index in result from (ROW_Index-1, e.g., 114(JFK)) to (OBJECTID, e.g., 120(LGA)).
    objectid_result = copy.copy(result)
    for key in result:
        index_list = result[key]
        new_route_collection = list()
        for route in index_list:
            new_route = [mapping_row_objectid[str(route[i])] for i in range(len(route))]
            new_route_collection.append(new_route)
        objectid_result[key] = new_route_collection    
        return objectid_result


# # Step 4: Route generation and optimization implementation

# In[7]:


#1 two functions
def route_generate(Q_tohub,Q_fromhub,hub_id,time,distance):
    #demand=np.genfromtxt('demand_matrix.csv', delimiter=';')
    demand=copy.deepcopy(Q_tohub)
    demand_copy=copy.deepcopy(Q_tohub)

    threshold=1.2
    #To hub
    routes_tohub,_=all_routes_tohub_exact(hub_id,demand,time,threshold)
    demand=Q_tohub.copy()
    demand_copy=Q_tohub.copy()
    #routes_tohub_heu,_=all_routes_tohub_heuristic(hub_id,demand,time,threshold)
    demand_list=[]
    demand_list_heu=[]
    #load the shortest travel distance using taxis in the diagnol
    for i in range(len(distance)):
        time[i,i]=time[i,hub_id]
    for r in routes_tohub:
        td=sum([demand_copy[i] for i in r if i!=hub_id])
        demand_list.append(td)
    weight_tohub = demand_list
    sr_tohub=np.cumsum(weight_tohub)/np.sum(weight_tohub)

    #From hub
    demand=copy.deepcopy(Q_fromhub)
    demand_copy=copy.deepcopy(Q_fromhub)
    for i in range(len(distance)):
        time[i,i]=time[hub_id,i]
    routes_fromhub,_=all_routes_fromhub_exact(hub_id,demand,time,threshold)
    demand=Q_fromhub.copy()
    demand_copy=Q_fromhub.copy()
    demand_list=[]
    demand_list_heu=[]
    for r in routes_fromhub:
        td=sum([demand_copy[i] for i in r if i!=hub_id])
        demand_list.append(td)  
    weight_fromhub = demand_list
    sr_fromhub=np.cumsum(weight_fromhub)/np.sum(weight_fromhub)
    #########################################################################################
    return {"routes_fromhub":routes_fromhub, "routes_tohub":routes_tohub,            "distance":distance, "time":time,            "weight_fromhub":weight_fromhub, "weight_tohub":weight_tohub,             "Q_fromhub":Q_fromhub, "Q_tohub":Q_tohub}
    print ("finish route generation!")
    
#input1: r_g_result: route generation results
#input2: K: top K route
#input3: hub_id. Hub_id = 114.
def route_combine(r_g_result, max_route, hub_id,Q_tohub,Q_fromhub): 
    #1. Conduct the route combination.
    routes_fromhub, routes_tohub = r_g_result["routes_fromhub"], r_g_result["routes_tohub"]
    distance, time = r_g_result["distance"], r_g_result["time"]
    weight_fromhub, weight_tohub = r_g_result["weight_fromhub"], r_g_result["weight_tohub"]
    
    row_ind, col_ind, unmatched_row, unmatched_col =     matching_routes(routes_fromhub, routes_tohub, distance/1000, weight_fromhub, weight_tohub,Q_tohub,time)
    #print("row_ind")
    #print(row_ind)
    #print(col_ind)
    #print(unmatched_row)
    #print(unmatched_col)
    #Final result of the route combination.
    #candidate_exact_tohub
    #candidate_exact_fromhub
    candidate_exact_tohub, candidate_exact_fromhub =     gen_candidate(routes_fromhub, routes_tohub, row_ind, col_ind, unmatched_row, unmatched_col,Q_fromhub,Q_tohub,time, distance, hub_id)
    #print(" candidate_exact_tohub")
    #print(candidate_exact_tohub)
    #print(candidate_exact_fromhub)
    weight_exact = candidate_exact_fromhub['weights'].values + candidate_exact_tohub['weights'].values
    weight_exact = weight_exact[np.argsort(-weight_exact)]
                 
    #2. Output matlab results for optimization
    #candidate_routes_fromhub
    #candidate_routes_tohub
    #Nroute = 20; #output top K routes to chose from.
    #print ("max_route",max_route)
    opt_index = (candidate_exact_fromhub['weights'].values +                  candidate_exact_tohub['weights'].values).argsort()[0-int(max_route):][::-1]  
    #print ("opt_index",opt_index)  
    candidate_routes_fromhub = candidate_exact_fromhub.iloc[opt_index]
    candidate_routes_tohub = candidate_exact_tohub.iloc[opt_index]
    
    ####################################################OUTPUTS#############################################################
    #(20, 234*2)
    #the 1-entry represents the stop.
    #OUTPUT1 for MATLAB: route stop    #Average number: 5.30.
    route_stop_index = np.zeros([len(candidate_routes_fromhub),234*2]) #two directions: from hub, to hub.
    for i in range(len(candidate_routes_fromhub)):
        for j in candidate_routes_fromhub.iloc[i]['route']:
            route_stop_index[i,j]=1
        for j in candidate_routes_tohub.iloc[i]['route']:
            route_stop_index[i,j+234]=1
    
    #OUTPUT2 for MATLAB: route_dist
    #(20, 1). the entry represents the loop distance (unit: km). Average: 38.733miles. 
    route_dist = (candidate_routes_fromhub['length'].values +                   candidate_routes_tohub['length'].values)/1609.0
    
    #OUTPUT3 for MATLAB: route_trip_time
    #(20, 1). the entry represents the loop travel time (unit: hour). Unit: hour. Average: 2.235 hours.
    route_trip_time = candidate_routes_fromhub['duration'].values/3600.0+candidate_routes_tohub['duration'].values/3600.0
    
    #OUTPUT4 for MATLAB: trip_time
    trip_time = np.ones([234*2,1])*1e6
    #trip time from hub
    #(468, 1). the entry represents the taxi travel time from and to hub. Unit: hour. Average: 0.524 hours.
    for i in range(len(candidate_routes_fromhub['route'].values)):
        for j in range(len(candidate_routes_fromhub.iloc[i]['route'])):
            trip_time[candidate_routes_fromhub.iloc[i]['route'][j]] = candidate_routes_fromhub.iloc[i]['travel_time'][j]/3600.0
    #trip time to hub
    for i in range(len(candidate_routes_tohub['route'].values)):
        for j in range(len(candidate_routes_tohub.iloc[i]['route'])):
            trip_time[candidate_routes_tohub.iloc[i]['route'][j]+234] = candidate_routes_tohub.iloc[i]['travel_time'][j]/3600.0
    
    #OUTPUT5 for MATLAB: taxi_time
    #(468, 1). the entry represents the taxi travel time from and to hub. Unit: hour. Average: 0.524 hours.
    taxi_time = list(time[hub_id,:]/3600)+list(time[:,hub_id]/3600.0) 
    
    #OUTPUT6 for MATLAB: taxi_dist
    #(468, 1). the entry represents the taxi travel distance from and to hub. Unit: mile. Average: 15.845 miles
    taxi_dist = list(distance[hub_id,:]/1609.0) + list(distance[:,hub_id]/1609.0)
    
    #OUTPUT7 for MATLAB: taxi_price
    #(468, 1). the entry represents the taxi price from and to hub. Unit: dollars. Average: 33.420 dollars
    taxi_price = list(0.275 * time[hub_id, :]/60 + 1.563 * distance[hub_id, :]/1609.0) +                 list(0.275 * time[:,hub_id]/60 + 1.563 * distance[:,hub_id]/1609.0)
    
    #OUTPUT8: Proceed
    proceed = np.zeros((234*2,234*2))
    for route in candidate_routes_fromhub['route'].values:
        for i in range(len(route)-1):
            for j in range(i,len(route)-1):
                proceed[route[j+1],route[i]] = 1
    for route in candidate_routes_tohub['route'].values:
        for i in range(len(route)-1):
            for j in range(i,len(route)-1):
                proceed[route[j+1]+234,route[i]+234] = 1
    route_optimization = {"candidate_exact_tohub" :candidate_exact_tohub,                           "candidate_exact_fromhub" :candidate_exact_fromhub,                         "candidate_routes_fromhub": candidate_routes_fromhub,                         "candidate_routes_tohub": candidate_routes_tohub,                         "route_stop_index": route_stop_index,                         "route_dist": route_dist,                         "route_trip_time": route_trip_time,                         "trip_time": trip_time,                         "taxi_time": taxi_time,                         "taxi_dist": taxi_dist,                         "taxi_price": taxi_price,                         "proceed": proceed,                         "Q_fromhub": r_g_result["Q_fromhub"],                         "Q_tohub": r_g_result["Q_tohub"]}
    return route_optimization


# In[8]:


# In[ ]:

class RouteGeneration(object):
    #1. initialize the demand location, start hour, hub 
    #2. initialize the bus route
    #3. update the travel time, and travel demand.
    #4. implement the bus route design, and update the bus route

    #1,2 initialization
    def __init__(self, hub_id,bus_ratio_file,demand_file_location_from,demand_file_location_to,taxi_zone_file,max_route,date_sim,hour_idx): 
        self.hub_id = hub_id
        self.bus_ratio_file=bus_ratio_file
        self.demand_file_location_from = demand_file_location_from 
        self.demand_file_location_to = demand_file_location_to        
        self.date_sim=date_sim  
        self.max_route =max_route  
        self.hour_idx=hour_idx
        taxi_zones = gp.read_file(taxi_zone_file)
        location='/home/umni2/a/umnilab/projects/DOE_METSR/METSR_HPC/METSR_HPC/bus_scheduling/input_route_generation/'
        self.bus_mat = {}                     
        keep_index = np.array(taxi_zones['OBJECTID'])-1  #[0,233]
        self.mapping_row_objectid = dict()    #{"0":3, "1":4, "2":7,...}
        for i in range(len(keep_index)):
            self.mapping_row_objectid[str(i)] = keep_index[i]+1
 
        Q_from = self.demand_file_location_from
        Q_to=self.demand_file_location_to
         
        locationIDList = list(np.unique(Q_from['DOLocationID']))
        #print("keep_index")
        #print(keep_index)
        #print("date")
        #print(date_sim)
        #print("hour")
        #print(hour_idx)
        #print("filename")
        #print(demand_file_location_from)
        date_2019_sum=[31,28,31,30,31,30,31,31,30,31,30,31]
        index_month=int(date_sim.split("-")[1])
        index_date=int(date_sim.split("-")[2])
        index_date_month=(sum(date_2019_sum[0:(index_month-1)])+index_date-1)*24
#################Attention1############################
#Attention Start: you need to redefine Q_fromhub, Q_tohub as the data from the row_idx by chaging row_idx
        Q_fromhub = [0.0 for i in range(len(keep_index))]
        for i in range(len(Q_fromhub)):
            objectID = keep_index[i] + 1
            #print("objectID")
            #print(objectID)
            #if objectID in columnList:
            #Q_fromhub[i] = abs(Q_from.iloc[row_idx][columnList.index(objectID)]*)+abs(Q_from.iloc[row_idx+1][columnList.index(objectID)]) 
            Q_fromhub[i] =max(abs(Q_from.iloc[(locationIDList.index(objectID))*sum(date_2019_sum)*24+index_date_month+hour_idx][2]*bus_ratio_file[str(hour_idx)][str(i)])+abs(Q_from.iloc[(locationIDList.index(objectID))*sum(date_2019_sum)*24+index_date_month+hour_idx+1][2]*bus_ratio_file[str(hour_idx+1)][str(i)]),0.01)
        Q_fromhub = np.array(Q_fromhub)
        #Q_tohub = copy.copy(Q_fromhub)
        locationIDtoList = list(np.unique(Q_to['PULocationID']))
        Q_tohub = [0.0 for i in range(len(keep_index))]
        for i in range(len(Q_tohub)):
            objectID = keep_index[i] + 1
            #print("objectID")
            #print(objectID)
            #if objectID in columnList:
            #Q_fromhub[i] = abs(Q_from.iloc[row_idx][columnList.index(objectID)]*)+abs(Q_from.iloc[row_idx+1][columnList.index(objectID)]) 
            Q_tohub[i] =max(abs(Q_to.iloc[(locationIDtoList.index(objectID))*sum(date_2019_sum)*24+index_date_month+hour_idx][2]*bus_ratio_file[str(hour_idx)][str(i)])+abs(Q_to.iloc[(locationIDtoList.index(objectID))*sum(date_2019_sum)*24+index_date_month+hour_idx+1][2]*bus_ratio_file[str(hour_idx+1)][str(i)]),0.01)
        Q_tohub = np.array(Q_tohub)

        #print("Q_fromhub")
        #print(Q_fromhub)
        #print(Q_tohub)
#Attention End: you need to redefine Q_fromhub, Q_tohub.
##########################################################

        Q_fromhub[hub_id] = 0
        Q_tohub[hub_id] = 0

#3. read the time and distance files
#time: each entry represents the travel time (unit: second) from zone i to zone j.
#distance: each entry represenst the travel distance (unit: second) from zone i to zone j.
        self.time = np.loadtxt(location+'time.csv')
        self.distance = np.loadtxt(location+'dist.csv')
        idx_to=Q_tohub.argsort()[-100:][::-1]    #Top K=100 demand
        idx_from=Q_fromhub.argsort()[-100:][::-1]    #Top K=100 demand
        Q_tohub=[Q_tohub[i] if i in idx_to else 0 for i in range(234)]                 #dimension: 234 
        Q_fromhub=[Q_fromhub[i] if i in idx_from else 0 for i in range(234)]           #dimension: 234
        #Q_fromhub=copy.copy(Q_tohub)
        self.Q_tohub=Q_tohub
        self.Q_fromhub=Q_fromhub
        #print ("Q-tohub ")
        #print(self.Q_tohub)
        #print ("Q-fromhub ")
        #print(self.Q_fromhub)

    #4. implement the bus route design, and update the bus route. 
    def run(self):
        print ("start route generation!")
        #implement the route generation process
        #self.bus_route 
	#2. implement 
        itera = 0
        while itera<20:
              itera+=1
              try:
                  r_g_result = route_generate(self.Q_tohub,self.Q_fromhub,self.hub_id,self.time, self.distance)  #route generation
                  result = {"routes_fromhub":r_g_result["routes_fromhub"], "routes_tohub":r_g_result["routes_tohub"]}
                  print ("1. route generation results")
                  print (result)
		
		#3. transforms the zone index in result from (ROW_Index-1, e.g., 114(JFK)) to (OBJECTID, e.g., 120(LGA)).
                  objectid_result = copy.copy(result)
                  for key in result:
                      index_list = result[key]
                      new_route_collection = list()
                      for route in index_list:
                          new_route = [self.mapping_row_objectid[str(route[i])] for i in range(len(route))]
                          new_route_collection.append(new_route)
                      objectid_result[key] = new_route_collection
                  result_combine_result = route_combine(r_g_result, int(self.max_route), self.hub_id,self.Q_tohub,self.Q_fromhub) #route combination
                  #print("route combine result")
                  #print(result_combine_result)
                  final_route_row = list()
                  num = np.min([len(result_combine_result["candidate_routes_fromhub"]),                     len(result_combine_result["candidate_routes_tohub"])])
                  for i in range(3):
                      route_from, route_to = result_combine_result["candidate_routes_fromhub"].iloc[i]["route"],                         result_combine_result["candidate_routes_tohub"].iloc[i]["route"]
                      #print("route from and route to")
                      #print(route_from)
                      #print(route_to)
                      if route_from[-1] == route_to[0]:
                         route = [self.hub_id] + list(route_from) + list(route_to[1:]) + [self.hub_id]
                      else:
                         route = [self.hub_id] + list(route_from) + list(route_to) + [self.hub_id]
                      final_route_row.append(route)
            
                 
              except BaseException:
                  print ("Error: We can not match these results!")
                  print ("We regenerate the route!")
                  print ("-----------------------------------------------------------")
                  continue
              else:
                  break
	# In[9]:
        print ("-----------------------------------------------------------")
        print ("2. route combination results in ROWID")
        print (final_route_row)
        print (type(final_route_row))
        final_route_objectid = list([list([self.mapping_row_objectid[str(final_route_row[i][j])] for j in range(len(final_route_row[i]))])                       for i in range(len(final_route_row))])
        print ("3. route combination results in OBJECTID")

        print (final_route_objectid)
        item4 = result_combine_result["route_stop_index"]
        item5 = result_combine_result["route_dist"].reshape((1,len(result_combine_result["route_dist"])))
        item6 = result_combine_result["route_trip_time"].reshape((1,len(result_combine_result["route_trip_time"])))
        item7 = np.array(result_combine_result["trip_time"]).reshape((1,len(result_combine_result["trip_time"])))
        item8 = np.array(result_combine_result["taxi_time"]).reshape((1,len(result_combine_result["taxi_time"])))
        item9 = np.array(result_combine_result["taxi_price"]).reshape((1,len(result_combine_result["taxi_price"])))
        item10 = np.array(result_combine_result["taxi_dist"]).reshape((1,len(result_combine_result["taxi_dist"])))
        q_f = np.array(result_combine_result["Q_fromhub"]).reshape((1,len(result_combine_result["Q_fromhub"]))) 
        q_t = np.array(result_combine_result["Q_tohub"]).reshape((1,len(result_combine_result["Q_tohub"])))
        item11, item12 = q_f, q_t
        Qstd_fromhub = copy.copy(q_f)
        for i in range(len(q_f[0])):
            Qstd_fromhub[0][i] = random.random()
        Qstd_tohub = copy.copy(q_t)
        for i in range(len(q_t[0])):
            Qstd_tohub[0][i] = random.random()
        item13, item14 = Qstd_fromhub, Qstd_tohub
        item15 = result_combine_result["proceed"]
        item16 = 0
        self.bus_mat = {"route_stop_index": item4, "route_dist": item5, "route_trip_time": item6,       "trip_time": item7, "taxi_time": item8, "taxi_price": item9,       "taxi_dist": item10, "Q_fromhub": item11, "Q_tohub": item12,        "Qstd_fromhub": item13, "Qstd_tohub":  item14, "Proceed": item15, "E": 0,        "final_route_row":final_route_row, "final_route_objectid":final_route_objectid}

##savemat(output_file_location + output_file, mdic)
##print ("finish the route generation and route combincation. save reuslts as a mat. file")
        print ("finish route generation!")

 

    def get_mat(self):
        return self.bus_mat;





