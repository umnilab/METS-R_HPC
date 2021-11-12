#!/usr/bin/env python
# coding: utf-8

# # RouteGeneration

# In[1]:


#The route design module generates the route
#Author: Jiawei Xue. 
#Time: Oct 2021


# In[2]:


import math
import numpy as np 
import geopandas as gp
import networkx as nx
import pandas as pd
from scipy.spatial import cKDTree 
from shapely.geometry import LineString, shape,Point
import matplotlib.pyplot as plt
import numpy as np
#from lapsolver import solve_dense
import scipy.io as sio
import copy
import seaborn as sns


# In[3]:


#0.1 shapefile
location = "/home/umni2/a/umnilab/users/wang5076/METSR_HPC/METSR_HPC/bus_scheduling/input_route_generation/"
file = location+"tax_zones_bus_version.gpkg"
taxi_zones = gp.read_file(file)
keep_index = np.array(taxi_zones['OBJECTID'])-1

#0.2 the hub
#114 is the hub id at JFK
hub_id=114;
period=3;
Q = np.load(location+'weekday_result.npy')[period,keep_index,:] #0 for AM peak, 1 for PM peak, 2 for Off Peak, 3 for daily average, 4 for night
Q = Q[:,keep_index]
Q_fromhub = Q[hub_id,:]
Q_tohub = Q[:,hub_id]
Q_fromhub[hub_id] = 0
Q_tohub[hub_id] = 0

#0.3 
time = np.loadtxt(location+'time.csv')
distance = np.loadtxt(location+'dist.csv')
#get the top K indices only
idx_to=Q_tohub.argsort()[-100:][::-1] #Top K demand
idx_from=Q_fromhub.argsort()[-100:][::-1] #Top K demand
Q_tohub=[Q_tohub[i] if i in idx_to else 0 for i in range(234)]
Q_fromhub=[Q_fromhub[i] if i in idx_from else 0 for i in range(234)]
print(sum(Q_tohub),sum(Q_fromhub))


# # Type 1 functions

# In[4]:


#1.1
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

#1.2
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

#1.3
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
    
#1.4
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

#1.5
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

#1.6
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


# # Type 2: route combination

# In[5]:


#Route combination
#2.1
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

#2.2 
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

#2.3 
def get_threshold(routes,routes_fromhub):
    lengths = []
    for route in routes:
        new_route,travel_time,length,duration, weight = process_one_route_tohub(route,Q_tohub,time,distance)
        lengths.append(length)
    return np.tile(np.array(lengths),(len(routes_fromhub),1))

#2.4 
def get_similarity(weight_fromhub, weight_tohub):
    return np.abs(np.tile(weight_fromhub,(len(weight_tohub),1)).T-np.tile(weight_tohub,(len(weight_fromhub),1)))

#2.5
def matching_routes(routes_fromhub, routes_tohub, distance, weight_fromhub, weight_tohub):
    dist_mat = 1e6*np.ones((len(routes_fromhub), len(routes_tohub)))
    for i in range(len(routes_fromhub)):
        for j in range(len(routes_tohub)):
            dist_mat[i][j] = distance[routes_fromhub[i][-1], routes_tohub[j][-1]]
    thres_mat = get_threshold(routes_tohub, routes_fromhub)
    sim_mat = get_similarity(weight_fromhub, weight_tohub)
    #print(sim_mat.shape)
    cost_mat = sim_mat #+dist_mat
    cost_mat[dist_mat>thres_mat]=1e6
    row_ind, col_ind = solve_dense(dist_mat)
    unmatched_row = [i for i in range(len(routes_fromhub)) if i not in row_ind]
    unmatched_col = [i for i in range(len(routes_tohub)) if i not in col_ind]
    return row_ind, col_ind, unmatched_row, unmatched_col

#2.6 
def gen_candidate(routes_fromhub,routes_tohub,row_ind,col_ind,unmatched_row,unmatched_col):
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
    candidate_routes_fromhub=pd.DataFrame({'route':routes, 'travel_time': travel_times, 'length': lengths, 'duration': durations,
                     'weights':weights})
    routes = []
    travel_times = []
    lengths = []
    durations = []
    weights = []
    for ind in range(len(col_ind)):
        new_routes,travel_time, length, duration, weight =         process_one_route_tohub(routes_tohub[col_ind[ind]], Q_tohub,time,distance)
        routes.append(new_routes[:-2])
        travel_times.append(travel_time[:-2])
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
        start_pt = hub_id
        end_pt = new_routes[0]
        routes.append(new_routes[:-2])
        travel_times.append(travel_time)
        lengths.append(length+distance[start_pt][end_pt])
        durations.append(duration+time[start_pt][end_pt])
        weights.append(weight)
    candidate_routes_tohub=pd.DataFrame({'route':routes, 'travel_time': travel_times, 'length': lengths, 'duration': durations,
                     'weights':weights})
    return candidate_routes_tohub,candidate_routes_fromhub


# In[6]:


#3.1
def route_generate():
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
    return {"routes_fromhub":routes_fromhub, "routes_tohub":routes_tohub}
    print ("finish route generation!")


# In[7]:


result = route_generate()


# In[8]:


print (result)


# # Class RouteGeneration

# In[9]:


class RouteGeneration(object):
    #1. initialize the travel time, and travel demand
    #2. initialize the bus route
    #3. update the travel time, and travel demand.
    #4. implement the bus route design, and update the bus route

    #1,2 initialization
    def __init__(self, travel_time, travel_demand):
        #super().__init__(travel_time, travel_demand)
        self.travel_time = travel_time        #the travel time between two different zones in the city
        self.travel_demand = travel_demand       #the travel demand from or to hubs 
        self.bus_route = {}     
    
    #3. update the travel time
    def update_time_demand(self, travel_time, travel_demand):
        self.travel_time = travel_time
        self.travel_demand = travel_demand
    
    #4. implement the bus route design, and update the bus route. 
    def run(self):
        print ("start route generation!")
        #implement the route generation process
        #self.bus_route 
        result = route_generate()    ##need to use the self information later
        print ("finish route generation!")
        print ("the route is")
        print (result)
        self.bus_route = result
    def get_route(self):
        return self.bus_route;


# In[ ]:





# In[ ]:




