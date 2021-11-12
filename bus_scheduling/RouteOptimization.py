
#The RouteOptimization class executes the the bus frequency planning module.
#Author: Jiawei Xue. 
#Time: Oct 2021

import math
import numpy as np 
import math
import scipy.io
import numpy as np
import gurobipy as gp
from gurobipy import GRB

files = ["/home/umni2/a/umnilab/users/wang5076/METSR_HPC/METSR_HPC/bus_scheduling/input_route_optimization/metro_case_exact_0.mat"]   ###r1
Blist = [200]                                   #fleet size           
Tlist = [10]                                    #uncertainty level 
mat = scipy.io.loadmat(files[0])


# # 1. Set parameters

# In[4]:


def set_params(mat_file):
    #1. first get those idxs to and from hubs
    params = dict()
    params["idx_to"] = [i for i, x in enumerate(mat_file["Q_tohub"][0]) if x > 0]        
    params["idx_from"] = [i for i, x in enumerate(mat_file["Q_fromhub"][0]) if x > 0]       
    params["nzone"] = 2*len(params["idx_to"])                           #number of zones
    params["nroute"] = len(mat_file["route_dist"][0])                   #number of routes
    #print ("distance matrix")
    #print (mat_file["route_dist"])
    params["B"] = 200
    params["h_min"] = 3.0         #min
    #params["h_max"] = 30.0        #headway range
    params["h_max"] = 30.0
    params["Capacity"] = 20
    params["cl"] = 3      #loss serve.       unit: 3$/mile
    
    params["cw"] = 0.5*60      #waiting cost.     unit: 30$/hour
    params["co"] = 70           #operation weight. unit: 70$/hour
    params["op_cost"] = 70      #weight for operation
    params["H"] = math.ceil(np.log(params["h_max"]+1)/np.log(2.0))
    
    #2. we solve a master and subproblems 
    #variabels for master problem:
    #y_s: number of vehicles per route
    params["mvar"] = dict()
    params["mvar"]["y_range"] = [i for i in range(params["nroute"])]                     #0
    #params g_s: 0,1 with H per s, binary, unvary relaxtion for h_s
    params["mvar"]["gs_range"] = [params["mvar"]["y_range"][-1]+1+j for j in range(params["nroute"]*params["H"])]
    #params z_s: relaxation of g_sy_s
    params["mvar"]["zs_range"] = [params["mvar"]["gs_range"][-1]+1+j for j in range(params["nroute"]*params["H"])]
    #params kappa_s: 0,1 variable for y_s constraint
    params["mvar"]["k_range"] = [params["mvar"]["zs_range"][-1]+1+j for j in range(params["nroute"])]
    #param eta
    params["mvar"]["eta_range"] = [params["mvar"]["k_range"][-1]+1]
    params["nvar_master"] = params["mvar"]["eta_range"][-1] + 1      #the number of variables
    
    #3. X_i: actual passenger demand serverd at each zone --- nzone
    #lambda_i: aux var for recourse problem -- nzone
    #mu_i: aux var for recourse problem -- nzone
    #p_i: budget for uncertainty variable -- nzone
    params["T"] = 100                               #budget for variances.
    #variable idx for recourse problems
    params["rvar"] = dict()
    params["rvar"]["p_range"] = [i for i in range(params["nzone"])]
    #demand equality constraint
    params["rvar"]["lambda_range"] = [params["rvar"]["p_range"][-1]+1+j for j in range(params["nzone"])]   ###2???
    #to/from hub capacity constraint
    params["rvar"]["u_range"] = [params["rvar"]["lambda_range"][-1]+1+j for j in range(2*params["nroute"])]
    #served demand constraint.
    params["rvar"]["v_range"] = [params["rvar"]["u_range"][-1]+1+j for j in range(params["nzone"])]        ###2???
    #relaxation of lambda_ip_i
    params["rvar"]["gamma_range"] = [params["rvar"]["v_range"][-1]+1+j for j in range(params["nzone"])]
    #relaxation of v_ip_i
    params["rvar"]["z_range"] = [params["rvar"]["gamma_range"][-1]+1+j for j in range(params["nzone"])]
    params["nvar_recourse"] = params["rvar"]["z_range"][-1] + 1    

    #4. model parameters
    columnList = list(params["idx_from"]) + [i+233 for i in params["idx_to"]]
    params["route_stop_idx"] = mat_file["route_stop_index"][:,columnList]
    params["stop_proceed"] = mat_file["Proceed"][columnList,columnList]
    params["X"] = [[np.round(mat_file["Q_fromhub"][0][idx],2)] for idx in params["idx_from"]] +                    [[np.round(mat_file["Q_tohub"][0][idx],2)] for idx in params["idx_to"]]                         #persons/hour
    params["Xstd"] = [[3*np.round(mat_file["Qstd_fromhub"][0][idx], 4)] for idx in params["idx_from"]] +                    [[3*np.round(mat_file["Qstd_tohub"][0][idx], 4)] for idx in params["idx_to"]]   ###3            #persons/hour
    params["trip_time"] = [[mat_file["trip_time"][0][idx]] for idx in params["idx_from"]] +                            [[mat_file["trip_time"][0][idx]] for idx in params["idx_to"]]
    
    #5. print (mat_file["route_trip_time"])
    params["route_trip_time"] = [[np.round(i*60.0)] for i in mat_file["route_trip_time"][0]]                               #min
    params["route_dist"] = [[np.round(mat_file["route_dist"][0][i], 2)] for i in range(len(mat_file["route_dist"][0]))]    #mile
    params["save_dist"] = [[np.round(mat_file["taxi_dist"][0][i])] for i in params["idx_from"]] +                            [[np.round(mat_file["taxi_dist"][0][i])] for i in params["idx_to"]]                             #mile
    params["y_max"] = [[math.ceil(params["route_trip_time"][i][0]/params["h_min"])] for i in range(len(params["route_trip_time"]))]
    params["y_min"] = [[math.floor(params["route_trip_time"][i][0]/params["h_max"])] for i in range(len(params["route_trip_time"]))]
    return params


# # 2. Master_milp

# In[5]:


def master_milp(Prob, constraint_activation):  #constraint_activation["3"] = True or False
    #Input: the solution for master_nlp'
    #Call cplex to solve the milp problem.
    #Outer apprxoimation for the master problem
    #x_master_milp: solution from the milp problem.
    #qs is the relaxtion for X_{s,i}^lh_s
    Prob["params"]["mvar"]["qs_range"] = [Prob["params"]["nvar_master"]+j                         for j in range(Prob["recourse_iter"]*Prob["params"]["H"]*Prob["params"]["nzone"])]
    Prob["params"]["mvar"]["L_range"] = [Prob["params"]["mvar"]["qs_range"][-1]+1+j                         for j in range(Prob["recourse_iter"]*Prob["params"]["nzone"])]
    Prob["params"]["mvar"]["X_range"] = [Prob["params"]["mvar"]["L_range"][-1]+1+j                         for j in range(Prob["recourse_iter"]*Prob["params"]["nzone"])]

    nvar = Prob["params"]["mvar"]["X_range"][-1] + 1
    #2. Define objective function first
    f = np.zeros(nvar)
    #operation cost per vehicle
    f[np.array(Prob["params"]["mvar"]["y_range"])] = Prob["params"]["co"]
    f[np.array(Prob["params"]["mvar"]["eta_range"])] = 1
    #----------------------Next define the linear constraints. -----------
    #3. 
    A, b = list(), list()
    if constraint_activation["3"] == True:
        print ("the current iteration is .....")
        #print (Prob["recourse_iter"])
        for i in range(Prob["recourse_iter"]):
            At = np.zeros((1,nvar))
            #At = np.zeros(nvar)   
            At[:,np.array(Prob["params"]["mvar"]["eta_range"])] = -1
            At[:,np.array([Prob["params"]["mvar"]["L_range"][k]                for k in range(i*Prob["params"]["nzone"], (i+1)*Prob["params"]["nzone"])])]=                    list(Prob["params"]["cl"]*(np.array(Prob["params"]["save_dist"]).reshape(-1)))
            tqs = np.zeros(Prob["params"]["nzone"] * Prob["params"]["H"])
            for j in range(Prob["params"]["H"]):
                tqs[np.array(range(j*Prob["params"]["nzone"], (j+1)*Prob["params"]["nzone"] ))] = np.power(2,j)
            k_in_range = range(i*Prob["params"]["nzone"]*Prob["params"]["H"], (i+1)*Prob["params"]["nzone"]*Prob["params"]["H"])
            At[:,np.array([Prob["params"]["mvar"]["qs_range"][k] for k in k_in_range])]  = 0.5*Prob["params"]["cw"] * tqs
            if i == 0:
                A = np.array(At)
                b = [0]
            else:
                A =  np.vstack((A,np.array(At)))   #?from to
                b = b + [0] 
        #print ("the current shape of matrix A is:", np.shape(A))
    else:
        for i in range(Prob["recourse_iter"]):
            At = np.zeros(nvar)    
            A = np.array([At])
            b = [0]
    #print ("At_equation_3")
    #for j in range(len(At)):
    #    print ("j",j,At[j])
        
        
    #4. sum 2^kz_s^k>=Ts*kappa_s
    if constraint_activation["4"] == True:
        tA = np.zeros((Prob["params"]["nroute"], nvar))
        for i in range(Prob["params"]["H"]):
            tA[:, np.array([int(Prob["params"]["mvar"]["zs_range"][k]) for k in                           range(i*Prob["params"]["nroute"], (i+1)*Prob["params"]["nroute"])])] =                                0.0-np.power(2,i)*np.eye(Prob["params"]["nroute"])
        tA[:, np.array(Prob["params"]["mvar"]["k_range"])] =            np.diag([Prob["params"]["route_trip_time"][r][0] for r in range(Prob["params"]["nroute"])])
    else:  
        tA = np.zeros((Prob["params"]["nroute"], nvar))
    A =  np.vstack((A,np.array(tA)))
    for i in range(Prob["params"]["nroute"]):
        b = b + [0]    #convert route time to minutes!
         
    #5. sum y_s <= B
    if constraint_activation["5"] == True:
        At = np.zeros(nvar)
        At[np.array(Prob["params"]["mvar"]["y_range"])] = 1
        A =  np.vstack((A,np.array(At)))
        b = b + list([Prob["params"]["B"]])
    else:
        At = np.zeros(nvar)
        A =  np.vstack((A,np.array(At)))
        b = b + list([0])

    #6. y and kappa
    #y_s<=Bkappas    
    if constraint_activation["6"] == True:
        At = np.zeros((Prob["params"]["nroute"], nvar))
        At[:, np.array(Prob["params"]["mvar"]["y_range"])] = np.eye(Prob["params"]["nroute"])
        At[:, np.array(Prob["params"]["mvar"]["k_range"])] =            -np.diag([Prob["params"]["y_max"][r][0] for r in range(len(Prob["params"]["y_max"]))])  
        A =  np.vstack((A,np.array(At)))
    else:
        At = np.zeros((Prob["params"]["nroute"], nvar))
        A =  np.vstack((A,np.array(At)))
    for i in range(Prob["params"]["nroute"]):
            b = b + [0]

    #7. kappa_s<=y
    if constraint_activation["7"] == True:
        At = np.zeros((Prob["params"]["nroute"], nvar))
        At[:, np.array(Prob["params"]["mvar"]["y_range"])]= -np.eye(Prob["params"]["nroute"])
        At[:, np.array(Prob["params"]["mvar"]["k_range"])] = np.eye(Prob["params"]["nroute"])
        A =  np.vstack((A,np.array(At)))  
    else:
        At = np.zeros((Prob["params"]["nroute"], nvar)) 
        A =  np.vstack((A,np.array(At)))
    for i in range(Prob["params"]["nroute"]):
        b = b + [0]  
    
    #8. sum qs from hub and tohub <=C
    if constraint_activation["8"] == True:
        for i in range(Prob["recourse_iter"]):
            At_to = np.zeros((Prob["params"]["nroute"], nvar))        
            At_from = np.zeros((Prob["params"]["nroute"], nvar))  
            tqs_to =  np.zeros((Prob["params"]["nroute"], Prob["params"]["nzone"]*Prob["params"]["H"]))
            tqs_from =  np.zeros((Prob["params"]["nroute"], Prob["params"]["nzone"]*Prob["params"]["H"]))         
            from_hub_idx = np.array(Prob["params"]["route_stop_idx"])
            params_nzone = Prob["params"]["nzone"]
            from_hub_idx[:, np.array([j for j in range(round(params_nzone/2))])] = 0   
            to_hub_idx = np.array(Prob["params"]["route_stop_idx"])
            to_hub_idx[:, np.array([round(params_nzone/2)+j for j in range(round(params_nzone/2))])] = 0 
            for j in range(Prob["params"]["H"]):
                tqs_from[:, np.array(range(params_nzone*j, params_nzone*(j+1)))] = np.power(2,j)* from_hub_idx
                tqs_to[:, np.array(range(params_nzone*j, params_nzone*(j+1)))] = np.power(2,j)* to_hub_idx
            At_to_from =                Prob["params"]["mvar"]["qs_range"][i*params_nzone*Prob["params"]["H"]:(i+1)*params_nzone*Prob["params"]["H"]]
            At_to[:, np.array(At_to_from)] = tqs_to
            At_from[:, np.array(At_to_from)] = tqs_from
            A =  np.vstack((A,np.array(At_from)))   #?from to
            A =  np.vstack((A,np.array(At_to)))             
            for j in range(2*Prob["params"]["nroute"]):
                b = b + [Prob["params"]["Capacity"]]
    else:
        for i in range(Prob["recourse_iter"]):
            At_to = np.zeros((Prob["params"]["nroute"], nvar))        
            At_from = np.zeros((Prob["params"]["nroute"], nvar))  
            A =  np.vstack((A,np.array(At_from)))   #?from to
            A =  np.vstack((A,np.array(At_to))) 
            for j in range(2*Prob["params"]["nroute"]):
                b = b + [0]
        
    #9. sum X + L=D_i*pi^*Zi
    Aeq, beq = list(), list()
    if constraint_activation["9"] == True:    
        for i in range(Prob["recourse_iter"]):
            At = np.zeros((Prob["params"]["nzone"],nvar))
            range_in = range(i*Prob["params"]["nzone"],(i+1)*Prob["params"]["nzone"])
            At[:,np.array(Prob["params"]["mvar"]["X_range"])[range_in]] = np.eye(Prob["params"]["nzone"])
            At[:,np.array(Prob["params"]["mvar"]["L_range"])[range_in]] = np.eye(Prob["params"]["nzone"])  
            tb = np.array(Prob["params"]["X"]) + np.multiply(np.array(Prob["params"]["recourse_results"]["p"][i]), np.array(Prob["params"]["Xstd"]))
            Aeq = np.array(At)
            beq = tb.reshape(-1)
    else:
        for i in range(Prob["recourse_iter"]):
            At = np.zeros((Prob["params"]["nzone"],nvar))
            Aeq = np.array(At)
            for j in range(Prob["params"]["nzone"]):
                beq = beq + [0]
    beq = np.array(beq)
    
    #10. zs<=gs^kymax
    B_value = Prob["params"]["B"]
    if constraint_activation["10"] == True:   
        for i in range(Prob["params"]["H"]):
            At = np.zeros((Prob["params"]["nroute"], nvar))
            range_in = range(i*Prob["params"]["nroute"], (i+1)*Prob["params"]["nroute"])
            At[:, np.array(Prob["params"]["mvar"]["zs_range"])[range_in]] = np.eye(Prob["params"]["nroute"])
            At[:, np.array(Prob["params"]["mvar"]["gs_range"])[range_in]] =                -np.diag([B_value for r in range(len(Prob["params"]["y_max"]))])
            A =  np.vstack((A,np.array(At))) 
            for j in range(Prob["params"]["nroute"]):
                b = b + [0] 
    else:
        for i in range(Prob["params"]["H"]):
            At = np.zeros((Prob["params"]["nroute"], nvar))
            A =  np.vstack((A,np.array(At))) 
            for j in range(Prob["params"]["nroute"]):
                b = b + [0] 
                
            
    #11. zs^k<=ys-ysmin(1-gsk)
    if constraint_activation["11"] == True:   
        for i in range(Prob["params"]["H"]):
            At = np.zeros((Prob["params"]["nroute"],nvar))
            At[:,Prob["params"]["mvar"]["y_range"]] = -np.eye(Prob["params"]["nroute"])
            range_in = range(i*Prob["params"]["nroute"], (i+1)*Prob["params"]["nroute"])
            At[:,np.array(Prob["params"]["mvar"]["zs_range"])[range_in]] = np.eye(Prob["params"]["nroute"])   
            #At[:,np.array(Prob["params"]["mvar"]["gs_range"])[range_in]] =\
            #    np.diag([Prob["params"]["y_min"][r][0] for r in range(len(Prob["params"]["y_min"]))])*0
            A =  np.vstack((A,np.array(At)))  
            for j in range(len(Prob["params"]["y_min"])):
                b = b + list(-np.array(Prob["params"]["y_min"][j])*0)
    else:
        for i in range(Prob["params"]["H"]):
            At = np.zeros((Prob["params"]["nroute"],nvar))
            A =  np.vstack((A,np.array(At)))  
            for j in range(len(Prob["params"]["y_min"])):
                b = b + list(-np.array(Prob["params"]["y_min"][j])*0)
                
    
    #12. qs<=gsk(D+pZ)
    if constraint_activation["12"] == True:  
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):
                At = np.zeros((Prob["params"]["nzone"], nvar))
                tqs =  np.zeros((Prob["params"]["nzone"], Prob["params"]["nzone"]*Prob["params"]["H"]))
                range_in_1 = range(j*Prob["params"]["nroute"], (j+1)*Prob["params"]["nroute"])
                right_1 = np.transpose(Prob["params"]["route_stop_idx"])
                right_2 = np.array(Prob["params"]["X"])
                right_3 = np.array(Prob["params"]["recourse_results"]["p"][i])
                right_4 = np.array(Prob["params"]["Xstd"])                  
                At[:, np.array(Prob["params"]["mvar"]["gs_range"])[range_in_1]]                    = -np.multiply(right_1, right_2+ np.multiply(right_3, right_4))
                range_in_2 = range(i*Prob["params"]["nzone"]*Prob["params"]["H"]+ j*Prob["params"]["nzone"],                                   i*Prob["params"]["nzone"]*Prob["params"]["H"]+ (j+1)*Prob["params"]["nzone"])
                #range_in_2 = range(i*Prob["recourse_iter"]*Prob["params"]["H"]+ j*Prob["params"]["H"],\
                #                   i*Prob["recourse_iter"]*Prob["params"]["H"]+ (j+1)*Prob["params"]["H"])
                At[:, np.array(Prob["params"]["mvar"]["qs_range"])[range_in_2]] = np.eye(Prob["params"]["nzone"])
                A =  np.vstack((A,np.array(At))) 
                for k in range(Prob["params"]["nzone"]):
                    b = b + [0]
    else:
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):
                At = np.zeros((Prob["params"]["nzone"], nvar))
                A =  np.vstack((A,np.array(At))) 
                for k in range(Prob["params"]["nzone"]):
                    b = b + [0]
    
    #13. qs<=Xsl   
    if constraint_activation["13"] == True:  
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):
                At = np.zeros((Prob["params"]["nzone"], nvar))
                range_in_1 = range(i*Prob["params"]["nzone"], (i+1)*Prob["params"]["nzone"])
                At[:, np.array(Prob["params"]["mvar"]["X_range"])[range_in_1]] = -np.eye(Prob["params"]["nzone"])
                range_in_2 = range(i*Prob["params"]["nzone"]*Prob["params"]["H"] + j *Prob["params"]["nzone"],                                   i*Prob["params"]["nzone"]*Prob["params"]["H"] + (j+1) *Prob["params"]["nzone"])
                At[:, np.array(Prob["params"]["mvar"]["qs_range"])[range_in_2]] = np.eye(Prob["params"]["nzone"])
                A =  np.vstack((A,np.array(At)))  
                for k in range(Prob["params"]["nzone"]):
                    b = b + [0] 
    else:
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):    
                At = np.zeros((Prob["params"]["nzone"], nvar))
                A =  np.vstack((A,np.array(At)))  
                for k in range(Prob["params"]["nzone"]):
                    b = b + [0]  
                
    #14.-qsk+Xs+(Di+piZ)gsk<=DipiZ
    if constraint_activation["14"] == True:  
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):  
                At = np.zeros((Prob["params"]["nzone"],nvar))
                Xmax = Prob["params"]["X"] + np.multiply(Prob["params"]["recourse_results"]["p"][i], Prob["params"]["Xstd"])
                range_in_1 = range(j*Prob["params"]["nroute"], (j+1)*Prob["params"]["nroute"])
                At[:, np.array(Prob["params"]["mvar"]["gs_range"])[range_in_1]] = np.multiply(np.transpose(Prob["params"]["route_stop_idx"]), Xmax)
                range_in_2 = range(i*Prob["params"]["nzone"]*Prob["params"]["H"]+ j*Prob["params"]["nzone"],                                   i*Prob["params"]["nzone"]*Prob["params"]["H"]+ (j+1)*Prob["params"]["nzone"])
                #range_in_2 = range(i*Prob["params"]["recourse_iter"]*Prob["params"]["H"]+ j*Prob["params"]["H"],\
                #                   i*Prob["params"]["recourse_iter"]*Prob["params"]["H"]+ (j+1)*Prob["params"]["H"])
                At[:, np.array(Prob["params"]["mvar"]["qs_range"])[range_in_2]] = -np.eye(Prob["params"]["nzone"])
                range_in_3 = range(i*Prob["params"]["nzone"], (i+1)*Prob["params"]["nzone"])
                At[:, np.array(Prob["params"]["mvar"]["X_range"])[range_in_3]] = np.eye(Prob["params"]["nzone"]) 
                A =  np.vstack((A,np.array(At)))
                b = b + list(Xmax)
    else:
        for i in range(Prob["recourse_iter"]):
            for j in range(Prob["params"]["H"]):   
                At = np.zeros((Prob["params"]["nzone"],nvar)) 
                A =  np.vstack((A,np.array(At)))
                for k in range(Prob["params"]["nzone"]):
                    b = b + [0]  
                
    #15. sum g_sk <= hmax
    if constraint_activation["15"] == True:  
        At = np.zeros((Prob["params"]["nroute"],nvar))
        for i in range(Prob["params"]["H"]):
            range_in_1 = range(i*Prob["params"]["nroute"], (i+1)*Prob["params"]["nroute"])
            At[:, np.array(Prob["params"]["mvar"]["gs_range"])[range_in_1]] = np.power(2, i)*np.eye(Prob["params"]["nroute"])
        At[:,np.array(Prob["params"]["mvar"]["k_range"])] = -Prob["params"]["h_max"]*np.eye(Prob["params"]["nroute"])
        #At[:, np.array(Prob["params"]["mvar"]["k_range"])] =\
        #        np.array([[0.0-Prob["params"]["h_max"] for r in range(Prob["params"]["nroute"])] for r in range(Prob["params"]["nroute"])])
        for j in range(Prob["params"]["nroute"]):
            b = b + [0] 
        A =  np.vstack((A,np.array(At)))
    else:
        At = np.zeros((Prob["params"]["nroute"],nvar))
        for j in range(Prob["params"]["nroute"]):
            b = b + [0] 
        A =  np.vstack((A,np.array(At)))

    #16. sum g_sk >= hmin
    if constraint_activation["16"] == True:  
        At = np.zeros((Prob["params"]["nroute"],nvar))
        for i in range(Prob["params"]["H"]):
            range_in_1 = np.array(range(i*Prob["params"]["nroute"], (i+1)*Prob["params"]["nroute"]))
            At[:, np.array(Prob["params"]["mvar"]["gs_range"])[range_in_1]] = -np.power(2, i)*np.eye(Prob["params"]["nroute"])
            #At[:, np.array(Prob["params"]["mvar"]["k_range"])] =\
            #    np.array([[Prob["params"]["h_min"] for r in range(Prob["params"]["nroute"])] for r in range(Prob["params"]["nroute"])]) 
        At[:,np.array(Prob["params"]["mvar"]["k_range"])] = Prob["params"]["h_min"]*np.eye(Prob["params"]["nroute"])
        for j in range(Prob["params"]["nroute"]):
            b = b + [0] 
        A =  np.vstack((A,np.array(At)))
    else:
        At = np.zeros((Prob["params"]["nroute"],nvar))
        for j in range(Prob["params"]["nroute"]):
            b = b + [0] 
        A =  np.vstack((A,np.array(At)))
    
    #17. X<=kappaXmax
    if constraint_activation["17"] == True:  
        At = np.zeros((Prob["params"]["nzone"],nvar))
        for i in range(Prob["recourse_iter"]):
            Xmax = Prob["params"]["X"] + np.multiply(Prob["params"]["recourse_results"]["p"][i], Prob["params"]["Xstd"])
            range_in_1 = range(i*Prob["params"]["nzone"], (i+1)*Prob["params"]["nzone"])
            At[:, np.array(Prob["params"]["mvar"]["X_range"])[range_in_1]] = np.eye(Prob["params"]["nzone"])
            At[:, np.array(Prob["params"]["mvar"]["k_range"])] = -np.transpose(Prob["params"]["route_stop_idx"])*Xmax
            A =  np.vstack((A,np.array(At)))
            for j in range(Prob["params"]["nzone"]):
                b = b + [0]       
        b = np.array(b)
    else:
        At = np.zeros((Prob["params"]["nzone"],nvar))
        for i in range(Prob["recourse_iter"]):
            A =  np.vstack((A,np.array(At)))
            for j in range(Prob["params"]["nzone"]):
                b = b + [0]    
        b = np.array(b)
    
    #18.1 create model
    milp_model = gp.Model("milp")
    vname = 'x'
    vtypeList = [GRB.CONTINUOUS] *  nvar
    for i in Prob["params"]["mvar"]["y_range"]:
        vtypeList[i] = GRB.INTEGER
    for i in Prob["params"]["mvar"]["zs_range"]:
        vtypeList[i] = GRB.INTEGER
    for i in Prob["params"]["mvar"]["gs_range"]:
        vtypeList[i] = GRB.BINARY
    for i in Prob["params"]["mvar"]["k_range"]:
        vtypeList[i] = GRB.BINARY
    lb1 = [0 for i in range(nvar)]  #!!!
    ub1 = [float('inf') for i in range(nvar)]
    
    x = milp_model.addMVar(nvar, lb=lb1, ub=ub1, vtype=vtypeList, name=vname)
    milp_model.setObjective(f @ x, GRB.MINIMIZE)
    
    A =  np.vstack((A,np.array(Aeq)))
    A =  np.vstack((A,np.array(-Aeq)))
    #for i in range(3546,3580):                                        ####check_range
    #    print ("i ",i, "row sum of A ", np.sum(A[i]))
    b = b.reshape(len(b),1)
    beq = beq.reshape(len(beq),1)
    b =  np.vstack((b,np.array(beq)))
    b =  np.vstack((b,np.array(-beq)))
    b = b.reshape(-1)
    
    milp_model.addConstr(A @ x <= b, name='inequality constraints')
    milp_model.optimize()
    x_solution = x.X
    y_value = milp_model.ObjVal
    result = milp_model
    #print ("y_range")
    #print ([x.X[params["mvar"]["y_range"][i]] for i in range(len(params["mvar"]["y_range"]))])  
    #print ("sum_of_y")
    #print (np.sum([x.X[params["mvar"]["y_range"][i]] for i in range(len(params["mvar"]["y_range"]))]))
    #print ("gs_range")
    #print ([x.X[params["mvar"]["gs_range"][i]] for i in range(len(params["mvar"]["gs_range"]))])  
    
    #print ("zs_range")
    #print ([x.X[params["mvar"]["zs_range"][i]] for i in range(len(params["mvar"]["zs_range"]))])  
    
    #print ("k_range")
    #print ([x.X[params["mvar"]["k_range"][i]] for i in range(len(params["mvar"]["k_range"]))])
    
    #print ("eta_range")
    #print ("eta", x.X[params["mvar"]["eta_range"][0]])
    #print ("-------------------------------Master-Solvered-------------------------------------------------------------")
    print ("check: obj for master is", y_value)
    return x_solution, y_value


# # 3. recourse_milp

# In[6]:


def recourse_milp(x_master_milp, Prob):
    #Recalibrate the route travel time for each path
    #decode the H 
    #1. 
    nzone = Prob["params"]["nzone"]
    nroute = Prob["params"]["nroute"]
    tA = np.zeros((nroute, len(x_master_milp[Prob["params"]["mvar"]["gs_range"]])))
    for i in range(Prob["params"]["H"]):
        tA[:,i*nroute:(i+1)*nroute] = np.power(2, i)*np.eye(nroute)  
    master_h = np.round(np.dot(tA, np.transpose([x_master_milp[Prob["params"]["mvar"]["gs_range"]]])))
    #for i in range(Prob["params"]["H"]):
        
    #print ("master_h", master_h)                                                        
    #2. obj fun
    #nzone = Prob["params"]["nzone"]
    n_recourse = Prob["params"]["nvar_recourse"]
    f = np.zeros(n_recourse)
    f[Prob["params"]["rvar"]["lambda_range"]] = np.array(Prob["params"]["X"]).reshape(-1)  #lambda coeff 
    f[Prob["params"]["rvar"]["gamma_range"]] = np.array(Prob["params"]["Xstd"]).reshape(-1) #gamma coeff
    f[Prob["params"]["rvar"]["u_range"]] = Prob["params"]["Capacity"]*np.ones(2*Prob["params"]["nroute"])
    array1 = Prob["params"]["route_stop_idx"]
    array2 = x_master_milp[Prob["params"]["mvar"]["k_range"]]
    array2 = array2.reshape(len(array2),1)
    array1_array2 = np.dot(np.transpose(array1), array2).reshape(-1)
    
    array3 = np.array(Prob["params"]["X"]).reshape(-1)
    array4 = np.array(Prob["params"]["Xstd"]).reshape(-1)
           
    f[Prob["params"]["rvar"]["v_range"]] = np.multiply(array1_array2, array3)
    f[Prob["params"]["rvar"]["z_range"]] = np.multiply(array1_array2, array4)
    
    #3.
    M = 1e4
    #4. lambda+mu+g>=xx
    #Original A: sum X_from<=C, sum X_to<=C, sum X_i+Li=D
    array1 = np.eye(Prob["params"]["nzone"]) #200,200
    
    array2 = np.multiply(Prob["params"]["route_stop_idx"][:, range(round(Prob["params"]["nzone"]/2))], master_h) #
    array3 = np.zeros((Prob["params"]["nroute"], round(0.5*Prob["params"]["nzone"])))
    array4 = np.zeros((Prob["params"]["nroute"], Prob["params"]["nzone"]))
    
    array5 = np.zeros((Prob["params"]["nroute"], round(0.5*Prob["params"]["nzone"])))
    array5_con = np.concatenate((array5, Prob["params"]["route_stop_idx"][:,round(Prob["params"]["nzone"]/2):]), axis=1)
    
    array6 = np.multiply(array5_con, master_h)
    array7 = np.zeros((Prob["params"]["nroute"], Prob["params"]["nzone"]))
    array8 = np.eye(Prob["params"]["nzone"])
    array9 = np.zeros((Prob["params"]["nzone"], Prob["params"]["nzone"]))
    
    OA_con1 = np.concatenate((array1, array1), axis=1)  #400
    OA_con2 = np.concatenate((array2, array3, array4), axis=1) #！！ 
    OA_con3 = np.concatenate((array6, array7), axis=1)
    OA_con4 = np.concatenate((array8, array9), axis=1)
    OA = np.concatenate((OA_con1, OA_con2, OA_con3, OA_con4), axis=0)
    
    tA = np.zeros((2*Prob["params"]["nzone"], Prob["params"]["nvar_recourse"]))  #400, 1068
    tA[:, range(Prob["params"]["rvar"]["lambda_range"][0], Prob["params"]["rvar"]["v_range"][-1]+1)] = np.transpose(OA)
    A = tA
    tA_tA = np.dot(np.transpose(tA),tA)
    b1 = 0.5*Prob["params"]["cw"] * np.dot(np.transpose(Prob["params"]["route_stop_idx"]), master_h)
    b2 = np.array(Prob["params"]["cl"] * np.array(Prob["params"]["save_dist"]))
    b = np.concatenate((b1, b2), axis=0)  #400*1

    #5. lambda < cd
    tA = np.zeros((Prob["params"]["nzone"], Prob["params"]["nvar_recourse"]))
    tA[:,Prob["params"]["rvar"]["gamma_range"]] = np.eye(nzone)
    tA[:,Prob["params"]["rvar"]["p_range"]] =        -Prob["params"]["cl"]*np.diag([Prob["params"]["save_dist"][r][0] for r in range(len(Prob["params"]["save_dist"]))])
    #print ("distance check", Prob["params"]["save_dist"])
    tb = np.zeros((nzone,1))
    A =  np.vstack((A,np.array(tA)))
    b =  np.vstack((b,np.array(tb)))
    
    #6. gamma_i<=piM
    tA = np.zeros((Prob["params"]["nzone"], Prob["params"]["nvar_recourse"]))
    tA[:,Prob["params"]["rvar"]["lambda_range"]] = -np.eye(nzone)
    tA[:,Prob["params"]["rvar"]["gamma_range"]] = np.eye(nzone)
    tb = np.zeros((nzone,1))*M
    A =  np.vstack((A,np.array(tA)))
    b =  np.vstack((b,np.array(tb)))
    
    #7. z_i>=-piM (or Zi<=piM?)
    tA = np.zeros((Prob["params"]["nzone"], Prob["params"]["nvar_recourse"]))
    tA[:,Prob["params"]["rvar"]["z_range"]] = - np.eye(nzone)
    tA[:,Prob["params"]["rvar"]["p_range"]] = - M * np.eye(Prob["params"]["nzone"])
    tb = np.zeros((nzone,1)) 
    A =  np.vstack((A,np.array(tA)))
    b =  np.vstack((b,np.array(tb)))
 
    #z>=v (or z<=v?)
    #z<=0, z>=-p*M, z<=v+(1-p)*M
    tA = np.zeros((Prob["params"]["nzone"], Prob["params"]["nvar_recourse"]))
    tA[:, Prob["params"]["rvar"]["z_range"]] = np.eye(nzone)
    tA[:, Prob["params"]["rvar"]["v_range"]] = -np.eye(nzone)
    tA[:, Prob["params"]["rvar"]["p_range"]] = M * np.eye(Prob["params"]["nzone"])
    tb = M * np.ones((nzone, 1))
    A =  np.vstack((A,np.array(tA)))
    b =  np.vstack((b,np.array(tb)))
    
    tA_tA = np.dot(np.transpose(tA),tA)
    Aeq = np.zeros((1,Prob["params"]["nvar_recourse"])) 
    Aeq[:,Prob["params"]["rvar"]["p_range"]] = 1
    beq = np.array([[Prob["params"]["T"]]])
  
    #8. now define lb and ub
    infinty = 1e8
    lb = np.array([-infinty for i in range(Prob["params"]["nvar_recourse"])])
    ub = np.zeros(Prob["params"]["nvar_recourse"])
    lb[np.array(Prob["params"]["rvar"]["p_range"])] = 0
    ub[np.array(Prob["params"]["rvar"]["p_range"])] = 1
    ub[np.array(Prob["params"]["rvar"]["lambda_range"])] = np.array(Prob["params"]["cl"]*np.array(Prob["params"]["save_dist"])).reshape(-1)
    ub[np.array(Prob["params"]["rvar"]["gamma_range"])] = np.array(Prob["params"]["cl"]*np.array(Prob["params"]["save_dist"])).reshape(-1)
    
    lb_v = np.minimum(np.zeros((Prob["params"]["nzone"],1)),
               -np.array(Prob["params"]["cl"]*np.array(Prob["params"]["save_dist"]))\
                   +0.5*Prob["params"]["cw"]*np.dot(np.transpose(Prob["params"]["route_stop_idx"]), master_h)).reshape(-1)
    lb[np.array(Prob["params"]["rvar"]["v_range"])] = np.array([lb_v[r] for r in range(len(lb_v))])
     
    lb_z = np.minimum(np.zeros((Prob["params"]["nzone"],1)),            -np.array(Prob["params"]["cl"]*np.array(Prob["params"]["save_dist"]))                +0.5*Prob["params"]["cw"]*np.dot(np.transpose(Prob["params"]["route_stop_idx"]), master_h)).reshape(-1)  
    lb[np.array(Prob["params"]["rvar"]["z_range"])] = np.array([lb_z[r] for r in range(len(lb_z))])
    
    #9. define the varibale type
    milp_model = gp.Model("milp")
    vname = 'x'
    vtypeList = [GRB.CONTINUOUS] *  n_recourse
    for i in Prob["params"]["rvar"]["p_range"]:
        vtypeList[i] = GRB.BINARY
    #10. solve the optimization
    x = milp_model.addMVar(n_recourse, lb=lb, ub=ub, vtype=vtypeList, name=vname)
 
    milp_model.setObjective(-f @ x, GRB.MINIMIZE)           
    b = b.reshape(-1)
    beq = beq.reshape(-1)
    milp_model.addConstr(A @ x <= b, name='inequality constraints')
    milp_model.addConstr(Aeq @ x == beq, name= 'equality constraint1')
    milp_model.optimize()
    x_solution = x.X
    y_value = milp_model.ObjVal
    result = milp_model
    print ("----------------------------------Recourse-Solvered-------------------------------------------------------------")
    print ("check: obj for recourse is", y_value)
    return x_solution, y_value


# # 4. Main function

# In[7]:


def route_optimization():
    constraint_activation  = dict()
    for i in range(15):
        constraint_activation[str(i+3)] = True
    #false_index = [3,4,5,6,7,8,9,10]
    #false_index = [11,12,13,14,15,16,17] 
    #false_index = [14,15,16,17] 
    #false_index = [11,12,13]
    #false_index = [14,15] 
    #false_index = [16,17] 
    false_index = [14] 
    for index in false_index:
        constraint_activation[str(index)] = False
    Results = dict()
    idx = 1
    i = 0
    mat = scipy.io.loadmat(files[i])
    test_name = files[i]
    params = set_params(mat)
    Prob = dict()
    Prob["params"] = params
    #loop over the different fleet sizes
    for j in range(len(Blist)):
        #loop over different uncertainty levels.
        Prob["params"]["B"] = Blist[j]
        for k in range(len(Tlist)):
            t = 0
            Prob["params"]["T"] = Tlist[k]
            Prob["recourse_iter"] = 1
            a = [[0] for q in range(Prob["params"]["nzone"])]
            z2 = np.array([params["Xstd"][q][0] for q in range(len(params["Xstd"]))]).argsort()[-Prob["params"]["T"]:][::-1]
            z1 = np.array([params["Xstd"][q][0] for q in range(len(params["Xstd"]))])[z2]
            for item in z2:
                a[item][0] = 1
            Prob["params"]["recourse_results"] = dict()
            Prob["params"]["recourse_results"]["p"] = [a]
            lb_vec, ub_vec, number_bus_vec = [-1e12], [1e12], [-1]
            LB, UB = -1e12, 1e12
            err = 1e-3
            results = dict()
            results["master"] = dict()
            results["recourse"] = dict()
            results["B"], results["name"], results["T"] = Blist[j], test_name, Tlist[k] #log the information.
            tf = [0]
            n = 0
            while n <=2:
                n=n+1
                #print ("check recourse result p")
                #print (Prob["params"]["recourse_results"]["p"])
                #x_master_milp, MLB, sol = master_milp(Prob)    #call the master_milp function, solve the milp
                x_master_milp, MLB = master_milp(Prob, constraint_activation)

                #t += sol["runtime"]
                master_results = dict()
                master_results["x"] = x_master_milp
                master_results["y"] = MLB
                #master_results["sol"] = sol
                results["master"][Prob["recourse_iter"]] = master_results

                LB, UB = MLB, MLB - x_master_milp[Prob["params"]["mvar"]["eta_range"]][0]  #numpy deletion
                #print ("check_reduce", x_master_milp[Prob["params"]["mvar"]["eta_range"]])
                x_recourse, fval = recourse_milp(x_master_milp, Prob)    #Call the recourse_milp
                print ("fval_check", fval)
                #t += sol["time"]
                UB = UB - fval   #fval is negative
                if UB > ub_vec[-1]:
                    UB = ub_vec[-1]
                recourse_results = dict()
                #recourse_results["x"], recourse_results["y"], recourse_results["sol"] = x_recourse, LB, sol
                recourse_results["x"], recourse_results["y"] = x_recourse, UB
                results["recourse"][Prob["recourse_iter"]] = recourse_results
                Prob["recourse_iter"] = Prob["recourse_iter"] + 1

                Prob["params"]["recourse_results"]["p"].append([[x_recourse[q]] for q in range(Prob["params"]["nzone"])])
                lb_vec = lb_vec + [LB]
                ub_vec = ub_vec + [UB]
                number_bus_vec = number_bus_vec + [np.sum(x_master_milp[np.array(Prob["params"]["mvar"]["y_range"])])]
                print ("Iteration solved")
                print ("----------------------------------Iteration-------------------------------------------------------------")
                #print (Prob["Recourse_iter"])
                print('lower_bound_vector', lb_vec)
                print ('upper_bound_vector', ub_vec)
                print ("number_bus_vec", number_bus_vec)
                print("--------------------------------------------------------------------------------------------")
                if t>1800:
                    print('Timeout\n')
                    break
            tA = 1*np.eye(Prob["params"]["nroute"])
            for h in range(Prob["params"]["H"]-1):
                tA = np.concatenate((tA, (2^(h+1))*np.eye(Prob["params"]["nroute"])), axis=1)
            master_h = tA * x_master_milp[Prob["params"]["mvar"]["gs_range"]]               #x_master_milp format
            results["headway"] = master_h

            niter = Prob["recourse_iter"]-1
            Results[str(idx)] = results
            idx += 1
            #print('Results recorded for B=%d, T=%d, file=%s \n' %(Blist[j], Tlist[k],test_name))
            #scipy.io.savemat('Results_%d.mat'%Blist[j], Results)    #?
            return x_master_milp


# In[10]:


#ss  = route_optimization()

#print("bus frequency")
#print(ss[0:33])
#print("route information")
#print(mat["route_stop_index"][33])
# In[11]:


class RouteOptimization(object):
    #1. initialize the travel time, travel demand, and potential route
    #2. initialize the route frequency
    #3. update the travel time, and travel demand, 
    #4. potential route
    #5. implement the bus frequency design, and update the route frequency
    
    #1. initialize the travel time, travel demand, and potential route
    def __init__(self, travel_time, travel_demand, bus_route):
        self.travel_time = travel_time        #the travel time between two different zones in the city
        self.travel_demand = travel_demand    
        self.bus_route = bus_route  
        self.bus_frequency = {}  
 
    #2. update the travel time, travel demand
    def update_time_demand(self, travel_time, travel_demand):
        self.travel_time = travel_time
        self.travel_demand = travel_demand
    
    #3. update the potential routes
    def update_route(self, bus_route):
        self.bus_route = bus_route 
    
    #4. implement the bus route optimization, and update the bus route frequency. 
    def run(self):
        print ("start route optimization!")
        #implement the route generation process
        #self.bus_route 
        results = route_optimization()
        print ("finish route optimization!")
        self.bus_frequency = results
        
    def get_frequency(self):
        return self.bus_frequency;
        print(self.bus_frequency)
 



