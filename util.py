import socket
import json
import os
import subprocess
import time
import shutil
from os import path
import platform
from contextlib import closing
from types import SimpleNamespace

"""
Helper functions for METSR-HPC
"""

# Function for checking whether the socket connection is on
def check_socket(host, port):
    flag = True
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            flag =  True
        else:
            flag =  False
    time.sleep(5)
    return flag

# Factory for processsing a str list with a given func
def str_list_mapper_gen(func):
    def str_list_mapper(str_list):
        return [func(str) for str in str_list]
    return str_list_mapper

# Function for modifying simulation properties
def modify_property_file(options, src_data_dir, dest_data_dir, port, scenario, case):
    fname = src_data_dir + "/Data.properties"
    f = open(fname, "r")
    lines = f.readlines()
    f.close()
    fname = dest_data_dir + "/Data.properties"
    f_new = open(fname, "w")
    for l in lines:
        if "NETWORK_LISTEN_PORT" in l:
            l = "NETWORK_LISTEN_PORT = " + str(port) + "\n"
        elif "DM_EVENT_FILE" in l:
            if(options.full_demand == "true"):
                l = "DM_EVENT_FILE = data/NYC/demand_full/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
            else:
                l = "DM_EVENT_FILE = data/NYC/demand/"+options.scenarios[scenario] + "/demand_"+ options.cases[scenario][case]+ "\n"
        elif "BT_EVENT_FILE" in l:
            l = "BT_EVENT_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
        elif "BT_STD_FILE" in l:
            l = "BT_STD_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_std_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
        elif "ECO_ROUTING_EV" in l:
            l = "ECO_ROUTING_EV = " + str(options.eco_routing) + "\n"
        elif "NUM_OF_EV" in l:
            l = "NUM_OF_EV = " + str(options.taxi_fleet_size) + "\n"
        elif "NUM_OF_BUS" in l:
            l = "NUM_OF_BUS = " + str(options.bus_fleet_size) + "\n"
        # elif "ECO_ROUTING_BUS" in l:
        #     l = "ECO_ROUTING_BUS = " + str(options.eco_routing) + "\n"
        elif "BUS_PLANNING" in l:
            l = "BUS_PLANNING = " + str(options.bus_scheduling) + "\n"
        elif "PASSENGER_SHARE_PERCENTAGE" in l:
            l = "PASSENGER_SHARE_PERCENTAGE = " + str(options.share_percentage) + "\n"
        elif "PASSENGER_DEMAND_FACTOR" in l:
            l = "PASSENGER_DEMAND_FACTOR = " + str(options.demand_factor) + "\n"
        elif "CHARGER_CSV" in l:
            l = "CHARGER_CSV = data/NYC/charging_station/" + options.charger_plan + "\n"
        elif ("BUS_SCHEDULE" in l) and options.bus_fleet_size >= 100 and options.bus_scheduling == 'false':
            l = "BUS_SCHEDULE = data/NYC/bus_planning/bus_routes" + str(options.bus_fleet_size // 100) + ".json\n"
        elif ("COLLABORATIVE_EV" in l):
            l = "COLLABORATIVE_EV = " + str(options.cooperative) + "\n"
        if "data/" in l:
            l = l.replace('data/', src_data_dir + '/')
        f_new.write(l)
    f_new.close()

# Copy necessary files for running the simulation
# Note: Need to update this function if the simulation is running on a different machine
def prepare_sim_dirs(options):
    src_data_dir = options.sim_dir + "data"
    if options.full_demand == "true":
        prepare_scenario_dict(options, src_data_dir + "/NYC/demand_full")
    else:
        prepare_scenario_dict(options, src_data_dir + "/NYC/demand")
    find_free_ports(options, options.num_simulations)
    if len(options.ports) != options.num_simulations:
        print("ERROR , cannot specify port number for all simulation instances")
        sys.exit(-1)
    for i in range(0, options.num_simulations):
        # make a directory to run the simulator
        dir_name = get_sim_dir(options, i)
        if not path.exists(dir_name):
            os.makedirs(dir_name)
        # copy the simulation config files
        dest_data_dir = dir_name + "/" + "data" 
        
        if not path.exists(dest_data_dir):
            os.mkdir(dest_data_dir)
            
            try:
                os.mkdir(dest_data_dir+"/NYC")
                shutil.copy(src_data_dir+"/NYC/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes.ser")
                shutil.copy(src_data_dir+"/NYC/candidate_routes_bus.ser", dest_data_dir+"/NYC/candidate_routes_bus.ser")
            except OSError as exc:
                print(f"ERROR :can not copy the data directory. exception {exc}")
                sys.exit(-1)
        modify_property_file(options, src_data_dir, dest_data_dir, options.ports[i], options.scenario_index, options.case_index)
        
    return dest_data_dir

# Function for getting the file name list of demand scenarios
def prepare_scenario_dict(options, path):
    scenarios = os.listdir(path)
    i = 0
    scenarios = sorted(scenarios)
    options.scenarios=[]
    options.cases = [[] for j in range(len(scenarios))]
    for scenario in scenarios:
        options.scenarios.append(scenario)
        options.cases[i] = []
        cases = os.listdir(path+"/"+scenario)
        cases = sorted(cases)
        for case in cases:
            options.cases[i].append(case.split("_")[1])
        i+=1

# Functions for finding available port
def find_free_ports(options, num_simulations):
    options.ports = []
    while True:
        for i in range(num_simulations):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                s.bind(('localhost', 0))
                options.ports.append(s.getsockname()[1])
        try:
            for port in options.ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', port))
                s.close()
            break
        except:
            print("The port is not valid anymore, regenerate it")
            continue
    time.sleep(1)
     
# Read json format configuration 
def read_run_config(fname):
    with open(fname, "r") as f:
        config = json.load(f)

    opts = SimpleNamespace()
    opts.java_path = config['java_path']
    opts.java_options = config['java_options']
    opts.sim_dir = config['sim_dir']
    opts.groovy_dir = config['groovy_dir']
    opts.repast_plugin_dir = config['repast_plugin_dir']
    opts.num_simulations = int(config['num_sim_instances'])
    opts.charger_plan = config['charger_plan']

    #if len(opts.ports) != opts.num_simulations:
    #    print("ERROR , please specify port number for all simulation instances")
    #    sys.exit(-1)

    return opts

# Construct the java classpath with all the required jar files. 
# If includeBin is False it won't add the METS_R/bin directory to classpath.
# This is needed for simulation command.
def get_classpath(options, includeBin=True, separator=":"):
    
    classpath = ""

    if not path.exists(options.groovy_dir):
        print(f"ERROR , groovy is not found at {options.groovy_dir}")
        sys.exit(-1)
    
    classpath += options.groovy_dir + "lib/*" + separator

    if not path.exists(options.repast_plugin_dir):
        print(f"ERROR , repast plugins not found at {options.repast_plugin_dir}")
        sys.exit(-1)
    
    classpath += options.repast_plugin_dir + "bin" + separator + \
                 options.repast_plugin_dir + "lib/*" + separator
    
    classpath += options.sim_dir + separator + \
                 options.sim_dir + "lib/*"
    
    if(includeBin):
        classpath += separator + options.sim_dir + "bin"

    return classpath

# DEPRECATED : java version of RDCM 
def run_rdcm_java(options, config_fname):
    # rdcm command
    rdcm_command = '"' + options.java_path + '"' + " " + \
                   "-classpath " + \
                   "../rdcm_java_version/target/rdcm-1.0-SNAPSHOT.jar:../rdcm/target/dependency/*" + " " + \
                   "com.metsr.hpc.RemoteDataClientManager " + \
                   config_fname + " " + options.sim_dir + "/data/"
    
    # run rdcm on a new terminal
    cwd = str(os.getcwd())
    os.system(rdcm_command + " > rdcm.log 2>&1  &")

# Function for starting the simulation
def run_simulations(options):
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        sim_dir = get_sim_dir(options, i)
        if platform.system() == "Windows":
             # go to sim directory
            os.chdir(sim_dir)
            # run the simulation on a new terminal
            sim_command = '"' + options.java_path + '"' + " " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    get_classpath(options, False, separator = ";") + " " + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            subprocess.Popen(sim_command + " > sim_{}.log 2>&1 &".format(i), shell=True)
        else:
            # go to sim directory
            os.chdir(sim_dir)
            # run simulator on new terminal 
            sim_command = options.java_path + " " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    get_classpath(options, False) + " " + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir(cwd)

# Get the directory for storing simulation outputs
def get_sim_dir(options, i):
    sim_dir = "output/scenario_" + str(options.scenario_index) +"_case_"+ str(options.case_index) + "_instance_" + str(i) + "_"
    sim_dir += "eco"+"_"+options.eco_routing + "_"
    sim_dir += "bus"+"_"+options.bus_scheduling + "_"
    sim_dir += "share"+"_"+str(int(options.share_percentage*100)) + "_"
    sim_dir += "demand"+"_"+str(int(options.demand_factor*100)) + "_"
    sim_dir += "taxi_" + str(options.taxi_fleet_size) + "_bus_" + str(options.bus_fleet_size) + "_"
    sim_dir += "co" if options.cooperative=="true" else "no_co"
    sim_dir += "_full" if options.full_demand=="true" else ""
    return sim_dir