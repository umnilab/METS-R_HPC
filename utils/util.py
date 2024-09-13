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
import sys
import zipfile

"""
Helper functions for METSR-SIM and METSR-HPC
"""

# Function for checking whether the socket connection is on
def check_socket(host, port):
    flag = True
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            flag =  True
        else:
            flag =  False
    time.sleep(1)
    return flag

# Factory for processsing a str list with a given func
def str_list_mapper_gen(func):
    def str_list_mapper(str_list):
        return [func(str) for str in str_list]
    return str_list_mapper

# Function for modifying simulation properties
def modify_property_file(options, src_data_dir, dest_data_dir, port, instance, template):
    fname = src_data_dir + "/Data.properties." + template
    if not path.exists(fname):
        print("ERROR, cannot find the property template file at ", fname)
        sys.exit(-1)

    if options.template == "NYC":
        scenario = options.scenario_index
        case = options.case_index

    f = open(fname, "r")
    lines = f.readlines()
    f.close()
    fname = dest_data_dir + "/Data.properties"
    f_new = open(fname, "w")
    for l in lines:
        if l.startswith("NETWORK_LISTEN_PORT"):
            l = "NETWORK_LISTEN_PORT = " + str(port) + "\n"
        elif (l.startswith("RANDOM_SEED")):
            l = "RANDOM_SEED = " + str(options.random_seeds[instance]) + "\n"
        elif (l.startswith("MULTI_THREADING")):
            if(options.num_threads > 1):
                l = "MULTI_THREADING = true"  + "\n"
            else:
                l = "MULTI_THREADING = false" + "\n" 
        elif (l.startswith("N_PARTITION")):
            if(options.num_threads > 1):
                l = "N_PARTITION = " + str(options.num_threads) + "\n"
            else:
                l = "N_PARTITION = 1" + "\n" 
        elif (l.startswith("N_THREADS")):
            if(options.num_threads > 1):
                l = "N_THREADS = " + str(options.num_threads) + "\n"
            else:
                l = "N_THREADS = 1" + "\n" 
        elif (l.startswith("SIMULATION_STEP_SIZE")):
            l = "SIMULATION_STEP_SIZE = " + str(options.sim_step_size) + "\n"
        elif (l.startswith("SIMULATION_STOP_TIME")):
            l = "SIMULATION_STOP_TIME = " + str(round(int(options.sim_minutes))) + "\n"
        elif (l.startswith("AGG_DEFAULT_PATH")):
            l = "AGG_DEFAULT_PATH = agg_output" + "\n"
        elif (l.startswith("JSON_DEFAULT_PATH")):
            l = "JSON_DEFAULT_PATH = trajectory_output" + "\n"
        elif (l.startswith("STANDALONE")):
            l = "STANDALONE = "+ str(options.standalone) + "\n"
        elif (l.startswith("SYNCHRONIZED")):
            l = "SYNCHRONIZED = " + str(options.synchronized) + "\n"

        if (template  == "NYC"):
            if l.startswith("RH_DEMAND_FILE"):
                if(options.full_demand):
                    if(options.sim_passenger):
                        l = "RH_DEMAND_FILE = data/NYC/demand/passenger_full/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
                    else:
                        l = "RH_DEMAND_FILE = data/NYC/demand/request_full/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
                else:
                    if(options.sim_passenger):
                        l = "RH_DEMAND_FILE = data/NYC/demand/passenger/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
                    else:
                        l = "RH_DEMAND_FILE =  data/NYC/demand/request/"+options.scenarios[scenario] + "/demand_"+ options.cases[scenario][case]+ "\n"
            elif l.startswith("ROADS_SHAPEFILE"):
                if(options.full_network):
                    l = "ROADS_SHAPEFILE = data/NYC/facility/road_full/road_fileNYC.shp\n"
                else:
                    l = "ROADS_SHAPEFILE = data/NYC/facility/road/road_fileNYC.shp\n"
            elif l.startswith("LANES_SHAPEFILE"):
                if(options.full_network):
                    l = "LANES_SHAPEFILE = data/NYC/facility/road_full/lane_fileNYC.shp\n"
                else:
                    l = "LANES_SHAPEFILE = data/NYC/facility/road/lane_fileNYC.shp\n"
            elif l.startswith("ROADS_CSV"):
                if(options.full_network):
                    l = "ROADS_CSV = data/NYC/facility/road_full/road_fileNYC.csv\n"
                    options.road_file = options.sim_dir + "/data/NYC/facility/road_full/road_fileNYC.csv"
                else:
                    l = "ROADS_CSV = data/NYC/facility/road/road_fileNYC.csv\n"
                    options.road_file = options.sim_dir + "/data/NYC/facility/road/road_fileNYC.csv"
            elif l.startswith("LANES_CSV"):
                if(options.full_network):
                    l = "LANES_CSV = data/NYC/facility/road_full/lane_fileNYC.csv\n"
                else:
                    l = "LANES_CSV = data/NYC/facility/road/lane_fileNYC.csv\n"
            elif l.startswith("RH_SHARE_PERCENTAGE"):
                if(options.full_demand):
                    l = "RH_SHARE_PERCENTAGE = data/NYC/demand/share_full/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
                else:
                    l = "RH_SHARE_PERCENTAGE = data/NYC/demand/share/" + options.scenarios[scenario] + "/demand_" + options.cases[scenario][case]+ "\n"
            elif l.startswith("BT_EVENT_FILE"):
                if(options.full_network):
                    l = "BT_EVENT_FILE = data/NYC/operation/speed_full/"+options.scenarios[scenario] + "/speed_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
                else:
                    l = "BT_EVENT_FILE = data/NYC/operation/speed/"+options.scenarios[scenario] + "/speed_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
            elif l.startswith("BT_STD_FILE"):
                if(options.full_network):
                    l = "BT_STD_FILE = data/NYC/operation/speed_full/"+options.scenarios[scenario] + "/speed_std_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
                else:
                    l = "BT_STD_FILE = data/NYC/operation/speed/"+options.scenarios[scenario] + "/speed_std_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
            elif l.startswith("RH_WAITING_TIME"):
                l = "RH_WAITING_TIME = data/NYC/demand/wait/" + options.scenarios[scenario] + "/demand_"+ options.cases[scenario][case].replace('json','csv')+ "\n"
            elif l.startswith("ECO_ROUTING_EV"):
                l = "ECO_ROUTING_EV = " + str(options.eco_routing) + "\n"
            elif l.startswith("NUM_OF_EV"):
                l = "NUM_OF_EV = " + str(options.taxi_fleet_size) + "\n"
            elif l.startswith("NUM_OF_BUS"):
                l = "NUM_OF_BUS = " + str(options.bus_fleet_size) + "\n"
            elif l.startswith("ECO_ROUTING_BUS"):
                l = "ECO_ROUTING_BUS = " + str(options.eco_routing_bus) + "\n"
            elif l.startswith("BUS_PLANNING"):
                l = "BUS_PLANNING = " + str(options.bus_scheduling) + "\n"
            elif l.startswith("RH_DEMAND_SHARABLE"):
                l = "RH_DEMAND_SHARABLE = " + str(options.demand_sharable) + "\n"
            elif l.startswith("RH_DEMAND_FACTOR"):
                l = "RH_DEMAND_FACTOR = " + str(options.demand_factor) + "\n"
            elif (l.startswith("BUS_SCHEDULE")) and options.bus_fleet_size >= 20 and options.bus_scheduling == 'false':
                l = "BUS_SCHEDULE = data/NYC/operation/bus_planning/bus_routes" + str(options.bus_fleet_size // 20) + ".json\n"
            elif (l.startswith("COLLABORATIVE_EV")):
                l = "COLLABORATIVE_EV = " + str(options.cooperative) + "\n"
        
        if "data/" in l:
            l = l.replace('data/', src_data_dir + '/')
        
        f_new.write(l)
    f_new.close()

def force_copytree(src, dst):
    """
    Recursively copy a directory tree, overwriting the destination directory if it exists.
    """
    # Check if the destination directory exists
    if os.path.exists(dst):
        # Remove the destination directory and all its contents
        shutil.rmtree(dst)
    
    # Copy the source directory to the destination
    shutil.copytree(src, dst)

# Copy necessary files for running the simulation
def prepare_sim_dirs(options):
    src_data_dir = "data"
    if options.template == "NYC":
        if options.full_demand:
            prepare_scenario_dict(options, src_data_dir + "/NYC/demand/request_full")
        else:
            prepare_scenario_dict(options, src_data_dir + "/NYC/demand/request")
    
    # check if metsr_port in the NameSpace options
    if hasattr(options, 'metsr_port'):
        # check if metsr_port number is equal to the number of simulations
        if options.num_simulations > len(options.metsr_port):
            print("ERROR , port number is less than the number of simulation instances")
            sys.exit(-1)
        else:
            options.ports = options.metsr_port
    else:
        print("No port number specified, find available ports for simulation instances")
        find_free_ports(options, options.num_simulations)
    if len(options.ports) != options.num_simulations:
        print("ERROR , cannot specify port number for all simulation instances")
        sys.exit(-1)


    dest_data_dirs = []
    for i in range(options.num_simulations):
        # make a directory to run the simulator
        dir_name = get_sim_dir(options, i)
        if not path.exists(dir_name):
            os.makedirs(dir_name)
        shutil.copy(src_data_dir+"/log4j.properties", dir_name + "/log4j.properties")
        # copy the simulation config files
        dest_data_dir = dir_name + "/" + "data"
        options.data_dir = dest_data_dir

        if not path.exists(dest_data_dir):
            os.mkdir(dest_data_dir)
            force_copytree(src_data_dir+"/Empty", dest_data_dir+"/Empty")

            if options.template == "NYC":
                # copy the subdirectories
                force_copytree(src_data_dir+"/NYC", dest_data_dir+"/NYC")

                if options.eco_routing:
                    if options.full_network:
                        shutil.copy("models/eco_routing/data/full/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes.ser")
                        shutil.copy("models/eco_routing/data/full/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes_bus.ser")
                    else:
                        shutil.copy("models/eco_routing/data/small/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes.ser")
                        shutil.copy("models/eco_routing/data/small/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes_bus.ser")

            elif options.template == "CARLA":
                force_copytree(src_data_dir+"/CARLA", dest_data_dir+"/CARLA")

        modify_property_file(options, src_data_dir, dest_data_dir, options.ports[i], i, options.template)
        dest_data_dirs.append(dest_data_dir)

    return dest_data_dirs

# Function for getting the file name list of demand scenarios
def prepare_scenario_dict(options, path):
    scenarios = os.listdir(path)
    i = 0
    scenarios = sorted(scenarios)
    options.scenarios=[]
    options.cases = [[] for j in range(len(scenarios))]
    for scenario in scenarios:
        options.scenarios.append(scenario)
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
        config = json.load(f, object_hook=lambda d: SimpleNamespace(**d))

    if len(config.random_seeds) != config.num_simulations:
       print("ERROR, please specify random seeds for all simulation instances")
       sys.exit(-1)

    return config

# Construct the java classpath with all the required jar files. 
# If includeBin is False it won't add the METS_R/bin directory to classpath.
# This is needed for simulation command.
def get_classpath(options, includeBin=True, separator=":"):
    
    classpath = ""

    if not path.exists(options.repast_plugin_dir):
        print(f"ERROR , repast plugins not found at {options.repast_plugin_dir}")
        sys.exit(-1)
    
    classpath += options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/lib/*" + separator + \
                 options.sim_dir + "lib/*" + separator    
 
    


    return classpath

def get_classpath2(options, includeBin=True, separator=":"):
    
    classpath = ""

    classpath += options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.runtime_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.batch_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.batch_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.distributed.batch_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.distributed.batch_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.core_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.core_2.7.0/lib/*" + separator + \
                 options.sim_dir + "bin" + separator + \
                 options.sim_dir + "lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.bin_and_src_2.7.0/repast.simphony.bin_and_src.jar" + separator + \
                 options.repast_plugin_dir + "repast.simphony.essentials_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.gis_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.gis_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.sql_2.7.0/bin" + separator + \
                 options.repast_plugin_dir + "repast.simphony.sql_2.7.0/lib/*" + separator + \
                 options.repast_plugin_dir + "repast.simphony.scenario_2.7.0/bin" + separator 

    return classpath

# Function for starting the simulation
def run_simulations(options):
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        sim_dir = get_sim_dir(options, i)
        if platform.system() == "Windows":
             # go to sim directory
            os.chdir(sim_dir)

            # print(get_classpath(options, False, separator = ";"))
            # run the simulation on a new terminal
            sim_command = '"' + options.java_path + 'java"' + " " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    '"' +get_classpath(options, False, separator = ";") + '" '  + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            # print(sim_command)
            if options.verbose: # print the sim output to the console
                subprocess.Popen(sim_command, shell=True)
            else:
                subprocess.Popen(sim_command + " > sim_{}.log 2>&1 &".format(i), shell=True)
        else:
            # go to sim directory
            os.chdir(sim_dir)
            # run simulator on new terminal
            sim_command = options.java_path + "java " + \
                    options.java_options + " " + \
                    "-classpath " + \
                    get_classpath(options, False) + " "  + \
                    "repast.simphony.runtime.RepastMain " + \
                    options.sim_dir + "mets_r.rs"
            if options.verbose:
                os.system(sim_command)
            else:
                os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir(cwd)

def run_simulations_in_background(options):
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        sim_dir = get_sim_dir(options, i)
        if platform.system() == "Windows":
             # go to sim directory
            os.chdir(sim_dir)
            # run the simulation on a new terminal
            sim_command = '"' +  options.java_path + 'java"'+ " -Xmx16G "  + \
                    "-cp " + \
                    '"' + get_classpath2(options, False, separator = ";") + '" ' + \
                    "repast.simphony.batch.BatchMain " + \
                    "-params " + options.sim_dir + "mets_r.rs/batch_params.xml " +\
                    "-interactive " + options.sim_dir + "mets_r.rs "
            # print(sim_command)
            if options.verbose: # print the sim output to the console
                subprocess.Popen(sim_command)
            else:
                subprocess.Popen(sim_command + " > sim_{}.log 2>&1 &".format(i), shell=True)
        else:
            # go to sim directory
            os.chdir(sim_dir)
            # run simulator on new terminal 
            sim_command = '' +  options.java_path + 'java'+ " -Xmx16G "  + \
                    "-cp " + \
                    get_classpath2(options, False) + ' ' + \
                    "repast.simphony.batch.BatchMain " + \
                    "-params " + options.sim_dir + "mets_r.rs/batch_params.xml " +\
                    "-interactive " + options.sim_dir + "mets_r.rs "
            if options.verbose:
                os.system(sim_command)
            else:
                os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir(cwd)

def run_simulation_in_docker(options):
    container_ids = []
    for i in range(0, options.num_simulations):
        cwd = str(os.getcwd())
        sim_dir = get_sim_dir(options, i)
        os.chdir(sim_dir)

        sim_command = '' +  options.java_path + 'java'+ " -Xmx16G "  + \
            "-cp " + \
            get_classpath2(options, False) + ' ' + \
            "repast.simphony.batch.BatchMain " + \
            "-params " + options.sim_dir + "mets_r.rs/batch_params.xml " +\
            "-interactive " + options.sim_dir + "mets_r.rs"
        
        docker_command = f'docker run -d --rm --mount src="{os.getcwd()}",target=/home/test,type=bind --net=host ennuilei/mets-r_sim  /bin/bash -c "cd /home/test && ' + sim_command + '"'
        result = subprocess.run(docker_command, shell=True, text=True, capture_output=True)
        if options.verbose:
            print("Container ID:", result.stdout)
            print("Error msg:", result.stderr)
        container_id = result.stdout.strip()
        container_ids.append(container_id)
        os.chdir(cwd)
    return container_ids


# Get the directory for storing simulation outputs
def get_sim_dir(options, i):
    if options.template == "NYC":
        sim_dir = "output/scenario_" + str(options.scenario_index) +"_case_"+ str(options.case_index) + "_seed_" + str(options.random_seeds[i]) + "_"
        sim_dir += "eco"+"_"+str(options.eco_routing) + "_"
        sim_dir += "bus"+"_"+str(options.bus_scheduling) + "_"
        sim_dir += "share"+"_"+str(options.demand_sharable) + "_"
        sim_dir += "demand"+"_"+str(int(options.demand_factor*100)) + "_"
        sim_dir += "taxi_" + str(options.taxi_fleet_size) + "_bus_" + str(options.bus_fleet_size)
        sim_dir += "_co" if options.cooperative=="true" else ""
        sim_dir += "_pass" if options.sim_passenger=="true" else ""
        sim_dir += "_full" if options.full_demand=="true" else ""
        sim_dir += "_" + str(int(options.demand_factor*100))
        sim_dir += "_" + str(options.num_threads)
    else:
        from datetime import datetime
        sim_dir = "output/"+ options.template + "_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_seed_" + str(options.random_seeds[i])
    return sim_dir