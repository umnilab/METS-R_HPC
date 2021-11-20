import os
import sys
import shutil
import json
from os import path
from rdcm import run_rdcm


class sim_options:

    def __init__(self):
        self.java_path = ""
        self.java_options = ""
        self.evacsim_dir = ""
        self.groovy_dir = ""
        self.repast_plugin_dir = ""
        self.num_simulations = 0
        self.ports = []
        self.scenarios = []
        self.cases = {}

    def __str__(self):
        
        return """java_path : {}\
                \njava_options : {}\
                \nevacsim_dir : {}\
                \ngroovy_dir : {}\
                \nrepast_plugin_dir : {}\
                \nnum_simulations : {}\
                \nports : {}""".format(self.java_path, self.java_options, self.evacsim_dir, self.groovy_dir, self.repast_plugin_dir, self.num_simulations, self.ports)


# selected scenarios and ports
def modify_property_file(options, dest_data_dir, port, scenario, case):
    fname = dest_data_dir + "/Data.properties"
    f = open(fname, "r")
    lines = f.readlines()
    f.close()

    f_new = open(fname, "w")
    for l in lines:
        if "NETWORK_LISTEN_PORT" in l:
            f_new.write("NETWORK_LISTEN_PORT = " + str(port) + "\n")
        elif "DM_EVENT_FILE" in l:
            f_new.write("DM_EVENT_FILE = data/NYC/demand/"+options.scenarios[scenario] + "/demand_"+ options.cases[scenario][case]+ "\n")
        elif "BT_EVENT_FILE" in l:
            f_new.write("BT_EVENT_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_"+ options.cases[scenario][case]+ "\n")
        elif "BT_STD_FILE" in l:
            f_new.write("BT_STD_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_std_"+ options.cases[scenario][case]+ "\n")
        elif "ECO_ROUTING_EV" in l:
            f_new.write("ECO_ROUTING_EV = " + str(options.eco_routing) + "\n")
        elif "ECO_ROUTING_BUS" in l:
            f_new.write("ECO_ROUTING_BUS = " + str(options.eco_routing) + "\n")
        elif "BUS_PLANNING" in l:
            f_new.write("BUS_PLANNING = " + str(options.bus_scheduling) + "\n")
        elif "PASSENGER_SHARE_PERCENTAGE" in l:
            f_new.write("PASSENGER_SHARE_PERCENTAGE = " + str(options.share_percentage) + "\n")
        elif "CHARGER_CSV" in l:
            f_new.write("CHARGER_CSV = data/NYC/charging_station/data/" + options.charger_plan + "\n")
        else:
            f_new.write(l)

    f_new.close()


def prepare_sim_dirs(options):
    src_data_dir = options.evacsim_dir + "data"
    prepare_scenario_dict(options, src_data_dir + "/NYC/demand")
    for i in range(0, options.num_simulations):
        # make a directory to run the simulator
        dir_name = "scenario" + str(options.scenario_index) + "_" + str(i)
        if not path.exists(dir_name):
            os.mkdir(dir_name)
        # copy the simulation config files
        dest_data_dir = dir_name + "/" + "data"
        
        if not path.exists(dest_data_dir):
            try:
                # print src_data_dir
                # print dest_data_dir
                shutil.copytree(src_data_dir, dest_data_dir)
            except OSError as exc:
                print(f"ERROR :can not copy the data directory. exception {exc}")
                sys.exit(-1)
        modify_property_file(options, dest_data_dir, options.ports[i], options.scenario_index, options.case_index)

def prepare_scenario_dict(options, path):
    scenarios = os.listdir(path)
    i = 0
    sorted(scenarios)
    for scenario in scenarios:
        options.scenarios.append(scenario)
        options.cases[i] = []
        cases = os.listdir(path+"/"+scenario)
        sorted(cases)
        for case in cases:
            options.cases[i].append(case.split("_")[1])
        i+=1
        
def read_run_config(fname):
    with open(fname, "r") as f:
        config = json.load(f)

    opts = sim_options()
    opts.java_path = config['java_path']
    opts.java_options = config['java_options']
    opts.evacsim_dir = config['evacsim_dir']
    opts.groovy_dir = config['groovy_dir']
    opts.repast_plugin_dir = config['repast_plugin_dir']
    opts.num_simulations = int(config['num_sim_instances'])
    opts.ports = config['socket_port_numbers']
    
    opts.scenario_index = int(config['scenario_index'])
    opts.case_index = int(config['case_index'])
    
    opts.charger_plan = config['charger_plan']
    opts.eco_routing = config['eco_routing']
    opts.bus_scheduling = config['bus_scheduling']
    opts.share_percentage = float(config['share_percentage'])

    if len(opts.ports) != opts.num_simulations:
        print("ERROR , please specify port number for all simulation instances")
        sys.exit(-1)


    return opts


# construct the java classpath with all the 
# required jar files. if includeBin is False it
# won't add the EvacSim/bin directory to classpath
# This is needed for simulation command
def get_classpath(options, includeBin=True):
    
    classpath = ""

    if not path.exists(options.groovy_dir):
        print(f"ERROR , groovy is not found at {options.groovy_dir}")
        sys.exit(-1)
    
    classpath += options.groovy_dir + "lib/*:"

    if not path.exists(options.repast_plugin_dir):
        print(f"ERROR , repast plugins not found at {options.repast_plugin_dir}")
        sys.exit(-1)
    
    classpath += options.repast_plugin_dir + "bin:" + \
                 options.repast_plugin_dir + "lib/*:"
    
    classpath += options.evacsim_dir + ":" + \
                 options.evacsim_dir + "lib/*"
    
    if(includeBin):
        classpath += ":" + options.evacsim_dir + "bin"

    return classpath
    
# NOTE : java version of RDCM is no longer used
def run_rdcm_java(options, config_fname):

    # rdcm command
    rdcm_command = options.java_path + " " + \
                   "-classpath " + \
                   "../rdcm_java_version/target/rdcm-1.0-SNAPSHOT.jar:../rdcm/target/dependency/*" + " " + \
                   "com.metsr.hpc.RemoteDataClientManager " + \
                   config_fname + " " + options.evacsim_dir + "/data/"
    
    # run rdcm on a new terminal
    cwd = str(os.getcwd())
    os.system(rdcm_command + " > rdcm.log 2>&1  &")

def run_simulations(options):

    for i in range(0, options.num_simulations):
        sim_command = options.java_path + " " + \
                   options.java_options + " " + \
                   "-classpath " + \
                   get_classpath(options, False) + " " + \
                   "repast.simphony.runtime.RepastMain " + \
                   options.evacsim_dir + "EvacSim.rs"
        # got to sim directory 
        sim_dir = "scenario" + str(options.scenario_index) + "_"+ str(i)
        os.chdir(sim_dir)
        cwd = str(os.getcwd())
        # run simulator on new terminal 
        # os.system("konsole --hold --workdir " + cwd + " -e " + sim_command + " &")
        os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir("..")
                   

def main():
    
    if len(sys.argv) < 2:
        print("Specify the config file name!")
        print("python3 run_hpc.py <config_file>")
        sys.exit(-1)

    options = read_run_config(sys.argv[1])
    print("---------------- HPC options ----------------")
    print(options)
    print("---------------------------------------------")

    # prepare simulation directories
    prepare_sim_dirs(options)
    # launch the simulations
    run_simulations(options)
    # run rdcm 
    run_rdcm(options.num_simulations, options.ports)

if __name__ ==  "__main__":
    main()
