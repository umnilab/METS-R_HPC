import socket
import json

#### utilities for rdcm
def check_socket(host, port):
    flag = True
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((host, port)) == 0:
            flag =  True
        else:
            flag =  False
    time.sleep(5)
    return flag

def str_list_mapper_gen(func):
    def str_list_mapper(str_list):
        return [func(str) for str in str_list]
    return str_list_mapper

#### utilities for run_hpc
class sim_options:
    def __init__(self):
        self.java_path = ""
        self.java_options = ""
        self.evacsim_dir = ""
        self.groovy_dir = ""
        self.repast_plugin_dir = ""
        self.num_simulations = 0
        self.ports = []
        #self.server_sockets = []
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
            l = "DM_EVENT_FILE = data/NYC/demand/"+options.scenarios[scenario] + "/demand_"+ options.cases[scenario][case]+ "\n"
        elif "BT_EVENT_FILE" in l:
            l = "BT_EVENT_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_"+ options.cases[scenario][case]+ "\n"
        elif "BT_STD_FILE" in l:
            l = "BT_STD_FILE = data/NYC/background_traffic/"+options.scenarios[scenario] + "/speed_std_"+ options.cases[scenario][case]+ "\n"
        elif "ECO_ROUTING_EV" in l:
            l = "ECO_ROUTING_EV = " + str(options.eco_routing) + "\n"
        # elif "ECO_ROUTING_BUS" in l:
        #     l = "ECO_ROUTING_BUS = " + str(options.eco_routing) + "\n"
        elif "BUS_PLANNING" in l:
            l = "BUS_PLANNING = " + str(options.bus_scheduling) + "\n"
        elif "PASSENGER_SHARE_PERCENTAGE" in l:
            l = "PASSENGER_SHARE_PERCENTAGE = " + str(options.share_percentage) + "\n"
        elif "CHARGER_CSV" in l:
            l = "CHARGER_CSV = data/NYC/charging_station/result/" + options.charger_plan + "\n"
        if "data/" in l:
            l = l.replace('data/', src_data_dir + '/')
        f_new.write(l)
    f_new.close()

def prepare_sim_dirs(options):
    src_data_dir = options.evacsim_dir + "data"
    prepare_scenario_dict(options, src_data_dir + "/NYC/demand")
    find_free_ports(options, options.num_simulations)
    if len(options.ports) != options.num_simulations:
        print("ERROR , cannot specify port number for all simulation instances")
        sys.exit(-1)
    for i in range(0, options.num_simulations):
        # make a directory to run the simulator
        dir_name = "scenario" + str(options.scenario_index) +"_"+ str(options.case_index) + "_" + str(i)
        if not path.exists(dir_name):
            os.mkdir(dir_name)
        # copy the simulation config files
        dest_data_dir = dir_name + "/" + "data" 
        output_data_dir = dir_name + "/" + "simulation_output" 
        
        if not path.exists(output_data_dir):
            os.mkdir(output_data_dir)
        
        if not path.exists(dest_data_dir):
            os.mkdir(dest_data_dir)
            
            try:
                # print src_data_dir
                # print dest_data_dir
                os.mkdir(dest_data_dir+"/NYC")
                shutil.copy(src_data_dir+"/NYC/candidate_routes.ser", dest_data_dir+"/NYC/candidate_routes.ser")
                shutil.copy(src_data_dir+"/NYC/candidate_routes_bus.ser", dest_data_dir+"/NYC/candidate_routes_bus.ser")
            except OSError as exc:
                print(f"ERROR :can not copy the data directory. exception {exc}")
                sys.exit(-1)
        modify_property_file(options, src_data_dir, dest_data_dir, options.ports[i], options.scenario_index, options.case_index)

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

def find_free_ports(options, num_simulations):
    while True:
        for i in range(num_simulations):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                #s.settimeout(1)
                s.bind(('localhost', 0))
                options.ports.append(s.getsockname()[1])
            #with socketserver.TCPServer(("localhost", 0), None) as server_socket:
            #server_socket = socketserver.TCPServer(("localhost", 0), None)
            #options.server_sockets.append(server_socket)
            #    options.ports.append(server_socket.server_address[1])
            # Double check the availability of the port
        try:
            for port in options.ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', port))
                #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.close()
                #os.system("npx kill-port '{0}'".format(port))
            break
        except:
            print("The port is not valid anymore, regenerate it")
            continue
    time.sleep(1)
     
    
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
    #opts.ports = config['socket_port_numbers']
    
    opts.scenario_index = int(config['scenario_index'])
    #opts.case_index = int(config['case_index'])
    
    opts.charger_plan = config['charger_plan']
    opts.eco_routing = config['eco_routing']
    opts.bus_scheduling = config['bus_scheduling']
    opts.share_percentage = float(config['share_percentage'])

    #if len(opts.ports) != opts.num_simulations:
    #    print("ERROR , please specify port number for all simulation instances")
    #    sys.exit(-1)

    return opts

# construct the java classpath with all the required jar files. if includeBin is False it
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
        sim_dir = "scenario" + str(options.scenario_index) + "_"+ str(options.case_index) + "_"+ str(i)
        os.chdir(sim_dir)
        cwd = str(os.getcwd())
        # run simulator on new terminal 
        # os.system("konsole --hold --workdir " + cwd + " -e " + sim_command + " &")
        os.system(sim_command + " > sim_{}.log 2>&1 &".format(i))
        # go back to test directory
        os.chdir("..")