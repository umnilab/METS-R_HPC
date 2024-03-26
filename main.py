import sys
import os
import argparse
import time
from RemoteDataClientManager import RemoteDataClientManager
from util import read_run_config, prepare_sim_dirs, run_simulations, run_simulations_in_background

"""
This is the entrance for METSR-HPC module
usage example: python main.py -s 0 -c 0 -tf 2000 -bf 20
"""

# TODO: Need to state clearly the meaning of the scenarios

def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-r','--run_config', default='configs/run.config.scenario.json',
                        help='the folder that contains all the input data')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario')
    parser.add_argument('-c','--case_index', type=int, 
                        help='the index within the scenario')
    parser.add_argument('-e', '--eco_routing', action='store_true', default=False,
                        help='enable ecorouting')
    parser.add_argument('-eb', '--eco_routing_bus', action='store_true', default=False,
                        help='enable eco routing for bus')
    parser.add_argument('-b', '--bus_scheduling', action='store_true', default=False,
                        help='enable bus scheduling')
    parser.add_argument('-tf', '--taxi_fleet', type=int, default=2000,
                        help='number of AEV taxis initialized')
    parser.add_argument('-bf', '--bus_fleet', type=int, default=0,
                        help='number of AEV buses initialized')
    parser.add_argument('-co', '--cooperative', action='store_true', default=False,
                        help='enable taxi-bus or bus-taxi cooperation')
    parser.add_argument('-ds', '--demand_sharable', action='store_true', default=False,
                        help='whether the request is sharable')
    parser.add_argument('-df', '--demand_factor', type=float, default  = -1, 
                        help='ratio of demand')
    parser.add_argument('-th', '--threads', type=int, default  = -1, 
                        help='number of threads')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='verbose mode')
    args = parser.parse_args(argv)

    return args

if __name__ ==  "__main__":
    # Load configs
    args = get_arguments(sys.argv[1:])
    config = read_run_config(args.run_config)
    config.case_index = args.case_index
    config.scenario_index = args.scenario_index
    config.eco_routing =  args.eco_routing
    config.eco_routing_bus = args.eco_routing_bus
    config.bus_scheduling = args.bus_scheduling
    config.cooperative = args.cooperative
    config.demand_sharable = args.demand_sharable
    config.bus_fleet_size = args.bus_fleet
    config.taxi_fleet_size = args.taxi_fleet
    config.verbose = args.verbose

    if(args.demand_factor > 0):
        config.demand_factor = args.demand_factor
    if(args.threads > 0):
        config.num_threads = args.threads
    
    print("---------------- HPC options ----------------")
    print(config)
    print("---------------------------------------------")

    # Start the Kafka brokers and databases
    # Note that you need to run docker build -t postgres postgres first
    # cd docker
    # docker-compose up -d
    os.chdir("docker")
    os.system("docker-compose up -d")
    os.chdir("..")

    time.sleep(5) # wait for the servers to be up

    # Prepare simulation directories
    prepare_sim_dirs(config)

    # Launch the simulations
    # run_simulations(config)
    run_simulations_in_background(config)
    
    # Run RDCM (remote data client manager) 
    rdcm = RemoteDataClientManager(config)
    rdcm.run()
